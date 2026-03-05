// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "variant_column.h"

#include <cctz/time_zone.h>

#include <unordered_map>
#include <unordered_set>

#include "base/coding.h"
#include "column/binary_column.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/mysql_row_buffer.h"
#include "column/nullable_column.h"
#include "column/variant_builder.h"
#include "column/variant_encoder.h"
#include "column/variant_merger.h"
#include "column/variant_path_parser.h"
#include "gutil/casts.h"
#include "gutil/strings/substitute.h"
#include "types/variant_value.h"

namespace starrocks {

static void append_null_base_payload_rows(BinaryColumn::MutablePtr& metadata_column,
                                          BinaryColumn::MutablePtr& remain_column, size_t count);

VariantColumn::VariantColumn() : SuperClass(0) {
    _metadata_column = BinaryColumn::create();
    _remain_value_column = BinaryColumn::create();
}

VariantColumn::VariantColumn(size_t size) : SuperClass(0) {
    _metadata_column = BinaryColumn::create();
    _remain_value_column = BinaryColumn::create();
    if (size > 0) {
        append_null_base_payload_rows(_metadata_column, _remain_value_column, size);
    }
}

// Typed-only variant stores data in typed columns. Read paths still need row-level VariantRowValue.
// encode_typed_row_as_variant is a shared helper for VariantColumn and VariantFunctions.
StatusOr<VariantColumn::EncodedVariantResult> VariantColumn::encode_typed_row_as_variant(
        const Column* typed_column, size_t typed_row, const TypeDescriptor& type_desc) {
    if (typed_column == nullptr) {
        return Status::InvalidArgument("typed column is null");
    }

    if (typed_column->is_null(typed_row)) {
        return EncodedVariantResult{
                .state = EncodedVariantState::kNull,
                .value = VariantRowValue::from_null(),
        };
    }

    if (type_desc.type == TYPE_VARIANT) {
        const auto* typed_variant_column = down_cast<const VariantColumn*>(ColumnHelper::get_data_column(typed_column));
        if (typed_variant_column == nullptr) {
            return Status::InvalidArgument("typed variant column is null");
        }
        VariantRowValue row_buffer;
        const VariantRowValue* variant = typed_variant_column->get_row_value(typed_row, &row_buffer);
        if (variant == nullptr) {
            return Status::InvalidArgument("failed to materialize typed variant row");
        }
        return EncodedVariantResult{
                .state = EncodedVariantState::kValue,
                .value = std::move(row_buffer),
        };
    }

    // Fast path: encode single Datum directly, avoiding column clone + full encode pipeline.
    Datum datum = typed_column->get(typed_row);
    auto encoded = VariantEncoder::encode_datum(datum, type_desc);
    if (!encoded.ok()) {
        return encoded.status();
    }
    return EncodedVariantResult{.state = EncodedVariantState::kValue, .value = std::move(encoded).value()};
}

static bool collect_typed_overlays(const VariantColumn* column, size_t row,
                                   std::vector<VariantBuilder::Overlay>* overlays) {
    if (column == nullptr || overlays == nullptr) {
        return false;
    }
    overlays->clear();
    overlays->reserve(column->typed_columns().size());
    for (size_t i = 0; i < column->typed_columns().size(); ++i) {
        const Column* typed_column = column->typed_column_by_index(i);
        if (typed_column == nullptr) {
            return false;
        }
        size_t typed_row = typed_column->is_constant() ? 0 : row;
        auto typed_read =
                VariantColumn::encode_typed_row_as_variant(typed_column, typed_row, column->shredded_types()[i]);
        if (!typed_read.ok()) {
            return false;
        }
        auto value = std::move(typed_read).value();
        if (value.state == VariantColumn::EncodedVariantState::kNull) {
            // Typed null is tombstone semantics: suppress this path in the output object.
            // By invariant, typed paths are exclusive with remain, so nothing to do.
            continue;
        }
        const std::vector<VariantPath>& cached = column->parsed_shredded_paths();
        if (i >= cached.size()) {
            return false;
        }
        overlays->emplace_back(VariantBuilder::Overlay{
                .path = cached[i],
                .value = std::move(value.value),
        });
    }
    return true;
}

static bool rebuild_row_with_optional_base(const VariantRowValue* base, std::vector<VariantBuilder::Overlay> overlays,
                                           VariantRowValue* output) {
    if (output == nullptr) {
        return false;
    }
    VariantBuilder builder(base);
    if (!builder.set_overlays(std::move(overlays)).ok()) {
        return false;
    }
    auto encoded = builder.build();
    if (!encoded.ok()) {
        return false;
    }
    *output = std::move(encoded.value());
    return true;
}

static bool rebuild_row_from_typed_columns(const VariantColumn* column, size_t row, VariantRowValue* output) {
    if (column == nullptr || output == nullptr || !column->is_typed_only_variant()) {
        return false;
    }
    std::vector<VariantBuilder::Overlay> overlays;
    if (!collect_typed_overlays(column, row, &overlays)) {
        return false;
    }
    return rebuild_row_with_optional_base(nullptr, std::move(overlays), output);
}

static bool rebuild_row_from_base_shredded(const VariantColumn* column, size_t row, VariantRowValue* output,
                                           std::string_view metadata_raw, std::string_view remain_raw) {
    if (column == nullptr || output == nullptr) {
        return false;
    }
    // Base shredded payload may come from legacy/external sources.
    // Avoid eager metadata validation here; defer to downstream decode/seek semantics.
    VariantRowValue base_row(metadata_raw, remain_raw);

    std::vector<VariantBuilder::Overlay> overlays;
    if (!collect_typed_overlays(column, row, &overlays)) {
        return false;
    }
    return rebuild_row_with_optional_base(&base_row, std::move(overlays), output);
}

static void append_null_base_payload_rows(BinaryColumn::MutablePtr& metadata_column,
                                          BinaryColumn::MutablePtr& remain_column, size_t count) {
    DCHECK(metadata_column != nullptr);
    DCHECK(remain_column != nullptr);
    VariantRowValue null_base = VariantRowValue::from_null();
    std::string_view metadata_raw = null_base.get_metadata().raw();
    std::string_view remain_raw = null_base.get_value().raw();
    Slice metadata_slice(metadata_raw.data(), metadata_raw.size());
    Slice remain_slice(remain_raw.data(), remain_raw.size());
    for (size_t i = 0; i < count; ++i) {
        metadata_column->append_datum(Datum(metadata_slice));
        remain_column->append_datum(Datum(remain_slice));
    }
}

// Do not use VariantColumn::clone() in append-prepare path:
// BaseClass::clone() clones by calling append(), and append() may re-enter
// _prepare_append_source(), causing recursive append-prepare on shredded inputs.
// Deep-copy shredded members directly to avoid that recursion.
MutableColumnPtr VariantColumn::deep_copy_shredded(const VariantColumn& src) {
    auto copied = VariantColumn::create();
    MutableColumns typed_columns;
    typed_columns.reserve(src.typed_columns().size());
    for (const auto& typed_col : src.typed_columns()) {
        typed_columns.emplace_back(typed_col->clone());
    }
    BinaryColumn::MutablePtr metadata =
            src.has_metadata_column() ? BinaryColumn::static_pointer_cast(src.metadata_column()->clone()) : nullptr;
    BinaryColumn::MutablePtr remain =
            src.has_remain_value() ? BinaryColumn::static_pointer_cast(src.remain_value_column()->clone()) : nullptr;
    copied->set_shredded_columns(src.shredded_paths(), src.shredded_types(), std::move(typed_columns),
                                 std::move(metadata), std::move(remain));
    return std::move(copied);
}

MutableColumnPtr VariantColumn::clone() const {
    auto cloned = BaseClass::clone();
    auto* variant_cloned = down_cast<VariantColumn*>(cloned.get());
    // BaseClass::clone() may preserve derived schema members in some clone paths.
    // Reset first to avoid duplicating schema/typed columns.
    variant_cloned->clear_shredded_columns();
    variant_cloned->_shredded_paths = _shredded_paths;
    variant_cloned->_path_index = _path_index;
    variant_cloned->_parsed_shredded_paths = _parsed_shredded_paths;
    variant_cloned->_shredded_types = _shredded_types;
    variant_cloned->_typed_columns.reserve(_typed_columns.size());
    for (const auto& column : _typed_columns) {
        variant_cloned->_typed_columns.emplace_back(column->clone());
    }
    if (_metadata_column != nullptr) {
        variant_cloned->_metadata_column = BinaryColumn::static_pointer_cast(_metadata_column->clone());
    }
    if (_remain_value_column != nullptr) {
        variant_cloned->_remain_value_column = BinaryColumn::static_pointer_cast(_remain_value_column->clone());
    }
    return cloned;
}

// Row-level serde for Column virtual interface is used in generic key paths
// (multi-column GROUP BY / hash key build+restore).
// Those paths restore into newly-created columns that may not carry shredded schema.
// Therefore we serialize as self-describing VariantRowValue bytes instead of shredded
// per-column layout.
//
// NOTE: This is intentionally different from ColumnArraySerde.
// - VariantColumn::serialize/deserialize_and_append: row-wise key serde in execution engine.
// - ColumnArraySerde::VariantColumnSerde: full-column serde with paths/types metadata.
//
// Tradeoff: materializing typed overlays to row values may be slower than directly emitting
// typed columns, but avoids schema mismatch when deserializing key columns.

uint32_t VariantColumn::serialize(size_t idx, uint8_t* pos) const {
    VariantRowValue row_buffer;
    const VariantRowValue* row = get_row_value(idx, &row_buffer);
    if (UNLIKELY(row == nullptr)) {
        LOG(WARNING) << "failed to materialize variant row at index " << idx << ", serialize null value";
        VariantRowValue null_row = VariantRowValue::from_null();
        return static_cast<uint32_t>(null_row.serialize(pos));
    }
    return static_cast<uint32_t>(row->serialize(pos));
}

void VariantColumn::serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                                    uint32_t max_one_row_size) const {
    for (size_t i = 0; i < chunk_size; ++i) {
        slice_sizes[i] += serialize(i, dst + i * max_one_row_size + slice_sizes[i]);
    }
}

uint32_t VariantColumn::serialize_size(size_t idx) const {
    VariantRowValue row_buffer;
    const VariantRowValue* row = get_row_value(idx, &row_buffer);
    if (UNLIKELY(row == nullptr)) {
        VariantRowValue null_row = VariantRowValue::from_null();
        return null_row.serialize_size();
    }
    return row->serialize_size();
}

const uint8_t* VariantColumn::deserialize_and_append(const uint8_t* pos) {
    const uint32_t payload_size = decode_fixed32_le(pos);
    const size_t row_wire_size = sizeof(uint32_t) + payload_size;
    Slice row_slice(reinterpret_cast<const char*>(pos), row_wire_size);
    auto row_value = VariantRowValue::create(row_slice);
    if (UNLIKELY(!row_value.ok())) {
        LOG(WARNING) << "failed to deserialize variant row value, append null. reason=" << row_value.status();
        append_nulls(1);
        return pos + row_wire_size;
    }
    append(&row_value.value());
    return pos + row_wire_size;
}

void VariantColumn::_rebuild_path_index() {
    _path_index.clear();
    _path_index.reserve(_shredded_paths.size());
    for (size_t i = 0; i < _shredded_paths.size(); ++i) {
        _path_index.emplace(_shredded_paths[i], static_cast<int>(i));
    }
    // Rebuild parsed-path cache in sync.
    _parsed_shredded_paths.clear();
    _parsed_shredded_paths.reserve(_shredded_paths.size());
    for (const auto& path : _shredded_paths) {
        auto parsed = VariantPathParser::parse_shredded_path(std::string_view(path));
        if (UNLIKELY(!parsed.ok())) {
            LOG(WARNING) << "invalid shredded path in schema cache rebuild: path=" << path
                         << ", reason=" << parsed.status();
            _parsed_shredded_paths.clear();
            return;
        }
        _parsed_shredded_paths.emplace_back(std::move(parsed).value());
    }
}

int VariantColumn::find_shredded_path(std::string_view path) const {
    auto it = _path_index.find(std::string(path));
    return it != _path_index.end() ? it->second : -1;
}

const Column* VariantColumn::typed_column_by_index(size_t idx) const {
    if (idx >= _typed_columns.size()) {
        return nullptr;
    }
    return _typed_columns[idx].get();
}

void VariantColumn::put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx, bool is_binary_protocol) const {
    VariantRowValue row;
    const VariantRowValue* variant = get_row_value(idx, &row);
    if (variant == nullptr) {
        LOG(WARNING) << "Variant value is null at index " << idx;
        buf->push_null();
        return;
    }
    auto json = variant->to_json();
    if (!json.ok()) {
        buf->push_null();
    } else {
        buf->push_string(json->data(), json->size(), '\'');
    }
}

void VariantColumn::append_datum(const Datum& datum) {
    auto* value = datum.get<VariantRowValue*>();
    append(value);
}

const VariantColumn* VariantColumn::_prepare_append_source(const VariantColumn& src,
                                                           MutableColumnPtr* src_working_copy) {
    DCHECK(src_working_copy != nullptr);
    const VariantColumn* append_src = &src;
    if (!is_equal_schema(&src)) {
        *src_working_copy = deep_copy_shredded(src);
        auto* src_cloned = down_cast<VariantColumn*>(src_working_copy->get());
        Status arbitrate_st = VariantMerger::arbitrate_type_conflicts(this, src_cloned);
        DCHECK(arbitrate_st.ok()) << "append type arbitration failed: " << arbitrate_st;
        if (!arbitrate_st.ok()) {
            LOG(ERROR) << "append type arbitration failed, err=" << arbitrate_st;
            return nullptr;
        }
        append_src = src_cloned;
    }

    bool aligned = align_schema_from(*append_src);
    DCHECK(aligned) << "failed to align schema for append after arbitration";
    if (!aligned) {
        LOG(ERROR) << "failed to align schema for append after arbitration";
        return nullptr;
    }
    DCHECK(is_equal_schema(append_src) || _typed_columns.size() >= append_src->_typed_columns.size());
    return append_src;
}

void VariantColumn::append(const Column& src, size_t offset, size_t count) {
    size_t before_size = size();
    const auto* other = down_cast<const VariantColumn*>(&src);
    MutableColumnPtr src_working_copy;
    const VariantColumn* append_src = _prepare_append_source(*other, &src_working_copy);
    if (append_src == nullptr) {
        LOG(ERROR) << "append: _prepare_append_source failed, skipping " << count << " rows";
        return;
    }
    _append_container_rows_impl(*append_src, count, [offset, count](Column* dst, const Column& src_col) {
        dst->append(src_col, offset, count);
    });
    DCHECK_EQ(size(), before_size + count);
}

void VariantColumn::append_value_multiple_times(const void* value, size_t count) {
    const auto* row = reinterpret_cast<const VariantRowValue*>(value);
    for (size_t i = 0; i < count; ++i) {
        append(row);
    }
}

void VariantColumn::append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t count) {
    const auto* other = down_cast<const VariantColumn*>(&src);
    MutableColumnPtr src_working_copy;
    const VariantColumn* append_src = _prepare_append_source(*other, &src_working_copy);
    if (append_src == nullptr) {
        LOG(ERROR) << "append_selective: _prepare_append_source failed, skipping " << count << " rows";
        return;
    }
    _append_container_rows_impl(*append_src, count, [indexes, from, count](Column* dst, const Column& src_col) {
        dst->append_selective(src_col, indexes, from, count);
    });
}

void VariantColumn::append_value_multiple_times(const Column& src, uint32_t index, uint32_t count) {
    const auto* other = down_cast<const VariantColumn*>(&src);
    MutableColumnPtr src_working_copy;
    const VariantColumn* append_src = _prepare_append_source(*other, &src_working_copy);
    if (append_src == nullptr) {
        LOG(ERROR) << "append_value_multiple_times: _prepare_append_source failed, skipping " << count << " rows";
        return;
    }
    _append_container_rows_impl(*append_src, count, [index, count](Column* dst, const Column& src_col) {
        dst->append_value_multiple_times(src_col, index, count);
    });
}

void VariantColumn::append_shredded(Slice metadata, Slice remain_value) {
    Status st = ensure_base_variant_column();
    if (!st.ok()) {
        LOG(WARNING) << "failed to ensure base variant column in append_shredded, err=" << st;
        return;
    }
    DCHECK(_metadata_column != nullptr);
    DCHECK(_remain_value_column != nullptr);
    _metadata_column->append_datum(Datum(metadata));
    _remain_value_column->append_datum(Datum(remain_value));
    for (auto& col : _typed_columns) {
        col->append_nulls(1);
    }
}

void VariantColumn::append_shredded_null() {
    append_nulls(1);
}

void VariantColumn::append(const VariantRowValue* object) {
    if (object == nullptr) {
        append_nulls(1);
        return;
    }
    auto metadata_raw = object->get_metadata().raw();
    auto value_raw = object->get_value().raw();
    append_shredded(Slice(metadata_raw.data(), metadata_raw.size()), Slice(value_raw.data(), value_raw.size()));
}

void VariantColumn::append(const VariantRowValue& object) {
    append(&object);
}

void VariantColumn::append(const VariantRowRef& object) {
    auto metadata_raw = object.get_metadata().raw();
    auto value_raw = object.get_value().raw();
    append_shredded(Slice(metadata_raw.data(), metadata_raw.size()), Slice(value_raw.data(), value_raw.size()));
}

bool VariantColumn::append_nulls(size_t count) {
    Status st = ensure_base_variant_column();
    if (!st.ok()) {
        LOG(WARNING) << "failed to ensure base variant column in append_nulls, err=" << st;
        return false;
    }
    DCHECK(has_metadata_column() && has_remain_value());
    append_null_base_payload_rows(_metadata_column, _remain_value_column, count);
    for (auto& col : _typed_columns) {
        col->append_nulls(count);
    }
    return true;
}

void VariantColumn::append_default() {
    append_nulls(1);
}

void VariantColumn::append_default(size_t count) {
    append_nulls(count);
}

size_t VariantColumn::size() const {
    return _shredded_num_rows();
}

size_t VariantColumn::capacity() const {
    size_t cap = 0;
    if (_metadata_column != nullptr) {
        cap += _metadata_column->capacity();
    }
    if (_remain_value_column != nullptr) {
        cap += _remain_value_column->capacity();
    }
    for (const auto& col : _typed_columns) {
        cap += col->capacity();
    }
    return cap;
}

size_t VariantColumn::byte_size(size_t from, size_t sz) const {
    size_t bytes = 0;
    if (_metadata_column != nullptr) {
        bytes += _metadata_column->byte_size(from, sz);
    }
    if (_remain_value_column != nullptr) {
        bytes += _remain_value_column->byte_size(from, sz);
    }
    for (const auto& col : _typed_columns) {
        bytes += col->byte_size(from, sz);
    }
    return bytes;
}

void VariantColumn::resize(size_t n) {
    if (_metadata_column != nullptr) {
        _metadata_column->resize(n);
    }
    if (_remain_value_column != nullptr) {
        _remain_value_column->resize(n);
    }
    for (auto& col : _typed_columns) {
        col->resize(n);
    }
}

void VariantColumn::assign(size_t n, size_t idx) {
    if (_metadata_column != nullptr) {
        _metadata_column->assign(n, idx);
    }
    if (_remain_value_column != nullptr) {
        _remain_value_column->assign(n, idx);
    }
    for (auto& col : _typed_columns) {
        col->assign(n, idx);
    }
}

size_t VariantColumn::filter_range(const Filter& filter, size_t from, size_t to) {
    // metadata and remain are always both present or both absent (schema invariant).
    // Do not initialize with (to - from): typed-only VariantColumn (for example,
    // array<variant> element variants) has no metadata/remain, so the first real
    // filtered column must define the resulting kept-row count.
    size_t result_size = 0;
    bool initialized = false;
    if (_metadata_column != nullptr) {
        // Base-shredded rows filter metadata/remain together; both must report the
        // same kept-row count.
        result_size = _metadata_column->filter_range(filter, from, to);
        size_t r = _remain_value_column->filter_range(filter, from, to);
        DCHECK_EQ(result_size, r);
        initialized = true;
    }
    for (auto& col : _typed_columns) {
        size_t r = col->filter_range(filter, from, to);
        if (!initialized) {
            result_size = r;
            initialized = true;
        } else {
            DCHECK_EQ(result_size, r);
        }
    }
    if (!initialized) {
        // Defensive fallback for an empty schema (should not normally happen).
        return to - from;
    }
    return result_size;
}

void VariantColumn::swap_column(Column& rhs) {
    auto& other = down_cast<VariantColumn&>(rhs);
    BaseClass::swap_column(other);
    std::swap(_shredded_paths, other._shredded_paths);
    std::swap(_path_index, other._path_index);
    std::swap(_parsed_shredded_paths, other._parsed_shredded_paths);
    std::swap(_shredded_types, other._shredded_types);
    std::swap(_typed_columns, other._typed_columns);
    std::swap(_metadata_column, other._metadata_column);
    std::swap(_remain_value_column, other._remain_value_column);
}

void VariantColumn::reset_column() {
    BaseClass::reset_column();
    clear_shredded_columns();
}

void VariantColumn::check_or_die() const {
    DCHECK(_is_shredded_schema_valid());
    DCHECK(_is_shredded_row_aligned());
    if (_metadata_column != nullptr) {
        _metadata_column->check_or_die();
    }
    if (_remain_value_column != nullptr) {
        _remain_value_column->check_or_die();
    }
    for (const auto& col : _typed_columns) {
        col->check_or_die();
    }
}

bool VariantColumn::is_shredded_variant() const {
    return !_typed_columns.empty() || (_metadata_column != nullptr && _remain_value_column != nullptr);
}

bool VariantColumn::is_typed_only_variant() const {
    return !_typed_columns.empty() && !has_metadata_column() && !has_remain_value();
}

const VariantRowValue* VariantColumn::get_row_value(size_t idx, VariantRowValue* output) const {
    if (output == nullptr || !try_materialize_row(idx, output)) {
        return nullptr;
    }
    return output;
}

static bool has_non_null_typed_overlay(const VariantColumn* column, size_t row) {
    if (column == nullptr) {
        return false;
    }
    for (const auto& typed_col : column->typed_columns()) {
        if (typed_col == nullptr) {
            continue;
        }
        const size_t typed_row = typed_col->is_constant() ? 0 : row;
        if (!typed_col->is_null(typed_row)) {
            return true;
        }
    }
    return false;
}

static bool try_materialize_from_typed_only_overlays(const VariantColumn* column, size_t row, VariantRowValue* output) {
    if (column == nullptr || output == nullptr || column->typed_columns().empty()) {
        return false;
    }
    std::vector<VariantBuilder::Overlay> overlays;
    if (!collect_typed_overlays(column, row, &overlays)) {
        return false;
    }
    return rebuild_row_with_optional_base(nullptr, std::move(overlays), output);
}

bool VariantColumn::try_get_row_ref(size_t idx, VariantRowRef* out) const {
    if (out == nullptr) {
        return false;
    }
    if (!has_metadata_column() || !has_remain_value()) {
        return false;
    }

    Slice metadata_slice;
    Slice remain_slice;
    bool has_metadata = ColumnHelper::get_binary_slice_at(_metadata_column.get(), idx, &metadata_slice);
    bool has_remain = ColumnHelper::get_binary_slice_at(_remain_value_column.get(), idx, &remain_slice);
    if (!has_metadata || !has_remain) {
        return false;
    }
    if (metadata_slice.size == 0 || remain_slice.size == 0) {
        return false;
    }

    if (has_non_null_typed_overlay(this, idx)) {
        return false;
    }
    *out = VariantRowRef(std::string_view(metadata_slice.data, metadata_slice.size),
                         std::string_view(remain_slice.data, remain_slice.size));
    return true;
}

bool VariantColumn::try_materialize_row(size_t idx, VariantRowValue* output) const {
    if (output == nullptr) {
        return false;
    }

    VariantRowRef row_ref;
    if (try_get_row_ref(idx, &row_ref)) {
        *output = row_ref.to_owned();
        return true;
    }

    if (has_metadata_column() && has_remain_value()) {
        Slice metadata_slice;
        Slice remain_slice;
        bool has_metadata = ColumnHelper::get_binary_slice_at(_metadata_column.get(), idx, &metadata_slice);
        bool has_remain = ColumnHelper::get_binary_slice_at(_remain_value_column.get(), idx, &remain_slice);
        if (!has_metadata || !has_remain) {
            if (try_materialize_from_typed_only_overlays(this, idx, output)) {
                return true;
            }
            *output = VariantRowValue::from_null();
            return true;
        }
        // Some legacy/external sources may provide empty base payload cells.
        // For rows with typed overlays, still try typed reconstruction first.
        // This preserves typed-only promotion semantics where base payload can be empty.
        if (metadata_slice.size == 0 || remain_slice.size == 0) {
            if (try_materialize_from_typed_only_overlays(this, idx, output)) {
                return true;
            }
            *output = VariantRowValue::from_null();
            return true;
        }
        const std::string_view metadata_raw(metadata_slice.data, metadata_slice.size);
        const std::string_view remain_raw(remain_slice.data, remain_slice.size);
        if (rebuild_row_from_base_shredded(this, idx, output, metadata_raw, remain_raw)) {
            return true;
        }
        // Keep query/result stability: if typed overlay rebuild fails, fall back to
        // the base payload instead of turning the whole row into NULL.
        *output = VariantRowValue(metadata_raw, remain_raw);
        LOG(WARNING) << "failed to rebuild base_shredded row with typed overlays, fallback to base payload";
        return true;
    }

    if (!is_typed_only_variant()) {
        return false;
    }
    return rebuild_row_from_typed_columns(this, idx, output);
}

void VariantColumn::set_shredded_columns(std::vector<std::string> paths, std::vector<TypeDescriptor> type_descs,
                                         MutableColumns columns, BinaryColumn::MutablePtr metadata_column,
                                         BinaryColumn::MutablePtr remain_value_column) {
    auto schema_st = validate_shredded_schema(paths, type_descs, columns, metadata_column, remain_value_column);
    DCHECK(schema_st.ok()) << "Invalid shredded schema in VariantColumn: " << schema_st;

    _shredded_paths = std::move(paths);
    _rebuild_path_index();
    _shredded_types = std::move(type_descs);
    _typed_columns = std::move(columns);
    _metadata_column = std::move(metadata_column);
    _remain_value_column = std::move(remain_value_column);

    DCHECK(_is_shredded_schema_valid()) << "Invalid shredded schema in VariantColumn (internal invariant)";
    DCHECK(_is_shredded_row_aligned()) << "Invalid shredded row alignment in VariantColumn";
}

Status VariantColumn::validate_shredded_schema(const std::vector<std::string>& paths,
                                               const std::vector<TypeDescriptor>& type_descs,
                                               const MutableColumns& columns,
                                               const BinaryColumn::MutablePtr& metadata_column,
                                               const BinaryColumn::MutablePtr& remain_value_column) {
    if (paths.size() != type_descs.size()) {
        return Status::InvalidArgument("shredded paths/types size mismatch");
    }
    if (paths.size() != columns.size()) {
        return Status::InvalidArgument("shredded paths/typed columns size mismatch");
    }
    for (size_t i = 0; i < columns.size(); ++i) {
        if (columns[i] == nullptr) {
            return Status::InvalidArgument("typed column is null");
        }
    }
    if ((metadata_column == nullptr) != (remain_value_column == nullptr)) {
        return Status::InvalidArgument("metadata/remain must both exist or both be absent");
    }
    if (metadata_column == nullptr && columns.empty()) {
        return Status::InvalidArgument("base metadata/remain are required when typed columns are empty");
    }

    std::unordered_set<std::string_view> unique_paths;
    unique_paths.reserve(paths.size());
    for (const auto& path : paths) {
        if (!unique_paths.emplace(path).second) {
            return Status::InvalidArgument("duplicate shredded path");
        }
        if (path.empty()) {
            return Status::InvalidArgument("empty shredded path is not allowed");
        }
        auto parsed = VariantPathParser::parse_shredded_path(std::string_view(path));
        if (!parsed.ok()) {
            return Status::InvalidArgument(
                    strings::Substitute("invalid shredded path '$0': $1", path, parsed.status().to_string()));
        }
    }
    return Status::OK();
}

void VariantColumn::clear_shredded_columns() {
    _shredded_paths.clear();
    _path_index.clear();
    _parsed_shredded_paths.clear();
    _shredded_types.clear();
    _typed_columns.clear();
    _metadata_column = BinaryColumn::create();
    _remain_value_column = BinaryColumn::create();
}

void VariantColumn::_init_schema_from(const VariantColumn& other) {
    BaseClass::reset_column();
    _shredded_paths = other._shredded_paths;
    _path_index = other._path_index;
    _parsed_shredded_paths = other._parsed_shredded_paths;
    _shredded_types = other._shredded_types;

    _typed_columns.clear();
    _typed_columns.reserve(other._typed_columns.size());
    for (const auto& col : other._typed_columns) {
        _typed_columns.emplace_back(col->clone_empty());
    }

    _metadata_column = other._metadata_column != nullptr ? BinaryColumn::create() : nullptr;
    _remain_value_column = other._remain_value_column != nullptr ? BinaryColumn::create() : nullptr;
}

// Helper template to unify the three container row append methods.
// append_func takes (Column* dst, const Column& src) and performs the specific append operation.
// Using Column* allows the same lambda to handle both BinaryColumn (metadata/remain) and
// NullableColumn (typed columns) without type-specific overloads.
template <typename AppendFunc>
void VariantColumn::_append_container_rows_impl(const VariantColumn& src, size_t count, AppendFunc&& append_func) {
    DCHECK_GE(_typed_columns.size(), src._typed_columns.size());
    DCHECK_EQ(has_metadata_column(), has_remain_value());
    DCHECK_EQ(src.has_metadata_column(), src.has_remain_value());

    if (has_metadata_column()) {
        DCHECK(has_remain_value());
        if (src.has_metadata_column()) {
            append_func(_metadata_column.get(), *src._metadata_column);
            append_func(_remain_value_column.get(), *src._remain_value_column);
        } else {
            append_null_base_payload_rows(_metadata_column, _remain_value_column, count);
        }
    } else {
        DCHECK(!src.has_metadata_column()) << "destination typed-only must be normalized before base append";
    }

    for (size_t i = 0; i < _typed_columns.size(); ++i) {
        if (i < src._typed_columns.size()) {
            append_func(_typed_columns[i].get(), *src._typed_columns[i]);
        } else {
            _typed_columns[i]->append_nulls(count);
        }
    }

    DCHECK(_is_shredded_row_aligned());
}

bool VariantColumn::_is_shredded_schema_valid() const {
    return validate_shredded_schema(_shredded_paths, _shredded_types, _typed_columns, _metadata_column,
                                    _remain_value_column)
            .ok();
}

size_t VariantColumn::_shredded_num_rows() const {
    if (_metadata_column != nullptr) {
        return _metadata_column->size();
    }
    if (!_typed_columns.empty()) {
        return _typed_columns[0]->size();
    }
    return 0;
}

Status VariantColumn::ensure_base_variant_column() {
    if (!is_typed_only_variant()) {
        return Status::OK();
    }

    size_t rows = _shredded_num_rows();
    auto [metadata, remain] = _create_metadata_remain_columns(rows);
    append_null_base_payload_rows(metadata, remain, rows);
    _metadata_column = std::move(metadata);
    _remain_value_column = std::move(remain);
    if (_is_shredded_schema_valid() && _is_shredded_row_aligned()) {
        return Status::OK();
    }
    return Status::InvalidArgument("failed to ensure base variant column");
}

std::pair<BinaryColumn::MutablePtr, BinaryColumn::MutablePtr> VariantColumn::_create_metadata_remain_columns(
        size_t rows) {
    auto metadata = BinaryColumn::create();
    auto remain = BinaryColumn::create();
    metadata->reserve(rows);
    remain->reserve(rows);
    return {std::move(metadata), std::move(remain)};
}

bool VariantColumn::_is_shredded_row_aligned() const {
    size_t rows = _shredded_num_rows();
    if (_metadata_column != nullptr && _metadata_column->size() != rows) {
        return false;
    }
    if (_remain_value_column != nullptr && _remain_value_column->size() != rows) {
        return false;
    }
    for (const auto& column : _typed_columns) {
        if (column->size() != rows) {
            return false;
        }
    }
    return true;
}

bool VariantColumn::is_equal_schema(const VariantColumn* other) const {
    if (other == nullptr) {
        return false;
    }
    if (_shredded_paths != other->_shredded_paths) {
        return false;
    }
    if (_shredded_types != other->_shredded_types) {
        return false;
    }
    if (has_metadata_column() != other->has_metadata_column()) {
        return false;
    }
    return _typed_columns.size() == other->_typed_columns.size();
}

bool VariantColumn::align_schema_from(const VariantColumn& src) {
    if (size() == 0) {
        _init_schema_from(src);
        return true;
    }
    if (is_equal_schema(&src)) {
        return true;
    }
    if (is_typed_only_variant() && src.has_metadata_column()) {
        Status st = ensure_base_variant_column();
        if (!st.ok()) {
            LOG(WARNING) << "failed to ensure base variant column before append alignment, err=" << st;
            return false;
        }
        if (is_equal_schema(&src)) {
            return true;
        }
    }
    // Preconditions should be guaranteed by callers
    // Preconditions should be guaranteed by callers
    DCHECK(_is_shredded_schema_valid());
    DCHECK(_is_shredded_row_aligned());
    DCHECK(src._is_shredded_schema_valid());
    DCHECK(src._is_shredded_row_aligned());

    if (is_equal_schema(&src)) {
        return true;
    }
    std::unordered_map<std::string_view, size_t> src_index_by_path;
    src_index_by_path.reserve(src._shredded_paths.size());
    for (size_t i = 0; i < src._shredded_paths.size(); ++i) {
        if (!src_index_by_path.emplace(src._shredded_paths[i], i).second) {
            LOG(WARNING) << "align_schema_from duplicate path in src: " << src._shredded_paths[i];
            return false;
        }
    }

    std::unordered_map<std::string_view, size_t> dst_index_by_path;
    dst_index_by_path.reserve(_shredded_paths.size());
    for (size_t i = 0; i < _shredded_paths.size(); ++i) {
        if (!dst_index_by_path.emplace(_shredded_paths[i], i).second) {
            LOG(WARNING) << "align_schema_from duplicate path in dst: " << _shredded_paths[i];
            return false;
        }
    }

    // Validate type compatibility for overlapping paths.
    for (size_t i = 0; i < src._shredded_paths.size(); ++i) {
        const auto& path = src._shredded_paths[i];
        auto dst_it = dst_index_by_path.find(path);
        if (dst_it == dst_index_by_path.end()) {
            continue;
        }
        if (_shredded_types[dst_it->second] != src._shredded_types[i]) {
            LOG(WARNING) << "align_schema_from type conflict on path=" << path
                         << ", dst_type=" << _shredded_types[dst_it->second].debug_string()
                         << ", src_type=" << src._shredded_types[i].debug_string();
            return false;
        }
    }

    std::vector<std::string> target_paths;
    std::vector<TypeDescriptor> target_types;
    target_paths.reserve(src._shredded_paths.size() + _shredded_paths.size());
    target_types.reserve(src._shredded_types.size() + _shredded_types.size());

    // Canonical order for append fast path:
    // 1) source paths in source order; 2) destination-only paths in destination order.
    for (size_t i = 0; i < src._shredded_paths.size(); ++i) {
        target_paths.emplace_back(src._shredded_paths[i]);
        target_types.emplace_back(src._shredded_types[i]);
    }
    for (size_t i = 0; i < _shredded_paths.size(); ++i) {
        if (src_index_by_path.find(_shredded_paths[i]) != src_index_by_path.end()) {
            continue;
        }
        target_paths.emplace_back(_shredded_paths[i]);
        target_types.emplace_back(_shredded_types[i]);
    }

    size_t dst_rows = _shredded_num_rows();
    std::vector<std::string> old_paths = std::move(_shredded_paths);
    MutableColumns old_typed_columns = std::move(_typed_columns);

    std::unordered_map<std::string_view, size_t> old_index_by_path;
    old_index_by_path.reserve(old_paths.size());
    for (size_t i = 0; i < old_paths.size(); ++i) {
        old_index_by_path.emplace(old_paths[i], i);
    }

    _shredded_paths = std::move(target_paths);
    _rebuild_path_index();
    _shredded_types = std::move(target_types);
    _typed_columns.clear();
    _typed_columns.reserve(_shredded_paths.size());

    for (size_t i = 0; i < _shredded_paths.size(); ++i) {
        auto old_it = old_index_by_path.find(_shredded_paths[i]);
        if (old_it != old_index_by_path.end()) {
            _typed_columns.emplace_back(std::move(old_typed_columns[old_it->second]));
            continue;
        }

        auto new_column = ColumnHelper::create_column(_shredded_types[i], true);
        if (dst_rows > 0) {
            new_column->append_nulls(dst_rows);
        }
        _typed_columns.emplace_back(std::move(new_column));
    }

    return _is_shredded_schema_valid() && _is_shredded_row_aligned();
}

std::string VariantColumn::debug_item(size_t idx) const {
    VariantRowValue row;
    const VariantRowValue* value = get_row_value(idx, &row);
    if (value == nullptr) {
        return "";
    }
    // For debug display, use UTC timezone to show timestamps consistently
    auto json_result = value->to_json(cctz::utc_time_zone());
    if (!json_result.ok()) {
        return "";
    }
    return json_result.value();
}

std::string VariantColumn::debug_string() const {
    std::string result = "[";
    for (size_t i = 0; i < size(); ++i) {
        if (i > 0) {
            result += ", ";
        }
        result += debug_item(i);
    }
    result += "]";
    return result;
}

} // namespace starrocks
