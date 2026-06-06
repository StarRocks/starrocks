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

#pragma once

#include <cstddef>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base/status.h"
#include "column/binary_column.h"
#include "column/column.h"
#include "column/object_column.h"
#include "column/variant_path_parser.h"
#include "column/vectorized_fwd.h"
#include "types/logical_type.h"
#include "types/type_descriptor.h"
#include "types/variant_value.h"

namespace starrocks {

class VariantColumn final
        : public CowFactory<ColumnFactory<ObjectColumn<VariantRowValue>, VariantColumn>, VariantColumn, Column> {
public:
    enum class EncodedVariantState : uint8_t {
        kNull,
        kValue,
    };

    struct EncodedVariantResult {
        EncodedVariantState state = EncodedVariantState::kNull;
        VariantRowValue value;
    };

    using ValueType = VariantRowValue;
    using SuperClass = CowFactory<ColumnFactory<ObjectColumn<VariantRowValue>, VariantColumn>, VariantColumn, Column>;
    using BaseClass = VariantColumnBase;

    VariantColumn();
    explicit VariantColumn(size_t size);
    DISALLOW_COPY(VariantColumn);

    VariantColumn(VariantColumn&& rhs) noexcept
            : SuperClass(std::move(rhs)),
              _shredded_paths(std::move(rhs._shredded_paths)),
              _parsed_shredded_paths(std::move(rhs._parsed_shredded_paths)),
              _path_index(std::move(rhs._path_index)),
              _shredded_types(std::move(rhs._shredded_types)),
              _typed_columns(std::move(rhs._typed_columns)),
              _metadata_column(std::move(rhs._metadata_column)),
              _remain_value_column(std::move(rhs._remain_value_column)) {}

    MutableColumnPtr clone() const override;
    MutableColumnPtr clone_empty() const override { return this->create(); }

    // Row-level serde used by generic Column key paths (e.g. multi-column GROUP BY / hash keys).
    // Use VariantRowValue wire format so each row is self-describing and does not depend on
    // receiver-side shredded schema.
    // Different from ColumnArraySerde:
    // - here: row-wise key serialization for execution engine internal hash paths.
    // - ColumnArraySerde: full-column serialization, including shredded schema metadata.
    // Tradeoff: this path may materialize typed overlays into row values and can be slower than
    // schema-aware shredded row encoding, but it is robust for schema-less key restoration.
    uint32_t serialize(size_t idx, uint8_t* pos) const override;
    uint32_t serialize_size(size_t idx) const override;
    void serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                         uint32_t max_one_row_size) const override;
    const uint8_t* deserialize_and_append(const uint8_t* pos) override;

    void append_datum(const Datum& datum) override;
    void append(const Column& src) override { append(src, 0, src.size()); }
    void append(const Column& src, size_t offset, size_t count) override;
    void append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) override;
    void append_value_multiple_times(const void* value, size_t count) override;
    void append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) override;

    void append(const VariantRowValue* object);
    void append(const VariantRowValue& object);
    void append(const VariantRowRef& object);
    void append_shredded(Slice metadata, Slice remain_value);
    void append_shredded_null();
    bool append_nulls(size_t count) override;
    void append_default() override;
    void append_default(size_t count) override;

    size_t size() const override;
    size_t capacity() const override;
    size_t byte_size(size_t from, size_t size) const override;
    void resize(size_t n) override;
    void assign(size_t n, size_t idx) override;
    size_t filter_range(const Filter& filter, size_t from, size_t to) override;
    int compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const override;
    int equals(size_t left, const Column& rhs, size_t right, bool safe_eq = true) const override;
    void swap_column(Column& rhs) override;
    void reset_column() override;
    void check_or_die() const override;

    bool is_variant() const override { return true; }
    // A VariantColumn is considered shredded if it has typed columns,
    // or has both metadata/remain columns.
    bool is_shredded_variant() const;
    // Typed-only shredded shape:
    // - typed columns exist
    // - metadata/remain are absent
    bool is_typed_only_variant() const;
    bool try_get_row_ref(size_t idx, VariantRowRef* out) const;
    const VariantRowValue* get_row_value(size_t idx, VariantRowValue* output) const;
    bool try_materialize_row(size_t idx, VariantRowValue* output) const;

    std::string get_name() const override { return "variant"; }

    void set_shredded_columns(std::vector<std::string> paths, std::vector<TypeDescriptor> type_descs,
                              MutableColumns columns, BinaryColumn::MutablePtr metadata_column,
                              BinaryColumn::MutablePtr remain_value_column);
    static Status validate_shredded_schema(const std::vector<std::string>& paths,
                                           const std::vector<TypeDescriptor>& type_descs, const MutableColumns& columns,
                                           const BinaryColumn::MutablePtr& metadata_column,
                                           const BinaryColumn::MutablePtr& remain_value_column);

    void clear_shredded_columns();

    const std::vector<std::string>& shredded_paths() const { return _shredded_paths; }

    // Cached parsed form of _shredded_paths. Populated by set_shredded_columns(),
    // kept in sync with _shredded_paths. Avoids per-row re-parsing in hot paths.
    const std::vector<VariantPath>& parsed_shredded_paths() const { return _parsed_shredded_paths; }

    const std::vector<TypeDescriptor>& shredded_types() const { return _shredded_types; }

    std::vector<TypeDescriptor>& mutable_shredded_types() { return _shredded_types; }

    const MutableColumns& typed_columns() const { return _typed_columns; }

    MutableColumns& mutable_typed_columns() { return _typed_columns; }
    int find_shredded_path(std::string_view path) const;
    const Column* typed_column_by_index(size_t idx) const;

    const BinaryColumn::MutablePtr& metadata_column() const { return _metadata_column; }

    const BinaryColumn::MutablePtr& remain_value_column() const { return _remain_value_column; }

    bool has_metadata_column() const { return _metadata_column != nullptr; }

    bool has_remain_value() const { return _remain_value_column != nullptr; }

    bool is_equal_schema(const VariantColumn* other) const;

    void mutate_each_subcolumn() override {
        for (auto& column : _typed_columns) {
            column = (std::move(*column)).mutate();
        }
        if (_metadata_column != nullptr) {
            _metadata_column = BinaryColumn::static_pointer_cast((std::move(*_metadata_column)).mutate());
        }
        if (_remain_value_column != nullptr) {
            _remain_value_column = BinaryColumn::static_pointer_cast((std::move(*_remain_value_column)).mutate());
        }
    }

    // Encode a single typed cell (from a typed column at a given row) into a VariantRowValue.
    // Handles TYPE_VARIANT recursion, null checks, and VariantEncoder encoding.
    // Used by both VariantColumn internal paths and VariantFunctions query paths.
    static StatusOr<EncodedVariantResult> encode_typed_row_as_variant(const Column* typed_column, size_t typed_row,
                                                                      const TypeDescriptor& type_desc);

    // Deep-copy a shredded VariantColumn, duplicating typed columns, metadata, and remain.
    // Caller must ensure src.is_shredded_variant().
    static MutableColumnPtr deep_copy_shredded(const VariantColumn& src);

    // Ensure base metadata/remain columns exist.
    // For typed-only schema, this attaches null base payload rows while preserving typed ownership.
    Status ensure_base_variant_column();

    // Align destination schema from `src` before append/merge.
    // Empty destination is initialized directly from `src`; otherwise schema
    // is aligned via path union and compatibility checks.
    bool align_schema_from(const VariantColumn& src);

    void put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx, bool is_binary_protocol = false) const override;

    std::string debug_item(size_t idx) const override;

    std::string debug_string() const override;

private:
    void _init_schema_from(const VariantColumn& other);

    // Template implementation for container row append methods
    template <typename AppendFunc>
    void _append_container_rows_impl(const VariantColumn& src, size_t count, AppendFunc&& append_func);

    const VariantColumn* _prepare_append_source(const VariantColumn& src, MutableColumnPtr* src_working_copy);

    bool _is_shredded_schema_valid() const;

    bool _is_shredded_row_aligned() const;

    size_t _shredded_num_rows() const;

    // Helper to create metadata/remain columns with pre-allocated capacity
    std::pair<BinaryColumn::MutablePtr, BinaryColumn::MutablePtr> _create_metadata_remain_columns(size_t rows);

    // Rebuild _path_index from _shredded_paths. Call whenever _shredded_paths changes.
    void _rebuild_path_index();

private:
    std::vector<std::string> _shredded_paths;
    // Cached parsed shredded paths (parallel to _shredded_paths).
    std::vector<VariantPath> _parsed_shredded_paths;
    // O(1) index into _shredded_paths for find_shredded_path. Kept in sync with _shredded_paths.
    std::unordered_map<std::string, int> _path_index;
    std::vector<TypeDescriptor> _shredded_types;
    MutableColumns _typed_columns;
    // base variant column. Always BinaryColumn: nulls are encoded as binary sentinel payloads,
    // not as NullableColumn null flags.
    BinaryColumn::MutablePtr _metadata_column;
    BinaryColumn::MutablePtr _remain_value_column;
};

} // namespace starrocks
