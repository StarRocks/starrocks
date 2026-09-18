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

#include "column/file_column.h"

#include <sstream>

#include "column/binary_column.h"
#include "column/fixed_length_column.h"
#include "column/mysql_row_buffer.h"
#include "column/nullable_column.h"
#include "gutil/strings/escaping.h"
#include "types/datum.h"

namespace starrocks {

namespace {

Column::MutablePtr make_nullable_binary() {
    return NullableColumn::create(BinaryColumn::create(), NullColumn::create());
}

Column::MutablePtr make_nullable_bigint() {
    return NullableColumn::create(Int64Column::create(), NullColumn::create());
}

NullableColumn* as_nullable(Column* column) {
    return down_cast<NullableColumn*>(column);
}

const NullableColumn* as_nullable(const Column* column) {
    return down_cast<const NullableColumn*>(column);
}

void append_slice(NullableColumn* column, const Slice& value) {
    down_cast<BinaryColumn*>(column->data_column_raw_ptr())->append(value);
    column->null_column_raw_ptr()->append(0);
}

void append_bigint(NullableColumn* column, int64_t value) {
    down_cast<Int64Column*>(column->data_column_raw_ptr())->append(value);
    column->null_column_raw_ptr()->append(0);
}

} // namespace

FileColumn::FileColumn()
        : _uri(make_nullable_binary()),
          _offset(make_nullable_bigint()),
          _size(make_nullable_bigint()),
          _content_type(make_nullable_binary()),
          _checksum(make_nullable_binary()),
          _inline(make_nullable_binary()) {}

FileColumn::FileColumn(size_t size) : FileColumn() {
    if (size > 0) {
        // Every sub-column is nullable, so append_default() appends NULLs.
        for_each_field([size](auto& column) { column->append_default(size); });
    }
}

FileColumn::FileColumn(FileColumn&& rhs) noexcept
        : _uri(std::move(rhs._uri)),
          _offset(std::move(rhs._offset)),
          _size(std::move(rhs._size)),
          _content_type(std::move(rhs._content_type)),
          _checksum(std::move(rhs._checksum)),
          _inline(std::move(rhs._inline)) {}

// ---- business append ----

void FileColumn::append_null_row() {
    for_each_field([](auto& column) { (void)column->append_nulls(1); });
}

void FileColumn::append_reference(const Slice& uri, int64_t offset, std::optional<int64_t> size) {
    append_slice(uri_column(), uri);
    append_bigint(offset_column(), offset);
    if (size.has_value()) {
        append_bigint(size_column(), *size);
    } else {
        (void)_size->append_nulls(1);
    }
    (void)_content_type->append_nulls(1);
    (void)_checksum->append_nulls(1);
    (void)_inline->append_nulls(1);
}

void FileColumn::append_inline(const Slice& bytes) {
    (void)_uri->append_nulls(1);
    (void)_offset->append_nulls(1);
    (void)_size->append_nulls(1);
    (void)_content_type->append_nulls(1);
    (void)_checksum->append_nulls(1);
    append_slice(inline_column(), bytes);
}

// ---- typed access ----

NullableColumn* FileColumn::uri_column() {
    return as_nullable(_uri.get());
}
NullableColumn* FileColumn::offset_column() {
    return as_nullable(_offset.get());
}
NullableColumn* FileColumn::size_column() {
    return as_nullable(_size.get());
}
NullableColumn* FileColumn::content_type_column() {
    return as_nullable(_content_type.get());
}
NullableColumn* FileColumn::checksum_column() {
    return as_nullable(_checksum.get());
}
NullableColumn* FileColumn::inline_column() {
    return as_nullable(_inline.get());
}
const NullableColumn* FileColumn::uri_column() const {
    return as_nullable(_uri.get());
}
const NullableColumn* FileColumn::offset_column() const {
    return as_nullable(_offset.get());
}
const NullableColumn* FileColumn::size_column() const {
    return as_nullable(_size.get());
}
const NullableColumn* FileColumn::content_type_column() const {
    return as_nullable(_content_type.get());
}
const NullableColumn* FileColumn::checksum_column() const {
    return as_nullable(_checksum.get());
}
const NullableColumn* FileColumn::inline_column() const {
    return as_nullable(_inline.get());
}

std::array<Column*, FileColumn::NUM_FIELDS> FileColumn::field_columns() {
    return {_uri.get(), _offset.get(), _size.get(), _content_type.get(), _checksum.get(), _inline.get()};
}

std::array<const Column*, FileColumn::NUM_FIELDS> FileColumn::field_columns() const {
    return {_uri.get(), _offset.get(), _size.get(), _content_type.get(), _checksum.get(), _inline.get()};
}

// ---- Column interface ----

size_t FileColumn::size() const {
    return _uri->size();
}

size_t FileColumn::capacity() const {
    return _uri->capacity();
}

size_t FileColumn::type_size() const {
    return sizeof(DatumStruct);
}

size_t FileColumn::byte_size() const {
    size_t total = 0;
    for_each_field([&](const auto& column) { total += column->byte_size(); });
    return total;
}

size_t FileColumn::byte_size(size_t idx) const {
    size_t total = 0;
    for_each_field([&](const auto& column) { total += column->byte_size(idx); });
    return total;
}

size_t FileColumn::byte_size(size_t from, size_t size) const {
    DCHECK_LE(from + size, this->size()) << "Range error";
    size_t total = 0;
    for_each_field([&](const auto& column) { total += column->byte_size(from, size); });
    return total;
}

void FileColumn::reserve(size_t n) {
    for_each_field([n](auto& column) { column->reserve(n); });
}

void FileColumn::resize(size_t n) {
    for_each_field([n](auto& column) { column->resize(n); });
}

StatusOr<MutableColumnPtr> FileColumn::upgrade_if_overflow() {
    Status status;
    for_each_field([&](auto& column) {
        if (!status.ok()) {
            return;
        }
        auto ret = upgrade_helper_func(column->as_mutable_raw_ptr());
        if (!ret.ok()) {
            status = ret.status();
        } else if (ret.value() != nullptr) {
            column = std::move(ret.value());
        }
    });
    RETURN_IF_ERROR(status);
    return nullptr;
}

StatusOr<MutableColumnPtr> FileColumn::downgrade() {
    Status status;
    for_each_field([&](auto& column) {
        if (!status.ok()) {
            return;
        }
        auto ret = downgrade_helper_func(column->as_mutable_raw_ptr());
        if (!ret.ok()) {
            status = ret.status();
        } else if (ret.value() != nullptr) {
            column = std::move(ret.value());
        }
    });
    RETURN_IF_ERROR(status);
    return nullptr;
}

bool FileColumn::has_large_column() const {
    bool res = false;
    for_each_field([&](const auto& column) { res = res || column->has_large_column(); });
    return res;
}

void FileColumn::assign(size_t n, size_t idx) {
    DCHECK_LE(idx, size()) << "Range error when assign FileColumn";
    auto desc = this->clone_empty();
    desc->append_value_multiple_times(*this, idx, n);
    swap_column(*desc);
    desc->reset_column();
}

void FileColumn::append_datum(const Datum& datum) {
    const auto& fields = datum.get<DatumStruct>();
    DCHECK_EQ(NUM_FIELDS, fields.size());
    auto columns = field_columns();
    for (size_t i = 0; i < NUM_FIELDS; ++i) {
        columns[i]->append_datum(fields[i]);
    }
}

void FileColumn::remove_first_n_values(size_t count) {
    for_each_field([count](auto& column) { column->remove_first_n_values(count); });
}

void FileColumn::append(const Column& src, size_t offset, size_t count) {
    DCHECK(src.is_file());
    const auto& src_column = down_cast<const FileColumn&>(src);
    auto dst = field_columns();
    auto srcs = src_column.field_columns();
    for (size_t i = 0; i < NUM_FIELDS; ++i) {
        dst[i]->append(*srcs[i], offset, count);
    }
}

void FileColumn::fill_default(const Filter& filter) {
    for_each_field([&filter](auto& column) { column->fill_default(filter); });
}

void FileColumn::update_rows(const Column& src, const uint32_t* indexes) {
    DCHECK(src.is_file());
    const auto& src_column = down_cast<const FileColumn&>(src);
    auto dst = field_columns();
    auto srcs = src_column.field_columns();
    for (size_t i = 0; i < NUM_FIELDS; ++i) {
        dst[i]->update_rows(*srcs[i], indexes);
    }
}

void FileColumn::append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    DCHECK(src.is_file());
    const auto& src_column = down_cast<const FileColumn&>(src);
    auto dst = field_columns();
    auto srcs = src_column.field_columns();
    for (size_t i = 0; i < NUM_FIELDS; ++i) {
        dst[i]->append_selective(*srcs[i], indexes, from, size);
    }
}

void FileColumn::append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) {
    DCHECK(src.is_file());
    const auto& src_column = down_cast<const FileColumn&>(src);
    auto dst = field_columns();
    auto srcs = src_column.field_columns();
    for (size_t i = 0; i < NUM_FIELDS; ++i) {
        dst[i]->append_value_multiple_times(*srcs[i], index, size);
    }
}

bool FileColumn::append_nulls(size_t count) {
    bool ok = true;
    for_each_field([&](auto& column) {
        if (!column->append_nulls(count)) {
            DCHECK(false) << "FileColumn sub-column append_nulls failed, that should not happen";
            ok = false;
        }
    });
    return ok;
}

size_t FileColumn::append_numbers(const void* buff, size_t length) {
    return -1;
}

void FileColumn::append_value_multiple_times(const void* value, size_t count) {
    const auto* datum = reinterpret_cast<const Datum*>(value);
    const auto& fields = datum->get_struct();
    DCHECK_EQ(NUM_FIELDS, fields.size());
    auto columns = field_columns();
    for (size_t c = 0; c < count; ++c) {
        for (size_t i = 0; i < NUM_FIELDS; ++i) {
            columns[i]->append_datum(fields[i]);
        }
    }
}

void FileColumn::append_default() {
    for_each_field([](auto& column) { column->append_default(); });
}

void FileColumn::append_default(size_t count) {
    for_each_field([count](auto& column) { column->append_default(count); });
}

uint32_t FileColumn::serialize(size_t idx, uint8_t* pos) const {
    uint32_t ser_size = 0;
    for_each_field([&](const auto& column) { ser_size += column->serialize(idx, pos + ser_size); });
    return ser_size;
}

uint32_t FileColumn::serialize_default(uint8_t* pos) const {
    uint32_t ser_size = 0;
    for_each_field([&](const auto& column) { ser_size += column->serialize_default(pos + ser_size); });
    return ser_size;
}

void FileColumn::serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                                 uint32_t max_one_row_size) const {
    for (size_t i = 0; i < chunk_size; ++i) {
        slice_sizes[i] += serialize(i, dst + i * max_one_row_size + slice_sizes[i]);
    }
}

const uint8_t* FileColumn::deserialize_and_append(const uint8_t* pos) {
    for_each_field([&](auto& column) { pos = column->deserialize_and_append(pos); });
    return pos;
}

void FileColumn::deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) {
    reserve(chunk_size);
    for (size_t i = 0; i < chunk_size; ++i) {
        srcs[i].data = (char*)deserialize_and_append((uint8_t*)srcs[i].data);
    }
}

uint32_t FileColumn::max_one_element_serialize_size() const {
    uint32_t max_size = 0;
    for_each_field([&](const auto& column) { max_size += column->max_one_element_serialize_size(); });
    return max_size;
}

uint32_t FileColumn::serialize_size(size_t idx) const {
    uint32_t ser_size = 0;
    for_each_field([&](const auto& column) { ser_size += column->serialize_size(idx); });
    return ser_size;
}

MutableColumnPtr FileColumn::clone_empty() const {
    return create();
}

size_t FileColumn::filter_range(const Filter& filter, size_t from, size_t to) {
    size_t result_offset = _uri->filter_range(filter, from, to);
    for (Column* column : field_columns()) {
        if (column == _uri.get()) {
            continue;
        }
        size_t tmp_offset = column->filter_range(filter, from, to);
        DCHECK_EQ(result_offset, tmp_offset);
    }
    return result_offset;
}

int FileColumn::compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const {
    const auto& rhs_file = down_cast<const FileColumn&>(rhs);
    auto lhs_fields = field_columns();
    auto rhs_fields = rhs_file.field_columns();
    for (size_t i = 0; i < NUM_FIELDS; ++i) {
        int cmp = lhs_fields[i]->compare_at(left, right, *rhs_fields[i], nan_direction_hint);
        if (cmp != 0) {
            return cmp;
        }
    }
    return 0;
}

int FileColumn::equals(size_t left, const Column& rhs, size_t right, bool safe_eq) const {
    const auto& rhs_file = down_cast<const FileColumn&>(rhs);
    auto lhs_fields = field_columns();
    auto rhs_fields = rhs_file.field_columns();
    int ret = EQUALS_TRUE;
    for (size_t i = 0; i < NUM_FIELDS; ++i) {
        int tmp = lhs_fields[i]->equals(left, *rhs_fields[i], right, safe_eq);
        if (tmp == EQUALS_FALSE) {
            return EQUALS_FALSE;
        } else if (tmp == EQUALS_NULL) {
            ret = EQUALS_NULL;
        }
    }
    return safe_eq ? EQUALS_TRUE : ret;
}

int64_t FileColumn::xor_checksum(uint32_t from, uint32_t to) const {
    int64_t checksum = 0;
    for_each_field([&](const auto& column) { checksum ^= column->xor_checksum(from, to); });
    return checksum;
}

void FileColumn::put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx, bool is_binary_protocol) const {
    DCHECK_LT(idx, size());
    auto columns = field_columns();
    buf->begin_push_bracket();
    for (size_t i = 0; i < NUM_FIELDS; ++i) {
        buf->push_string(kFieldNames[i], strlen(kFieldNames[i]));
        buf->separator(':');
        const NullableColumn* nullable = as_nullable(columns[i]);
        if (nullable->is_null(idx)) {
            buf->push_null();
        } else {
            const Column* data = nullable->data_column_raw_ptr();
            switch (static_cast<FileField>(i)) {
            case OFFSET:
            case SIZE:
                buf->push_bigint(down_cast<const Int64Column*>(data)->get_data()[idx]);
                break;
            case INLINE: {
                // Payload bytes are always rendered as hex, independent of binary_encoding_format.
                Slice bytes = down_cast<const BinaryColumn*>(data)->get_slice(idx);
                std::string hex = strings::b2a_hex(bytes.data, static_cast<int>(bytes.size));
                buf->push_string(hex.data(), hex.size());
                break;
            }
            default:
                buf->push_string(down_cast<const BinaryColumn*>(data)->get_slice(idx));
                break;
            }
        }
        if (i + 1 < NUM_FIELDS) {
            buf->separator(',');
        }
    }
    buf->finish_push_bracket();
}

std::string FileColumn::debug_item(size_t idx) const {
    DCHECK_LT(idx, size());
    std::stringstream ss;
    ss << '{';
    auto columns = field_columns();
    for (size_t i = 0; i < NUM_FIELDS; ++i) {
        ss << kFieldNames[i] << ':' << columns[i]->debug_item(idx);
        if (i + 1 < NUM_FIELDS) {
            ss << ',';
        }
    }
    ss << '}';
    return ss.str();
}

std::string FileColumn::debug_string() const {
    std::stringstream ss;
    for (size_t i = 0; i < size(); ++i) {
        if (i > 0) {
            ss << ", ";
        }
        ss << debug_item(i);
    }
    return ss.str();
}

std::string FileColumn::get_name() const {
    return "file";
}

Datum FileColumn::get(size_t idx) const {
    DCHECK_LT(idx, size());
    DatumStruct res(NUM_FIELDS);
    auto columns = field_columns();
    for (size_t i = 0; i < NUM_FIELDS; ++i) {
        res[i] = columns[i]->get(idx);
    }
    return {res};
}

size_t FileColumn::memory_usage() const {
    size_t usage = 0;
    for_each_field([&](const auto& column) { usage += column->memory_usage(); });
    return usage;
}

size_t FileColumn::container_memory_usage() const {
    size_t usage = 0;
    for_each_field([&](const auto& column) { usage += column->container_memory_usage(); });
    return usage;
}

size_t FileColumn::reference_memory_usage(size_t from, size_t size) const {
    DCHECK_LE(from + size, this->size()) << "Range error";
    size_t usage = 0;
    for_each_field([&](const auto& column) { usage += column->reference_memory_usage(from, size); });
    return usage;
}

void FileColumn::swap_column(Column& rhs) {
    auto& rhs_file = down_cast<FileColumn&>(rhs);
    auto lhs_fields = field_columns();
    auto rhs_fields = rhs_file.field_columns();
    for (size_t i = 0; i < NUM_FIELDS; ++i) {
        lhs_fields[i]->swap_column(*rhs_fields[i]);
    }
}

void FileColumn::reset_column() {
    Column::reset_column();
    for_each_field([](auto& column) { column->reset_column(); });
}

Status FileColumn::capacity_limit_reached() const {
    Status status;
    for_each_field([&](const auto& column) {
        if (status.ok()) {
            status = column->capacity_limit_reached();
        }
    });
    return status;
}

void FileColumn::check_or_die() const {
    const size_t num_rows = _uri->size();
    for_each_field([&](const auto& column) {
        DCHECK(column->is_nullable()) << "FileColumn sub-columns must be nullable";
        DCHECK_EQ(num_rows, column->size());
        column->check_or_die();
    });
}

void FileColumn::mutate_each_subcolumn() {
    for_each_field([](auto& column) { column = (std::move(*column)).mutate(); });
}

} // namespace starrocks
