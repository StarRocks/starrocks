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

#include <optional>
#include <sstream>

#include "column/binary_column.h"
#include "column/fixed_length_column.h"
#include "column/mysql_row_buffer.h"
#include "column/nullable_column.h"
#include "types/datum.h"

namespace starrocks {

namespace {

Column::MutablePtr make_nullable_binary() {
    return NullableColumn::create(BinaryColumn::create(), NullColumn::create());
}

Column::MutablePtr make_nullable_bigint() {
    return NullableColumn::create(Int64Column::create(), NullColumn::create());
}

const NullableColumn* as_nullable(const Column* column) {
    return down_cast<const NullableColumn*>(column);
}

} // namespace

FileColumn::FileColumn()
        : _fields{make_nullable_binary(), make_nullable_bigint(), make_nullable_bigint(),
                  make_nullable_binary(), make_nullable_binary(), make_nullable_binary()} {}

FileColumn::FileColumn(const size_t size) : FileColumn() {
    if (size > 0) {
        // Every sub-column is nullable, so append_default() appends NULLs.
        for (auto& column : _fields) {
            column->append_default(size);
        }
    }
}

std::array<ColumnPtr, FileColumn::NUM_FIELDS> FileColumn::fields() const {
    return {_fields[URI], _fields[OFFSET], _fields[SIZE], _fields[CONTENT_TYPE], _fields[CHECKSUM], _fields[INLINE]};
}

size_t FileColumn::size() const {
    return _fields[0]->size();
}

size_t FileColumn::capacity() const {
    return _fields[0]->capacity();
}

size_t FileColumn::type_size() const {
    return sizeof(DatumStruct);
}

size_t FileColumn::byte_size() const {
    size_t total = 0;
    for (const auto& column : _fields) {
        total += column->byte_size();
    }
    return total;
}

size_t FileColumn::byte_size(const size_t idx) const {
    size_t total = 0;
    for (const auto& column : _fields) {
        total += column->byte_size(idx);
    }
    return total;
}

size_t FileColumn::byte_size(const size_t from, const size_t size) const {
    DCHECK_LE(from + size, this->size()) << "Range error";
    size_t total = 0;
    for (const auto& column : _fields) {
        total += column->byte_size(from, size);
    }
    return total;
}

void FileColumn::reserve(const size_t n) {
    for (auto& column : _fields) {
        column->reserve(n);
    }
}

void FileColumn::resize(const size_t n) {
    for (auto& column : _fields) {
        column->resize(n);
    }
}

StatusOr<MutableColumnPtr> FileColumn::upgrade_if_overflow() {
    for (auto& column : _fields) {
        auto ret = upgrade_helper_func(column->as_mutable_raw_ptr());
        if (!ret.ok()) {
            return ret;
        }
        if (ret.value() != nullptr) {
            column = std::move(ret.value());
        }
    }
    return nullptr;
}

StatusOr<MutableColumnPtr> FileColumn::downgrade() {
    for (auto& column : _fields) {
        auto ret = downgrade_helper_func(column->as_mutable_raw_ptr());
        if (!ret.ok()) {
            return ret;
        }
        if (ret.value() != nullptr) {
            column = std::move(ret.value());
        }
    }
    return nullptr;
}

bool FileColumn::has_large_column() const {
    for (const auto& column : _fields) {
        if (column->has_large_column()) {
            return true;
        }
    }
    return false;
}

void FileColumn::assign(const size_t n, const size_t idx) {
    DCHECK_LE(idx, size()) << "Range error when assign FileColumn";
    const auto desc = this->clone_empty();
    desc->append_value_multiple_times(*this, idx, n);
    swap_column(*desc);
    desc->reset_column();
}

void FileColumn::append_datum(const Datum& datum) {
    const auto& fields = datum.get<DatumStruct>();
    DCHECK_EQ(NUM_FIELDS, fields.size());
    for (size_t i = 0; i < NUM_FIELDS; ++i) {
        _fields[i]->append_datum(fields[i]);
    }
}

void FileColumn::remove_first_n_values(size_t count) {
    for (auto& column : _fields) {
        column->remove_first_n_values(count);
    }
}

void FileColumn::append(const Column& src, size_t offset, size_t count) {
    DCHECK(src.is_file());
    const auto& src_column = down_cast<const FileColumn&>(src);
    for (size_t i = 0; i < NUM_FIELDS; ++i) {
        _fields[i]->append(*src_column._fields[i], offset, count);
    }
}

void FileColumn::fill_default(const Filter& filter) {
    for (auto& column : _fields) {
        column->fill_default(filter);
    }
}

void FileColumn::update_rows(const Column& src, const uint32_t* indexes) {
    DCHECK(src.is_file());
    const auto& src_column = down_cast<const FileColumn&>(src);
    for (size_t i = 0; i < NUM_FIELDS; ++i) {
        _fields[i]->update_rows(*src_column._fields[i], indexes);
    }
}

void FileColumn::append_selective(const Column& src, const uint32_t* indexes, const uint32_t from,
                                  const uint32_t size) {
    DCHECK(src.is_file());
    const auto& src_column = down_cast<const FileColumn&>(src);
    for (size_t i = 0; i < NUM_FIELDS; ++i) {
        _fields[i]->append_selective(*src_column._fields[i], indexes, from, size);
    }
}

void FileColumn::append_value_multiple_times(const Column& src, const uint32_t index, const uint32_t size) {
    DCHECK(src.is_file());
    const auto& src_column = down_cast<const FileColumn&>(src);
    for (size_t i = 0; i < NUM_FIELDS; ++i) {
        _fields[i]->append_value_multiple_times(*src_column._fields[i], index, size);
    }
}

bool FileColumn::append_nulls(size_t count) {
    bool ok = true;
    for (auto& column : _fields) {
        if (!column->append_nulls(count)) {
            DCHECK(false) << "FileColumn sub-column append_nulls failed, that should not happen";
            ok = false;
        }
    }
    return ok;
}

size_t FileColumn::append_numbers(const void* buff, size_t length) {
    return -1;
}

void FileColumn::append_value_multiple_times(const void* value, size_t count) {
    const auto* datum = static_cast<const Datum*>(value);
    const auto& fields = datum->get_struct();
    DCHECK_EQ(NUM_FIELDS, fields.size());
    for (size_t c = 0; c < count; ++c) {
        for (size_t i = 0; i < NUM_FIELDS; ++i) {
            _fields[i]->append_datum(fields[i]);
        }
    }
}

void FileColumn::append_default() {
    for (auto& column : _fields) {
        column->append_default();
    }
}

void FileColumn::append_default(size_t count) {
    for (auto& column : _fields) {
        column->append_default(count);
    }
}

uint32_t FileColumn::serialize(size_t idx, uint8_t* pos) const {
    uint32_t ser_size = 0;
    for (const auto& column : _fields) {
        ser_size += column->serialize(idx, pos + ser_size);
    }
    return ser_size;
}

uint32_t FileColumn::serialize_default(uint8_t* pos) const {
    uint32_t ser_size = 0;
    for (const auto& column : _fields) {
        ser_size += column->serialize_default(pos + ser_size);
    }
    return ser_size;
}

void FileColumn::serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                                 uint32_t max_one_row_size) const {
    for (size_t i = 0; i < chunk_size; ++i) {
        slice_sizes[i] += serialize(i, dst + i * max_one_row_size + slice_sizes[i]);
    }
}

const uint8_t* FileColumn::deserialize_and_append(const uint8_t* pos) {
    DCHECK(false) << "Don't support file column deserialize and append";
    return pos;
}

void FileColumn::deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) {
    DCHECK(false) << "Don't support deserialize and append";
    throw std::runtime_error("FileColumn::deserialize_and_append_batch() is not supported");
}

uint32_t FileColumn::max_one_element_serialize_size() const {
    uint32_t max_size = 0;
    for (const auto& column : _fields) {
        max_size += column->max_one_element_serialize_size();
    }
    return max_size;
}

uint32_t FileColumn::serialize_size(const size_t idx) const {
    uint32_t ser_size = 0;
    for (const auto& column : _fields) {
        ser_size += column->serialize_size(idx);
    }
    return ser_size;
}

MutableColumnPtr FileColumn::clone_empty() const {
    return create();
}

MutableColumnPtr FileColumn::clone() const {
    auto p = clone_empty();
    p->append(*this, 0, size());
    return p;
}

size_t FileColumn::filter_range(const Filter& filter, size_t from, size_t to) {
    const size_t result_offset = _fields[0]->filter_range(filter, from, to);
    for (size_t i = 1; i < NUM_FIELDS; ++i) {
        size_t tmp_offset = _fields[i]->filter_range(filter, from, to);
        DCHECK_EQ(result_offset, tmp_offset);
    }
    return result_offset;
}

int FileColumn::compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const {
    const auto& rhs_file = down_cast<const FileColumn&>(rhs);
    for (size_t i = 0; i < NUM_FIELDS; ++i) {
        int cmp = _fields[i]->compare_at(left, right, *rhs_file._fields[i], nan_direction_hint);
        if (cmp != 0) {
            return cmp;
        }
    }
    return 0;
}

int FileColumn::equals(size_t left, const Column& rhs, const size_t right, const bool safe_eq) const {
    const auto& rhs_file = down_cast<const FileColumn&>(rhs);
    int ret = EQUALS_TRUE;
    for (size_t i = 0; i < NUM_FIELDS; ++i) {
        const int tmp = _fields[i]->equals(left, *rhs_file._fields[i], right, safe_eq);
        if (tmp == EQUALS_FALSE) {
            return EQUALS_FALSE;
        }
        if (tmp == EQUALS_NULL) {
            ret = EQUALS_NULL;
        }
    }
    return safe_eq ? EQUALS_TRUE : ret;
}

int64_t FileColumn::xor_checksum(uint32_t from, uint32_t to) const {
    int64_t checksum = 0;
    for (const auto& column : _fields) {
        checksum ^= column->xor_checksum(from, to);
    }
    return checksum;
}

void FileColumn::put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx, bool is_binary_protocol) const {
    DCHECK_LT(idx, size());
    buf->begin_push_bracket();
    for (size_t i = 0; i < NUM_FIELDS; ++i) {
        buf->push_string(kFieldNames[i], strlen(kFieldNames[i]));
        buf->separator(':');
        const NullableColumn* nullable = as_nullable(_fields[i].get());
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
                // Payload bytes follow the regular VARBINARY rendering rules: inside the bracket they
                // count as nested binary, so binary_encoding_format / binary_encoding_level apply.
                Slice bytes = down_cast<const BinaryColumn*>(data)->get_slice(idx);
                buf->push_binary(bytes.data, bytes.size);
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
    for (size_t i = 0; i < NUM_FIELDS; ++i) {
        ss << kFieldNames[i] << ':' << _fields[i]->debug_item(idx);
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
    for (size_t i = 0; i < NUM_FIELDS; ++i) {
        res[i] = _fields[i]->get(idx);
    }
    return {res};
}

size_t FileColumn::memory_usage() const {
    size_t usage = 0;
    for (const auto& column : _fields) {
        usage += column->memory_usage();
    }
    return usage;
}

size_t FileColumn::container_memory_usage() const {
    size_t usage = 0;
    for (const auto& column : _fields) {
        usage += column->container_memory_usage();
    }
    return usage;
}

size_t FileColumn::reference_memory_usage(size_t from, size_t size) const {
    DCHECK_LE(from + size, this->size()) << "Range error";
    size_t usage = 0;
    for (const auto& column : _fields) {
        usage += column->reference_memory_usage(from, size);
    }
    return usage;
}

void FileColumn::swap_column(Column& rhs) {
    auto& rhs_file = down_cast<FileColumn&>(rhs);
    for (size_t i = 0; i < NUM_FIELDS; ++i) {
        _fields[i]->swap_column(*rhs_file._fields[i]);
    }
}

void FileColumn::reset_column() {
    Column::reset_column();
    for (auto& column : _fields) {
        column->reset_column();
    }
}

Status FileColumn::capacity_limit_reached() const {
    for (const auto& column : _fields) {
        RETURN_IF_ERROR(column->capacity_limit_reached());
    }
    return Status::OK();
}

void FileColumn::check_or_die() const {
    const size_t num_rows = _fields[0]->size();
    for (const auto& column : _fields) {
        DCHECK(column->is_nullable()) << "FileColumn sub-columns must be nullable";
        DCHECK_EQ(num_rows, column->size());
        column->check_or_die();
    }
}

void FileColumn::mutate_each_subcolumn() {
    for (auto& column : _fields) {
        column = (std::move(*column)).mutate();
    }
}

Datum FileDatumBuilder::make(const std::optional<Slice>& uri, const std::optional<int64_t>& offset,
                             const std::optional<int64_t>& size, const std::optional<Slice>& content_type,
                             const std::optional<Slice>& checksum, const std::optional<Slice>& inline_bytes) {
    DatumStruct fields(FileColumn::NUM_FIELDS);
    if (uri) {
        fields[FileColumn::URI] = Datum(*uri);
    }
    if (offset) {
        fields[FileColumn::OFFSET] = Datum(*offset);
    }
    if (size) {
        fields[FileColumn::SIZE] = Datum(*size);
    }
    if (content_type) {
        fields[FileColumn::CONTENT_TYPE] = Datum(*content_type);
    }
    if (checksum) {
        fields[FileColumn::CHECKSUM] = Datum(*checksum);
    }
    if (inline_bytes) {
        fields[FileColumn::INLINE] = Datum(*inline_bytes);
    }
    return Datum(fields);
}

} // namespace starrocks
