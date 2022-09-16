// This file is licensed under the Elastic License 2.0. Copyright 2022-present, StarRocks Inc.

#include "column/struct_column.h"

#include "column/column_helper.h"
#include "util/mysql_row_buffer.h"

namespace starrocks::vectorized {

bool StructColumn::is_struct() const {
    return true;
}

const uint8_t* StructColumn::raw_data() const {
    // TODO(SmithCruise)
    DCHECK(false) << "Don't support struct column raw_data";
    return 0;
}

uint8_t* StructColumn::mutable_raw_data() {
    // TODO(SmithCruise)
    DCHECK(false) << "Don't support struct column raw_data";
    return 0;
}
size_t StructColumn::size() const {
    return _fields[0]->size();
}

size_t StructColumn::capacity() const {
    return _fields[0]->capacity();
}

size_t StructColumn::type_size() const {
    // TODO(SmithCruise)
    DCHECK(false) << "Don't support struct column datum";
    return 0;
}

size_t StructColumn::byte_size() const {
    size_t total_size = 0;
    for (const auto& column : _fields) {
        total_size += column->byte_size();
    }
    total_size += _field_names->byte_size();
    return total_size;
}

size_t StructColumn::byte_size(size_t idx) const {
    size_t total_size = 0;
    for (const auto& column : _fields) {
        total_size += column->byte_size(idx);
    }
    total_size += _field_names->byte_size();
    return total_size;
}

void StructColumn::reserve(size_t n) {
    for (const auto& column : _fields) {
        column->reserve(n);
    }
    // Don't need to reserve _field_names, because the number of struct field is fixed.
}

void StructColumn::resize(size_t n) {
    for (const auto& column : _fields) {
        column->resize(n);
    }
    // Don't need to resize _field_names, because the number of struct field is fixed.
}

StatusOr<ColumnPtr> StructColumn::upgrade_if_overflow() {
    // TODO(SmithCruise)
    return Status::NotSupported("StructColumn don't support upgrade_if_overflow");
}

StatusOr<ColumnPtr> StructColumn::downgrade() {
    // TODO(SmithCruise)
    return Status::NotSupported("StructColumn don't support downgrade");
}

bool StructColumn::has_large_column() const {
    bool res = true;
    for (const auto& column : _fields) {
        res = res && column->has_large_column();
    }
    return res;
}

void StructColumn::assign(size_t n, size_t idx) {
    DCHECK(false) << "Dont support it";
}

void StructColumn::append_datum(const Datum& datum) {
    DCHECK(false) << "Dont support datum";
}

void StructColumn::remove_first_n_values(size_t count) {
    for (const auto& column : _fields) {
        column->remove_first_n_values(count);
    }
}

void StructColumn::append(const Column& src, size_t offset, size_t count) {
    const auto& struct_column = down_cast<const StructColumn&>(src);
    for (size_t i = 0; i < struct_column.size(); i++) {
        const Column& source_column = *struct_column._fields[i];
        _fields[i]->append(source_column, offset, count);
    }
}

void StructColumn::fill_default(const Filter& filter) {
    DCHECK(false) << "Dont support it";
}

Status StructColumn::update_rows(const Column& src, const uint32_t* indexes) {
    return Status::NotSupported("Dont support it.");
}

void StructColumn::append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    DCHECK(false) << "Dont support it";
}

void StructColumn::append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) {
    DCHECK(false) << "Dont support it";
}

bool StructColumn::append_nulls(size_t count) {
    DCHECK(false) << "Dont support it";
    return false;
}

bool StructColumn::append_strings(const Buffer<Slice>& strs) {
    DCHECK(false) << "Dont support it";
    return false;
}

size_t StructColumn::append_numbers(const void* buff, size_t length) {
    DCHECK(false) << "Dont support it";
    return 0;
}

void StructColumn::append_value_multiple_times(const void* value, size_t count) {
    DCHECK(false) << "Dont support it";
}

void StructColumn::append_default() {
    DCHECK(false) << "Dont support it";
}

void StructColumn::append_default(size_t count) {
    DCHECK(false) << "Dont support it";
}

uint32_t StructColumn::serialize(size_t idx, uint8_t* pos) {
    DCHECK(false) << "Dont support it";
    return 0;
}

uint32_t StructColumn::serialize_default(uint8_t* pos) {
    DCHECK(false) << "Dont support it";
    return 0;
}

void StructColumn::serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                                   uint32_t max_one_row_size) {
    DCHECK(false) << "Dont support it";
}

const uint8_t* StructColumn::deserialize_and_append(const uint8_t* pos) {
    DCHECK(false) << "Dont support it";
    return nullptr;
}

void StructColumn::deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) {
    DCHECK(false) << "Dont support it";
}

uint32_t StructColumn::serialize_size(size_t idx) const {
    DCHECK(false) << "Dont support it";
    return 0;
}

MutableColumnPtr StructColumn::clone_empty() const {
    Columns fields;
    for (const auto& field : _fields) {
        fields.emplace_back(field->clone_empty());
    }
    BinaryColumn::Ptr another_field_names = BinaryColumn::create();
    another_field_names->reserve(_field_names->size());
    for (size_t i = 0; i < _field_names->size(); i++) {
        another_field_names->append(_field_names->get_slice(i));
    }
    return create_mutable(fields, another_field_names);
}

size_t StructColumn::filter_range(const Filter& filter, size_t from, size_t to) {
    DCHECK(false) << "Dont support it";
    return 0;
}

int StructColumn::compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const {
    DCHECK(false) << "Dont support it";
    return 0;
}

void StructColumn::fnv_hash(uint32_t* seed, uint32_t from, uint32_t to) const {
    DCHECK(false) << "Dont support it";
}

void StructColumn::crc32_hash(uint32_t* seed, uint32_t from, uint32_t to) const {
    DCHECK(false) << "Dont support it";
}

int64_t StructColumn::xor_checksum(uint32_t from, uint32_t to) const {
    DCHECK(false) << "Dont support it";
    return 0;
}

void StructColumn::put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx) const {
    DCHECK_LT(idx, size());

    buf->begin_push_struct();
    for (size_t i = 0; i < _fields.size(); ++i) {
        const auto& field = _fields[i];
        buf->push_string(_field_names->get_slice(i).to_string());
        buf->separator(':');
        field->put_mysql_row_buffer(buf, idx);
        if (i < _fields.size() - 1) {
            // Add struct field separator, last field don't need ','.
            buf->separator(',');
        }
    }
    buf->finish_push_struct();
}

std::string StructColumn::get_name() const {
    return "struct";
}

Datum StructColumn::get(size_t n) const {
    DCHECK(false) << "Dont support it";
    return Datum();
}

size_t StructColumn::container_memory_usage() const {
    size_t memory_usage = 0;
    for (const auto& column : _fields) {
        memory_usage += column->container_memory_usage();
    }
    memory_usage += _field_names->container_memory_usage();
    return memory_usage;
}

void StructColumn::swap_column(Column& rhs) {
    StructColumn& struct_column = down_cast<StructColumn&>(rhs);
    for (size_t i = 0; i < _fields.size(); i++) {
        _fields[i]->swap_column(*struct_column.fields_column()[i]);
    }
    _field_names->swap_column(*struct_column.field_names_column());
}

bool StructColumn::capacity_limit_reached(std::string* msg) const {
    bool res = false;
    for (const auto& column : _fields) {
        res = res || column->capacity_limit_reached(msg);
    }
    return res;
}

void StructColumn::check_or_die() const {
    // Struct must have at least one field.
    DCHECK(_fields.size() > 0);
    DCHECK(_field_names->size() > 0);

    // fields and field_names must have the same size.
    DCHECK(_fields.size() == _field_names->size());

    for (const auto& column : _fields) {
        column->check_or_die();
    }
    _field_names->check_or_die();
}

} // namespace starrocks::vectorized