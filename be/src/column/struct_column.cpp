// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
    return nullptr;
}

uint8_t* StructColumn::mutable_raw_data() {
    // TODO(SmithCruise)
    DCHECK(false) << "Don't support struct column raw_data";
    return nullptr;
}
size_t StructColumn::size() const {
    return _fields[0]->size();
}

size_t StructColumn::capacity() const {
    return _fields[0]->capacity();
}

size_t StructColumn::type_size() const {
    return sizeof(DatumStruct);
}

size_t StructColumn::byte_size() const {
    size_t total_size = 0;
    for (const auto& column : _fields) {
        total_size += column->byte_size();
    }
    return total_size;
}

size_t StructColumn::byte_size(size_t idx) const {
    size_t total_size = 0;
    for (const auto& column : _fields) {
        total_size += column->byte_size(idx);
    }
    return total_size;
}

void StructColumn::reserve(size_t n) {
    for (const auto& column : _fields) {
        column->reserve(n);
    }
    // Don't need to reserve _field_names, because the number of struct subfield is fixed.
}

void StructColumn::resize(size_t n) {
    for (const auto& column : _fields) {
        column->resize(n);
    }
    // Don't need to resize _field_names, because the number of struct subfield is fixed.
}

StatusOr<ColumnPtr> StructColumn::upgrade_if_overflow() {
    for (ColumnPtr& column : _fields) {
        StatusOr<ColumnPtr> status = upgrade_helper_func(&column);
        if (!status.ok()) {
            return status;
        }
    }
    return nullptr;
}

StatusOr<ColumnPtr> StructColumn::downgrade() {
    for (ColumnPtr& column : _fields) {
        StatusOr<ColumnPtr> status = downgrade_helper_func(&column);
        if (!status.ok()) {
            return status;
        }
    }
    return nullptr;
}

bool StructColumn::has_large_column() const {
    bool res = false;
    for (const auto& column : _fields) {
        res = res || column->has_large_column();
    }
    return res;
}

void StructColumn::assign(size_t n, size_t idx) {
    DCHECK_LE(idx, size()) << "Range error when assign StructColumn";
    auto desc = this->clone_empty();
    auto datum = get(idx);
    desc->append_value_multiple_times(&datum, n);
    swap_column(*desc);
    desc->reset_column();
}

void StructColumn::append_datum(const Datum& datum) {
    const DatumStruct& datum_struct = datum.get<DatumStruct>();
    DCHECK_EQ(_fields.size(), datum_struct.size());
    for (size_t col = 0; col < datum_struct.size(); col++) {
        _fields[col]->append_datum(datum_struct[col]);
    }
}

void StructColumn::remove_first_n_values(size_t count) {
    for (const auto& column : _fields) {
        column->remove_first_n_values(count);
    }
}

void StructColumn::append(const Column& src, size_t offset, size_t count) {
    const auto& struct_column = down_cast<const StructColumn&>(src);
    DCHECK_EQ(_fields.size(), struct_column.fields().size());
    for (size_t i = 0; i < _fields.size(); i++) {
        const Column& source_column = *struct_column.fields()[i];
        _fields[i]->append(source_column, offset, count);
    }
}

void StructColumn::fill_default(const Filter& filter) {
    for (const ColumnPtr& field : _fields) {
        field->fill_default(filter);
    }
}

Status StructColumn::update_rows(const Column& src, const uint32_t* indexes) {
    DCHECK(src.is_struct());
    const StructColumn& src_column = down_cast<const StructColumn&>(src);
    DCHECK_EQ(_fields.size(), src_column._fields.size());
    for (size_t i = 0; i < _fields.size(); i++) {
        RETURN_IF_ERROR(_fields[i]->update_rows(*src_column._fields[i], indexes));
    }
    return Status::OK();
}

void StructColumn::append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    DCHECK(src.is_struct());
    const auto& src_column = down_cast<const StructColumn&>(src);
    DCHECK_EQ(_fields.size(), src_column._fields.size());
    for (size_t i = 0; i < _fields.size(); i++) {
        _fields[i]->append_selective(*src_column._fields[i], indexes, from, size);
    }
}

void StructColumn::append_value_multiple_times(const Column& src, uint32_t index, uint32_t size, bool deep_copy) {
    DCHECK(src.is_struct());
    const auto& src_column = down_cast<const StructColumn&>(src);
    DCHECK_EQ(_fields.size(), src_column._fields.size());
    for (size_t i = 0; i < _fields.size(); i++) {
        _fields[i]->append_value_multiple_times(*src_column._fields[i], index, size, deep_copy);
    }
}

bool StructColumn::append_nulls(size_t count) {
    // check subfield column is nullable column first
    for (const ColumnPtr& field : _fields) {
        if (!field->is_nullable()) {
            return false;
        }
    }
    for (const ColumnPtr& field : _fields) {
        if (!field->append_nulls(count)) {
            DCHECK(false) << "StructColumn subfield append_nulls failed, that should not happened!";
            return false;
        }
    }
    return true;
}

bool StructColumn::append_strings(const Buffer<Slice>& strs) {
    return false;
}

size_t StructColumn::append_numbers(const void* buff, size_t length) {
    return -1;
}

void StructColumn::append_value_multiple_times(const void* value, size_t count) {
    const Datum* datum = reinterpret_cast<const Datum*>(value);
    const auto& struct_datum = datum->get_struct();

    DCHECK_EQ(_fields.size(), struct_datum.size());
    for (size_t c = 0; c < count; ++c) {
        for (size_t i = 0; i < struct_datum.size(); ++i) {
            _fields[i]->append_datum(struct_datum[i]);
        }
    }
}

void StructColumn::append_default() {
    for (ColumnPtr& column : _fields) {
        column->append_default();
    }
}

void StructColumn::append_default(size_t count) {
    for (ColumnPtr& column : _fields) {
        column->append_default(count);
    }
}

uint32_t StructColumn::serialize(size_t idx, uint8_t* pos) {
    uint32_t ser_size = 0;
    for (ColumnPtr& column : _fields) {
        ser_size += column->serialize(idx, pos + ser_size);
    }
    return ser_size;
}

uint32_t StructColumn::serialize_default(uint8_t* pos) {
    uint32_t ser_size = 0;
    for (ColumnPtr& column : _fields) {
        ser_size += column->serialize_default(pos + ser_size);
    }
    return ser_size;
}

void StructColumn::serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                                   uint32_t max_one_row_size) {
    for (size_t i = 0; i < chunk_size; ++i) {
        slice_sizes[i] += serialize(i, dst + i * max_one_row_size + slice_sizes[i]);
    }
}

const uint8_t* StructColumn::deserialize_and_append(const uint8_t* pos) {
    for (size_t i = 0; i < _fields.size(); i++) {
        pos = _fields[i]->deserialize_and_append(pos);
    }
    return pos;
}

void StructColumn::deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) {
    reserve(chunk_size);
    for (size_t i = 0; i < chunk_size; ++i) {
        srcs[i].data = (char*)deserialize_and_append((uint8_t*)srcs[i].data);
    }
}

uint32_t StructColumn::max_one_element_serialize_size() const {
    uint32_t max_size = 0;
    for (const auto& column : _fields) {
        max_size += column->max_one_element_serialize_size();
    }
    return max_size;
}

uint32_t StructColumn::serialize_size(size_t idx) const {
    uint32_t ser_size = 0;
    for (const ColumnPtr& column : _fields) {
        ser_size += column->serialize_size(idx);
    }
    return ser_size;
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
    size_t result_offset = _fields[0]->filter_range(filter, from, to);
    for (size_t i = 1; i < _fields.size(); i++) {
        size_t tmp_offset = _fields[i]->filter_range(filter, from, to);
        DCHECK_EQ(result_offset, tmp_offset);
    }
    // Don't need resize() anymore, because subfield's column will resize() by itself.
    return result_offset;
}

int StructColumn::compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const {
    DCHECK(false) << "Dont support it";
    return 0;
}

void StructColumn::fnv_hash(uint32_t* seed, uint32_t from, uint32_t to) const {
    // TODO(SmithCruise) Not tested.
    for (const ColumnPtr& column : _fields) {
        column->fnv_hash(seed, from, to);
    }
}

void StructColumn::crc32_hash(uint32_t* seed, uint32_t from, uint32_t to) const {
    // TODO(SmithCruise) Not tested.
    for (const ColumnPtr& column : _fields) {
        column->crc32_hash(seed, from, to);
    }
}

int64_t StructColumn::xor_checksum(uint32_t from, uint32_t to) const {
    // TODO(SmithCruise) Not tested.
    int64_t xor_checksum = 0;
    for (const ColumnPtr& column : _fields) {
        column->xor_checksum(from, to);
    }
    return xor_checksum;
}

void StructColumn::put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx) const {
    DCHECK_LT(idx, size());
    buf->begin_push_bracket();
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
    buf->finish_push_bracket();
}

std::string StructColumn::debug_item(uint32_t idx) const {
    DCHECK_LT(idx, size());
    std::stringstream ss;
    ss << '{';
    for (size_t i = 0; i < _fields.size(); i++) {
        const auto& field = _fields[i];
        ss << _field_names->get_slice(i).to_string();
        ss << ":";
        ss << field->debug_item(idx);
        if (i < _fields.size() - 1) {
            // Add struct field separator, last field don't need ','.
            ss << ",";
        }
    }
    ss << '}';

    return ss.str();
}

std::string StructColumn::debug_string() const {
    std::stringstream ss;
    for (size_t i = 0; i < size(); ++i) {
        if (i > 0) {
            ss << ", ";
        }
        ss << debug_item(i);
    }
    return ss.str();
}

std::string StructColumn::get_name() const {
    return "struct";
}

Datum StructColumn::get(size_t idx) const {
    DCHECK(idx < size());
    DatumStruct res(_fields.size());
    for (size_t i = 0; i < _fields.size(); i++) {
        res[i] = _fields[i]->get(idx);
    }
    return Datum(res);
}

size_t StructColumn::memory_usage() const {
    size_t memory_usage = 0;
    for (const auto& column : _fields) {
        memory_usage += column->memory_usage();
    }
    memory_usage += _field_names->memory_usage();
    return memory_usage;
}

size_t StructColumn::container_memory_usage() const {
    size_t memory_usage = 0;
    for (const auto& column : _fields) {
        memory_usage += column->container_memory_usage();
    }
    memory_usage += _field_names->container_memory_usage();
    return memory_usage;
}

size_t StructColumn::element_memory_usage(size_t from, size_t size) const {
    DCHECK_LE(from + size, this->size()) << "Range error";
    size_t memorg_usage = 0;
    for (const auto& column : _fields) {
        memorg_usage += column->element_memory_usage(from, size);
    }

    // Do not need to include _field_names's element_memory_usage, because it's BinaryColumn, always return 0.
    return memorg_usage;
}

void StructColumn::swap_column(Column& rhs) {
    StructColumn& struct_column = down_cast<StructColumn&>(rhs);
    for (size_t i = 0; i < _fields.size(); i++) {
        _fields[i]->swap_column(*struct_column.fields_column()[i]);
    }
    // _field_names dont need swap
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

void StructColumn::reset_column() {
    Column::reset_column();
    for (ColumnPtr& field : _fields) {
        field->reset_column();
    }
}

const Columns& StructColumn::fields() const {
    return _fields;
}

Columns& StructColumn::fields_column() {
    return _fields;
}

ColumnPtr StructColumn::field_column(const std::string& field_name) {
    for (size_t i = 0; i < _field_names->size(); i++) {
        if (field_name == _field_names->get_slice(i)) {
            return _fields[i];
        }
    }
    DCHECK(false) << "Struct subfield name: " << field_name << " not found!";
    return nullptr;
}

const BinaryColumn& StructColumn::field_names() const {
    return *_field_names;
}

BinaryColumn::Ptr& StructColumn::field_names_column() {
    return _field_names;
}

} // namespace starrocks::vectorized
