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
    return _fields.at(0)->size();
}

size_t StructColumn::capacity() const {
    return _fields.at(0)->capacity();
}

size_t StructColumn::type_size() const {
    return sizeof(DatumStruct);
}

size_t StructColumn::byte_size() const {
    size_t total_size = 0;
    for (const auto& column : _fields) {
        total_size += column->byte_size();
    }
    // We need plus _field_names size
    total_size += _field_names->byte_size();
    return total_size;
}

size_t StructColumn::byte_size(size_t idx) const {
    size_t total_size = 0;
    for (const auto& column : _fields) {
        total_size += column->byte_size(idx);
    }
    // We need plus _field_names size
    total_size += _field_names->byte_size();
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
    for (size_t col = 0; col < datum_struct.size(); col++) {
        _fields.at(col)->append_datum(datum_struct.at(col));
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
        const Column& source_column = *struct_column.fields().at(i);
        _fields.at(i)->append(source_column, offset, count);
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
        RETURN_IF_ERROR(_fields.at(i)->update_rows(*src_column._fields.at(i), indexes));
    }
    return Status::OK();
}

void StructColumn::append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    DCHECK(src.is_struct());
    const auto& src_column = down_cast<const StructColumn&>(src);
    DCHECK_EQ(_fields.size(), src_column._fields.size());
    for (size_t i = 0; i < _fields.size(); i++) {
        _fields.at(i)->append_selective(*src_column._fields.at(i), indexes, from, size);
    }
}

void StructColumn::append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) {
    DCHECK(src.is_struct());
    const auto& src_column = down_cast<const StructColumn&>(src);
    DCHECK_EQ(_fields.size(), src_column._fields.size());
    for (size_t i = 0; i < _fields.size(); i++) {
        _fields.at(i)->append_value_multiple_times(*src_column._fields.at(i), index, size);
    }
}

bool StructColumn::append_nulls(size_t count) {
    for (const ColumnPtr& field : _fields) {
        if (!field->append_nulls(count)) {
            LOG(WARNING) << "StructColumn append_nulls may not be atomic!";
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
            _fields.at(i)->append_datum(struct_datum.at(i));
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
    // TODO(SmithCruise) Not tested.
    uint32_t ser_size = 0;
    for (ColumnPtr& column : _fields) {
        ser_size += column->serialize(idx, pos);
    }
    for (size_t i = 0; i < _field_names->size(); i++) {
        ser_size += _field_names->serialize(i, pos);
    }
    return ser_size;
}

uint32_t StructColumn::serialize_default(uint8_t* pos) {
    // TODO(SmithCruise) Not tested.
    uint32_t ser_size = 0;
    for (ColumnPtr& column : _fields) {
        ser_size += column->serialize_default(pos);
    }
    for (size_t i = 0; i < _field_names->size(); i++) {
        ser_size += _field_names->serialize(i, pos);
    }
    return ser_size;
}

void StructColumn::serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                                   uint32_t max_one_row_size) {
    // TODO(SmithCruise) Not tested.
    for (size_t i = 0; i < chunk_size; ++i) {
        slice_sizes[i] += serialize(i, dst + i * max_one_row_size + slice_sizes[i]);
    }
}

const uint8_t* StructColumn::deserialize_and_append(const uint8_t* pos) {
    DCHECK(false) << "Dont support it";
    return nullptr;
}

void StructColumn::deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) {
    DCHECK(false) << "Dont support it";
}

uint32_t StructColumn::serialize_size(size_t idx) const {
    // TODO(SmithCruise) Not tested.
    uint32_t ser_size = 0;
    for (const ColumnPtr& column : _fields) {
        ser_size += column->serialize_size(idx);
    }
    for (size_t i = 0; i < _field_names->size(); i++) {
        ser_size += _field_names->serialize_size(i);
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
    // TODO(SmithCruise) Not tested.
    size_t result_offset = _fields.at(0)->filter_range(filter, from, to);
    for (size_t i = 1; i < _fields.size(); i++) {
        DCHECK_EQ(result_offset, _fields.at(i)->filter_range(filter, from, to));
    }
    return result_offset;
}

int StructColumn::compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const {
    DCHECK(false) << "Dont support it";
    return 0;
}

void StructColumn::fnv_hash(uint32_t* seed, uint32_t from, uint32_t to) const {
    // TODO(SmithCruise) Not tested.
    for(const ColumnPtr& column : _fields) {
        column->fnv_hash(seed, from, to);
    }
}

void StructColumn::crc32_hash(uint32_t* seed, uint32_t from, uint32_t to) const {
    // TODO(SmithCruise) Not tested.
    for(const ColumnPtr& column : _fields) {
        column->crc32_hash(seed, from, to);
    }
}

int64_t StructColumn::xor_checksum(uint32_t from, uint32_t to) const {
    // TODO(SmithCruise) Not tested.
    int64_t xor_checksum = 0;
    for(const ColumnPtr& column : _fields) {
        column->xor_checksum(from, to);
    }
    return xor_checksum;
}

void StructColumn::put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx) const {
    DCHECK_LT(idx, size());
    buf->begin_push_bracket();
    for (size_t i = 0; i < _fields.size(); ++i) {
        const auto& field = _fields.at(i);
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
        const auto& field = _fields.at(i);
        ss << _field_names->get_slice(i).to_string();
        ss << ':';
        ss << field->debug_item(idx);
        if (i < _fields.size() - 1) {
            // Add struct field separator, last field don't need ','.
            ss << ',';
        }
    }
    ss << '}';

    return ss.str();
}

std::string StructColumn::debug_string() const {
    std::stringstream ss;
    for (size_t i = 0; i < size(); ++i) {
        ss << debug_item(i);
        if (i < _fields.size() - 1) {
            // Add struct field separator, last field don't need ','.
            ss << ',';
        }
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
        res.at(i) = _fields.at(i)->get(idx);
    }
    return Datum(res);
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
        _fields.at(i)->swap_column(*struct_column.fields_column().at(i));
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