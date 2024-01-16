// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/fixed_length_column.h"
#include "common/logging.h"

namespace starrocks::vectorized {

using NullData = FixedLengthColumn<uint8_t>::Container;
using NullColumn = FixedLengthColumn<uint8_t>;
using NullColumnPtr = FixedLengthColumn<uint8_t>::Ptr;
using NullColumns = std::vector<NullColumnPtr>;

using NullValueType = NullColumn::ValueType;
static constexpr NullValueType DATUM_NULL = NullValueType(1);
static constexpr NullValueType DATUM_NOT_NULL = NullValueType(0);

class NullableColumn : public ColumnFactory<Column, NullableColumn> {
    friend class ColumnFactory<Column, NullableColumn>;

public:
    NullableColumn(MutableColumnPtr&& data_column, MutableColumnPtr&& null_column);
    NullableColumn(ColumnPtr data_column, NullColumnPtr null_column);

    NullableColumn(const NullableColumn& rhs)
            : _data_column(rhs._data_column->clone_shared()),
              _null_column(std::static_pointer_cast<NullColumn>(rhs._null_column->clone_shared())),
              _has_null(rhs._has_null) {}

    NullableColumn(NullableColumn&& rhs) noexcept
            : _data_column(std::move(rhs._data_column)),
              _null_column(std::move(rhs._null_column)),
              _has_null(rhs._has_null) {}

    NullableColumn& operator=(const NullableColumn& rhs) {
        NullableColumn tmp(rhs);
        this->swap_column(tmp);
        return *this;
    }

    NullableColumn& operator=(NullableColumn&& rhs) noexcept {
        NullableColumn tmp(std::move(rhs));
        this->swap_column(tmp);
        return *this;
    }

    NullableColumn() = default;

    ~NullableColumn() override = default;

    bool has_null() const override { return _has_null; }

    void set_has_null(bool has_null) { _has_null = _has_null | has_null; }

    // Update null element to default value
    void fill_null_with_default();

    void fill_default(const Filter& filter) override {}

    void update_has_null();

    bool is_nullable() const override { return true; }

    bool is_null(size_t index) const override {
        DCHECK_EQ(_null_column->size(), _data_column->size());
        return _has_null && immutable_null_column_data()[index];
    }

    const uint8_t* raw_data() const override { return _data_column->raw_data(); }

    uint8_t* mutable_raw_data() override { return reinterpret_cast<uint8_t*>(_data_column->mutable_raw_data()); }

    size_t size() const override {
        DCHECK_EQ(_data_column->size(), _null_column->size());
        return _data_column->size();
    }

    size_t capacity() const override { return _data_column->capacity(); }

    size_t type_size() const override { return _data_column->type_size() + _null_column->type_size(); }

    size_t byte_size() const override { return byte_size(0, size()); }

    size_t byte_size(size_t from, size_t size) const override {
        DCHECK_LE(from + size, this->size()) << "Range error";
        return _data_column->byte_size(from, size) + _null_column->Column::byte_size(from, size);
    }

    size_t byte_size(size_t idx) const override { return _data_column->byte_size(idx) + sizeof(bool); }

    void reserve(size_t n) override {
        _data_column->reserve(n);
        _null_column->reserve(n);
    }

    void resize(size_t n) override {
        _data_column->resize(n);
        _null_column->resize(n);
    }

    void resize_uninitialized(size_t n) override {
        _data_column->resize_uninitialized(n);
        _null_column->resize_uninitialized(n);
    }

    void assign(size_t n, size_t idx) override {
        _data_column->assign(n, idx);
        _null_column->assign(n, idx);
    }

    void remove_first_n_values(size_t count) override;

    void append_datum(const Datum& datum) override;

    void append(const Column& src, size_t offset, size_t count) override;

    void append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) override;

    void append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) override;

    bool append_nulls(size_t count) override;

    StatusOr<ColumnPtr> upgrade_if_overflow() override;

    StatusOr<ColumnPtr> downgrade() override;

    bool has_large_column() const override { return _data_column->has_large_column(); }

    bool append_strings(const Buffer<Slice>& strs) override;

    bool append_strings_overflow(const Buffer<Slice>& strs, size_t max_length) override;

    bool append_continuous_strings(const Buffer<Slice>& strs) override;

    bool append_continuous_fixed_length_strings(const char* data, size_t size, int fixed_length) override;

    size_t append_numbers(const void* buff, size_t length) override;

    void append_value_multiple_times(const void* value, size_t count) override;

    void append_default() override { append_nulls(1); }

    void append_default_not_null_value() {
        _data_column->append_default();
        _null_column->append(0);
    }

    void append_default(size_t count) override { append_nulls(count); }

    Status update_rows(const Column& src, const uint32_t* indexes) override;

    uint32_t max_one_element_serialize_size() const override {
        return sizeof(bool) + _data_column->max_one_element_serialize_size();
    }

    uint32_t serialize(size_t idx, uint8_t* pos) override;

    uint32_t serialize_default(uint8_t* pos) override;

    void serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                         uint32_t max_one_row_size) override;

    const uint8_t* deserialize_and_append(const uint8_t* pos) override;

    void deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) override;

    uint32_t serialize_size(size_t idx) const override {
        if (_null_column->get_data()[idx]) {
            return sizeof(uint8_t);
        }
        return sizeof(uint8_t) + _data_column->serialize_size(idx);
    }

    MutableColumnPtr clone_empty() const override {
        return create_mutable(_data_column->clone_empty(), _null_column->clone_empty());
    }

    size_t serialize_batch_at_interval(uint8_t* dst, size_t byte_offset, size_t byte_interval, size_t start,
                                       size_t count) override;

    size_t filter_range(const Column::Filter& filter, size_t from, size_t to) override;

    int compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const override;

    void fnv_hash(uint32_t* hash, uint32_t from, uint32_t to) const override;

    void crc32_hash(uint32_t* hash, uint32_t from, uint32_t to) const override;

    int64_t xor_checksum(uint32_t from, uint32_t to) const override;

    void put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx) const override;

    std::string get_name() const override { return "nullable-" + _data_column->get_name(); }

    NullData& null_column_data() { return _null_column->get_data(); }
    const NullData& immutable_null_column_data() const { return _null_column->get_data(); }

    Column* mutable_data_column() { return _data_column.get(); }

    NullColumn* mutable_null_column() { return _null_column.get(); }

    const Column& data_column_ref() const { return *_data_column; }

    const ColumnPtr& data_column() const { return _data_column; }

    ColumnPtr& data_column() { return _data_column; }

    const NullColumn& null_column_ref() const { return *_null_column; }
    const NullColumnPtr& null_column() const { return _null_column; }

    size_t null_count() const;
    size_t null_count(size_t offset, size_t count) const;

    Datum get(size_t n) const override {
        if (_has_null && _null_column->get_data()[n]) {
            return Datum();
        } else {
            return _data_column->get(n);
        }
    }

    bool set_null(size_t idx) override {
        null_column_data()[idx] = 1;
        _has_null = true;
        return true;
    }
    ColumnPtr replicate(const std::vector<uint32_t>& offsets) override;

    size_t memory_usage() const override {
        return _data_column->memory_usage() + _null_column->memory_usage() + sizeof(bool);
    }

    size_t container_memory_usage() const override {
        return _data_column->container_memory_usage() + _null_column->container_memory_usage();
    }

    size_t element_memory_usage(size_t from, size_t size) const override {
        DCHECK_LE(from + size, this->size()) << "Range error";
        return _data_column->element_memory_usage(from, size) + _null_column->element_memory_usage(from, size);
    }

    void swap_column(Column& rhs) override {
        auto& r = down_cast<NullableColumn&>(rhs);
        _data_column->swap_column(*r._data_column);
        _null_column->swap_column(*r._null_column);
        std::swap(_delete_state, r._delete_state);
        std::swap(_has_null, r._has_null);
    }

    void reset_column() override {
        Column::reset_column();
        _data_column->reset_column();
        _null_column->reset_column();
        _has_null = false;
    }

    std::string debug_item(uint32_t idx) const override {
        DCHECK(_null_column->size() == _data_column->size());
        std::stringstream ss;
        if (_null_column->get_data()[idx]) {
            ss << "NULL";
        } else {
            ss << _data_column->debug_item(idx);
        }
        return ss.str();
    }

    std::string debug_string() const override {
        DCHECK(_null_column->size() == _data_column->size());
        std::stringstream ss;
        ss << "[";
        int size = _data_column->size();
        for (int i = 0; i < size - 1; ++i) {
            ss << debug_item(i) << ", ";
        }
        if (size > 0) {
            ss << debug_item(size - 1);
        }
        ss << "]";
        return ss.str();
    }

    bool capacity_limit_reached(std::string* msg = nullptr) const override {
        return _data_column->capacity_limit_reached(msg) || _null_column->capacity_limit_reached(msg);
    }

    void check_or_die() const override;

protected:
    ColumnPtr _data_column;
    NullColumnPtr _null_column;
    mutable bool _has_null;
};

} // namespace starrocks::vectorized
