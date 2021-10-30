// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <cstdint>

#include "column/column.h"
#include "column/fixed_length_column.h"

namespace starrocks::vectorized {

class ArrayColumn final : public ColumnFactory<Column, ArrayColumn> {
    friend class ColumnFactory<Column, ArrayColumn>;

public:
    using ValueType = void;

    ArrayColumn(ColumnPtr elements, UInt32Column::Ptr offests);

    // Copy constructor
    ArrayColumn(const ArrayColumn& rhs)
            : _elements(rhs._elements->clone_shared()),
              _offsets(std::static_pointer_cast<UInt32Column>(rhs._offsets->clone_shared())) {}

    // Move constructor
    ArrayColumn(ArrayColumn&& rhs) noexcept : _elements(std::move(rhs._elements)), _offsets(std::move(rhs._offsets)) {}

    // Copy assignment
    ArrayColumn& operator=(const ArrayColumn& rhs) {
        ArrayColumn tmp(rhs);
        this->swap_column(tmp);
        return *this;
    }

    // Move assignment
    ArrayColumn& operator=(ArrayColumn&& rhs) noexcept {
        ArrayColumn tmp(std::move(rhs));
        this->swap_column(tmp);
        return *this;
    }

    ~ArrayColumn() override = default;

    bool is_array() const override { return true; }

    const uint8_t* raw_data() const override;

    uint8_t* mutable_raw_data() override;

    // Return number of values in column.
    size_t size() const override;

    size_t type_size() const override { return sizeof(DatumArray); }

    // Size of column data in memory (may be approximate). Zero, if could not be determined.
    size_t byte_size() const override { return _elements->byte_size() + _offsets->byte_size(); }
    size_t byte_size(size_t from, size_t size) const override;

    // The byte size for serialize, for varchar, we need to add the len byte size
    size_t byte_size(size_t idx) const override;

    void reserve(size_t n) override;

    void resize(size_t n) override;

    // Assign specified idx element to the column container content,
    // and modifying column size accordingly.
    void assign(size_t n, size_t idx) override;

    // Appends one value at the end of column (column's size is increased by 1).
    void append_datum(const Datum& datum) override;

    // Append |count| elements from |src|, started from the offset |offset|, into |this| column.
    // It's undefined behaviour if |offset+count| greater than the size of |src|.
    // The type of |src| and |this| must be exactly matched.
    void append(const Column& src, size_t offset, size_t count) override;

    // This function will append data from src according to the input indexes. 'indexes' contains
    // the row index of the src.
    // This function will get row index from indexes and append the data to this column.
    // This function will handle indexes start from input 'from' and will append 'size' times
    // For example:
    //      input indexes: [5, 4, 3, 2, 1]
    //      from: 2
    //      size: 2
    // This function will copy the [3, 2] row of src to this column.
    void append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) override;

    void append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) override;

    // Append multiple `null` values into this column.
    // Return false if this is a non-nullable column, i.e, if `is_nullable` return false.
    bool append_nulls(size_t count) override;

    // Append multiple strings into this column.
    // Return false if the column is not a binary column.
    bool append_strings(const std::vector<Slice>& strs) override { return false; }

    // Copy |length| bytes from |buff| into this column and cast them as integers.
    // The count of copied integers depends on |length| and the size of column value:
    //  - `int8_t` column:  |length| integers will be copied.
    //  - `int16_t` column: |length| / 2 integers will be copied.
    //  - `int32_t` column: |length| / 4 integers will be copied.
    //  - ...
    // |buff| must NOT be nullptr.
    // Return
    //  - the count of copied integers on success.
    //  - -1 if this is not a numeric column.
    size_t append_numbers(const void* buff, size_t length) override { return -1; }

    // Append |*value| |count| times, this is only used when load default value.
    void append_value_multiple_times(const void* value, size_t count) override;

    // Append one default value into this column.
    // NOTE:
    //  - for `NullableColumn`, the default value is `null`.
    //  - for `BinaryColumn`, the default value is empty string.
    //  - for `FixedLengthColumn`, the default value is zero.
    //  - for `ConstColumn`, the default value is the const value itself.
    void append_default() override;

    // Append multiple default values into this column.
    void append_default(size_t count) override;

    void remove_first_n_values(size_t count) override {}

    // Sometimes(Hash group by multi columns),
    // we need one buffer to hold tmp serialize data,
    // So we need to know the max serialize_size for all column element
    // The bad thing is we couldn't get the string defined len from FE when query
    uint32_t max_one_element_serialize_size() const override;

    // serialize one data,The memory must allocate firstly from mempool
    uint32_t serialize(size_t idx, uint8_t* pos) override;

    uint32_t serialize_default(uint8_t* pos) override;

    void serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                         uint32_t max_one_row_size) override;

    // deserialize one data and append to this column
    const uint8_t* deserialize_and_append(const uint8_t* pos) override;

    void deserialize_and_append_batch(std::vector<Slice>& srcs, size_t batch_size) override;

    // One element serialize_size
    uint32_t serialize_size(size_t idx) const override;

    // The serialize bytes size when serialize by directly copy whole column data
    size_t serialize_size() const override;

    // Serialize whole column data to dst
    // The return value is dst + column serialize_size
    uint8_t* serialize_column(uint8_t* dst) override;

    // Deserialize whole column from the src
    // The return value is src + column serialize_size
    // TODO(kks): validate the input src column data
    const uint8_t* deserialize_column(const uint8_t* src) override;

    // return new empty column with the same type
    MutableColumnPtr clone_empty() const override;

    size_t filter_range(const Filter& filter, size_t from, size_t to) override;

    // Compares (*this)[left] and rhs[right]. Column rhs should have the same type.
    // Returns negative number, 0, or positive number (*this)[left] is less, equal, greater than
    // rhs[right] respectively.
    //
    // If one of element's value is NaN or NULLs, then:
    // - if nan_direction_hint == -1, NaN and NULLs are considered as least than everything other;
    // - if nan_direction_hint ==  1, NaN and NULLs are considered as greatest than everything other.
    // For example, if nan_direction_hint == -1 is used by descending sorting, NaNs will be at the end.
    //
    // For non Nullable and non floating point types, nan_direction_hint is ignored.
    int compare_at(size_t left, size_t right, const Column& right_column, int nan_direction_hint) const override;

    void crc32_hash_at(uint32_t* seed, int32_t idx) const override;
    void fnv_hash_at(uint32_t* seed, int32_t idx) const override;
    // Compute fvn hash, mainly used by shuffle column data
    // Note: shuffle hash function should be different from Aggregate and Join Hash map hash function
    void fnv_hash(uint32_t* hash, uint32_t from, uint32_t to) const override;

    // used by data loading compute tablet bucket
    void crc32_hash(uint32_t* hash, uint32_t from, uint32_t to) const override;

    // Push one row to MysqlRowBuffer
    void put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx) const override;

    std::string get_name() const override { return "array"; }

    // Return the value of the n-th element.
    Datum get(size_t idx) const override;

    // return false if this is a non-nullable column.
    // |idx| must less than the size of column.
    bool set_null(size_t idx) override;

    size_t memory_usage() const override { return _elements->memory_usage() + _offsets->memory_usage(); }

    size_t shrink_memory_usage() const override {
        return _elements->shrink_memory_usage() + _offsets->shrink_memory_usage();
    }

    size_t container_memory_usage() const override {
        return _elements->container_memory_usage() + _offsets->container_memory_usage();
    }

    size_t element_memory_usage(size_t from, size_t size) const override;

    void swap_column(Column& rhs) override;

    void reset_column() override;

    const Column& elements() const;
    ColumnPtr& elements_column();

    const UInt32Column& offsets() const;
    UInt32Column::Ptr& offsets_column();

    bool is_nullable() const override { return false; }

    // Only used for debug one item in this column
    std::string debug_item(uint32_t idx) const override;

    std::string debug_string() const override;

    bool reach_capacity_limit() const override {
        return _elements->reach_capacity_limit() || _offsets->reach_capacity_limit();
    }

private:
    ColumnPtr _elements;
    // Offsets column will store the start position of every array element.
    // Offsets store more one data to indicate the end position.
    // For example, [1, 2, 3], [4, 5, 6].
    // The two element array has three offsets(0, 3, 6)
    UInt32Column::Ptr _offsets;
};

} // namespace starrocks::vectorized
