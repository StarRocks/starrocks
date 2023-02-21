// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <memory>
#include <utility>

#include "column/column.h"
#include "column/datum.h"
#include "common/statusor.h"
#include "runtime/datetime_value.h"
#include "runtime/decimalv2_value.h"
#include "types/date_value.hpp"
#include "types/timestamp_value.h"
#include "util/raw_container.h"
#include "util/value_generator.h"

namespace starrocks::vectorized {

template <typename T>
constexpr bool IsDecimal = false;
template <>
inline constexpr bool IsDecimal<DecimalV2Value> = true;

template <typename T>
constexpr bool IsDate = false;
template <>
inline constexpr bool IsDate<DateValue> = true;

template <typename T>
constexpr bool IsTimestamp = false;
template <>
inline constexpr bool IsTimestamp<TimestampValue> = true;

template <typename T>
constexpr bool IsTemporal() {
    return std::is_same_v<T, DateValue> || std::is_same_v<T, TimestampValue> || std::is_same_v<T, DateTimeValue>;
}

template <typename T>
class FixedLengthColumnBase : public ColumnFactory<Column, FixedLengthColumnBase<T>> {
    friend class ColumnFactory<Column, FixedLengthColumnBase>;

public:
    using ValueType = T;
    using Container = Buffer<ValueType>;

    FixedLengthColumnBase() = default;

    explicit FixedLengthColumnBase(const size_t n) : _data(n) {}

    FixedLengthColumnBase(const size_t n, const ValueType x) : _data(n, x) {}

    FixedLengthColumnBase(const FixedLengthColumnBase& src) : _data(src._data.begin(), src._data.end()) {}

    // Only used as a underlying type for other column type(i.e. DecimalV3Column), C++
    // is weak to implement delegation for composite type like golang, so we have to use
    // inheritance to wrap an underlying type. When constructing a wrapper object, we must
    // construct the wrapped object first, move constructor is used to prevent the unnecessary
    // time-consuming copy operation.
    FixedLengthColumnBase(FixedLengthColumnBase&& src) noexcept : _data(std::move(src._data)) {}

    bool is_numeric() const override { return std::is_arithmetic_v<ValueType>; }

    bool is_decimal() const override { return IsDecimal<ValueType>; }

    bool is_date() const override { return IsDate<ValueType>; }

    bool is_timestamp() const override { return IsTimestamp<ValueType>; }

    const uint8_t* raw_data() const override { return reinterpret_cast<const uint8_t*>(_data.data()); }

    uint8_t* mutable_raw_data() override { return reinterpret_cast<uint8_t*>(_data.data()); }

    size_t type_size() const override { return sizeof(T); }

    size_t size() const override { return _data.size(); }

    size_t capacity() const override { return _data.capacity(); }

    size_t byte_size() const override { return _data.size() * sizeof(ValueType); }

    size_t byte_size(size_t idx __attribute__((unused))) const override { return sizeof(ValueType); }

    void reserve(size_t n) override { _data.reserve(n); }

    void resize(size_t n) override { _data.resize(n); }

    void resize_uninitialized(size_t n) override { raw::stl_vector_resize_uninitialized(&_data, n); }

    void assign(size_t n, size_t idx) override { _data.assign(n, _data[idx]); }

    void remove_first_n_values(size_t count) override;

    void append(const T value) { _data.emplace_back(value); }

    void append(const Buffer<T>& values) { _data.insert(_data.end(), values.begin(), values.end()); }

    void append_datum(const Datum& datum) override { _data.emplace_back(datum.get<ValueType>()); }

    void append(const Column& src, size_t offset, size_t count) override;

    void append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) override;

    void append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) override;

    bool append_nulls(size_t count __attribute__((unused))) override { return false; }

    bool append_strings(const Buffer<Slice>& slices __attribute__((unused))) override { return false; }

    bool contain_value(size_t start, size_t end, T value) const {
        DCHECK_LE(start, end);
        DCHECK_LE(start, _data.size());
        DCHECK_LE(end, _data.size());
        for (size_t i = start; i < end; i++) {
            if (_data[i] == value) {
                return true;
            }
        }
        return false;
    }

    size_t append_numbers(const void* buff, size_t length) override {
        const size_t count = length / sizeof(ValueType);
        const T* const ptr = reinterpret_cast<const T*>(buff);
        _data.insert(_data.end(), ptr, ptr + count);
        return count;
    }

    void append_value_multiple_times(const void* value, size_t count) override {
        _data.insert(_data.end(), count, *reinterpret_cast<const T*>(value));
    }

    void append_default() override { _data.emplace_back(DefaultValueGenerator<ValueType>::next_value()); }

    void append_default(size_t count) override {
        _data.resize(_data.size() + count, DefaultValueGenerator<ValueType>::next_value());
    }

    ColumnPtr replicate(const std::vector<uint32_t>& offsets) override;

    void fill_default(const Filter& filter) override;

    Status update_rows(const Column& src, const uint32_t* indexes) override;

    // The `_data` support one size(> 2^32), but some interface such as update_rows() will use uint32_t to
    // access the item, so we should use 2^32 as the limit
    StatusOr<ColumnPtr> upgrade_if_overflow() override;

    StatusOr<ColumnPtr> downgrade() override { return nullptr; }

    bool has_large_column() const override { return false; }

    uint32_t serialize(size_t idx, uint8_t* pos) override;

    uint32_t serialize_default(uint8_t* pos) override;

    uint32_t max_one_element_serialize_size() const override { return sizeof(ValueType); }

    void serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                         uint32_t max_one_row_size) override;

    void serialize_batch_with_null_masks(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                                         uint32_t max_one_row_size, uint8_t* null_masks, bool has_null) override;

    size_t serialize_batch_at_interval(uint8_t* dst, size_t byte_offset, size_t byte_interval, size_t start,
                                       size_t count) override;

    const uint8_t* deserialize_and_append(const uint8_t* pos) override;

    void deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) override;

    uint32_t serialize_size(size_t idx) const override { return sizeof(ValueType); }

    MutableColumnPtr clone_empty() const override { return this->create_mutable(); }

    size_t filter_range(const Column::Filter& filter, size_t from, size_t to) override;

    int compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const override;

    void fnv_hash(uint32_t* hash, uint32_t from, uint32_t to) const override;

    void crc32_hash(uint32_t* hash, uint32_t from, uint32_t to) const override;

    int64_t xor_checksum(uint32_t from, uint32_t to) const override;

    void put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx) const override;

    std::string get_name() const override;

    Container& get_data() { return _data; }

    const Container& get_data() const { return _data; }

    Datum get(size_t n) const override { return Datum(_data[n]); }

    std::string debug_item(uint32_t idx) const override;

    std::string debug_string() const override;

    size_t container_memory_usage() const override { return _data.capacity() * sizeof(ValueType); }
    size_t reference_memory_usage(size_t from, size_t size) const override { return 0; }

    void swap_column(Column& rhs) override {
        auto& r = down_cast<FixedLengthColumnBase&>(rhs);
        std::swap(this->_delete_state, r._delete_state);
        std::swap(this->_data, r._data);
    }

    void reset_column() override {
        Column::reset_column();
        _data.clear();
    }

    // The `_data` support one size(> 2^32), but some interface such as update_rows() will use index of uint32_t to
    // access the item, so we should use 2^32 as the limit
    bool capacity_limit_reached(std::string* msg = nullptr) const override {
        if (_data.size() > Column::MAX_CAPACITY_LIMIT) {
            if (msg != nullptr) {
                msg->append("row count of fixed length column exceend the limit: " +
                            std::to_string(Column::MAX_CAPACITY_LIMIT));
            }
            return true;
        }
        return false;
    }

    void check_or_die() const override {}

protected:
    Container _data;

private:
    using Column::append;
};

} // namespace starrocks::vectorized
