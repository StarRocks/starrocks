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

#include <span>
#include <utility>

#include "column/column.h"
#include "column/container_resource.h"
#include "column/datum.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "util/raw_container.h"

namespace starrocks {

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
class FixedLengthColumnBase : public Column {
public:
    using ValueType = T;
    using Container = Buffer<ValueType>;
    using ImmContainer = std::span<const ValueType>;

    FixedLengthColumnBase() = default;

    explicit FixedLengthColumnBase(const size_t n) : _data(n) {}

    FixedLengthColumnBase(const size_t n, const ValueType x) : _data(n, x) {}

    FixedLengthColumnBase(const FixedLengthColumnBase& src)
            : _resource(src._resource), _data(src.immutable_data().begin(), src.immutable_data().end()) {}

    // Only used as a underlying type for other column type(i.e. DecimalV3Column), C++
    // is weak to implement delegation for composite type like golang, so we have to use
    // inheritance to wrap an underlying type. When constructing a wrapper object, we must
    // construct the wrapped object first, move constructor is used to prevent the unnecessary
    // time-consuming copy operation.
    FixedLengthColumnBase(FixedLengthColumnBase&& src) noexcept
            : _resource(std::move(src._resource)), _data(std::move(src._data)) {}

    bool is_numeric() const override { return std::is_arithmetic_v<ValueType>; }

    bool is_decimal() const override { return IsDecimal<ValueType>; }

    bool is_date() const override { return IsDate<ValueType>; }

    bool is_timestamp() const override { return IsTimestamp<ValueType>; }

    const uint8_t* raw_data() const override { return reinterpret_cast<const uint8_t*>(immutable_data().data()); }

    uint8_t* mutable_raw_data() override {
        get_data();
        return reinterpret_cast<uint8_t*>(_data.data());
    }

    size_t type_size() const override { return sizeof(T); }

    size_t size() const override { return immutable_data().size(); }

    size_t capacity() const override { return _data.capacity(); }

    size_t byte_size() const override { return this->size() * sizeof(ValueType); }

    size_t byte_size(size_t idx __attribute__((unused))) const override { return sizeof(ValueType); }

    size_t byte_size(size_t from, size_t size) const override {
        DCHECK_LE(from + size, this->size()) << "Range error";
        return sizeof(ValueType) * size;
    }

    void reserve(size_t n) override { _data.reserve(n); }

    void resize(size_t n) override { get_data().resize(n); }

    void resize_uninitialized(size_t n) override {
        auto& data = get_data();
        raw::stl_vector_resize_uninitialized(&data, n);
    }

    void assign(size_t n, size_t idx) override {
        auto& datas = get_data();
        datas.assign(n, _data[idx]);
    }

    void remove_first_n_values(size_t count) override;

    void append(const T value) {
        auto& datas = get_data();
        datas.emplace_back(value);
    }

    void append(const Buffer<T>& values) {
        auto& datas = get_data();
        datas.insert(datas.end(), values.begin(), values.end());
    }

    void append(const ImmBuffer<T> values) {
        auto& datas = get_data();
        datas.insert(datas.end(), values.begin(), values.end());
    }

    void append_datum(const Datum& datum) override {
        auto& datas = get_data();
        datas.emplace_back(datum.get<ValueType>());
    }

    void append(const Column& src, size_t offset, size_t count) override;

    void append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) override;

    void append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) override;

    [[nodiscard]] bool append_nulls(size_t count __attribute__((unused))) override { return false; }

    [[nodiscard]] bool contain_value(size_t start, size_t end, T value) const {
        const auto datas = this->immutable_data();
        DCHECK_LE(start, end);
        DCHECK_LE(start, datas.size());
        DCHECK_LE(end, datas.size());
        for (size_t i = start; i < end; i++) {
            if (datas[i] == value) {
                return true;
            }
        }
        return false;
    }

    size_t append_numbers(const void* buff, size_t length) override {
        DCHECK(length % sizeof(ValueType) == 0);
        const size_t count = length / sizeof(ValueType);
        auto& datas = this->get_data();
        size_t dst_offset = datas.size();
        raw::stl_vector_resize_uninitialized(&datas, datas.size() + count);
        T* dst = datas.data() + dst_offset;
        memcpy(dst, buff, length);
        return count;
    }

    size_t append_numbers(const ContainerResource& res) override;

    void append_value_multiple_times(const void* value, size_t count) override {
        auto& datas = get_data();
        datas.insert(datas.end(), count, *reinterpret_cast<const T*>(value));
    }

    void append_default() override;

    void append_default(size_t count) override;

    StatusOr<ColumnPtr> replicate(const Buffer<uint32_t>& offsets) override;

    void fill_default(const Filter& filter) override;

    Status fill_range(const std::vector<T>& ids, const Filter& filter);

    void update_rows(const Column& src, const uint32_t* indexes) override;

    // The `_data` support one size(> 2^32), but some interface such as update_rows() will use uint32_t to
    // access the item, so we should use 2^32 as the limit
    StatusOr<ColumnPtr> upgrade_if_overflow() override;

    StatusOr<ColumnPtr> downgrade() override { return nullptr; }

    bool has_large_column() const override { return false; }

    uint32_t serialize(size_t idx, uint8_t* pos) const override;

    uint32_t serialize_default(uint8_t* pos) const override;

    uint32_t max_one_element_serialize_size() const override { return sizeof(ValueType); }

    void serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                         uint32_t max_one_row_size) const override;

    void serialize_batch_with_null_masks(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                                         uint32_t max_one_row_size, const uint8_t* null_masks,
                                         bool has_null) const override;

    size_t serialize_batch_at_interval(uint8_t* dst, size_t byte_offset, size_t byte_interval, uint32_t max_row_size,
                                       size_t start, size_t count) const override;

    const uint8_t* deserialize_and_append(const uint8_t* pos) override;

    void deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) override;

    uint32_t serialize_size(size_t idx) const override { return sizeof(ValueType); }

    size_t filter_range(const Filter& filter, size_t from, size_t to) override;

    int compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const override;

    void fnv_hash(uint32_t* hash, uint32_t from, uint32_t to) const override;
    void fnv_hash_with_selection(uint32_t* seed, uint8_t* selection, uint16_t from, uint16_t to) const override;
    void fnv_hash_selective(uint32_t* hash, uint16_t* sel, uint16_t sel_size) const override;

    void crc32_hash(uint32_t* hash, uint32_t from, uint32_t to) const override;
    void crc32_hash_with_selection(uint32_t* seed, uint8_t* selection, uint16_t from, uint16_t to) const override;
    void crc32_hash_selective(uint32_t* hash, uint16_t* sel, uint16_t sel_size) const override;

    void murmur_hash3_x86_32(uint32_t* hash, uint32_t from, uint32_t to) const override;

    int64_t xor_checksum(uint32_t from, uint32_t to) const override;

    void put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx, bool is_binary_protocol = false) const override;

    std::string get_name() const override;

    Container& get_data() {
        // Note: not thread safe !
        if (!_resource.empty()) {
            auto span = _resource.span<T>();
            _data.assign(span.begin(), span.end());
            _resource.reset();
        }
        return _data;
    }

    const ImmContainer immutable_data() const {
        if (!_resource.empty()) {
            return _resource.span<T>();
        }
        return _data;
    }

    Datum get(size_t n) const override {
        const auto datas = immutable_data();
        return Datum(datas[n]);
    }

    std::string debug_item(size_t idx) const override;

    std::string debug_string() const override;

    size_t container_memory_usage() const override { return _data.capacity() * sizeof(ValueType); }
    size_t reference_memory_usage(size_t from, size_t size) const override { return 0; }

    void swap_column(Column& rhs) override {
        auto& r = down_cast<FixedLengthColumnBase&>(rhs);
        std::swap(this->_delete_state, r._delete_state);
        std::swap(this->_data, r._data);
        std::swap(this->_resource, r._resource);
    }

    void reset_column() override {
        Column::reset_column();
        _resource.reset();
        _data.clear();
    }

    // The `_data` support one size(> 2^32), but some interface such as update_rows() will use index of uint32_t to
    // access the item, so we should use 2^32 as the limit
    Status capacity_limit_reached() const override;

    void check_or_die() const override {}

protected:
    ContainerResource _resource;
    Container _data;

private:
    using Column::append;
};

} // namespace starrocks
