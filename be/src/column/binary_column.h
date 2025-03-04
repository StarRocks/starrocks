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

#include "column/bytes.h"
#include "column/column.h"
#include "column/datum.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "gutil/strings/fastmem.h"
#include "util/slice.h"

namespace starrocks {

template <typename T>
class BinaryColumnBase final : public ColumnFactory<Column, BinaryColumnBase<T>> {
    friend class ColumnFactory<Column, BinaryColumnBase<T>>;

public:
    using ValueType = Slice;

    using Offset = T;
    using Offsets = Buffer<T>;
    using Byte = uint8_t;
    using Bytes = starrocks::raw::RawVectorPad16<uint8_t, ColumnAllocator<uint8_t>>;

    struct BinaryDataProxyContainer {
        BinaryDataProxyContainer(const BinaryColumnBase& column) : _column(column) {}

        Slice operator[](size_t index) const { return _column.get_slice(index); }

        size_t size() const { return _column.size(); }

    private:
        const BinaryColumnBase& _column;
    };

    using Container = Buffer<Slice>;
    using ProxyContainer = BinaryDataProxyContainer;

    // TODO(kks): when we create our own vector, we could let vector[-1] = 0,
    // and then we don't need explicitly emplace_back zero value
    BinaryColumnBase() { _offsets.emplace_back(0); }
    // Default value is empty string
    explicit BinaryColumnBase(size_t size) : _offsets(size + 1, 0) {}
    BinaryColumnBase(Bytes bytes, Offsets offsets) : _bytes(std::move(bytes)), _offsets(std::move(offsets)) {
        if (_offsets.empty()) {
            _offsets.emplace_back(0);
        }
    }

    // NOTE: do *NOT* copy |_slices|
    BinaryColumnBase(const BinaryColumnBase<T>& rhs) : _bytes(rhs._bytes), _offsets(rhs._offsets) {}

    // NOTE: do *NOT* copy |_slices|
    BinaryColumnBase(BinaryColumnBase<T>&& rhs) noexcept
            : _bytes(std::move(rhs._bytes)), _offsets(std::move(rhs._offsets)) {}

    BinaryColumnBase<T>& operator=(const BinaryColumnBase<T>& rhs) {
        BinaryColumnBase<T> tmp(rhs);
        this->swap_column(tmp);
        return *this;
    }

    BinaryColumnBase<T>& operator=(BinaryColumnBase<T>&& rhs) noexcept {
        BinaryColumnBase<T> tmp(std::move(rhs));
        this->swap_column(tmp);
        return *this;
    }

    StatusOr<ColumnPtr> upgrade_if_overflow() override;

    StatusOr<ColumnPtr> downgrade() override;

    bool has_large_column() const override;

    ~BinaryColumnBase() override {
#ifndef NDEBUG
        // sometimes we may fill _bytes and _offsets separately and resize them in the final stage,
        // if an exception is thrown in the middle process, _offsets maybe inconsistent with _bytes,
        // we should skip the check.
        if (std::uncaught_exception()) {
            return;
        }
#endif
        if (!_offsets.empty()) {
            DCHECK_EQ(_bytes.size(), _offsets.back());
        } else {
            DCHECK_EQ(_bytes.size(), 0);
        }
    }

    bool is_binary() const override { return std::is_same_v<T, uint32_t> != 0; }
    bool is_large_binary() const override { return std::is_same_v<T, uint64_t> != 0; }

    const uint8_t* raw_data() const override {
        if (!_slices_cache) {
            _build_slices();
        }
        return reinterpret_cast<const uint8_t*>(_slices.data());
    }

    uint8_t* mutable_raw_data() override {
        if (!_slices_cache) {
            _build_slices();
        }
        return reinterpret_cast<uint8_t*>(_slices.data());
    }

    size_t size() const override { return _offsets.size() - 1; }

    size_t capacity() const override { return _offsets.capacity() - 1; }

    size_t type_size() const override { return sizeof(Slice); }

    size_t byte_size() const override { return _bytes.size() * sizeof(uint8_t) + _offsets.size() * sizeof(Offset); }

    size_t byte_size(size_t from, size_t size) const override {
        DCHECK_LE(from + size, this->size()) << "Range error";
        return (_offsets[from + size] - _offsets[from]) + size * sizeof(Offset);
    }

    size_t byte_size(size_t idx) const override { return _offsets[idx + 1] - _offsets[idx] + sizeof(uint32_t); }

    Slice get_slice(size_t idx) const {
        return Slice(_bytes.data() + _offsets[idx], _offsets[idx + 1] - _offsets[idx]);
    }

    void check_or_die() const override;

    // For n value, the offsets size is n + 1
    // For example, for string "I","love","you"
    // the _bytes array is "Iloveyou"
    // the _offsets array is [0,1,5,8]
    void reserve(size_t n) override {
        // hard to know how the best reserve size of |_bytes|, inaccurate reserve may
        // affect the performance.
        // _bytes.reserve(n * 4);
        _offsets.reserve(n + 1);
        _slices_cache = false;
    }

    // If you know the size of the Byte array in advance, you can call this method,
    // n means the number of strings, byte_size is the total length of the string
    void reserve(size_t n, size_t byte_size) {
        _offsets.reserve(n + 1);
        _bytes.reserve(byte_size);
        _slices_cache = false;
    }

    void resize(size_t n) override {
        _offsets.resize(n + 1, _offsets.back());
        _bytes.resize(_offsets.back());
        _slices_cache = false;
    }

    void assign(size_t n, size_t idx) override;

    void remove_first_n_values(size_t count) override;

    // No complain about the overloaded-virtual for this function
    DIAGNOSTIC_PUSH
    DIAGNOSTIC_IGNORE("-Woverloaded-virtual")
    void append(const Slice& str);
    DIAGNOSTIC_POP

    void append_datum(const Datum& datum) override {
        append(datum.get_slice());
        _slices_cache = false;
    }

    void append(const Column& src, size_t offset, size_t count) override;

    void append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) override;

    void append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) override;

    bool append_nulls(size_t count) override { return false; }

    void append_string(const std::string& str) {
        _bytes.insert(_bytes.end(), str.data(), str.data() + str.size());
        _offsets.emplace_back(_bytes.size());
        _slices_cache = false;
    }

    bool append_strings(const Slice* data, size_t size) override;

    bool append_strings_overflow(const Slice* data, size_t size, size_t max_length) override;

    bool append_continuous_strings(const Slice* data, size_t size) override;

    bool append_continuous_fixed_length_strings(const char* data, size_t size, int fixed_length) override;

    size_t append_numbers(const void* buff, size_t length) override { return -1; }

    void append_value_multiple_times(const void* value, size_t count) override;

    void append_default() override {
        _offsets.emplace_back(_bytes.size());
        _slices_cache = false;
    }

    void append_default(size_t count) override {
        _offsets.insert(_offsets.end(), count, static_cast<uint32_t>(_bytes.size()));
        _slices_cache = false;
    }

    StatusOr<ColumnPtr> replicate(const Buffer<uint32_t>& offsets) override;

    void fill_default(const Filter& filter) override;

    void update_rows(const Column& src, const uint32_t* indexes) override;

    uint32_t max_one_element_serialize_size() const override;

    ALWAYS_INLINE uint32_t serialize(size_t idx, uint8_t* pos) override {
        // max size of one string is 2^32, so use uint32_t not T
        auto binary_size = static_cast<uint32_t>(_offsets[idx + 1] - _offsets[idx]);
        T offset = _offsets[idx];

        strings::memcpy_inlined(pos, &binary_size, sizeof(uint32_t));
        strings::memcpy_inlined(pos + sizeof(uint32_t), &_bytes[offset], binary_size);

        return sizeof(uint32_t) + binary_size;
    }

    uint32_t serialize_default(uint8_t* pos) override;

    void serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                         uint32_t max_one_row_size) override;

    void serialize_batch_with_null_masks(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                                         uint32_t max_one_row_size, uint8_t* null_masks, bool has_null) override;

    const uint8_t* deserialize_and_append(const uint8_t* pos) override;

    void deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) override;

    void deserialize_and_append_batch_nullable(Buffer<Slice>& srcs, size_t chunk_size, Buffer<uint8_t>& is_nulls,
                                               bool& has_null) override;

    uint32_t serialize_size(size_t idx) const override {
        // max size of one string is 2^32, so use sizeof(uint32_t) not sizeof(T)
        return static_cast<uint32_t>(sizeof(uint32_t) + _offsets[idx + 1] - _offsets[idx]);
    }

    MutableColumnPtr clone_empty() const override { return BinaryColumnBase<T>::create_mutable(); }

    ColumnPtr cut(size_t start, size_t length) const;
    size_t filter_range(const Filter& filter, size_t start, size_t to) override;

    int compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const override;

    void fnv_hash(uint32_t* hashes, uint32_t from, uint32_t to) const override;
    void fnv_hash_with_selection(uint32_t* seed, uint8_t* selection, uint16_t from, uint16_t to) const override;
    void fnv_hash_selective(uint32_t* hashes, uint16_t* sel, uint16_t sel_size) const override;

    void crc32_hash(uint32_t* hash, uint32_t from, uint32_t to) const override;
    void crc32_hash_with_selection(uint32_t* seed, uint8_t* selection, uint16_t from, uint16_t to) const override;
    void crc32_hash_selective(uint32_t* hashes, uint16_t* sel, uint16_t sel_size) const override;

    int64_t xor_checksum(uint32_t from, uint32_t to) const override;

    void put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx, bool is_binary_protocol = false) const override;

    std::string get_name() const override {
        static_assert(std::is_same_v<T, uint32_t> || std::is_same_v<T, uint64_t>);
        if (std::is_same_v<T, uint32_t>) {
            return "binary";
        } else {
            return "large-binary";
        }
    }

    Container& get_data() {
        if (!_slices_cache) {
            _build_slices();
        }
        return _slices;
    }
    const Container& get_data() const {
        if (!_slices_cache) {
            _build_slices();
        }
        return _slices;
    }

    const BinaryDataProxyContainer& get_proxy_data() const { return _immuable_container; }

    Bytes& get_bytes() { return _bytes; }

    const Bytes& get_bytes() const { return _bytes; }

    const uint8_t* continuous_data() const override { return reinterpret_cast<const uint8_t*>(_bytes.data()); }

    Offsets& get_offset() { return _offsets; }
    const Offsets& get_offset() const { return _offsets; }

    Datum get(size_t n) const override { return Datum(get_slice(n)); }

    size_t container_memory_usage() const override {
        return _bytes.capacity() + _offsets.capacity() * sizeof(_offsets[0]) + _slices.capacity() * sizeof(_slices[0]);
    }

    size_t reference_memory_usage(size_t from, size_t size) const override { return 0; }

    void swap_column(Column& rhs) override {
        auto& r = down_cast<BinaryColumnBase<T>&>(rhs);
        using std::swap;
        swap(this->_delete_state, r._delete_state);
        swap(_bytes, r._bytes);
        swap(_offsets, r._offsets);
        swap(_slices, r._slices);
        swap(_slices_cache, r._slices_cache);
    }

    void reset_column() override {
        Column::reset_column();
        // TODO(zhuming): shrink size if needed.
        _bytes.clear();
        _offsets.resize(1, 0);
        _slices.clear();
        _slices_cache = false;
    }

    void invalidate_slice_cache() { _slices_cache = false; }

    std::string debug_item(size_t idx) const override;

    std::string raw_item_value(size_t idx) const override;

    std::string debug_string() const override {
        std::stringstream ss;
        size_t size = this->size();
        ss << "[";
        for (size_t i = 0; i + 1 < size; ++i) {
            ss << debug_item(i) << ", ";
        }
        if (size > 0) {
            ss << debug_item(size - 1);
        }
        ss << "]";
        return ss.str();
    }

    Status capacity_limit_reached() const override;

private:
    void _build_slices() const;

    Bytes _bytes;
    Offsets _offsets;

    mutable Container _slices;
    mutable bool _slices_cache = false;
    BinaryDataProxyContainer _immuable_container = BinaryDataProxyContainer(*this);
};

using Offsets = BinaryColumnBase<uint32_t>::Offsets;
using LargeOffsets = BinaryColumnBase<uint64_t>::Offsets;

} // namespace starrocks
