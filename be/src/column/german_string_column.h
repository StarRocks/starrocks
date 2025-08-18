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

#include "column/binary_column.h"
#include "column/bytes.h"
#include "column/column.h"
#include "column/datum.h"
#include "column/german_string.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "gutil/strings/fastmem.h"
#include "util/misc.h"
#include "util/slice.h"

namespace starrocks {

class GermanStringColumn final : public CowFactory<ColumnFactory<Column, GermanStringColumn>, GermanStringColumn> {
    friend class CowFactory<ColumnFactory<Column, GermanStringColumn>, GermanStringColumn>;

public:
    using ValueType = GermanString;
    using Container = std::vector<ValueType, ColumnAllocator<ValueType>>;
    using Offsets = BinaryColumn::Offsets;
    using Bytes = BinaryColumn::Bytes;

    GermanStringColumn() {}

    explicit GermanStringColumn(size_t size) { _data.resize(size); }

    // TODO(by satanson): time-consuming, but we need to support it
    GermanStringColumn(Bytes bytes, Offsets offsets) {
        auto size = offsets.size() - 1;
        _data.reserve(size);
        if (size == 0) {
            return;
        }
        auto* p = bytes.data();
        for (auto i = 0; i < size; i++) {
            const auto* str = p + offsets[i];
            auto len = offsets[i + 1] - offsets[i];
            _data.emplace_back(reinterpret_cast<const char*>(str), len, _allocator->allocate(len));
        }
    }

    GermanStringColumn(const GermanStringColumn& rhs) {
        auto num_rows = rhs._data.size();
        _data.reserve(num_rows);
        for (auto i = 0; i < num_rows; i++) {
            const auto& s = rhs._data[i];
            _data.emplace_back(s, _allocator->allocate(s.len));
        }
    }

    GermanStringColumn(GermanStringColumn&& rhs) noexcept
            : _data(std::move(rhs._data)), _allocator(std::move(rhs._allocator)) {}

    GermanStringColumn& operator=(const GermanStringColumn& rhs) {
        GermanStringColumn tmp(rhs);
        this->swap_column(tmp);
        return *this;
    }

    GermanStringColumn& operator=(GermanStringColumn&& rhs) noexcept {
        GermanStringColumn tmp(std::move(rhs));
        this->swap_column(tmp);
        return *this;
    }

    StatusOr<ColumnPtr> upgrade_if_overflow() override;

    StatusOr<ColumnPtr> downgrade() override;

    bool has_large_column() const override;

    ~GermanStringColumn() override {
#ifndef NDEBUG
        // sometimes we may fill _bytes and _offsets separately and resize them in the final stage,
        // if an exception is thrown in the middle process, _offsets maybe inconsistent with _bytes,
        // we should skip the check.
        if (std::uncaught_exception()) {
            return;
        }
#endif
    }

    bool is_binary() const override { return false; }
    bool is_large_binary() const override { return false; }

    const uint8_t* raw_data() const override { return reinterpret_cast<const uint8_t*>(_data.data()); }

    uint8_t* mutable_raw_data() override { return reinterpret_cast<uint8_t*>(_data.data()); }

    size_t size() const override { return _data.size(); }

    size_t capacity() const override { return _data.capacity(); }

    size_t type_size() const override { return sizeof(GermanString); }

    size_t byte_size() const override { return _data.size() * sizeof(GermanString) + _allocator->size(); }

    size_t byte_size(size_t from, size_t size) const override {
        DCHECK(from + size <= _data.size());
        size_t nbytes = 0;
        for (size_t i = from; i < from + size; ++i) {
            auto& str = _data[i];
            if (str.is_inline()) {
                nbytes += sizeof(GermanString);
            } else {
                nbytes += sizeof(GermanString) + str.len - GermanString::PREFIX_LENGTH;
            }
        }
        return nbytes;
    }

    Offsets& get_offset() { NOT_SUPPORT(); }
    const Offsets& get_offset() const { NOT_SUPPORT(); }

    const Container& get_proxy_data() const { return _data; }

    size_t byte_size(size_t idx) const override {
        DCHECK(idx < _data.size());
        auto& str = _data[idx];
        if (str.is_inline()) {
            return sizeof(GermanString);
        } else {
            return sizeof(GermanString) + str.len - GermanString::PREFIX_LENGTH;
        }
    }

    Slice get_slice(size_t idx) const {
        NOT_SUPPORT();
        return nullptr;
    }

    void check_or_die() const override;

    void reserve(size_t n) override { _data.reserve(n); }

    // If you know the size of the Byte array in advance, you can call this method,
    // n means the number of strings, byte_size is the total length of the string
    void reserve(size_t n, size_t byte_size) { reserve(n); }

    void resize(size_t n) override { _data.resize(n); }

    void assign(size_t n, size_t idx) override;

    void remove_first_n_values(size_t count) override;

    // No complain about the overloaded-virtual for this function
    DIAGNOSTIC_PUSH
    DIAGNOSTIC_IGNORE("-Woverloaded-virtual")
    void append(const GermanString& str);
    DIAGNOSTIC_POP

    void append_datum(const Datum& datum) override { append(datum.get_german_string()); }

    void append(const Column& src, size_t offset, size_t count) override;

    void append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) override;

    void append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) override;

    bool append_nulls(size_t count) override { return false; }

    void append_string(const std::string& str);

    void append_string(const char* str, size_t size);

    bool append_strings(const GermanString* data, size_t size);
    bool append_strings(const Slice* data, size_t size) override;

    bool append_strings_overflow(const Slice* data, size_t size, size_t max_length) override;

    bool append_continuous_strings(const Slice* data, size_t size) override;

    bool append_continuous_fixed_length_strings(const char* data, size_t size, int fixed_length) override;

    size_t append_numbers(const void* buff, size_t length) override { return -1; }

    void append_value_multiple_times(const void* value, size_t count) override;

    void append_default() override { _data.emplace_back(); }

    void append_default(size_t count) override {
        _data.reserve(_data.size() + count);
        for (size_t i = 0; i < count; ++i) {
            _data.emplace_back();
        }
    }

    StatusOr<ColumnPtr> replicate(const Buffer<uint32_t>& offsets) override;

    void fill_default(const Filter& filter) override;

    void update_rows(const Column& src, const uint32_t* indexes) override;

    uint32_t max_one_element_serialize_size() const override;

    ALWAYS_INLINE uint32_t serialize(size_t idx, uint8_t* pos) const override {
        auto& str = _data[idx];
        strings::memcpy_inlined(pos, &str.len, sizeof(uint32_t));
        pos += sizeof(uint32_t);
        if (str.is_inline()) {
            strings::memcpy_inlined(pos, str.short_rep.str, str.len);
        } else {
            strings::memcpy_inlined(pos, str.long_rep.prefix, GermanString::PREFIX_LENGTH);
            pos += GermanString::PREFIX_LENGTH;
            auto* remaining = reinterpret_cast<const char*>(str.long_rep.ptr);
            strings::memcpy_inlined(pos, remaining, str.len - GermanString::PREFIX_LENGTH);
        }
        return sizeof(uint32_t) + str.len;
    }

    uint32_t serialize_default(uint8_t* pos) const override;

    void serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                         uint32_t max_one_row_size) const override;

    void serialize_batch_with_null_masks(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                                         uint32_t max_one_row_size, const uint8_t* null_masks,
                                         bool has_null) const override;

    const uint8_t* deserialize_and_append(const uint8_t* pos) override;

    void deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) override;

    void deserialize_and_append_batch_nullable(Buffer<Slice>& srcs, size_t chunk_size, Buffer<uint8_t>& is_nulls,
                                               bool& has_null) override;

    uint32_t serialize_size(size_t idx) const override {
        DCHECK(idx < _data.size());
        return static_cast<uint32_t>(sizeof(uint32_t) + _data[idx].len);
    }

    MutableColumnPtr clone_empty() const override { return GermanStringColumn::create(); }

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

    std::string get_name() const override { return "german_string_column"; }

    Container& get_data() { return _data; }
    const Container& get_data() const { return _data; }

    Bytes& get_bytes() { NOT_SUPPORT(); }

    const Bytes& get_bytes() const { NOT_SUPPORT(); }

    const uint8_t* continuous_data() const override { NOT_SUPPORT(); }

    Datum get(size_t n) const override { return Datum(_data[n]); }

    size_t container_memory_usage() const override {
        return _data.capacity() * sizeof(GermanString) + _allocator->size();
    }

    bool is_german_string() const override { return true; }

    size_t reference_memory_usage(size_t from, size_t size) const override { return 0; }

    void swap_column(Column& rhs) override {
        auto& r = down_cast<GermanStringColumn&>(rhs);
        using std::swap;
        swap(this->_delete_state, r._delete_state);
        swap(this->_data, r._data);
        swap(this->_allocator, r._allocator);
    }

    void reset_column() override {
        Column::reset_column();
        _allocator->clear();
        _data.clear();
    }

    void invalidate_slice_cache() { NOT_SUPPORT(); }

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
    ColumnPtr to_binary() const;
    void from_binary(BinaryColumn&& binary_column);
    void from_large_binary(LargeBinaryColumn&& binary_column);
    size_t get_binary_size() const;
    static std::optional<ColumnPtr> create_from_binary(ColumnPtr&& column);

    void append_entire_column(GermanStringColumn&& src_col);
    static ColumnPtr append_entire_column(ColumnPtr& dst_col, ColumnPtr&& src_col);
    static void to_binary(ColumnPtr& column);
    std::shared_ptr<GermanStringExternalAllocator> get_allocator() const { return _allocator; }
    void set_allocator(std::shared_ptr<GermanStringExternalAllocator>&& allocator) {
        _allocator = std::move(allocator);
    }

private:
    void _append_string(const char* str, size_t len);
    template <typename T>
    ColumnPtr _to_binary_impl(size_t num_bytes) const;

    Container _data;
    std::shared_ptr<GermanStringExternalAllocator> _allocator = std::make_shared<GermanStringExternalAllocator>();
};

} // namespace starrocks
