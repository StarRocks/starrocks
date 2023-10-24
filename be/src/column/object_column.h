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

#include <memory>

#include "column/column.h"
#include "column/datum.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "types/bitmap_value.h"
#include "types/hll.h"
#include "util/percentile_value.h"

namespace starrocks {

//class Object {
//    Object();
//
//    Object(const Slice& s);
//
//    void clear();
//
//    size_t serialize_size() const;
//    size_t serialize(uint8_t* dst) const;
//};

template <typename T>
class ObjectColumn : public ColumnFactory<Column, ObjectColumn<T>> {
    friend class ColumnFactory<Column, ObjectColumn>;

public:
    using ValueType = T;
    using Container = Buffer<ValueType*>;

    ObjectColumn() = default;

    explicit ObjectColumn(size_t size) : _pool(size) {}

    ObjectColumn(const ObjectColumn& column) { DCHECK(false) << "Can't copy construct object column"; }

    ObjectColumn(ObjectColumn&& object_column) noexcept : _pool(std::move(object_column._pool)) {}

    void operator=(const ObjectColumn&) = delete;

    ObjectColumn& operator=(ObjectColumn&& rhs) noexcept {
        ObjectColumn tmp(std::move(rhs));
        this->swap_column(tmp);
        return *this;
    }

    ~ObjectColumn() override = default;

    bool is_object() const override { return true; }

    const uint8_t* raw_data() const override {
        _build_slices();
        return reinterpret_cast<const uint8_t*>(_slices.data());
    }

    uint8_t* mutable_raw_data() override {
        _build_slices();
        return reinterpret_cast<uint8_t*>(_slices.data());
    }

    size_t size() const override { return _pool.size(); }

    size_t capacity() const override { return _pool.capacity(); }

    size_t type_size() const override { return sizeof(T); }

    size_t byte_size() const override { return byte_size(0, size()); }
    size_t byte_size(size_t from, size_t size) const override;

    size_t byte_size(size_t idx) const override;

    void reserve(size_t n) override { _pool.reserve(n); }

    void resize(size_t n) override { _pool.resize(n); }

    void assign(size_t n, size_t idx) override;

    void append(const T* object);

    void append(T&& object);

    void append(const T& object);

    void append_datum(const Datum& datum) override { append(datum.get<T*>()); }

    void remove_first_n_values(size_t count) override;

    void append(const Column& src, size_t offset, size_t count) override;

    void append_shallow_copy(const Column& src, size_t offset, size_t count) override;

    void append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) override;

    void append_selective_shallow_copy(const Column& src, const uint32_t* indexes, uint32_t from,
                                       uint32_t size) override;

    void append_value_multiple_times(const Column& src, uint32_t index, uint32_t size, bool deep_copy) override;

    bool append_nulls(size_t count) override { return false; }

    bool append_strings(const Buffer<Slice>& strs) override;

    size_t append_numbers(const void* buff, size_t length) override { return -1; }

    // append from slice, call in SCAN_NODE append default values
    void append_value_multiple_times(const void* value, size_t count) override;

    void append_default() override;

    void append_default(size_t count) override;

    void fill_default(const Filter& filter) override;

    void update_rows(const Column& src, const uint32_t* indexes) override;

    uint32_t serialize(size_t idx, uint8_t* pos) override;
    uint32_t serialize_default(uint8_t* pos) override;

    void serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                         uint32_t max_one_row_size) override;

    const uint8_t* deserialize_and_append(const uint8_t* pos) override;

    void deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) override;

    uint32_t serialize_size(size_t idx) const override;

    MutableColumnPtr clone_empty() const override { return this->create_mutable(); }

    MutableColumnPtr clone() const override;

    ColumnPtr clone_shared() const override;

    size_t filter_range(const Filter& filter, size_t from, size_t to) override;

    int compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const override;

    void fnv_hash(uint32_t* seed, uint32_t from, uint32_t to) const override;

    void crc32_hash(uint32_t* hash, uint32_t from, uint32_t to) const override;

    int64_t xor_checksum(uint32_t from, uint32_t to) const override;

    void put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx) const override;

    std::string get_name() const override { return std::string{"object"}; }

    T* get_object(size_t n) const { return const_cast<T*>(&_pool[n]); }

    Buffer<T*>& get_data() {
        _build_cache();
        return _cache;
    }

    const Buffer<T*>& get_data() const {
        _build_cache();
        return _cache;
    }

    Datum get(size_t n) const override { return Datum(get_object(n)); }

    size_t container_memory_usage() const override { return _pool.capacity() * type_size(); }

    size_t reference_memory_usage() const override { return byte_size(); }

    size_t reference_memory_usage(size_t from, size_t size) const override { return byte_size(from, size); }

    void swap_column(Column& rhs) override {
        auto& r = down_cast<ObjectColumn&>(rhs);
        std::swap(this->_delete_state, r._delete_state);
        std::swap(this->_pool, r._pool);
        std::swap(this->_cache_ok, r._cache_ok);
        std::swap(this->_cache, r._cache);
        std::swap(this->_buffer, r._buffer);
        std::swap(this->_slices, r._slices);
    }

    void reset_column() override {
        Column::reset_column();
        _pool.clear();
        _cache_ok = false;
        _cache.clear();
        _slices.clear();
        _buffer.clear();
    }

    void reset_cache() {
        _cache_ok = false;
        _cache.clear();
    }

    Buffer<T>& get_pool() { return _pool; }

    const Buffer<T>& get_pool() const { return _pool; }

    std::string debug_item(size_t idx) const override;

    std::string debug_string() const override {
        std::stringstream ss;
        ss << "[";
        size_t size = this->size();
        for (size_t i = 0; i < size - 1; ++i) {
            ss << debug_item(i) << ", ";
        }
        if (size > 0) {
            ss << debug_item(size - 1);
        }
        ss << "]";
        return ss.str();
    }

    bool capacity_limit_reached(std::string* msg = nullptr) const override {
        if (_pool.size() > Column::MAX_CAPACITY_LIMIT) {
            if (msg != nullptr) {
                msg->append("row count of object column exceed the limit: " +
                            std::to_string(Column::MAX_CAPACITY_LIMIT));
            }
            return true;
        }
        return false;
    }

    StatusOr<ColumnPtr> upgrade_if_overflow() override;

    StatusOr<ColumnPtr> downgrade() override { return nullptr; }

    bool has_large_column() const override { return false; }

private:
    // add this to avoid warning clang-diagnostic-overloaded-virtual
    using Column::append;

    void _build_cache() const {
        if (_cache_ok) {
            return;
        }

        _cache.clear();
        _cache.reserve(_pool.size());
        for (int i = 0; i < _pool.size(); ++i) {
            _cache.emplace_back(const_cast<T*>(&_pool[i]));
        }

        _cache_ok = true;
    }

    // Currently, only for data loading
    void _build_slices() const;

    void check_or_die() const override {}

private:
    Buffer<T> _pool;
    mutable bool _cache_ok = false;
    mutable Buffer<T*> _cache;

    // Only for data loading
    mutable Buffer<Slice> _slices;
    mutable Buffer<uint8_t> _buffer;
};
} // namespace starrocks
