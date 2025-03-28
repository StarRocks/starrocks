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

#include <cstdint>
#include <memory>
#include <stdexcept>

#include "column/array_column.h"
#include "column/column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"

namespace starrocks {

class ArrayViewColumn final : public CowFactory<ColumnFactory<Column, ArrayViewColumn>, ArrayViewColumn> {
    friend class CowFactory<ColumnFactory<Column, ArrayViewColumn>, ArrayViewColumn>;

public:
    using ValueType = void;

    ArrayViewColumn(ColumnPtr elements, UInt32Column::Ptr offsets, UInt32Column::Ptr lengths)
            : _elements(std::move(elements)), _offsets(std::move(offsets)), _lengths(std::move(lengths)) {}

    ArrayViewColumn(const ArrayViewColumn& rhs)
            : _elements(rhs._elements),
              _offsets(UInt32Column::static_pointer_cast(rhs._offsets->clone())),
              _lengths(UInt32Column::static_pointer_cast(rhs._lengths->clone())) {}

    ArrayViewColumn(ArrayViewColumn&& rhs) noexcept
            : _elements(std::move(rhs._elements)),
              _offsets(std::move(rhs._offsets)),
              _lengths(std::move(rhs._lengths)) {}

    ArrayViewColumn& operator=(const ArrayViewColumn& rhs) {
        ArrayViewColumn tmp(rhs);
        this->swap_column(tmp);
        return *this;
    }
    ArrayViewColumn& operator=(ArrayViewColumn&& rhs) noexcept {
        ArrayViewColumn tmp(std::move(rhs));
        this->swap_column(tmp);
        return *this;
    }

    ~ArrayViewColumn() override = default;

    bool is_array_view() const override { return true; }

    const uint8_t* raw_data() const override {
        DCHECK(false) << "ArrayViewColumn::raw_data() is not supported";
        throw std::runtime_error("ArrayViewColumn::raw_data() is not supported");
    }

    uint8_t* mutable_raw_data() override {
        DCHECK(false) << "ArrayViewColumn::mutable_raw_data() is not supported";
        throw std::runtime_error("ArrayViewColumn::mutable_raw_data() is not supported");
    }

    size_t size() const override { return _offsets->size(); }
    size_t capacity() const override { return _offsets->capacity() + _lengths->capacity(); }

    size_t type_size() const override { return _offsets->type_size() + _lengths->type_size(); }

    size_t byte_size() const override { return byte_size(0, size()); }
    size_t byte_size(size_t from, size_t size) const override {
        return _offsets->byte_size(from, size) + _lengths->byte_size(from, size);
    }

    size_t byte_size(size_t idx) const override { return _offsets->byte_size(idx) + _lengths->byte_size(idx); }

    void reserve(size_t n) override {
        _offsets->reserve(n);
        _lengths->reserve(n);
    }

    void resize(size_t n) override {
        _offsets->resize(n);
        _lengths->resize(n);
    }
    void assign(size_t n, size_t idx) override;

    void append_datum(const Datum& datum) override {
        DCHECK(false) << "ArrayViewColumn::append_datum() is not supported";
        throw std::runtime_error("ArrayViewColumn::append_datum() is not supported");
    }

    void append(const Column& src, size_t offset, size_t count) override;

    void append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) override;

    void append_value_multiple_times(const Column& src, uint32_t idx, uint32_t count) override;

    bool append_nulls(size_t count) override { return false; }

    size_t append_numbers(const void* buff, size_t length) override { return -1; }

    void append_value_multiple_times(const void* value, size_t count) override {
        DCHECK(false) << "ArrayViewColumn::append_value_multiple_times() is not supported";
    }

    void append_default() override { DCHECK(false) << "ArrayViewColumn::append_default() is not supported"; }
    void append_default(size_t count) override {
        DCHECK(false) << "ArrayViewColumn::append_default() is not supported";
    }

    void fill_default(const Filter& filter) override {
        DCHECK(false) << "ArrayViewColumn::fill_default() is not supported";
    }

    void update_rows(const Column& src, const uint32_t* indexes) override {
        DCHECK(false) << "ArrayViewColumn:::update_rows() is not supported";
    }

    void remove_first_n_values(size_t count) override;

    uint32_t max_one_element_serialize_size() const override;

    uint32_t serialize(size_t idx, uint8_t* pos) const override;

    uint32_t serialize_default(uint8_t* pos) const override;

    void serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sized, size_t chunk_size,
                         uint32_t max_one_row_size) const override;

    const uint8_t* deserialize_and_append(const uint8_t* pos) override;

    uint32_t serialize_size(size_t idx) const override;
    void deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) override;

    MutableColumnPtr clone_empty() const override;

    size_t filter_range(const Filter& filter, size_t from, size_t to) override;

    int compare_at(size_t left, size_t right, const Column& right_column, int nan_direction_hint) const override;

    void compare_column(const Column& rhs, std::vector<int8_t>* output) const;

    int equals(size_t left, const Column& rhs, size_t right, bool safe_eq = true) const override;

    void fnv_hash_at(uint32_t* seed, uint32_t idx) const override;
    void fnv_hash(uint32_t* hash, uint32_t from, uint32_t to) const override;

    void crc32_hash_at(uint32_t* seed, uint32_t idx) const override;
    void crc32_hash(uint32_t* hash, uint32_t from, uint32_t to) const override;

    int64_t xor_checksum(uint32_t from, uint32_t to) const override;

    void put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx, bool is_binary_protocol = false) const override;

    StatusOr<ColumnPtr> replicate(const Buffer<uint32_t>& offsets) override;

    std::string get_name() const override { return "array-view-" + _elements->get_name(); }

    Datum get(size_t idx) const override;

    size_t get_element_null_count(size_t idx) const;
    size_t get_element_size(size_t idx) const;

    bool set_null(size_t idx) override { return false; }

    size_t memory_usage() const override { return _elements->memory_usage() + _offsets->memory_usage(); }

    size_t container_memory_usage() const override {
        return _elements->container_memory_usage() + _offsets->container_memory_usage();
    }

    size_t reference_memory_usage(size_t from, size_t size) const override { return 0; }

    void swap_column(Column& rhs) override;

    void reset_column() override;

    const Column& elements() const { return *_elements; }
    const ColumnPtr elements_column() const { return _elements; }

    const UInt32Column& offsets() const { return *_offsets; }
    UInt32Column::Ptr& offsets_column() { return _offsets; }
    const UInt32Column& lengths() const { return *_lengths; }
    UInt32Column::Ptr& lengths_column() { return _lengths; }

    bool is_nullable() const override { return false; }

    std::string debug_item(size_t idx) const override;

    std::string debug_string() const override;

    Status capacity_limit_reached() const override {
        RETURN_IF_ERROR(_elements->capacity_limit_reached());
        return _offsets->capacity_limit_reached();
    }

    StatusOr<ColumnPtr> upgrade_if_overflow() override { return nullptr; }

    StatusOr<ColumnPtr> downgrade() override { return nullptr; }

    bool has_large_column() const override { return _elements->has_large_column(); }

    void check_or_die() const override;

    Status unfold_const_children(const starrocks::TypeDescriptor& type) override {
        return Status::NotSupported("ArrayViewColumn::unfold_const_children() is not supported");
    }

    // build array_view column from array_column
    // if array_column is nullable, return Nullable(ArrayViewColumn), otherwise return ArrayViewColumn
    static ColumnPtr from_array_column(const ColumnPtr& column);
    static ColumnPtr to_array_column(const ColumnPtr& column);
    ColumnPtr to_array_column() const;

    void mutate_each_subcolumn() override {
        // _elements
        _elements = (std::move(*_elements)).mutate();
        // offsets
        _offsets = UInt32Column::static_pointer_cast((std::move(*_offsets)).mutate());
        // _lengths
        _lengths = UInt32Column::static_pointer_cast((std::move(*_lengths)).mutate());
    }

private:
    ColumnPtr _elements;
    UInt32Column::Ptr _offsets;
    UInt32Column::Ptr _lengths;
};
} // namespace starrocks