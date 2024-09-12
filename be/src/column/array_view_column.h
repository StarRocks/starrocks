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

#include "column/column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"

namespace starrocks {

class ArrayViewColumn final: public ColumnFactory<Column, ArrayViewColumn> {
    friend class ColumnFactory<Column, ArrayViewColumn>;

public:
    using ValueType = void; // @TODO need array view?

    ArrayViewColumn(ColumnPtr elements, UInt32Column::Ptr offsets, UInt32Column::Ptr lengths):
        _elements(std::move(elements)), _offsets(std::move(offsets)), _lengths(std::move(lengths)) {}

    ArrayViewColumn(const ArrayViewColumn& rhs)
        : _elements(rhs._elements),
          _offsets(std::static_pointer_cast<UInt32Column>(rhs._offsets->clone_shared())),
          _lengths(std::static_pointer_cast<UInt32Column>(rhs._lengths->clone_shared())) {}

    ArrayViewColumn(ArrayViewColumn&& rhs) noexcept:
     _elements(std::move(rhs._elements)), _offsets(std::move(rhs._offsets)), _lengths(std::move(rhs._lengths)) {}

    ArrayViewColumn& operator=(const ArrayViewColumn& rhs) {
        // @TODO
        return *this;
    }
    ArrayViewColumn& operator=(ArrayViewColumn&& rhs) noexcept {
        // @TODO
        return *this;
    }

    ~ArrayViewColumn() override = default;

    bool is_array_view() const override {
        return true;
    } 

    const uint8_t* raw_data() const override{
        DCHECK(false) << "ArrayViewColumn::raw_data() is not supported";
        return nullptr;
    }

    uint8_t* mutable_raw_data() override {
        DCHECK(false) << "ArrayViewColumn::mutable_raw_data() is not supported";
        return nullptr;
    }

    size_t size() const override {
        return _offsets->size();
    }
    size_t capacity() const override {
        return _offsets->capacity() + _lengths->capacity();
    }

    size_t type_size() const override {
        // @TODO need a array view type
        return 0;
    }

    size_t byte_size() const override {
        // @TODO
        return 0;
    }
    size_t byte_size(size_t from, size_t size) const override {
        // @TODO
        return 0;
    }

    size_t byte_size(size_t idx) const override {
        // @TODO
        return 0;
    }

    void reserve(size_t n) override {
        _elements->reserve(n);
        _offsets->reserve(n);
        _lengths->reserve(n);

    }
    void resize(size_t n) override {
        // DCHECK(false) << "ArrayViewColumn::resize() is not supported";
        _elements->resize(n);
        _offsets->resize(n);
        _lengths->resize(n);
    }
    void assign(size_t n, size_t idx) override {
        // @TODO
        DCHECK(false) << "ArrayViewColumn::assign() is not supported";
    }
    void append_datum(const Datum& datum) override {
        DCHECK(false) << "ArrayViewColumn::append_datum() is not supported";
    }

    void append(const Column& src, size_t offset, size_t count) override;

    void append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) override {
        DCHECK(false) << "ArrayViewColumn::append_selective() is not supported";
    }

    void append_value_multiple_times(const Column& src, uint32_t idx, uint32_t size) override {
        DCHECK(false) << "ArrayViewColumn::append_value_multiple_times() is not supported";
    }

    bool append_nulls(size_t count) override {
        // @TODO
        return false;
    }

    size_t append_numbers(const void* buff, size_t length) override {
        // @TODO
        return -1;
    }
    void append_value_multiple_times(const void* value, size_t count) override {
        DCHECK(false) << "ArrayViewColumn::append_value_multiple_times() is not supported";
    }

    void append_default() override {
        DCHECK(false) << "ArrayViewColumn::append_default() is not supported";
    }
    void append_default(size_t count) override {
        DCHECK(false) << "ArrayViewColumn::append_default() is not supported";
    }
    void fill_default(const Filter& filter) override {
        // @TODO
    }
    void update_rows(const Column& src, const uint32_t* indexes) override {
        DCHECK(false) << "ArrayViewColumn:::update_rows() is not supported";
    }
    void remove_first_n_values(size_t count) override {
        // @TODO
    }
    uint32_t max_one_element_serialize_size() const override {
        DCHECK(false) << "ArrayViewColumn::max_one_element_serialize_size() is not supported";
        return 0;
    }
    uint32_t serialize(size_t idx, uint8_t* pos) override {
        DCHECK(false);
        return 0;
    }
    uint32_t serialize_default(uint8_t* pos) override {
        DCHECK(false);
        return 0;
    }

    void serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sized, size_t chunk_size, uint32_t max_one_row_size) override {
        DCHECK(false);
    }
    const uint8_t* deserialize_and_append(const uint8_t* pos) override {
        DCHECK(false);
        return nullptr;
    }

    uint32_t serialize_size(size_t idx) const override {
        DCHECK(false);
        return 0;
    }
    void deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) override {
        DCHECK(false);
    }

    MutableColumnPtr clone_empty() const override;

    size_t filter_range(const Filter& filter, size_t from, size_t to) override {
        // @TODO
        return 0;
    }

    int compare_at(size_t left, size_t rifht, const Column& right_column, int nan_direction_hint) const override {
        // @TODO
        return 0;
    }

    void compare_column(const Column& rhs, std::vector<int8_t>* output) const {

    }

    int equals(size_t left, const Column& rhs, size_t right, bool safe_eq = true) const override {
        return 0;
    }

    void crc32_hash_at(uint32_t *seed, uint32_t idx) const override {

    }
    void fnv_hash_at(uint32_t* seed, uint32_t idx) const override {

    }
    void fnv_hash(uint32_t* hash, uint32_t from, uint32_t to) const override {

    }

    void crc32_hash(uint32_t* hash, uint32_t from, uint32_t to) const override {

    }

    int64_t xor_checksum(uint32_t from, uint32_t to) const override {
        return 0;
    }

    void put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx, bool is_binary_protocol = false) const override {

    }

    ColumnPtr replicate(const Buffer<uint32_t>& offsets) override;

    std::string get_name() const override { return "array-view"; }

    Datum get(size_t idx) const override {
        return Datum();
    }

    size_t get_element_null_count(size_t idx) const {
        return 0;
    }
    size_t get_element_size(size_t idx) const {
        return 0;
    }

    bool set_null(size_t idx) override {
        return false;
    }

    size_t memory_usage() const override { return _elements->memory_usage() + _offsets->memory_usage(); }

    size_t container_memory_usage() const override {
        return _elements->container_memory_usage() + _offsets->container_memory_usage();
    }

    size_t reference_memory_usage(size_t from, size_t size) const override {
        return 0;
    }

    void swap_column(Column& rhs) override {}

    void reset_column() override {}

    const Column& elements() const { return *_elements; }
    ColumnPtr& elements_column() { return _elements; }
    ColumnPtr elements_column() const { return _elements; }

    const UInt32Column& offsets() const { return *_offsets; }
    UInt32Column::Ptr& offsets_column() { return _offsets; }
    const UInt32Column& lengths() const {
        return *_lengths;
    }
    UInt32Column::Ptr& lengths_column() { return _lengths; }

    bool is_nullable() const override { return false; }

    std::string debug_item(size_t idx) const override {
        return "";
    }

    std::string debug_string() const override {
        return "array-view-column";
    }

    Status capacity_limit_reached() const override {
        RETURN_IF_ERROR(_elements->capacity_limit_reached());
        return _offsets->capacity_limit_reached();
    }

    StatusOr<ColumnPtr> upgrade_if_overflow() override {
        return nullptr;
    }

    StatusOr<ColumnPtr> downgrade() override {
        return nullptr;
    }

    bool has_large_column() const override { return _elements->has_large_column(); }

    void check_or_die() const override;

    Status unfold_const_children(const starrocks::TypeDescriptor& type) override {
        return Status::NotSupported("TBD");
    }
    
    // build array_view column from array_column, how to solve null??
    // if array_column is nullable, return Nullable(ArrayViewColumn)
    // else return ArrayViewColumn
    static StatusOr<ColumnPtr> from_array_column(const ColumnPtr& column);
    static StatusOr<ColumnPtr> to_array_column(const ColumnPtr& column);
    // @TODO to_array_column
    StatusOr<ColumnPtr> to_array_column() const;

private:
    // Elements must be NullableColumn to facilitate handling nested types.
    ColumnPtr _elements;
    UInt32Column::Ptr _offsets;
    UInt32Column::Ptr _lengths;
};
}