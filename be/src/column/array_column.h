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

#include "column/column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"

namespace starrocks {

class ArrayColumn final : public ColumnFactory<Column, ArrayColumn> {
    friend class ColumnFactory<Column, ArrayColumn>;

public:
    using ValueType = void;

    ArrayColumn(ColumnPtr elements, UInt32Column::Ptr offsets);

    ArrayColumn(const ArrayColumn& rhs)
            : _elements(rhs._elements->clone_shared()),
              _offsets(std::static_pointer_cast<UInt32Column>(rhs._offsets->clone_shared())) {}

    ArrayColumn(ArrayColumn&& rhs) noexcept : _elements(std::move(rhs._elements)), _offsets(std::move(rhs._offsets)) {}

    ArrayColumn& operator=(const ArrayColumn& rhs) {
        ArrayColumn tmp(rhs);
        this->swap_column(tmp);
        return *this;
    }

    ArrayColumn& operator=(ArrayColumn&& rhs) noexcept {
        ArrayColumn tmp(std::move(rhs));
        this->swap_column(tmp);
        return *this;
    }

    ~ArrayColumn() override = default;

    bool is_array() const override { return true; }

    const uint8_t* raw_data() const override;

    uint8_t* mutable_raw_data() override;

    size_t size() const override;

    size_t capacity() const override;

    size_t type_size() const override { return sizeof(DatumArray); }

    size_t byte_size() const override { return _elements->byte_size() + _offsets->byte_size(); }
    size_t byte_size(size_t from, size_t size) const override;

    size_t byte_size(size_t idx) const override;

    void reserve(size_t n) override;

    void resize(size_t n) override;

    void assign(size_t n, size_t idx) override;

    void append_datum(const Datum& datum) override;

    void append(const Column& src, size_t offset, size_t count) override;

    // Append a single element, which is represented as a column
    void append_array_element(const Column& elem, size_t null_elem);

    void append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) override;

    void append_value_multiple_times(const Column& src, uint32_t index, uint32_t size, bool deep_copy) override;

    bool append_nulls(size_t count) override;

    bool append_strings(const Buffer<Slice>& strs) override { return false; }

    size_t append_numbers(const void* buff, size_t length) override { return -1; }

    void append_value_multiple_times(const void* value, size_t count) override;

    void append_default() override;

    void append_default(size_t count) override;

    void fill_default(const Filter& filter) override;

    Status update_rows(const Column& src, const uint32_t* indexes) override;

    void remove_first_n_values(size_t count) override;

    uint32_t max_one_element_serialize_size() const override;

    uint32_t serialize(size_t idx, uint8_t* pos) override;

    uint32_t serialize_default(uint8_t* pos) override;

    void serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                         uint32_t max_one_row_size) override;

    const uint8_t* deserialize_and_append(const uint8_t* pos) override;

    void deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) override;

    uint32_t serialize_size(size_t idx) const override;

    MutableColumnPtr clone_empty() const override;

    size_t filter_range(const Filter& filter, size_t from, size_t to) override;

    int compare_at(size_t left, size_t right, const Column& right_column, int nan_direction_hint) const override;
    void compare_column(const Column& rhs, std::vector<int8_t>* output) const;

    int equals(size_t left, const Column& rhs, size_t right, bool safe_eq = true) const override;

    void crc32_hash_at(uint32_t* seed, uint32_t idx) const override;
    void fnv_hash_at(uint32_t* seed, uint32_t idx) const override;
    void fnv_hash(uint32_t* hash, uint32_t from, uint32_t to) const override;

    void crc32_hash(uint32_t* hash, uint32_t from, uint32_t to) const override;

    int64_t xor_checksum(uint32_t from, uint32_t to) const override;

    void put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx) const override;

    std::string get_name() const override { return "array"; }

    Datum get(size_t idx) const override;

    std::pair<size_t, size_t> get_element_offset_size(size_t idx) const;
    size_t get_element_null_count(size_t idx) const;
    size_t get_element_size(size_t idx) const;

    bool set_null(size_t idx) override;

    size_t memory_usage() const override { return _elements->memory_usage() + _offsets->memory_usage(); }

    size_t container_memory_usage() const override {
        return _elements->container_memory_usage() + _offsets->container_memory_usage();
    }

    size_t reference_memory_usage(size_t from, size_t size) const override;

    void swap_column(Column& rhs) override;

    void reset_column() override;

    const Column& elements() const { return *_elements; }
    ColumnPtr& elements_column() { return _elements; }
    ColumnPtr elements_column() const { return _elements; }

    const UInt32Column& offsets() const { return *_offsets; }
    UInt32Column::Ptr& offsets_column() { return _offsets; }

    bool is_nullable() const override { return false; }

    std::string debug_item(size_t idx) const override;

    std::string debug_string() const override;

    bool capacity_limit_reached(std::string* msg = nullptr) const override {
        return _elements->capacity_limit_reached(msg) || _offsets->capacity_limit_reached(msg);
    }

    StatusOr<ColumnPtr> upgrade_if_overflow() override;

    StatusOr<ColumnPtr> downgrade() override;

    bool has_large_column() const override { return _elements->has_large_column(); }

    void check_or_die() const override;

    Status unfold_const_children(const starrocks::TypeDescriptor& type) override;

private:
    // Elements must be NullableColumn to facilitate handling nested types.
    ColumnPtr _elements;
    // Offsets column will store the start position of every array element.
    // Offsets store more one data to indicate the end position.
    // For example, [1, 2, 3], [4, 5, 6].
    // The two element array has three offsets(0, 3, 6)
    UInt32Column::Ptr _offsets;
};

} // namespace starrocks
