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
#include "column/column.h"
#include "column/fixed_length_column.h"

namespace starrocks {
class StructColumn final : public CowFactory<ColumnFactory<Column, StructColumn>, StructColumn> {
    friend class CowFactory<ColumnFactory<Column, StructColumn>, StructColumn>;
    using Base = CowFactory<ColumnFactory<Column, StructColumn>, StructColumn>;

public:
    using ValueType = void;
    using Container = Buffer<std::string>;

    // Used to construct an unnamed struct
    StructColumn(MutableColumns&& fields);
    StructColumn(MutableColumns&& fields, std::vector<std::string> field_names);
    StructColumn(const StructColumn& rhs) {
        for (const auto& field : rhs._fields) {
            _fields.emplace_back(field->clone());
        }
        _field_names = rhs._field_names;
    }
    StructColumn(StructColumn&& rhs) noexcept
            : _fields(std::move(rhs._fields)), _field_names(std::move(rhs._field_names)) {}

    static Ptr create(const Columns& columns, std::vector<std::string> field_names);
    static Ptr create(const Columns& columns);

    static MutablePtr create(MutableColumns&& columns, std::vector<std::string> field_names) {
        return Base::create(std::move(columns), std::move(field_names));
    }
    static MutablePtr create(MutableColumns&& columns) { return Base::create(std::move(columns)); }

    ~StructColumn() override = default;

    bool is_struct() const override;

    const uint8_t* raw_data() const override;

    uint8_t* mutable_raw_data() override;

    size_t size() const override;

    size_t capacity() const override;

    size_t type_size() const override;

    size_t byte_size() const override;

    size_t byte_size(size_t idx) const override;

    size_t byte_size(size_t from, size_t size) const override;

    void reserve(size_t n) override;

    void resize(size_t n) override;

    StatusOr<ColumnPtr> upgrade_if_overflow() override;

    StatusOr<ColumnPtr> downgrade() override;

    bool has_large_column() const override;

    void assign(size_t n, size_t idx) override;

    void append_datum(const Datum& datum) override;

    void remove_first_n_values(size_t count) override;

    void append(const Column& src, size_t offset, size_t count) override;

    void fill_default(const Filter& filter) override;

    void update_rows(const Column& src, const uint32_t* indexes) override;

    void append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) override;

    void append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) override;

    [[nodiscard]] bool append_nulls(size_t count) override;

    [[nodiscard]] size_t append_numbers(const void* buff, size_t length) override;

    void append_value_multiple_times(const void* value, size_t count) override;

    void append_default() override;

    void append_default(size_t count) override;

    uint32_t serialize(size_t idx, uint8_t* pos) const override;

    uint32_t serialize_default(uint8_t* pos) const override;

    void serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                         uint32_t max_one_row_size) const override;

    const uint8_t* deserialize_and_append(const uint8_t* pos) override;

    void deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) override;

    uint32_t max_one_element_serialize_size() const override;

    uint32_t serialize_size(size_t idx) const override;

    MutableColumnPtr clone_empty() const override;

    size_t filter_range(const Filter& filter, size_t from, size_t to) override;

    int compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const override;

    int equals(size_t left, const Column& rhs, size_t right, bool safe_eq = true) const override;

    void fnv_hash(uint32_t* seed, uint32_t from, uint32_t to) const override;

    void crc32_hash(uint32_t* seed, uint32_t from, uint32_t to) const override;

    int64_t xor_checksum(uint32_t from, uint32_t to) const override;

    void put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx, bool is) const override;

    std::string debug_item(size_t idx) const override;

    std::string debug_string() const override;

    std::string get_name() const override;

    Datum get(size_t n) const override;

    size_t memory_usage() const override;

    size_t container_memory_usage() const override;

    size_t reference_memory_usage(size_t from, size_t size) const override;

    void swap_column(Column& rhs) override;

    void reset_column() override;

    Status capacity_limit_reached() const override;

    void check_or_die() const override;

    const Columns& fields() const { return _fields; }
    const Columns& fields_column() const { return _fields; }
    Columns& fields_column() { return _fields; }

    const ColumnPtr& field_column(const std::string& field_name) const;
    ColumnPtr& field_column(const std::string& field_name);

    const std::vector<std::string>& field_names() const { return _field_names; }

    Status unfold_const_children(const TypeDescriptor& type) override;

    void mutate_each_subcolumn() override {
        for (auto& column : _fields) {
            column = (std::move(*column)).mutate();
        }
    }

private:
    size_t _find_field_idx_by_name(const std::string& field_name) const;

    // A collection that contains StructType's subfield column.
    Columns _fields;

    // A collection that contains each struct subfield name.
    // _fields and _field_names should have the same size (_fields.size() == _field_names.size()).
    // _field_names will not participate in serialization because it is created based on meta information
    // must be nullable column
    std::vector<std::string> _field_names;
};

} // namespace starrocks
