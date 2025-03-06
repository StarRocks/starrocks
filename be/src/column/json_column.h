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

#include <unordered_map>
#include <utility>
#include <vector>

#include "column/column.h"
#include "column/object_column.h"
#include "column/vectorized_fwd.h"
#include "types/logical_type.h"
#include "util/json.h"

namespace starrocks {

// JsonColumn column for JSON type
// format_version 1: store each JSON in binary encoding individually
// format_version 2: TODO columnar encoding for JSON
class JsonColumn final : public CowFactory<ColumnFactory<ObjectColumn<JsonValue>, JsonColumn>, JsonColumn, Column> {
public:
    using ValueType = JsonValue;
    using SuperClass = CowFactory<ColumnFactory<ObjectColumn<JsonValue>, JsonColumn>, JsonColumn, Column>;
    using BaseClass = JsonColumnBase;

    JsonColumn() = default;
    explicit JsonColumn(size_t size) : SuperClass(size) {}
    JsonColumn(const JsonColumn& rhs) : SuperClass(rhs) {}

    JsonColumn(JsonColumn&& rhs) noexcept : SuperClass(std::move(rhs)) {
        _flat_columns = std::move(rhs._flat_columns);
        _flat_column_paths = std::move(rhs._flat_column_paths);
        _flat_column_types = std::move(rhs._flat_column_types);
    }

    MutableColumnPtr clone() const override;
    MutableColumnPtr clone_empty() const override { return this->create(); }

    void append_datum(const Datum& datum) override;
    void put_mysql_row_buffer(starrocks::MysqlRowBuffer* buf, size_t idx,
                              bool is_binary_protocol = false) const override;
    std::string get_name() const override;
    bool is_json() const override { return true; }

    const uint8_t* deserialize_and_append(const uint8_t* pos) override;
    uint32_t serialize_size(size_t idx) const override;
    uint32_t serialize(size_t idx, uint8_t* pos) const override;
    void serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                         uint32_t max_one_row_size) const override;

    // json column & flat column may used
    std::string debug_item(size_t idx) const override;
    size_t size() const override;
    size_t capacity() const override;
    size_t byte_size(size_t from, size_t size) const override;

    void resize(size_t n) override;

    void reserve(size_t n) override{};

    void assign(size_t n, size_t idx) override;

    void append(const JsonValue* object);

    void append(JsonValue&& object);

    void append(const JsonValue& object);

    void append(const Column& src, size_t offset, size_t count) override;

    void append_value_multiple_times(const void* value, size_t count) override;

    void append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) override;

    void append_default() override;

    void append_default(size_t count) override;

    size_t filter_range(const Filter& filter, size_t from, size_t to) override;
    int compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const override;

    void fnv_hash(uint32_t* seed, uint32_t from, uint32_t to) const override;

    size_t container_memory_usage() const override;
    size_t reference_memory_usage() const override;
    size_t reference_memory_usage(size_t from, size_t size) const override;

    void swap_column(Column& rhs) override;
    void reset_column() override;

    Status capacity_limit_reached() const override;
    void check_or_die() const override;

    // support flat json on storage
    bool is_flat_json() const { return !_flat_columns.empty(); }

    ColumnPtr& get_flat_field(const std::string& path);

    const ColumnPtr& get_flat_field(const std::string& path) const;

    LogicalType get_flat_field_type(const std::string& path) const;

    Columns& get_flat_fields() { return _flat_columns; };

    const Columns& get_flat_fields() const { return _flat_columns; };

    Columns get_flat_fields_ptrs() const {
        Columns columns;
        columns.reserve(_flat_columns.size());
        columns.assign(_flat_columns.begin(), _flat_columns.end());
        return columns;
    };

    ColumnPtr& get_flat_field(int index);

    const ColumnPtr& get_flat_field(int index) const;

    ColumnPtr& get_remain();

    const ColumnPtr& get_remain() const;

    const std::vector<std::string>& flat_column_paths() const { return _flat_column_paths; }

    const std::vector<LogicalType>& flat_column_types() const { return _flat_column_types; }

    bool has_flat_column(const std::string& path) const;

    bool has_remain() const { return _flat_columns.size() == (_flat_column_paths.size() + 1); }

    void set_flat_columns(const std::vector<std::string>& paths, const std::vector<LogicalType>& types,
                          const Columns& flat_columns);

    bool is_equallity_schema(const Column* other) const;

    std::string debug_flat_paths() const;

    void mutate_each_subcolumn() override {
        for (auto& column : _flat_columns) {
            column = (std::move(*column)).mutate();
        }
    }

private:
    // flat-columns[sub_columns, remain_column]
    Columns _flat_columns;

    // flat-column paths, doesn't contains remain column
    std::vector<std::string> _flat_column_paths;
    std::vector<LogicalType> _flat_column_types;
    std::unordered_map<std::string, int> _path_to_index;
};

} // namespace starrocks
