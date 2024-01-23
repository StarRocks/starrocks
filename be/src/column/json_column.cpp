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

#include "column/json_column.h"

#include <sstream>
#include <type_traits>

#include "column/bytes.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/compiler_util.h"
#include "glog/logging.h"
#include "gutil/casts.h"
#include "simd/simd.h"
#include "util/hash_util.hpp"
#include "util/mysql_row_buffer.h"

namespace starrocks {

void JsonColumn::append_datum(const Datum& datum) {
    BaseClass::append(datum.get<JsonValue*>());
}

int JsonColumn::compare_at(size_t left_idx, size_t right_idx, const starrocks::Column& rhs,
                           int nan_direction_hint) const {
    JsonValue* x = get_object(left_idx);
    const JsonValue* y = rhs.get(right_idx).get_json();
    return x->compare(*y);
}

void JsonColumn::fnv_hash(uint32_t* hash, uint32_t from, uint32_t to) const {
    for (uint32_t i = from; i < to; i++) {
        JsonValue* json = get_object(i);
        int64_t h = json->hash();
        hash[i] = HashUtil::fnv_hash(&h, sizeof(h), hash[i]);
    }
}

void JsonColumn::put_mysql_row_buffer(starrocks::MysqlRowBuffer* buf, size_t idx) const {
    JsonValue* value = get_object(idx);
    DCHECK(value != nullptr);
    auto json_str = value->to_string();
    if (!json_str.ok()) {
        buf->push_null();
    } else {
        buf->push_string(json_str->data(), json_str->size(), '\'');
    }
}

std::string JsonColumn::debug_item(size_t idx) const {
    if (is_flat_json()) {
        std::ostringstream ss;
        ss << "{";
        size_t i = 0;
        for (; i < _flat_column_paths.size() - i; i++) {
            ss << _flat_column_paths[i] << ": ";
            ss << get_flat_field(i)->debug_item(idx) << ", ";
        }
        ss << _flat_column_paths[i] << ": ";
        ss << get_flat_field(i)->debug_item(idx);
        ss << "}";
        return ss.str();
    } else {
        return get_object(idx)->to_string_uncheck();
    }
}

std::string JsonColumn::get_name() const {
    return "json";
}

MutableColumnPtr JsonColumn::clone() const {
    if (this->is_flat_json()) {
        auto p = this->create_mutable();
        p->_flat_column_paths = this->_flat_column_paths;
        for (auto& f : this->_flat_columns) {
            p->_flat_columns.emplace_back(f->clone());
        }
        return p;
    } else {
        return BaseClass::clone();
    }
}

MutableColumnPtr JsonColumn::clone_empty() const {
    return this->create_mutable();
}

ColumnPtr JsonColumn::clone_shared() const {
    if (this->is_flat_json()) {
        auto p = this->create_mutable();
        p->_flat_column_paths = this->_flat_column_paths;
        for (auto& f : this->_flat_columns) {
            p->_flat_columns.emplace_back(f->clone_shared());
        }
        return p;
    } else {
        return BaseClass::clone_shared();
    }
}

const uint8_t* JsonColumn::deserialize_and_append(const uint8_t* data) {
    JsonValue value((JsonValue::VSlice(data)));
    size_t size = value.serialize_size();
    append(std::move(value));
    return data + size;
}

uint32_t JsonColumn::serialize_size(size_t idx) const {
    return static_cast<uint32_t>(get_object(idx)->serialize_size());
}

uint32_t JsonColumn::serialize(size_t idx, uint8_t* pos) {
    return static_cast<uint32_t>(get_object(idx)->serialize(pos));
}

void JsonColumn::serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                                 uint32_t max_one_row_size) {
    for (size_t i = 0; i < chunk_size; ++i) {
        slice_sizes[i] += serialize(i, dst + i * max_one_row_size + slice_sizes[i]);
    }
}

ColumnPtr& JsonColumn::get_flat_field(const std::string& path) {
    if (_path_to_index.count(path) > 0) {
        return _flat_columns[_path_to_index.at(path)];
    }
    DCHECK(false) << "Json path: " << path << " not found!";
    return _flat_columns[0];
}

const ColumnPtr& JsonColumn::get_flat_field(const std::string& path) const {
    if (_path_to_index.count(path) > 0) {
        return _flat_columns[_path_to_index.at(path)];
    }
    DCHECK(false) << "Json path: " << path << " not found!";
    return _flat_columns[0];
}

ColumnPtr& JsonColumn::get_flat_field(int index) {
    DCHECK(index < _flat_columns.size());
    return _flat_columns[index];
}

const ColumnPtr& JsonColumn::get_flat_field(int index) const {
    DCHECK(index < _flat_columns.size());
    return _flat_columns[index];
}

void JsonColumn::init_flat_columns(const std::vector<std::string>& paths) {
    if (_flat_column_paths.empty()) {
        _flat_column_paths.insert(_flat_column_paths.cbegin(), paths.cbegin(), paths.cend());
        for (size_t i = 0; i < _flat_column_paths.size(); i++) {
            // nullable column
            _flat_columns.emplace_back(NullableColumn::create(JsonColumn::create(), NullColumn::create()));
            _path_to_index[_flat_column_paths[i]] = i;
        }
    } else {
        DCHECK(_flat_column_paths.size() == paths.size());
        DCHECK(_flat_columns.size() == paths.size());
        for (size_t i = 0; i < _flat_column_paths.size(); i++) {
            DCHECK(_flat_column_paths[i] == paths[i]);
            DCHECK(_flat_columns[i]->is_nullable());
        }
    }
}

// json column & flat column used
size_t JsonColumn::size() const {
    if (is_flat_json()) {
        return _flat_columns[0]->size();
    } else {
        return SuperClass::size();
    }
}

size_t JsonColumn::capacity() const {
    size_t s = SuperClass::capacity();
    for (const auto& col : _flat_columns) {
        s += col->capacity();
    }
    return s;
}

size_t JsonColumn::byte_size(size_t from, size_t size) const {
    size_t s = SuperClass::byte_size(from, size);
    for (const auto& col : _flat_columns) {
        s += col->byte_size(from, size);
    }
    return s;
}

void JsonColumn::append_value_multiple_times(const void* value, size_t count) {
    // JSON doesn't support default now
    DCHECK(!is_flat_json());
    return SuperClass::append_value_multiple_times(value, count);
}

void JsonColumn::append_default() {
    if (is_flat_json()) {
        for (auto& col : _flat_columns) {
            col->append_default();
        }
    } else {
        SuperClass::append_default();
    }
}

void JsonColumn::append_default(size_t count) {
    if (is_flat_json()) {
        for (auto& col : _flat_columns) {
            col->append_default(count);
        }
    } else {
        SuperClass::append_default(count);
    }
}

void JsonColumn::resize(size_t n) {
    BaseClass::resize(n);
    for (auto& col : _flat_columns) {
        col->resize(n);
    }
}

void JsonColumn::assign(size_t n, size_t idx) {
    BaseClass::assign(n, idx);
    for (auto& col : _flat_columns) {
        col->assign(n, idx);
    }
}

void JsonColumn::append(const JsonValue* object) {
    BaseClass::append(object);
}

void JsonColumn::append(JsonValue&& object) {
    BaseClass::append(object);
}

void JsonColumn::append(const JsonValue& object) {
    BaseClass::append(object);
}

void JsonColumn::append(const Column& src, size_t offset, size_t count) {
    const auto* other_json = down_cast<const JsonColumn*>(&src);
    if (other_json->is_flat_json() && !is_flat_json()) {
        // only hit in AggregateIterator (Aggregate mode in storage)
        DCHECK_EQ(0, this->size());
        init_flat_columns(other_json->_flat_column_paths);
    }

    if (is_flat_json()) {
        DCHECK(src.is_object());
        DCHECK(_flat_column_paths.size() == other_json->_flat_columns.size());
        DCHECK(_flat_columns.size() == other_json->_flat_column_paths.size());

        for (size_t i = 0; i < _flat_columns.size(); i++) {
            DCHECK_EQ(_flat_column_paths[i], other_json->_flat_column_paths[i]);
            _flat_columns[i]->append(*other_json->get_flat_field(i), offset, count);
        }
    } else {
        SuperClass::append(src, offset, count);
    }
}

size_t JsonColumn::filter_range(const Filter& filter, size_t from, size_t to) {
    if (is_flat_json()) {
        size_t result_offset = _flat_columns[0]->filter_range(filter, from, to);
        for (size_t i = 1; i < _flat_columns.size(); i++) {
            size_t tmp_offset = _flat_columns[i]->filter_range(filter, from, to);
            DCHECK_EQ(result_offset, tmp_offset);
        }
        return result_offset;
    } else {
        return SuperClass::filter_range(filter, from, to);
    }
}

size_t JsonColumn::container_memory_usage() const {
    size_t s = SuperClass::container_memory_usage();
    for (const auto& col : _flat_columns) {
        s += col->container_memory_usage();
    }
    return s;
}

size_t JsonColumn::reference_memory_usage() const {
    size_t s = SuperClass::reference_memory_usage();
    for (const auto& col : _flat_columns) {
        s += col->reference_memory_usage();
    }
    return s;
}

size_t JsonColumn::reference_memory_usage(size_t from, size_t size) const {
    size_t s = SuperClass::reference_memory_usage(from, size);
    for (const auto& col : _flat_columns) {
        s += col->reference_memory_usage(from, size);
    }
    return s;
}

void JsonColumn::swap_column(Column& rhs) {
    SuperClass::swap_column(rhs);
    JsonColumn& json_column = down_cast<JsonColumn&>(rhs);
    std::swap(_flat_column_paths, json_column._flat_column_paths);
    std::swap(_path_to_index, json_column._path_to_index);
    std::swap(_flat_columns, json_column._flat_columns);
}

void JsonColumn::reset_column() {
    SuperClass::reset_column();
    _flat_column_paths.clear();
    _flat_columns.clear();
    _path_to_index.clear();
}

bool JsonColumn::capacity_limit_reached(std::string* msg) const {
    if (size() > Column::MAX_CAPACITY_LIMIT) {
        if (msg != nullptr) {
            msg->append("row count of object column exceed the limit: " + std::to_string(Column::MAX_CAPACITY_LIMIT));
        }
        return true;
    }
    return false;
}

void JsonColumn::check_or_die() const {
    DCHECK(_flat_column_paths.size() == _flat_columns.size());
    if (!_flat_columns.empty()) {
        size_t rows = _flat_columns[0]->size();
        for (size_t i = 0; i < _flat_columns.size(); i++) {
            DCHECK(_flat_columns[i]->is_nullable());
            DCHECK(_flat_columns[i]->size() == rows);
            _flat_columns[i]->check_or_die();
        }
    }
}

bool JsonColumn::has_flat_column(const std::string& path) const {
    for (const auto& p : _flat_column_paths) {
        if (p == path) {
            return true;
        }
    }
    return false;
}

std::string JsonColumn::debug_flat_paths() const {
    if (_flat_column_paths.empty()) {
        return "[]";
    }
    std::ostringstream ss;
    ss << "[";
    for (size_t i = 0; i < _flat_column_paths.size() - 1; i++) {
        ss << _flat_column_paths[i] << ", ";
    }
    ss << _flat_column_paths.back() << "]";
    return ss.str();
}
} // namespace starrocks
