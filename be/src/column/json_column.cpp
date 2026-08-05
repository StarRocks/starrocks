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

#include <velocypack/Slice.h>

#include <sstream>

#include "base/hash/hash_util.hpp"
#include "column/flat_json/json_merger.h"
#include "column/mysql_row_buffer.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/compiler_util.h"
#include "glog/logging.h"
#include "gutil/casts.h"
#include "gutil/strings/substitute.h"
#include "types/logical_type.h"

namespace starrocks {

void JsonColumn::append_datum(const Datum& datum) {
    BaseClass::append(datum.get<JsonValue*>());
}

bool JsonColumn::append_strings_overflow(const Slice* data, size_t size, size_t max_length) {
    for (size_t i = 0; i < size; i++) {
        const auto& s = data[i];
        append(JsonValue(s));
    }
    return true;
}

int JsonColumn::compare_at(size_t left_idx, size_t right_idx, const starrocks::Column& rhs,
                           int nan_direction_hint) const {
    JsonValue* x = get_object(left_idx);
    const JsonValue* y = rhs.get(right_idx).get_json();
    return x->compare(*y);
}

void JsonColumn::put_mysql_row_buffer(starrocks::MysqlRowBuffer* buf, size_t idx, bool is_binary_protocol) const {
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
        // flat json debug is different with normal, lose quota
        for (; i < _flat_column_paths.size() - 1; i++) {
            ss << _flat_column_paths[i] << ": ";
            ss << get_flat_field(i)->debug_item(idx) << ", ";
        }
        ss << _flat_column_paths[i] << ": ";
        ss << get_flat_field(i)->debug_item(idx);
        if (has_remain()) {
            ss << ", remain: " << get_remain()->debug_item(idx);
        }
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
        auto p = this->create();
        p->_flat_column_paths = this->_flat_column_paths;
        p->_flat_column_types = this->_flat_column_types;
        p->_path_to_index = this->_path_to_index;
        for (auto& f : this->_flat_columns) {
            p->_flat_columns.emplace_back(f->clone());
        }
        return p;
    } else {
        return BaseClass::clone();
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

uint32_t JsonColumn::serialize(size_t idx, uint8_t* pos) const {
    return static_cast<uint32_t>(get_object(idx)->serialize(pos));
}

void JsonColumn::serialize_batch(uint8_t* dst, Buffer<uint32_t>& slice_sizes, size_t chunk_size,
                                 uint32_t max_one_row_size) const {
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

ColumnPtr& JsonColumn::get_remain() {
    DCHECK(_flat_columns.size() == _flat_column_paths.size() + 1);
    return _flat_columns[_flat_columns.size() - 1];
}

const ColumnPtr& JsonColumn::get_remain() const {
    DCHECK(_flat_columns.size() == _flat_column_paths.size() + 1);
    return _flat_columns[_flat_columns.size() - 1];
}

LogicalType JsonColumn::get_flat_field_type(const std::string& path) const {
    DCHECK(_path_to_index.count(path) > 0);
    return _flat_column_types[_path_to_index.at(path)];
}

void JsonColumn::set_flat_columns(const std::vector<std::string>& paths, const std::vector<LogicalType>& types,
                                  MutableColumns&& flat_columns) {
    DCHECK_EQ(paths.size(), types.size());
    DCHECK(paths.size() == flat_columns.size() || paths.size() + 1 == flat_columns.size()); // may remain column

    if (is_flat_json()) {
        DCHECK_EQ(_flat_columns.size(), flat_columns.size());
        DCHECK_EQ(_flat_column_paths.size(), paths.size());
        DCHECK_EQ(_flat_column_types.size(), types.size());
        DCHECK_EQ(_path_to_index.size(), paths.size());
        for (size_t i = 0; i < _flat_column_paths.size(); i++) {
            DCHECK_EQ(_flat_column_paths[i], paths[i]);
            DCHECK_EQ(_flat_column_types[i], types[i]);
            DCHECK_EQ(i, _path_to_index[paths[i]]);
        }
        if (flat_columns.size() != 0) {
            for (size_t i = 0; i < _flat_columns.size(); i++) {
                _flat_columns[i]->append(*flat_columns[i], 0, flat_columns[i]->size());
            }
        } else {
            // change column ptr to wrapper ptr
            _flat_columns.reserve(flat_columns.size());
            for (auto& col : flat_columns) {
                _flat_columns.emplace_back(std::move(col));
            }
        }
    } else {
        _flat_column_paths = paths;
        _flat_column_types = types;
        // change column ptr to wrapper ptr
        _flat_columns.reserve(flat_columns.size());
        for (auto& col : flat_columns) {
            _flat_columns.emplace_back(std::move(col));
        }

        for (size_t i = 0; i < _flat_column_paths.size(); i++) {
            _path_to_index[_flat_column_paths[i]] = i;
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
    if (is_flat_json()) {
        size_t s = 0;
        for (const auto& col : _flat_columns) {
            s += col->capacity();
        }
        return s;
    } else {
        return SuperClass::capacity();
    }
}

size_t JsonColumn::byte_size(size_t from, size_t size) const {
    if (is_flat_json()) {
        size_t s = 0;
        for (const auto& col : _flat_columns) {
            s += col->byte_size(from, size);
        }
        return s;
    } else {
        return SuperClass::byte_size(from, size);
    }
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

void JsonColumn::append_selective(const Column& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
    if (src.is_json_view()) {
        src.append_selective_to(*this, indexes, from, size);
        return;
    }
    const auto* other_json = down_cast<const JsonColumn*>(&src);
    if (other_json->is_flat_json() && !is_flat_json() && this->size() == 0) {
        // only hit in AggregateIterator (Aggregate mode in storage)
        MutableColumns copy;
        copy.reserve(other_json->_flat_columns.size());
        for (const auto& col : other_json->_flat_columns) {
            copy.emplace_back(col->clone_empty());
        }
        set_flat_columns(other_json->flat_column_paths(), other_json->flat_column_types(), std::move(copy));
    }

    if (LIKELY(is_equallity_schema(other_json))) {
        if (is_flat_json()) {
            for (size_t i = 0; i < _flat_columns.size(); i++) {
                _flat_columns[i]->append_selective(*other_json->get_flat_field(i), indexes, from, size);
            }
        } else {
            SuperClass::append_selective(src, indexes, from, size);
        }
        return;
    }

    // Appending nothing must not cost this column its flat representation.
    if (size == 0) {
        return;
    }

    ColumnPtr plain_src = other_json->unflatten();
    _degrade_to_plain_json(*other_json);
    SuperClass::append_selective(plain_src != nullptr ? *plain_src : src, indexes, from, size);
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
    if (is_flat_json()) {
        DCHECK_EQ(0, BaseClass::size());
        for (auto& col : _flat_columns) {
            col->resize(n);
        }
    } else {
        for (auto& col : _flat_columns) {
            DCHECK_EQ(0, col->size());
        }
        BaseClass::resize(n);
    }
}

void JsonColumn::assign(size_t n, size_t idx) {
    if (is_flat_json()) {
        for (auto& col : _flat_columns) {
            col->assign(n, idx);
        }
    } else {
        BaseClass::assign(n, idx);
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
    if (other_json->is_flat_json() && !is_flat_json() && this->size() == 0) {
        // only hit in AggregateIterator (Aggregate mode in storage)
        MutableColumns copy;
        copy.reserve(other_json->_flat_columns.size());
        for (const auto& col : other_json->_flat_columns) {
            copy.emplace_back(col->clone_empty());
        }
        set_flat_columns(other_json->flat_column_paths(), other_json->flat_column_types(), std::move(copy));
    }

    if (LIKELY(is_equallity_schema(other_json))) {
        if (is_flat_json()) {
            for (size_t i = 0; i < _flat_columns.size(); i++) {
                _flat_columns[i]->append(*other_json->get_flat_field(i), offset, count);
            }
        } else {
            SuperClass::append(src, offset, count);
        }
        return;
    }

    // Appending nothing must not cost this column its flat representation.
    if (count == 0) {
        return;
    }

    ColumnPtr plain_src = other_json->unflatten();
    _degrade_to_plain_json(*other_json);
    SuperClass::append(plain_src != nullptr ? *plain_src : src, offset, count);
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
    std::swap(_flat_column_types, json_column._flat_column_types);
    std::swap(_path_to_index, json_column._path_to_index);
    std::swap(_flat_columns, json_column._flat_columns);
}

void JsonColumn::reset_column() {
    SuperClass::reset_column();
    _flat_column_paths.clear();
    _flat_column_types.clear();
    _flat_columns.clear();
    _path_to_index.clear();
}

Status JsonColumn::capacity_limit_reached() const {
    if (size() > Column::MAX_CAPACITY_LIMIT) {
        return Status::CapacityLimitExceed(strings::Substitute("row count of object column exceed the limit: $0",
                                                               std::to_string(Column::MAX_CAPACITY_LIMIT)));
    }
    return Status::OK();
}

void JsonColumn::check_or_die() const {
    if (has_remain()) {
        DCHECK(_flat_column_paths.size() + 1 == _flat_columns.size());
        DCHECK(_flat_column_types.size() + 1 == _flat_columns.size());
    } else {
        DCHECK(_flat_column_paths.size() == _flat_columns.size());
        DCHECK(_flat_column_types.size() == _flat_columns.size());
    }
    if (!_flat_columns.empty()) {
        size_t rows = _flat_columns[0]->size();
        for (size_t i = 0; i < _flat_columns.size() - 1; i++) {
            DCHECK(_flat_columns[i]->is_nullable());
            DCHECK(_flat_columns[i]->size() == rows);
            _flat_columns[i]->check_or_die();
        }
        DCHECK(has_remain() ? _flat_columns.back()->is_json() : _flat_columns.back()->is_nullable());
        DCHECK(_flat_columns.back()->size() == rows);
        _flat_columns.back()->check_or_die();
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

bool JsonColumn::is_equallity_schema(const Column* other) const {
    if (!other->is_json()) {
        return false;
    }
    auto* other_json = down_cast<const JsonColumn*>(other);
    if (this->is_flat_json() && other_json->is_flat_json()) {
        if (this->_flat_column_paths.size() != other_json->_flat_column_paths.size()) {
            return false;
        }
        for (size_t i = 0; i < this->_flat_column_paths.size(); i++) {
            if (this->_flat_column_paths[i] != other_json->_flat_column_paths[i]) {
                return false;
            }
            if (this->_flat_column_types[i] != other_json->_flat_column_types[i]) {
                return false;
            }
        }
        return _flat_columns.size() == other_json->_flat_columns.size();
    }
    return !this->is_flat_json() && !other_json->is_flat_json();
}

ColumnPtr JsonColumn::unflatten() const {
    if (!is_flat_json()) {
        return nullptr;
    }
    JsonMerger merger(_flat_column_paths, _flat_column_types, has_remain());
    // merge() keeps the result alive through the returned ColumnPtr, so it outlives `merger`.
    return merger.merge(get_flat_fields());
}

void JsonColumn::_clear_flat_schema() {
    _flat_columns.clear();
    _flat_column_paths.clear();
    _flat_column_types.clear();
    _path_to_index.clear();
}

void JsonColumn::to_plain_json() {
    if (!is_flat_json()) {
        return;
    }
    // While a column is flat every row lives in the flat sub-columns, so the object storage
    // of the base class must be empty and can simply take over the merged values. Report it
    // in release builds too: silently ending up with more rows than the sub-columns held is
    // far harder to diagnose than the mismatch itself.
    LOG_IF(WARNING, BaseClass::size() != 0)
            << "flat JSON column also holds " << BaseClass::size() << " plain rows, row count will not match";
    DCHECK_EQ(0, BaseClass::size());
    ColumnPtr plain = unflatten();
    _clear_flat_schema();
    SuperClass::append(*plain, 0, plain->size());
}

void JsonColumn::_degrade_to_plain_json(const JsonColumn& src) {
    LOG_FIRST_N(WARNING, 10) << "flat JSON schema mismatch while appending, falling back to plain JSON. dest="
                             << debug_flat_paths() << ", src=" << src.debug_flat_paths();
    to_plain_json();
}

std::string JsonColumn::debug_flat_paths() const {
    if (_flat_column_paths.empty()) {
        return "[]";
    }
    std::ostringstream ss;
    ss << "[";
    size_t i = 0;
    for (; i < _flat_column_paths.size() - 1; i++) {
        ss << _flat_column_paths[i] << "(" << type_to_string(_flat_column_types[i]) << "), ";
    }
    ss << _flat_column_paths[i] << "(" << type_to_string(_flat_column_types[i]) << ")";
    ss << (has_remain() ? "]" : "}");
    return ss.str();
}
} // namespace starrocks
