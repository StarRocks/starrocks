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

#include "glog/logging.h"
#include "util/hash_util.hpp"
#include "util/mysql_row_buffer.h"

namespace starrocks {

void JsonColumn::append_datum(const Datum& datum) {
    append(datum.get<JsonValue*>());
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
    return get_object(idx)->to_string_uncheck();
}

std::string JsonColumn::get_name() const {
    return "json";
}

MutableColumnPtr JsonColumn::clone() const {
    return BaseClass::clone();
}

MutableColumnPtr JsonColumn::clone_empty() const {
    return this->create_mutable();
}

ColumnPtr JsonColumn::clone_shared() const {
    return BaseClass::clone_shared();
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

} // namespace starrocks
