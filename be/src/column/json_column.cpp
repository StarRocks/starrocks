// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "column/json_column.h"

#include "glog/logging.h"
#include "util/hash_util.hpp"
#include "util/mysql_row_buffer.h"

namespace starrocks::vectorized {

void JsonColumn::append_datum(const Datum& datum) {
    append(datum.get<JsonValue*>());
}

int JsonColumn::compare_at(size_t left_idx, size_t right_idx, const starrocks::vectorized::Column& rhs,
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

std::string JsonColumn::debug_item(uint32_t idx) const {
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

} // namespace starrocks::vectorized
