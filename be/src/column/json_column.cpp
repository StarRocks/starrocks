// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "column/json_column.h"

#include "util/mysql_row_buffer.h"

namespace starrocks::vectorized {

int JsonColumn::compare_at(size_t left, size_t right, const starrocks::vectorized::Column& rhs,
                           int nan_direction_hint) const {
    JsonValue* x = get_object(left);
    const JsonValue* y = rhs.get(right).get_json();
    return x->compare(*y);
}

void JsonColumn::fnv_hash(uint32_t* hash, uint32_t from, uint32_t to) const {
    for (uint32_t i = from; i < to; i++) {
        JsonValue* json = get_object(i);
        hash[i] = json->hash();
    }
}

void JsonColumn::put_mysql_row_buffer(starrocks::MysqlRowBuffer* buf, size_t idx) const {
    for (int i = 0; i < size(); i++) {
        JsonValue* value = get_object(i);
        auto json_str = value->to_string();
        if (!json_str.ok()) {
            buf->push_null();
        } else {
            buf->push_string(json_str->data(), json_str->size());
        }
    }
}

std::string JsonColumn::debug_item(uint32_t idx) const {
    return get_object(idx)->to_string_uncheck();
}

std::string JsonColumn::get_name() const {
    return "json";
}

} // namespace starrocks::vectorized