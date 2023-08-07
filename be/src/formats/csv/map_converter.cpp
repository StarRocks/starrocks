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

#include "formats/csv/map_converter.h"

#include "column/map_column.h"
#include "common/logging.h"

namespace starrocks::csv {

Status MapConverter::write_string(OutputStream* os, const Column& column, size_t row_num,
                                  const Options& options) const {
    auto* map = down_cast<const MapColumn*>(&column);
    auto& offsets = map->offsets();
    auto& keys = map->keys();
    auto& values = map->values();

    auto begin = offsets.get_data()[row_num];
    auto end = offsets.get_data()[row_num + 1];

    RETURN_IF_ERROR(os->write('{'));
    for (auto i = begin; i < end; i++) {
        RETURN_IF_ERROR(_key_converter->write_quoted_string(os, keys, i, options));
        RETURN_IF_ERROR(os->write(_kv_delimiter));
        RETURN_IF_ERROR(_value_converter->write_quoted_string(os, values, i, options));

        if (i + 1 < end) {
            RETURN_IF_ERROR(os->write(_map_delimiter));
        }
    }
    return os->write('}');
}

Status MapConverter::write_quoted_string(OutputStream* os, const Column& column, size_t row_num,
                                         const Options& options) const {
    return write_string(os, column, row_num, options);
}

bool MapConverter::validate(const Slice& s) const {
    if (s.size < 2) {
        return false;
    }
    if (s[0] != '{' || s[s.size - 1] != '}') {
        return false;
    }
    return true;
}

bool MapConverter::split_map_key_value(Slice s, std::vector<Slice>& keys, std::vector<Slice>& values) const {
    s.remove_prefix(1);
    s.remove_suffix(1);
    if (s.empty()) {
        // Consider empty map {}.
        return true;
    }

    bool in_quote = false;
    int map_nest_level = 0;
    int last_index = 0;
    size_t i = 0;
    for (; i < s.size; i++) {
        char c = s[i];
        if (c == '"') {
            in_quote = !in_quote;
        } else if (!in_quote && c == '{') {
            map_nest_level++;
        } else if (!in_quote && c == '}') {
            map_nest_level--;
        } else if (!in_quote && map_nest_level == 0 && c == _kv_delimiter) {
            if (i == last_index) { // size should not be 0
                return false;
            }
            keys.emplace_back(s.data + last_index, i - last_index);
            last_index = i + 1;
        } else if (!in_quote && map_nest_level == 0 && c == _map_delimiter) {
            if (i == last_index) {
                return false;
            }
            values.emplace_back(s.data + last_index, i - last_index);
            last_index = i + 1;
        }
    }
    if (!in_quote && map_nest_level == 0 && i == s.size) {
        if (i == last_index) {
            return false;
        }
        values.emplace_back(s.data + last_index, i - last_index);
    }
    if (map_nest_level != 0 || in_quote || values.size() != keys.size()) {
        return false;
    }
    return true;
}

bool MapConverter::read_string(Column* column, Slice s, const Options& options) const {
    if (!validate(s)) {
        return false;
    }
    auto* map = down_cast<MapColumn*>(column);
    auto* offsets = map->offsets_column().get();
    auto* keys = map->keys_column().get();
    auto* values = map->values_column().get();
    std::vector<Slice> key_fields, value_fields;
    if (!s.empty() && !split_map_key_value(s, key_fields, value_fields)) {
        return false;
    }
    size_t old_size = keys->size();
    DCHECK_EQ(old_size, offsets->get_data().back());
    DCHECK_EQ(old_size, values->size());

    // get unique keys
    std::vector<bool> unique_keys;
    int unique_num = 0;
    for (auto i = 0; i < key_fields.size(); ++i) {
        bool unique = true;
        for (auto j = i + 1; unique && (j < key_fields.size()); ++j) {
            if (key_fields[i] == key_fields[j]) {
                unique = false;
            }
        }
        unique_num += unique;
        unique_keys.emplace_back(unique);
    }

    for (auto i = 0; i < key_fields.size(); ++i) {
        if (unique_keys[i] && !_key_converter->read_quoted_string(keys, key_fields[i], options)) {
            keys->resize(old_size);
            return false;
        }
    }
    for (auto i = 0; i < value_fields.size(); ++i) {
        if (unique_keys[i] && !_value_converter->read_quoted_string(values, value_fields[i], options)) {
            values->resize(old_size);
            return false;
        }
    }
    offsets->append(old_size + unique_num);
    return true;
}

bool MapConverter::read_quoted_string(Column* column, Slice s, const Options& options) const {
    return read_string(column, s, options);
}

} // namespace starrocks::csv
