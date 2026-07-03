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

#include <string_view>
#include <unordered_map>

#include "column/map_column.h"
#include "common/logging.h"
#include "formats/csv/array_reader.h"

namespace starrocks::csv {

Status MapConverter::write_string(formats::FormattedOutputStream* os, const Column& column, size_t row_num,
                                  const Options& options) const {
    auto* map = down_cast<const MapColumn*>(&column);
    auto& offsets = map->offsets();
    auto& keys = map->keys();
    auto& values = map->values();
    const auto offsets_data = offsets.immutable_data();

    auto begin = offsets_data[row_num];
    auto end = offsets_data[row_num + 1];

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

Status MapConverter::write_quoted_string(formats::FormattedOutputStream* os, const Column& column, size_t row_num,
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

bool MapConverter::read_hive_map(Column* column, const Slice& s, const Options& options) const {
    // Separator layout follows Hive's LazySerDeParameters: a map at nesting level L
    // splits entries on separator L and each entry splits at the FIRST occurrence of
    // separator L+1. At the top level these are the collection delimiter and the
    // mapkey delimiter.
    char entry_delim = HiveTextArrayReader::get_collection_delimiter(options.array_hive_collection_delimiter,
                                                                     options.array_hive_mapkey_delimiter,
                                                                     options.array_hive_nested_level);
    char kv_delim = HiveTextArrayReader::get_collection_delimiter(options.array_hive_collection_delimiter,
                                                                  options.array_hive_mapkey_delimiter,
                                                                  options.array_hive_nested_level + 1);

    auto* map = down_cast<MapColumn*>(column);
    auto* offsets = map->offsets_column_raw_ptr();
    auto* keys = map->keys_column_raw_ptr();
    auto* values = map->values_column_raw_ptr();
    size_t old_size = keys->size();
    DCHECK_EQ(old_size, offsets->get_data().back());
    DCHECK_EQ(old_size, values->size());

    // An empty field is an EMPTY map (matches Hive), not null: field-level null was
    // already decided upstream, in NullableConverter, against the raw "\N" literal.
    const char escape = options.escape;
    std::vector<Slice> key_fields;
    std::vector<Slice> value_fields;
    std::vector<bool> has_value; // an entry without a kv separator has a null value
    if (!s.empty()) {
        size_t start = 0;
        for (size_t i = 0; i <= s.size; i++) {
            // An escaped delimiter is not a real boundary; entries/keys/values stay RAW
            // here (escape bytes intact) -- NullableConverter unescapes each one level
            // down, once it knows whether it is a scalar leaf or a nested complex type.
            if (i < s.size && escape != 0 && s[i] == escape && i + 1 < s.size) {
                i++;
                continue;
            }
            if (i < s.size && s[i] != entry_delim) {
                continue;
            }
            Slice entry(s.data + start, i - start);
            const char* sep = nullptr;
            for (size_t j = 0; j < entry.size; j++) {
                if (escape != 0 && entry[j] == escape && j + 1 < entry.size) {
                    j++;
                    continue;
                }
                if (entry[j] == kv_delim) {
                    sep = entry.data + j;
                    break;
                }
            }
            if (sep == nullptr) {
                key_fields.emplace_back(entry);
                value_fields.emplace_back();
                has_value.push_back(false);
            } else {
                key_fields.emplace_back(entry.data, sep - entry.data);
                value_fields.emplace_back(sep + 1, entry.data + entry.size - (sep + 1));
                has_value.push_back(true);
            }
            start = i + 1;
        }
    }

    // Deduplicate keys the same way the default (brace) format below does -- the
    // last occurrence wins -- which also matches Hive's map overwrite semantics.
    // A key is unique iff its own index is the LAST index at which its content
    // appears: one pass records each key's last-seen index (later duplicates
    // overwrite earlier ones), a second pass checks each index against it -- O(n)
    // instead of the O(n^2) pairwise comparison this replaces.
    std::unordered_map<std::string_view, size_t> last_index_of_key;
    last_index_of_key.reserve(key_fields.size());
    for (size_t i = 0; i < key_fields.size(); ++i) {
        last_index_of_key[std::string_view(key_fields[i].data, key_fields[i].size)] = i;
    }
    std::vector<bool> unique_keys(key_fields.size());
    int unique_num = 0;
    for (size_t i = 0; i < key_fields.size(); ++i) {
        bool unique = last_index_of_key[std::string_view(key_fields[i].data, key_fields[i].size)] == i;
        unique_num += unique;
        unique_keys[i] = unique;
    }

    // A map at level L consumes separators L and L+1, so its keys/values parse at
    // level L+2 (mirrors LazyFactory). Hive text has no quotes: use read_string.
    Options sub_options = options;
    sub_options.array_hive_nested_level += 2;
    for (auto i = 0; i < key_fields.size(); ++i) {
        if (unique_keys[i] && !_key_converter->read_string(keys, key_fields[i], sub_options)) {
            keys->resize(old_size);
            return false;
        }
    }
    for (auto i = 0; i < value_fields.size(); ++i) {
        if (!unique_keys[i]) {
            continue;
        }
        bool ok = has_value[i] ? _value_converter->read_string(values, value_fields[i], sub_options)
                               : values->append_nulls(1);
        if (!ok) {
            keys->resize(old_size);
            values->resize(old_size);
            return false;
        }
    }
    offsets->append(old_size + unique_num);
    return true;
}

bool MapConverter::read_string(Column* column, const Slice& s, const Options& options) const {
    if (options.array_format_type == ArrayFormatType::kHive) {
        return read_hive_map(column, s, options);
    }
    if (!validate(s)) {
        return false;
    }
    auto* map = down_cast<MapColumn*>(column);
    auto* offsets = map->offsets_column_raw_ptr();
    auto* keys = map->keys_column_raw_ptr();
    auto* values = map->values_column_raw_ptr();
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

bool MapConverter::read_quoted_string(Column* column, const Slice& s, const Options& options) const {
    return read_string(column, s, options);
}

} // namespace starrocks::csv
