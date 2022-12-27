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

#include "array_reader.h"

namespace starrocks::csv {

std::unique_ptr<ArrayReader> ArrayReader::create_array_reader(const Converter::Options& options) {
    std::unique_ptr<ArrayReader> array_reader;
    if (options.array_format_type == ArrayFormatType::kHive) {
        array_reader = std::make_unique<HiveTextArrayReader>(options);
    } else {
        array_reader = std::make_unique<DefaultArrayReader>();
    }
    return array_reader;
}

bool DefaultArrayReader::validate(const Slice& s) const {
    if (s.size < 2) {
        return false;
    }
    if (s[0] != '[' || s[s.size - 1] != ']') {
        return false;
    }
    return true;
}

bool DefaultArrayReader::split_array_elements(Slice s, std::vector<Slice>* elements) const {
    s.remove_prefix(1);
    s.remove_suffix(1);
    if (s.empty()) {
        // Consider empty array [].
        return true;
    }

    bool in_quote = false;
    int array_nest_level = 0;
    elements->push_back(s);
    for (size_t i = 0; i < s.size; i++) {
        char c = s[i];
        // TODO(zhuming): handle escaped double quotes
        if (c == '"') {
            in_quote = !in_quote;
        } else if (!in_quote && c == '[') {
            array_nest_level++;
        } else if (!in_quote && c == ']') {
            array_nest_level--;
        } else if (!in_quote && array_nest_level == 0 && c == _array_delimiter) {
            elements->back().remove_suffix(s.size - i);
            elements->push_back(Slice(s.data + i + 1, s.size - i - 1));
        }
    }
    if (array_nest_level != 0 || in_quote) {
        return false;
    }
    return true;
}

bool DefaultArrayReader::read_quoted_string(const std::unique_ptr<Converter>& elem_converter, Column* column,
                                            const Slice& s, const Converter::Options& options) const {
    return elem_converter->read_quoted_string(column, s, options);
}

bool HiveTextArrayReader::validate(const Slice& s) const {
    // In Hive, always return true, because slice maybe an empty array [].
    return true;
}

bool HiveTextArrayReader::split_array_elements(Slice s, std::vector<Slice>* elements) const {
    if (s.size == 0) {
        // consider empty array
        return true;
    }

    size_t left = 0;
    size_t right = 0;
    for (/**/; right < s.size; right++) {
        char c = s[right];
        if (c == _array_delimiter) {
            elements->push_back(Slice(s.data + left, right - left));
            left = right + 1;
        }
    }
    if (right > left) {
        elements->push_back(Slice(s.data + left, right - left));
    }

    return true;
}

bool HiveTextArrayReader::read_quoted_string(const std::unique_ptr<Converter>& elem_converter, Column* column,
                                             const Slice& s, const Converter::Options& options) const {
    // In Hive, we should use read_string() instead of read_quoted_string
    return elem_converter->read_string(column, s, options);
}

char HiveTextArrayReader::get_collection_delimiter(char collection_delimiter, char mapkey_delimiter,
                                                   size_t nested_array_level) {
    DCHECK(nested_array_level >= 1 && nested_array_level <= 153);

    // tmp maybe negative, dont use size_t.
    // 1 (\001) means default 1D array collection delimiter.
    int32_t tmp = 1;

    if (nested_array_level == 1) {
        // If level is 1, use collection_delimiter directly.
        return collection_delimiter;
    } else if (nested_array_level == 2) {
        // If level is 2, use mapkey_delimiter directly.
        return mapkey_delimiter;
    } else if (nested_array_level <= 7) {
        // [3, 7] -> [4, 8]
        tmp = static_cast<int32_t>(nested_array_level) + (4 - 3);
    } else if (nested_array_level == 8) {
        // [8] -> [11]
        tmp = 11;
    } else if (nested_array_level <= 21) {
        // [9, 21] -> [14, 26]
        tmp = static_cast<int32_t>(nested_array_level) + (14 - 9);
    } else if (nested_array_level <= 25) {
        // [22, 25] -> [28, 31]
        tmp = static_cast<int32_t>(nested_array_level) + (28 - 22);
    } else if (nested_array_level <= 153) {
        // [26, 153] -> [-128, -1]
        tmp = static_cast<int32_t>(nested_array_level) + (-128 - 26);
    }

    return static_cast<char>(tmp);
}

} // namespace starrocks::csv
