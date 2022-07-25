// This file is licensed under the Elastic License 2.0. Copyright 2022-present, StarRocks Limited.

#include "array_reader.h"

#include "column/array_column.h"

namespace starrocks::vectorized::csv {

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
    return elem_converter->read_string(column, s, options);
}

} // namespace starrocks::vectorized::csv