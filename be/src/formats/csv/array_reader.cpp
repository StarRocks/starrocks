// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "array_reader.h"

#include "column/array_column.h"

namespace starrocks::vectorized::csv {

bool StandardArrayReader::read_string(const std::unique_ptr<Converter>& elem_converter, Column* column, Slice s, const Converter::Options& options) const {
    if (s.size < 2) {
        return false;
    }
    if (s[0] != '[' || s[s.size - 1] != ']') {
        return false;
    }
    s.remove_prefix(1);
    s.remove_suffix(1);

    auto* array = down_cast<ArrayColumn*>(column);
    auto* offsets = array->offsets_column().get();
    auto* elements = array->elements_column().get();

    std::vector<Slice> fields;
    if (!s.empty() && !split_array_elements(s, &fields)) {
        return false;
    }
    size_t old_size = elements->size();
    Converter::Options sub_options = options;
    sub_options.invalid_field_as_null = false;
    DCHECK_EQ(old_size, offsets->get_data().back());
    for (const auto& f : fields) {
        if (!elem_converter->read_quoted_string(elements, f, sub_options)) {
            elements->resize(old_size);
            return false;
        }
    }
    offsets->append(old_size + fields.size());
    return true;
}

bool StandardArrayReader::split_array_elements(Slice s, std::vector<Slice>* elements) const {
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

bool HiveTextArrayReader::read_string(const std::unique_ptr<Converter>& elem_converter, Column* column, Slice s, const Converter::Options& options) const {
    auto* array = down_cast<ArrayColumn*>(column);
    auto* offsets = array->offsets_column().get();
    auto* elements = array->elements_column().get();

    std::vector<Slice> fields;
    if (!s.empty() && !split_array_elements(s, &fields)) {
        return false;
    }
    size_t old_size = elements->size();
    Converter::Options sub_options = options;
    sub_options.invalid_field_as_null = false;
    DCHECK_EQ(old_size, offsets->get_data().back());
    for (const auto& f : fields) {
        if (!elem_converter->read_string(elements, f, sub_options)) {
            elements->resize(old_size);
            return false;
        }
    }
    offsets->append(old_size + fields.size());
    return true;
}

bool HiveTextArrayReader::split_array_elements(Slice s, std::vector<Slice>* elements) const {
    if (s.size == 0) {
        // consider empty array
        return true;
    }

    size_t left = 0;
    size_t right = 0;
    for (; right < s.size; right++) {
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
} // namespace starrocks::vectorized::csv
