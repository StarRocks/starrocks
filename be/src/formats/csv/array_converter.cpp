// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "formats/csv/array_converter.h"

#include "column/array_column.h"
#include "common/logging.h"

namespace starrocks::vectorized::csv {

Status ArrayConverter::write_string(OutputStream* os, const Column& column, size_t row_num,
                                    const Options& options) const {
    auto* array = down_cast<const ArrayColumn*>(&column);
    auto& offsets = array->offsets();
    auto& elements = array->elements();

    auto begin = offsets.get_data()[row_num];
    auto end = offsets.get_data()[row_num + 1];

    RETURN_IF_ERROR(os->write('['));
    for (auto i = begin; i < end; i++) {
        RETURN_IF_ERROR(_element_converter->write_quoted_string(os, elements, i, options));
        if (i + 1 < end) {
            RETURN_IF_ERROR(os->write(','));
        }
    }
    return os->write(']');
}

Status ArrayConverter::write_quoted_string(OutputStream* os, const Column& column, size_t row_num,
                                           const Options& options) const {
    return write_string(os, column, row_num, options);
}

bool ArrayConverter::read_string(Column* column, Slice s, const Options& options) const {
    if (!_validate_array(s, options)) {
        return false;
    }

    auto* array = down_cast<ArrayColumn*>(column);
    auto* offsets = array->offsets_column().get();
    auto* elements = array->elements_column().get();

    std::vector<Slice> fields;
    if (!s.empty() && !_split_array_elements(s, &fields, options)) {
        return false;
    }
    size_t old_size = elements->size();
    Options sub_options = options;
    sub_options.invalid_field_as_null = false;
    sub_options.array_hive_nested_level++;
    DCHECK_EQ(old_size, offsets->get_data().back());
    for (const auto& f : fields) {
        if (options.array_is_quoted_string) {
            if (!_element_converter->read_quoted_string(elements, f, sub_options)) {
                elements->resize(old_size);
                return false;
            }
        } else {
            if (!_element_converter->read_string(elements, f, sub_options)) {
                elements->resize(old_size);
                return false;
            }
        }
    }
    offsets->append(old_size + fields.size());
    return true;
}

bool ArrayConverter::read_quoted_string(Column* column, Slice s, const Options& options) const {
    return read_string(column, s, options);
}

bool ArrayConverter::_validate_array(Slice s, const Converter::Options& options) {
    if (options.array_format_type == ArrayFormatType::DEFAULT) {
        if (s.size < 2) {
            return false;
        }
        if (s[0] != '[' || s[s.size - 1] != ']') {
            return false;
        }
        return true;
    } else if (options.array_format_type == ArrayFormatType::HIVE) {
        // In Hive, always return true, because slice maybe an empty array [].
        return true;
    }
    return false;
}
bool ArrayConverter::_split_array_elements(Slice s, std::vector<Slice>* elements, const Options& options) {
    if (options.array_format_type == ArrayFormatType::DEFAULT) {
        return _split_default_array_elements(s, elements, options);
    } else if (options.array_format_type == ArrayFormatType::HIVE) {
        return _split_hive_array_elements(s, elements, options);
    }
    return false;
}
bool ArrayConverter::_split_default_array_elements(Slice s, std::vector<Slice>* elements,
                                                   const Converter::Options& options) {
    s.remove_prefix(1);
    s.remove_suffix(1);
    if (s.empty()) {
        // Consider empty array [].
        return true;
    }

    char element_delimiter = options.array_element_delimiter;
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
        } else if (!in_quote && array_nest_level == 0 && c == element_delimiter) {
            elements->back().remove_suffix(s.size - i);
            elements->push_back(Slice(s.data + i + 1, s.size - i - 1));
        }
    }
    if (array_nest_level != 0 || in_quote) {
        return false;
    }
    return true;
}
bool ArrayConverter::_split_hive_array_elements(Slice s, std::vector<Slice>* elements,
                                                const Converter::Options& options) {
    if (s.size == 0) {
        // consider empty array
        return true;
    }

    char delimiter = get_collection_delimiter(options.array_element_delimiter, options.array_hive_mapkey_delimiter,
                                              options.array_hive_nested_level);

    size_t left = 0;
    size_t right = 0;
    for (/**/; right < s.size; right++) {
        char c = s[right];
        if (c == delimiter) {
            elements->push_back(Slice(s.data + left, right - left));
            left = right + 1;
        }
    }
    if (right > left) {
        elements->push_back(Slice(s.data + left, right - left));
    }

    return true;
}

// Hive collection delimiter generate rule is quiet complex,
// if you want to know the details, you can refer to:
// https://github.com/apache/hive/blob/90428cc5f594bd0abb457e4e5c391007b2ad1cb8/serde/src/java/org/apache/hadoop/hive/serde2/lazy/LazySerDeParameters.java#L250
// Next let's begin the story:
// There is a 3D-array [[[1, 2]], [[3, 4], [5, 6]]], in Hive it will be stored as 1^D2^B3^D4^C5^D6 (without '[', ']').
// ^B = (char)2, ^C = (char)3, ^D = (char)4 ....
// In the first level, Hive will use collection_delimiter (user can specify it, default is ^B) as an element separator,
// then origin array split into [[1, 2]] (1^D2) and [[3, 4], [5, 6]] (3^D4^C5^D6).
// In the second level, Hive will use mapkey_delimiter (user can specify it, default is ^C) as a separator, then
// array split into [1, 2], [3, 4] and [5, 6].
// In the third level, Hive will use ^D (user can't specify it) as a separator, then we can get
// each element in this array.
char get_collection_delimiter(char collection_delimiter, char mapkey_delimiter, size_t nested_array_level) {
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

} // namespace starrocks::vectorized::csv
