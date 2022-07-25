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
    if (!_array_reader->validate(s)) {
        return false;
    }

    auto* array = down_cast<ArrayColumn*>(column);
    auto* offsets = array->offsets_column().get();
    auto* elements = array->elements_column().get();

    std::vector<Slice> fields;
    if (!s.empty() && !_array_reader->split_array_elements(s, &fields)) {
        return false;
    }
    size_t old_size = elements->size();
    Options sub_options = options;
    sub_options.invalid_field_as_null = false;
    DCHECK_EQ(old_size, offsets->get_data().back());
    for (const auto& f : fields) {
        if (!_array_reader->read_quoted_string(_element_converter, elements, f, sub_options)) {
            elements->resize(old_size);
            return false;
        }
    }
    offsets->append(old_size + fields.size());
    return true;
}

bool ArrayConverter::read_quoted_string(Column* column, Slice s, const Options& options) const {
    return read_string(column, s, options);
}

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
