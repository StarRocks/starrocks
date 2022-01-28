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
    if (!s.empty() && !_split_array_elements(s, &fields)) {
        return false;
    }
    size_t old_size = elements->size();
    Options sub_options = options;
    sub_options.invalid_field_as_null = false;
    DCHECK_EQ(old_size, offsets->get_data().back());
    for (const auto& f : fields) {
        if (!_element_converter->read_quoted_string(elements, f, sub_options)) {
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

bool ArrayConverter::_split_array_elements(Slice s, std::vector<Slice>* elements) const {
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
        } else if (!in_quote && array_nest_level == 0 && c == ',') {
            elements->back().remove_suffix(s.size - i);
            elements->push_back(Slice(s.data + i + 1, s.size - i - 1));
        }
    }
    if (array_nest_level != 0 || in_quote) {
        return false;
    }
    return true;
}

} // namespace starrocks::vectorized::csv
