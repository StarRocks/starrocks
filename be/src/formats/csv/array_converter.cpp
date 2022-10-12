// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
    if (_array_reader == nullptr) {
        _array_reader = ArrayReader::create_array_reader(options);
    }

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
    sub_options.array_hive_nested_level++;
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

} // namespace starrocks::vectorized::csv
