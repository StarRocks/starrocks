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
    return _array_reader->read_string(_element_converter, column, s, options);
}

bool ArrayConverter::read_quoted_string(Column* column, Slice s, const Options& options) const {
    return read_string(column, s, options);
}

} // namespace starrocks::vectorized::csv
