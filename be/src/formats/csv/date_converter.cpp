// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "formats/csv/date_converter.h"

#include "column/fixed_length_column.h"
#include "common/logging.h"
#include "gutil/casts.h"
#include "types/date_value.hpp"

namespace starrocks::vectorized::csv {

Status DateConverter::write_string(OutputStream* os, const Column& column, size_t row_num,
                                   const Options& options) const {
    auto date_column = down_cast<const FixedLengthColumn<DateValue>*>(&column);
    return os->write(date_column->get_data()[row_num]);
}

Status DateConverter::write_quoted_string(OutputStream* os, const Column& column, size_t row_num,
                                          const Options& options) const {
    RETURN_IF_ERROR(os->write('"'));
    RETURN_IF_ERROR(write_string(os, column, row_num, options));
    return os->write('"');
}

bool DateConverter::read_string(Column* column, Slice s, const Options& options) const {
    DateValue v{};
    bool r = v.from_string(s.data, s.size);
    if (r) {
        down_cast<FixedLengthColumn<DateValue>*>(column)->append(v);
    }
    return r;
}

bool DateConverter::read_quoted_string(Column* column, Slice s, const Options& options) const {
    if (!remove_enclosing_quotes<'"'>(&s)) {
        return false;
    }
    return read_string(column, s, options);
}

} // namespace starrocks::vectorized::csv
