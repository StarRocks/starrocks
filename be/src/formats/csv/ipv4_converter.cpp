// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "formats/csv/ipv4_converter.h"

#include "column/fixed_length_column.h"
#include "common/logging.h"
#include "gutil/casts.h"
#include "types/date_value.hpp"

namespace starrocks::vectorized::csv {

Status Ipv4Converter::write_string(OutputStream* os, const Column& column, size_t row_num,
                                   const Options& options) const {
    auto date_column = down_cast<const FixedLengthColumn<Ipv4Value>*>(&column);
    return os->write(date_column->get_data()[row_num]);
}

Status Ipv4Converter::write_quoted_string(OutputStream* os, const Column& column, size_t row_num,
                                          const Options& options) const {
    RETURN_IF_ERROR(os->write('"'));
    RETURN_IF_ERROR(write_string(os, column, row_num, options));
    return os->write('"');
}

bool Ipv4Converter::read_string(Column* column, Slice s, const Options& options) const {
    Ipv4Value v{};
    bool r = v.from_string(s.data, s.size);
    if (r) {
        down_cast<FixedLengthColumn<Ipv4Value>*>(column)->append(v);
    }
    return r;
}

bool Ipv4Converter::read_quoted_string(Column* column, Slice s, const Options& options) const {
    if (!remove_enclosing_quotes<'"'>(&s)) {
        return false;
    }
    return read_string(column, s, options);
}

} // namespace starrocks::vectorized::csv
