// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "formats/csv/decimalv2_converter.h"

#include "column/fixed_length_column.h"
#include "common/logging.h"
#include "gutil/casts.h"
#include "runtime/decimalv2_value.h"

namespace starrocks::vectorized::csv {

Status DecimalV2Converter::write_string(OutputStream* os, const Column& column, size_t row_num,
                                        const Options& options) const {
    auto decimal_column = down_cast<const FixedLengthColumn<DecimalV2Value>*>(&column);
    return os->write(decimal_column->get_data()[row_num]);
}

Status DecimalV2Converter::write_quoted_string(OutputStream* os, const Column& column, size_t row_num,
                                               const Options& options) const {
    return write_string(os, column, row_num, options);
}

bool DecimalV2Converter::read_string(Column* column, Slice s, const Options& options) const {
    DecimalV2Value v;
    int err = v.parse_from_str(s.data, s.size);
    if (err == 0) {
        down_cast<FixedLengthColumn<DecimalV2Value>*>(column)->append(v);
    }
    return err == 0;
}

bool DecimalV2Converter::read_quoted_string(Column* column, Slice s, const Options& options) const {
    return read_string(column, s, options);
}

} // namespace starrocks::vectorized::csv
