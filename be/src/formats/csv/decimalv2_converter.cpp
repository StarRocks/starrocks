// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "formats/csv/decimalv2_converter.h"

#include "column/fixed_length_column.h"
#include "common/logging.h"
#include "gutil/casts.h"
#include "runtime/decimalv2_value.h"

namespace starrocks::csv {

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

} // namespace starrocks::csv
