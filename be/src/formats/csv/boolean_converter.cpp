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

#include "formats/csv/boolean_converter.h"

#include "column/fixed_length_column.h"
#include "common/logging.h"
#include "util/string_parser.hpp"

namespace starrocks::csv {

Status BooleanConverter::write_string(OutputStream* os, const Column& column, size_t row_num,
                                      const Options& options) const {
    const static Slice kTrue("true");
    const static Slice kFalse("false");
    auto boolean_col = down_cast<const FixedLengthColumn<uint8_t>*>(&column);
    if (LIKELY(options.bool_alpha)) {
        return os->write(boolean_col->get_data()[row_num] ? kTrue : kFalse);
    } else {
        return os->write<int16_t>(boolean_col->get_data()[row_num]);
    }
}

Status BooleanConverter::write_quoted_string(OutputStream* os, const Column& column, size_t row_num,
                                             const Options& options) const {
    return write_string(os, column, row_num, options);
}

bool BooleanConverter::read_string(Column* column, Slice s, const Options& options) const {
    StringParser::ParseResult r;
    bool v = StringParser::string_to_bool(s.data, s.size, &r);
    if (r == StringParser::PARSE_SUCCESS) {
        down_cast<FixedLengthColumn<uint8_t>*>(column)->append(v);
        return true;
    }
    v = implicit_cast<bool>(StringParser::string_to_float<double>(s.data, s.size, &r));
    if (r == StringParser::PARSE_SUCCESS) {
        down_cast<FixedLengthColumn<uint8_t>*>(column)->append(v);
        return true;
    } else if (r == StringParser::PARSE_OVERFLOW || r == StringParser::PARSE_UNDERFLOW) {
        DecimalV2Value decimal;
        if (decimal.parse_from_str(s.data, s.size) != 0) {
            return false;
        }
        down_cast<FixedLengthColumn<uint8_t>*>(column)->append(!decimal.is_zero());
        return true;
    } else {
        return false;
    }
}

bool BooleanConverter::read_quoted_string(Column* column, Slice s, const Options& options) const {
    return read_string(column, s, options);
}

} // namespace starrocks::csv
