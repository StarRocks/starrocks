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

#include "formats/csv/datetime_converter.h"

#include "column/fixed_length_column.h"
#include "common/logging.h"
#include "gutil/casts.h"
#include "types/timestamp_value.h"

namespace starrocks::csv {

Status DatetimeConverter::write_string(OutputStream* os, const Column& column, size_t row_num,
                                       const Options& options) const {
    auto datetime_column = down_cast<const FixedLengthColumn<TimestampValue>*>(&column);
    return os->write(datetime_column->get_data()[row_num]);
}

Status DatetimeConverter::write_quoted_string(OutputStream* os, const Column& column, size_t row_num,
                                              const Options& options) const {
    RETURN_IF_ERROR(os->write('"'));
    RETURN_IF_ERROR(write_string(os, column, row_num, options));
    return os->write('"');
}

bool DatetimeConverter::read_string(Column* column, Slice s, const Options& options) const {
    TimestampValue v{};
    bool r = v.from_string(s.data, s.size);
    if (r) {
        down_cast<FixedLengthColumn<TimestampValue>*>(column)->append(v);
    }
    return r;
}

bool DatetimeConverter::read_quoted_string(Column* column, Slice s, const Options& options) const {
    if (!remove_enclosing_quotes<'"'>(&s)) {
        return false;
    }
    return read_string(column, s, options);
}

} // namespace starrocks::csv
