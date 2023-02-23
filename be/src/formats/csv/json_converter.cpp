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

#include "formats/csv/json_converter.h"

#include "column/json_column.h"
#include "common/logging.h"
#include "gutil/casts.h"
#include "types/date_value.hpp"

namespace starrocks::csv {

Status JsonConverter::write_string(OutputStream* os, const Column& column, size_t row_num,
                                   const Options& options) const {
    auto data_column = down_cast<const JsonColumn*>(&column);
    auto json_value = data_column->get_object(row_num);
    auto json_str = json_value->to_string_uncheck();
    return os->write(json_str);
}

Status JsonConverter::write_quoted_string(OutputStream* os, const Column& column, size_t row_num,
                                          const Options& options) const {
    RETURN_IF_ERROR(os->write('"'));
    RETURN_IF_ERROR(write_string(os, column, row_num, options));
    return os->write('"');
}

bool JsonConverter::read_string(Column* column, Slice s, const Options& options) const {
    auto json = JsonValue::parse(s);
    if (json.ok()) {
        auto json_column = down_cast<JsonColumn*>(column);
        json_column->append(&json.value());
        return true;
    }
    return false;
}

bool JsonConverter::read_quoted_string(Column* column, Slice s, const Options& options) const {
    if (!remove_enclosing_quotes<'"'>(&s)) {
        return false;
    }
    return read_string(column, s, options);
}

} // namespace starrocks::csv
