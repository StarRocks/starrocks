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

#include "formats/csv/nullable_converter.h"

#include "column/adaptive_nullable_column.h"
#include "column/nullable_column.h"
#include "common/logging.h"

namespace starrocks::csv {

Status NullableConverter::write_string(OutputStream* os, const Column& column, size_t row_num,
                                       const Options& options) const {
    auto nullable_column = down_cast<const NullableColumn*>(&column);
    auto data_column = nullable_column->data_column().get();
    auto null_column = nullable_column->null_column().get();
    if (null_column->get_data()[row_num] != 0) {
        return os->write("\\N");
    } else {
        return _base_converter->write_string(os, *data_column, row_num, options);
    }
}

Status NullableConverter::write_quoted_string(OutputStream* os, const Column& column, size_t row_num,
                                              const Options& options) const {
    auto nullable_column = down_cast<const NullableColumn*>(&column);
    auto data_column = nullable_column->data_column().get();
    auto null_column = nullable_column->null_column().get();
    if (null_column->get_data()[row_num] != 0) {
        return os->write("null");
    } else {
        return _base_converter->write_quoted_string(os, *data_column, row_num, options);
    }
}

bool NullableConverter::read_string_for_adaptive_null_column(Column* column, Slice s, const Options& options) const {
    auto* nullable_column = down_cast<AdaptiveNullableColumn*>(column);

    if (s == "\\N") {
        return nullable_column->append_nulls(1);
    }
    auto* data = nullable_column->mutable_begin_append_not_default_value();
    if (_base_converter->read_string(data, s, options)) {
        nullable_column->finish_append_one_not_default_value();
        return true;
    } else if (options.invalid_field_as_null) {
        return nullable_column->append_nulls(1);
    } else {
        return false;
    }
}

bool NullableConverter::read_string(Column* column, Slice s, const Options& options) const {
    auto* nullable = down_cast<NullableColumn*>(column);
    auto* data = nullable->data_column().get();

    if (s == "\\N") {
        return nullable->append_nulls(1);
    } else if (_base_converter->read_string(data, s, options)) {
        nullable->null_column()->append(0);
        return true;
    } else if (options.invalid_field_as_null) {
        return nullable->append_nulls(1);
    } else {
        return false;
    }
}

bool NullableConverter::read_quoted_string(Column* column, Slice s, const Options& options) const {
    auto* nullable = down_cast<NullableColumn*>(column);
    auto* data = nullable->data_column().get();

    if (s == "null" || s == "\\N") {
        return nullable->append_nulls(1);
    } else if (_base_converter->read_quoted_string(data, s, options)) {
        nullable->null_column()->append(0);
        return true;
    } else if (options.invalid_field_as_null) {
        return nullable->append_nulls(1);
    } else {
        return false;
    }
}

} // namespace starrocks::csv
