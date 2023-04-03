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

#include "formats/csv/default_value_converter.h"

#include "column/column.h"

namespace starrocks::csv {

bool DefaultValueConverter::read_quoted_string(Column* column, Slice s, const Options& options) const {
    if (options.invalid_field_as_null) {
        column->append_default();
        return true;
    } else {
        return false;
    }
}

bool DefaultValueConverter::read_string(Column* column, Slice s, const Options& options) const {
    return read_quoted_string(column, s, options);
}

Status DefaultValueConverter::write_quoted_string(OutputStream* os, const Column& column, size_t row_num,
                                                  const Options& options) const {
    return Status::InternalError("Csv DefaultValueConverter only support for read, not write");
}

Status DefaultValueConverter::write_string(OutputStream* os, const Column& column, size_t row_num,
                                           const Options& options) const {
    return write_quoted_string(os, column, row_num, options);
}
} // namespace starrocks::csv