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

#pragma once

#include "formats/csv/converter.h"
#include "util/decimal_types.h"

namespace starrocks::csv {

template <typename T>
class DecimalV3Converter final : public Converter {
    static_assert(is_underlying_type_of_decimal<T>, "not underlying type of decimal");

public:
    DecimalV3Converter(int precision, int scale) : _precision(precision), _scale(scale) {}

    Status write_string(OutputStream* os, const Column& column, size_t row_num, const Options& options) const override;
    Status write_quoted_string(OutputStream* os, const Column& column, size_t row_num,
                               const Options& options) const override;
    bool read_string(Column* column, Slice s, const Options& options) const override;
    bool read_quoted_string(Column* column, Slice s, const Options& options) const override;

private:
    int _precision;
    int _scale;
};

} // namespace starrocks::csv
