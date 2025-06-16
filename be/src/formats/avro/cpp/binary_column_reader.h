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

#include <cctz/time_zone.h>

#include "column/vectorized_fwd.h"
#include "formats/avro/cpp/column_reader.h"

namespace starrocks::avrocpp {

class BinaryColumnReader final : public ColumnReader {
public:
    explicit BinaryColumnReader(const std::string& col_name, const TypeDescriptor& type_desc,
                                const cctz::time_zone& timezone)
            : ColumnReader(col_name, type_desc), _timezone(timezone) {}
    ~BinaryColumnReader() override = default;

    Status read_datum(const avro::GenericDatum& datum, Column* column) override;

private:
    Status read_numeric_value(const avro::GenericDatum& datum, BinaryColumn* column);
    Status read_string_value(const avro::GenericDatum& datum, BinaryColumn* column);
    Status read_enum_value(const avro::GenericDatum& datum, BinaryColumn* column);
    Status read_complex_value(const avro::GenericDatum& datum, BinaryColumn* column);

    const cctz::time_zone& _timezone;
};

} // namespace starrocks::avrocpp
