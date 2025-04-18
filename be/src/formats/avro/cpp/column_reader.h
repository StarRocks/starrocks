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

#include <avrocpp/GenericDatum.hh>

#include "common/status.h"
#include "runtime/types.h"

namespace starrocks {

class Column;

namespace avrocpp {

class ColumnReader;

using ColumnReaderUniquePtr = std::unique_ptr<ColumnReader>;

class ColumnReader {
public:
    explicit ColumnReader(const std::string& col_name, const TypeDescriptor& type_desc)
            : _col_name(col_name), _type_desc(type_desc) {}
    virtual ~ColumnReader() = default;

    static ColumnReaderUniquePtr get_nullable_column_reader(const std::string& col_name,
                                                            const TypeDescriptor& type_desc,
                                                            const cctz::time_zone& timezone, bool invalid_as_null);

    virtual Status read_datum_for_adaptive_column(const avro::GenericDatum& datum, Column* column);
    virtual Status read_datum(const avro::GenericDatum& datum, Column* column) = 0;

protected:
    const std::string& _col_name;
    const TypeDescriptor& _type_desc;
};

} // namespace avrocpp
} // namespace starrocks
