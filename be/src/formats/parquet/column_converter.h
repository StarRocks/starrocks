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

#include "column/column_helper.h"
#include "column/type_traits.h"
#include "common/status.h"
#include "formats/parquet/types.h"
#include "formats/parquet/utils.h"
#include "gen_cpp/parquet_types.h"
#include "runtime/types.h"
#include "util/bit_util.h"

namespace starrocks {
class Column;
} // namespace starrocks

namespace starrocks::parquet {

struct ParquetField;

class ColumnConverter {
public:
    ColumnConverter() = default;
    virtual ~ColumnConverter() = default;

    void init_info(bool _need_convert, tparquet::Type::type _parquet_type) {
        need_convert = _need_convert;
        parquet_type = _parquet_type;
    }

    // create column according parquet data type
    ColumnPtr create_src_column();

    virtual Status convert(const ColumnPtr& src, Column* dst) { return Status::OK(); };

public:
    bool need_convert = false;
    tparquet::Type::type parquet_type;
};

class ColumnConverterFactory {
public:
    static Status create_converter(const ParquetField& field, const TypeDescriptor& typeDescriptor,
                                   const std::string& timezone, std::unique_ptr<ColumnConverter>* converter);
};

} // namespace starrocks::parquet
