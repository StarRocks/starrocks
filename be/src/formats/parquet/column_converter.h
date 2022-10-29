// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
namespace vectorized {
class Column;
} // namespace vectorized
} // namespace starrocks

namespace starrocks::parquet {

class ParquetField;

class ColumnConverter {
public:
    ColumnConverter() = default;
    virtual ~ColumnConverter() = default;

    void init_info(bool _need_convert, tparquet::Type::type _parquet_type) {
        need_convert = _need_convert;
        parquet_type = _parquet_type;
    }

    // create column according parquet data type
    vectorized::ColumnPtr create_src_column();

    virtual Status convert(const vectorized::ColumnPtr& src, vectorized::Column* dst) { return Status::OK(); };

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
