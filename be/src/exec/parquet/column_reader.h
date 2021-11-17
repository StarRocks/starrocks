// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <memory>

#include "common/status.h"
#include "exec/parquet/types.h"
#include "gen_cpp/parquet_types.h"
#include "runtime/types.h"
#include "utils.h"
#include "column/column.h"

namespace starrocks {
class RandomAccessFile;
namespace vectorized {
struct HdfsScanStats;
} // namespace vectorized
} // namespace starrocks

namespace starrocks::parquet {

class ParquetField;

struct ColumnReaderOptions {
    vectorized::HdfsScanStats* stats = nullptr;
    std::string timezone;
};

class ColumnReader {
public:
    // TODO(zc): review this,
    // create a column reader
    static Status create(RandomAccessFile* file, const ParquetField* field, const tparquet::RowGroup& row_group,
                         const TypeDescriptor& col_type, const ColumnReaderOptions& opts,
                         std::unique_ptr<ColumnReader>* reader);

    virtual ~ColumnReader() = default;

    virtual Status prepare_batch(size_t* num_records, ColumnContentType content_type, vectorized::Column* column, bool check, size_t check_count) = 0;
    virtual Status finish_batch() = 0;

    Status next_batch(size_t* num_records, ColumnContentType content_type, vectorized::Column* column, bool check, size_t check_count) {
        DCHECK(column->is_nullable());
        RETURN_IF_ERROR(prepare_batch(num_records, content_type, column, check, check_count));
        return finish_batch();
    }

    virtual void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) = 0;

    virtual Status get_dict_values(vectorized::Column* column) {
        return Status::NotSupported("get_dict_values is not supported");
    }

    virtual Status get_dict_values(const std::vector<int32_t>& dict_codes, vectorized::Column* column) {
        return Status::NotSupported("get_dict_values is not supported");
    }

    virtual Status get_dict_codes(const std::vector<Slice>& dict_values, std::vector<int32_t>* dict_codes) {
        return Status::NotSupported("get_dict_codes is not supported");
    }
};

} // namespace starrocks::parquet
