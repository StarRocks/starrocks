// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <memory>

#include "column_converter.h"

namespace starrocks::parquet {

class ColumnReader {
public:
    // TODO(zc): review this,
    // create a column reader
    static Status create(RandomAccessFile* file, const ParquetField* field, const tparquet::RowGroup& row_group,
                         const TypeDescriptor& col_type, const ColumnReaderOptions& opts,
                         std::unique_ptr<ColumnReader>* reader);

    virtual ~ColumnReader() = default;

    virtual Status prepare_batch(size_t* num_records, ColumnContentType content_type, vectorized::Column* column) = 0;
    virtual Status finish_batch() = 0;

    Status next_batch(size_t* num_records, ColumnContentType content_type, vectorized::Column* column) {
        RETURN_IF_ERROR(prepare_batch(num_records, content_type, column));
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

public:
    std::unique_ptr<ColumnConverter> converter;
};

} // namespace starrocks::parquet
