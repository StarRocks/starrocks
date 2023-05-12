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
#include "formats/parquet/column_converter.h"
#include "gen_cpp/PlanNodes_types.h"
#include "io/shared_buffered_input_stream.h"

namespace starrocks {
class RandomAccessFile;
struct HdfsScanStats;
} // namespace starrocks

namespace starrocks::parquet {

struct ColumnReaderOptions {
    std::string timezone;
    bool case_sensitive = false;
    int chunk_size = 0;
    HdfsScanStats* stats = nullptr;
    RandomAccessFile* file = nullptr;
    io::SharedBufferedInputStream* sb_stream = nullptr;
    tparquet::RowGroup* row_group_meta = nullptr;
};

class ColumnReader {
public:
    // TODO(zc): review this,
    // create a column reader
    static Status create(const ColumnReaderOptions& opts, const ParquetField* field, const TypeDescriptor& col_type,
                         std::unique_ptr<ColumnReader>* output);

    // Create with iceberg schema
    static Status create(const ColumnReaderOptions& opts, const ParquetField* field, const TypeDescriptor& col_type,
                         const TIcebergSchemaField* iceberg_schema_field, std::unique_ptr<ColumnReader>* output);

    virtual ~ColumnReader() = default;

    // Will return actually rows read or InternalError
    // If meet EOF, will return 0
    virtual StatusOr<size_t> prepare_batch(size_t rows_to_read, ColumnContentType* content_type, Column* dst) = 0;

    // Skip specific rows in this ColumnReader
    // Will return actually rows skipped or InternalError
    // If meet EOF, will return 0
    virtual StatusOr<size_t> skip(const size_t rows_to_skip) = 0;

    // Will be called when finish next_batch()
    // You can do some clean work
    virtual Status finish_batch() = 0;

    // Batch read n rows into column
    StatusOr<size_t> next_batch(size_t rows_to_read, ColumnContentType* content_type, Column* dst) {
        ASSIGN_OR_RETURN(size_t rows_read, prepare_batch(rows_to_read, content_type, dst));
        RETURN_IF_ERROR(finish_batch());
        return rows_read;
    }

    // Get last next_batch()'s def/rep levels, like complex types will need this.
    // For required column, will return def_levels=nullptr, rep_levels=nullptr, *num_levels=0
    // For repeated column, will return def_levels=xxx, rep_levels=xxx, *num_levels=xxx
    // For optional column, if you do set_need_parse_levels(), will return def_levels=xxx, rep_levels=nullptr, *num_levels=xxx
    //                      otherwise, def_levels=nullptr, rep_levels=nullptr, *num_levels=0
    virtual void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) = 0;

    // Only works for optional column, if you want to read complex types, you should call this function.
    virtual void set_need_parse_levels(bool need_parse_levels) = 0;

    virtual Status get_dict_values(Column* column) { return Status::NotSupported("get_dict_values is not supported"); }

    virtual Status get_dict_values(const std::vector<int32_t>& dict_codes, Column* column) {
        return Status::NotSupported("get_dict_values is not supported");
    }

    virtual Status get_dict_codes(const std::vector<Slice>& dict_values, std::vector<int32_t>* dict_codes) {
        return Status::NotSupported("get_dict_codes is not supported");
    }

    std::unique_ptr<ColumnConverter> converter;
};

} // namespace starrocks::parquet
