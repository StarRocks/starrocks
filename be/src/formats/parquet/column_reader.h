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

#include "column/column_access_path.h"
#include "formats/parquet/column_converter.h"
#include "gen_cpp/PlanNodes_types.h"
#include "io/shared_buffered_input_stream.h"
#include "storage/column_predicate.h"
#include "storage/range.h"

namespace starrocks {
class RandomAccessFile;
struct HdfsScanStats;
} // namespace starrocks

namespace starrocks::parquet {
struct ColumnReaderContext {
    Buffer<uint8_t>* filter = nullptr;
    size_t next_row = 0;
    size_t rows_to_skip = 0;

    void advance(size_t num_rows) { next_row += num_rows; }
};

struct ColumnReaderOptions {
    std::string timezone;
    bool case_sensitive = false;
    int chunk_size = 0;
    HdfsScanStats* stats = nullptr;
    RandomAccessFile* file = nullptr;
    const tparquet::RowGroup* row_group_meta = nullptr;
    uint64_t first_row_index = 0;
    ColumnReaderContext* context = nullptr;
};

class StoredColumnReader;

struct ColumnDictFilterContext {
    constexpr static const LogicalType kDictCodePrimitiveType = TYPE_INT;
    constexpr static const LogicalType kDictCodeFieldType = TYPE_INT;
    // conjunct ctxs for each dict filter column
    std::vector<ExprContext*> conjunct_ctxs;
    // preds transformed from `_conjunct_ctxs` for each dict filter column
    ColumnPredicate* predicate;
    // is output column ? if just used for filter, decode is no need
    bool is_decode_needed;
    SlotId slot_id;
    std::vector<std::string> sub_field_path;
    ObjectPool obj_pool;

public:
    Status rewrite_conjunct_ctxs_to_predicate(StoredColumnReader* reader, bool* is_group_filtered);
};

class ColumnReader {
public:
    // TODO(zc): review this,
    // create a column reader
    static Status create(const ColumnReaderOptions& opts, const ParquetField* field, const TypeDescriptor& col_type,
                         const ColumnAccessPathPtr* column_access_path, std::unique_ptr<ColumnReader>* output);

    // Create with iceberg schema
    static Status create(const ColumnReaderOptions& opts, const ParquetField* field, const TypeDescriptor& col_type,
                         const ColumnAccessPathPtr* column_access_path, const TIcebergSchemaField* iceberg_schema_field,
                         std::unique_ptr<ColumnReader>* output);

    // for struct type without schema change
    static void get_subfield_pos_mapping(const ParquetField& field, const TypeDescriptor& col_type, bool case_sensitive,
                                         std::vector<int32_t>& pos);

    // for schema changed
    static void get_subfield_pos_mapping(const ParquetField& field, const TypeDescriptor& col_type, bool case_sensitive,
                                         const TIcebergSchemaField* iceberg_schema_field, std::vector<int32_t>& pos,
                                         std::vector<const TIcebergSchemaField*>& iceberg_schema_subfield);

    virtual ~ColumnReader() = default;

    virtual Status prepare_batch(size_t* num_records, ColumnPtr& column) = 0;
    virtual Status finish_batch() = 0;

    Status next_batch(size_t* num_records, ColumnPtr& column) {
        RETURN_IF_ERROR(prepare_batch(num_records, column));
        return finish_batch();
    }

    virtual Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) = 0;

    virtual void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) = 0;

    virtual void set_need_parse_levels(bool need_parse_levels) = 0;

    virtual Status get_dict_values(const std::vector<int32_t>& dict_codes, const NullableColumn& nulls,
                                   Column* column) {
        return Status::NotSupported("get_dict_values is not supported");
    }

    virtual bool try_to_use_dict_filter(ExprContext* ctx, bool is_decode_needed, const SlotId slotId,
                                        const std::vector<std::string>& sub_field_path, const size_t& layer) {
        return false;
    }

    virtual Status rewrite_conjunct_ctxs_to_predicate(bool* is_group_filtered,
                                                      const std::vector<std::string>& sub_field_path,
                                                      const size_t& layer) {
        return Status::OK();
    }

    virtual void init_dict_column(ColumnPtr& column, const std::vector<std::string>& sub_field_path,
                                  const size_t& layer) {}

    virtual Status filter_dict_column(const ColumnPtr& column, Filter* filter,
                                      const std::vector<std::string>& sub_field_path, const size_t& layer) {
        return Status::OK();
    }

    virtual Status fill_dst_column(ColumnPtr& dst, const ColumnPtr& src) {
        // src column maybe const column, we can't use column::swap() function
        dst = src;
        return Status::OK();
    }

    std::unique_ptr<ColumnConverter> converter;
};

} // namespace starrocks::parquet
