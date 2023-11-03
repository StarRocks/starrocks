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

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "formats/parquet/column_reader.h"
#include "formats/parquet/metadata.h"
#include "gen_cpp/parquet_types.h"
#include "io/shared_buffered_input_stream.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
namespace starrocks {
class RandomAccessFile;

struct HdfsScanStats;
} // namespace starrocks

namespace starrocks::parquet {

struct GroupReaderParam {
    struct Column {
        // parquet field index in root node's children
        int32_t field_idx_in_parquet;

        // column index in chunk
        int32_t col_idx_in_chunk;

        // column type in parquet file
        tparquet::Type::type col_type_in_parquet;

        // column type in chunk
        TypeDescriptor col_type_in_chunk;

        const TIcebergSchemaField* t_iceberg_schema_field = nullptr;

        SlotId slot_id;
        bool decode_needed;
    };

    const TupleDescriptor* tuple_desc = nullptr;
    // conjunct_ctxs that column is materialized in group reader
    std::unordered_map<SlotId, std::vector<ExprContext*>> conjunct_ctxs_by_slot;

    // columns
    std::vector<Column> read_cols;

    std::string timezone;

    HdfsScanStats* stats = nullptr;

    io::SharedBufferedInputStream* sb_stream = nullptr;

    int chunk_size = 0;

    RandomAccessFile* file = nullptr;

    uint64_t file_size = 0;

    int64_t file_mtime = 0;

    bool use_file_pagecache = false;

    FileMetaData* file_metadata = nullptr;

    bool case_sensitive = false;
};

class GroupReader {
public:
    GroupReader(GroupReaderParam& param, int row_group_number, const std::set<int64_t>* need_skip_rowids,
                int64_t row_group_first_row);
    ~GroupReader() = default;

    Status init();
    Status get_next(ChunkPtr* chunk, size_t* row_count);
    void close();
    void collect_io_ranges(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset);
    void set_end_offset(int64_t value) { _end_offset = value; }

    void _use_as_dict_filter_column(int col_idx, SlotId slot_id, std::vector<std::string>& sub_field_path);
    Status _rewrite_conjunct_ctxs_to_predicates(bool* is_group_filtered);

    void _init_chunk_dict_column(ChunkPtr* chunk);
    bool _filter_chunk_with_dict_filter(ChunkPtr* chunk, Filter* filter);
    Status _fill_dst_chunk(const ChunkPtr& read_chunk, ChunkPtr* chunk);

    Status _init_column_readers();
    Status _create_column_reader(const GroupReaderParam::Column& column);
    ChunkPtr _create_read_chunk(const std::vector<int>& column_indices);
    // Extract dict filter columns and conjuncts
    void _process_columns_and_conjunct_ctxs();

    bool _try_to_use_dict_filter(const GroupReaderParam::Column& column, ExprContext* ctx,
                                 std::vector<std::string>& sub_field_path, bool is_decode_needed);

    void _init_read_chunk();

    Status _read(const std::vector<int>& read_columns, size_t* row_count, ChunkPtr* chunk);
    Status _lazy_skip_rows(const std::vector<int>& read_columns, const ChunkPtr& chunk, size_t chunk_size);
    void _collect_field_io_range(const ParquetField& field, const TypeDescriptor& col_type,
                                 std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset);
    void _collect_field_io_range(const ParquetField& field, const TypeDescriptor& col_type,
                                 const TIcebergSchemaField* iceberg_schema_field,
                                 std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset);

    // row group meta
    const tparquet::RowGroup* _row_group_metadata = nullptr;
    int64_t _row_group_first_row = 0;
    const std::set<int64_t>* _need_skip_rowids;
    int64_t _raw_rows_read = 0;

    // column readers for column chunk in row group
    std::unordered_map<SlotId, std::unique_ptr<ColumnReader>> _column_readers;

    // conjunct ctxs that eval after chunk is dict decoded
    std::vector<ExprContext*> _left_conjunct_ctxs;

    // active columns that hold read_col index
    std::vector<int> _active_column_indices;
    // lazy conlumns that hold read_col index
    std::vector<int> _lazy_column_indices;

    // dict value is empty after conjunct eval, file group can be skipped
    bool _is_group_filtered = false;

    ChunkPtr _read_chunk;

    // param for read row group
    const GroupReaderParam& _param;

    ObjectPool _obj_pool;

    ColumnReaderOptions _column_reader_opts;

    int64_t _end_offset = 0;

    // columns(index) use as dict filter column
    std::vector<int> _dict_column_indices;
    std::unordered_map<int, std::vector<std::vector<std::string>>> _dict_column_sub_field_paths;
};

} // namespace starrocks::parquet
