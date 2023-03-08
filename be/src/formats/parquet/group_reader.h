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
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/column_predicate.h"
#include "util/buffered_stream.h"
#include "util/runtime_profile.h"
namespace starrocks {
class RandomAccessFile;

struct HdfsScanStats;
} // namespace starrocks

namespace starrocks::parquet {

struct GroupReaderParam {
    struct Column {
        // column index in parquet file
        int col_idx_in_parquet;

        // column index in chunk
        int col_idx_in_chunk;

        // column type in parquet file
        tparquet::Type::type col_type_in_parquet;

        // column type in chunk
        TypeDescriptor col_type_in_chunk;

        const TIcebergSchemaField* t_iceberg_schema_field = nullptr;

        SlotId slot_id;
    };

    const TupleDescriptor* tuple_desc = nullptr;
    // conjunct_ctxs that column is materialized in group reader
    std::unordered_map<SlotId, std::vector<ExprContext*>> conjunct_ctxs_by_slot;

    // columns
    std::vector<Column> read_cols;

    std::string timezone;

    HdfsScanStats* stats = nullptr;

    SharedBufferedInputStream* shared_buffered_stream = nullptr;

    int chunk_size = 0;

    RandomAccessFile* file = nullptr;

    FileMetaData* file_metadata = nullptr;

    bool case_sensitive = false;
};

class GroupReader {
public:
    GroupReader(GroupReaderParam& param, int row_group_number);
    ~GroupReader() = default;

    Status init();
    Status get_next(ChunkPtr* chunk, size_t* row_count);
    void close();
    void collect_io_ranges(std::vector<SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset);
    void set_end_offset(int64_t value) { _end_offset = value; }

private:
    struct DictFilterContext {
    public:
        void init(size_t column_number);
        void use_as_dict_filter_column(int col_idx, SlotId slot_id, const std::vector<ExprContext*>& conjunct_ctxs);
        Status rewrite_conjunct_ctxs_to_predicates(
                const GroupReaderParam& param,
                std::unordered_map<SlotId, std::unique_ptr<ColumnReader>>& column_readers, ObjectPool* obj_pool,
                bool* is_group_filtered);

        void init_chunk(const GroupReaderParam& param, ChunkPtr* chunk);
        bool filter_chunk(ChunkPtr* chunk, Filter* filter);
        Status decode_chunk(const GroupReaderParam& param,
                            std::unordered_map<SlotId, std::unique_ptr<ColumnReader>>& column_readers,
                            const ChunkPtr& read_chunk, ChunkPtr* chunk);
        ColumnContentType column_content_type(int col_idx) {
            if (_is_dict_filter_column[col_idx]) {
                return ColumnContentType::DICT_CODE;
            }
            return ColumnContentType::VALUE;
        }

    private:
        // if this column use as dict filter?
        std::vector<bool> _is_dict_filter_column;
        // columns(index) use as dict filter column
        std::vector<int> _dict_column_indices;
        // conjunct ctxs for each dict filter column
        std::unordered_map<SlotId, std::vector<ExprContext*>> _conjunct_ctxs_by_slot;
        // preds transformed from `_conjunct_ctxs_by_slot` for each dict filter column
        std::unordered_map<SlotId, ColumnPredicate*> _predicates;
    };

    using SlotIdExprContextsMap = std::unordered_map<int, std::vector<ExprContext*>>;

    Status _init_column_readers();
    Status _create_column_reader(const GroupReaderParam::Column& column);
    ChunkPtr _create_read_chunk(const std::vector<int>& column_indices);
    // Extract dict filter columns and conjuncts
    void _process_columns_and_conjunct_ctxs();
    bool _can_use_as_dict_filter_column(const SlotDescriptor* slot, const SlotIdExprContextsMap& slot_conjunct_ctxs,
                                        const tparquet::ColumnMetaData& column_metadata);
    // Returns true if all of the data pages in the column chunk are dict encoded
    static bool _column_all_pages_dict_encoded(const tparquet::ColumnMetaData& column_metadata);
    void _init_read_chunk();

    Status _read(const std::vector<int>& read_columns, size_t* row_count, ChunkPtr* chunk);
    Status _lazy_skip_rows(const std::vector<int>& read_columns, const ChunkPtr& chunk, size_t chunk_size);
    void _dict_filter(ChunkPtr* chunk, Filter* filter_ptr);
    Status _dict_decode(ChunkPtr* chunk);
    void _collect_field_io_range(const ParquetField& field, std::vector<SharedBufferedInputStream::IORange>* ranges,
                                 int64_t* end_offset);

    // row group meta
    std::shared_ptr<tparquet::RowGroup> _row_group_metadata;

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

    DictFilterContext _dict_filter_ctx;
};

} // namespace starrocks::parquet
