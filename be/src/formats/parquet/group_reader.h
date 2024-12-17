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

#include <atomic>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "column/vectorized_fwd.h"
#include "common/global_types.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/expr_context.h"
#include "formats/parquet/column_read_order_ctx.h"
#include "formats/parquet/column_reader.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/utils.h"
#include "gen_cpp/parquet_types.h"
#include "io/shared_buffered_input_stream.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/range.h"

namespace starrocks {
class RandomAccessFile;
struct HdfsScanStats;
class ExprContext;
class TIcebergSchemaField;

namespace parquet {
class FileMetaData;
} // namespace parquet
struct TypeDescriptor;
} // namespace starrocks

namespace starrocks::parquet {

struct GroupReaderParam {
    struct Column {
        // parquet field index in root node's children
        int32_t idx_in_parquet;

        // column type in parquet file
        tparquet::Type::type type_in_parquet;

        SlotDescriptor* slot_desc = nullptr;

        const TIcebergSchemaField* t_iceberg_schema_field = nullptr;

        bool decode_needed;

        const TypeDescriptor& slot_type() const { return slot_desc->type(); }
        const SlotId slot_id() const { return slot_desc->id(); }
    };

    // conjunct_ctxs that column is materialized in group reader
    std::unordered_map<SlotId, std::vector<ExprContext*>> conjunct_ctxs_by_slot;

    // columns
    std::vector<Column> read_cols;

    std::string timezone;

    HdfsScanStats* stats = nullptr;

    io::SharedBufferedInputStream* sb_stream = nullptr;

    int chunk_size = 0;

    RandomAccessFile* file = nullptr;

    FileMetaData* file_metadata = nullptr;

    bool case_sensitive = false;

    // used to identify io coalesce
    std::atomic<int32_t>* lazy_column_coalesce_counter = nullptr;

    // used for pageIndex
    std::vector<ExprContext*> min_max_conjunct_ctxs;
    const PredicateTree* predicate_tree = nullptr;

    // partition column
    const std::vector<HdfsScannerContext::ColumnInfo>* partition_columns = nullptr;
    // partition column value which read from hdfs file path
    const std::vector<ColumnPtr>* partition_values = nullptr;
    // not existed column
    const std::vector<SlotDescriptor*>* not_existed_slots = nullptr;
};

class PageIndexReader;

class GroupReader {
    friend class PageIndexReader;

public:
    GroupReader(GroupReaderParam& param, int row_group_number, const std::set<int64_t>* need_skip_rowids,
                int64_t row_group_first_row);
    ~GroupReader();

    // init used to init column reader, and devide active/lazy
    // then we can use inited column collect io range.
    Status init();
    Status prepare();
    const tparquet::ColumnChunk* get_chunk_metadata(SlotId slot_id);
    const ParquetField* get_column_parquet_field(SlotId slot_id);
    ColumnReader* get_column_reader(SlotId slot_id);
    uint64_t get_row_group_first_row() const { return _row_group_first_row; }
    const tparquet::RowGroup* get_row_group_metadata() const;
    Status get_next(ChunkPtr* chunk, size_t* row_count);
    void collect_io_ranges(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                           ColumnIOType type = ColumnIOType::PAGES);

    SparseRange<uint64_t> get_range() const { return _range; }

private:
    void _set_end_offset(int64_t value) { _end_offset = value; }

    // deal_with_pageindex need collect pageindex io range first, it will collect all row groups' io together,
    // so it should be done in file reader. when reading the current row group, we need first deal_with_pageindex,
    // and then we can collect io range based on pageindex.
    Status _deal_with_pageindex();

    void _use_as_dict_filter_column(int col_idx, SlotId slot_id, std::vector<std::string>& sub_field_path);
    Status _rewrite_conjunct_ctxs_to_predicates(bool* is_group_filtered);

    StatusOr<bool> _filter_chunk_with_dict_filter(ChunkPtr* chunk, Filter* filter);
    Status _fill_dst_chunk(ChunkPtr& read_chunk, ChunkPtr* chunk);

    Status _create_column_readers();
    StatusOr<ColumnReaderPtr> _create_column_reader(const GroupReaderParam::Column& column);
    Status _prepare_column_readers() const;
    ChunkPtr _create_read_chunk(const std::vector<int>& column_indices);
    // Extract dict filter columns and conjuncts
    void _process_columns_and_conjunct_ctxs();

    bool _try_to_use_dict_filter(const GroupReaderParam::Column& column, ExprContext* ctx,
                                 std::vector<std::string>& sub_field_path, bool is_decode_needed);

    void _init_read_chunk();

    Status _read_range(const std::vector<int>& read_columns, const Range<uint64_t>& range, const Filter* filter,
                       ChunkPtr* chunk);

    StatusOr<size_t> _read_range_round_by_round(const Range<uint64_t>& range, Filter* filter, ChunkPtr* chunk);

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
    // load lazy column or not
    bool _lazy_column_needed = false;

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
    std::unordered_map<SlotId, std::vector<ExprContext*>> _left_no_dict_filter_conjuncts_by_slot;

    SparseRange<uint64_t> _range;
    SparseRangeIterator<uint64_t> _range_iter;

    // round by round ctx
    std::unique_ptr<ColumnReadOrderCtx> _column_read_order_ctx;

    // a flag to reflect prepare() is called
    bool _has_prepared = false;
};

using GroupReaderPtr = std::shared_ptr<GroupReader>;

} // namespace starrocks::parquet
