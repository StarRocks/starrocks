// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "formats/parquet/column_reader.h"
#include "formats/parquet/metadata.h"
#include "gen_cpp/parquet_types.h"
#include "io/shared_buffered_input_stream.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/vectorized_column_predicate.h"
#include "util/runtime_profile.h"
namespace starrocks {
class RandomAccessFile;

namespace vectorized {
struct HdfsScanStats;
}
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

        SlotId slot_id;
    };

    const TupleDescriptor* tuple_desc = nullptr;
    // conjunct_ctxs that column is materialized in group reader
    std::unordered_map<SlotId, std::vector<ExprContext*>> conjunct_ctxs_by_slot;

    // columns
    std::vector<Column> read_cols;

    std::string timezone;

    vectorized::HdfsScanStats* stats = nullptr;

    io::SharedBufferedInputStream* sb_stream = nullptr;

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
    Status get_next(vectorized::ChunkPtr* chunk, size_t* row_count);
    void close();
    void collect_io_ranges(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset);
    void set_end_offset(int64_t value) { _end_offset = value; }

private:
    using SlotIdExprContextsMap = std::unordered_map<int, std::vector<ExprContext*>>;

    Status _init_column_readers();
    Status _create_column_reader(const GroupReaderParam::Column& column);
    vectorized::ChunkPtr _create_read_chunk(const std::vector<int>& column_indices);
    // Extract dict filter columns and conjuncts
    void _process_columns_and_conjunct_ctxs();
    bool _can_using_dict_filter(const SlotDescriptor* slot, const SlotIdExprContextsMap& slot_conjunct_ctxs,
                                const tparquet::ColumnMetaData& column_metadata);
    // Returns true if all of the data pages in the column chunk are dict encoded
    bool _column_all_pages_dict_encoded(const tparquet::ColumnMetaData& column_metadata);
    Status _rewrite_dict_column_predicates();
    void _init_read_chunk();

    Status _read(const std::vector<int>& read_columns, size_t* row_count, vectorized::ChunkPtr* chunk);
    Status _lazy_skip_rows(const std::vector<int>& read_columns, const vectorized::ChunkPtr& chunk, size_t chunk_size);
    void _dict_filter(vectorized::ChunkPtr* chunk, vectorized::Filter* filter_ptr);
    Status _dict_decode(vectorized::ChunkPtr* chunk);
    void _collect_field_io_range(const ParquetField& field, const TypeDescriptor& col_type,
                                 std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset);

    // row group meta
    std::shared_ptr<tparquet::RowGroup> _row_group_metadata;

    // column readers for column chunk in row group
    std::unordered_map<SlotId, std::unique_ptr<ColumnReader>> _column_readers;
    // conjunct ctxs for each dict filter column
    std::unordered_map<SlotId, std::vector<ExprContext*>> _dict_filter_conjunct_ctxs;
    // preds transformed from conjunct ctxs for each dict filter column
    std::unordered_map<SlotId, vectorized::ColumnPredicate*> _dict_filter_preds;
    // conjunct ctxs that eval after chunk is dict decoded
    std::vector<ExprContext*> _left_conjunct_ctxs;

    // dict filter column index
    std::vector<int> _dict_filter_column_indices;
    std::vector<bool> _use_as_dict_filter_column;
    // direct read conlumn index
    std::vector<int> _direct_read_column_indices;
    // active columns that hold read_col index
    std::vector<int> _active_column_indices;
    // lazy conlumns that hold read_col index
    std::vector<int> _lazy_column_indices;

    // dict value is empty after conjunct eval, file group can be skipped
    bool _is_group_filtered = false;

    vectorized::ChunkPtr _read_chunk;

    // param for read row group
    const GroupReaderParam& _param;

    ObjectPool _obj_pool;

    ColumnReaderOptions _column_reader_opts;

    int64_t _end_offset = 0;
};

} // namespace starrocks::parquet
