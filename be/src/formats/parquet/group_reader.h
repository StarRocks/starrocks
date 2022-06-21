// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "formats/parquet/column_reader.h"
#include "formats/parquet/metadata.h"
#include "gen_cpp/parquet_types.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/vectorized_column_predicate.h"
#include "util/buffered_stream.h"
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
};

class GroupReader {
public:
    GroupReader(int chunk_size, RandomAccessFile* file, FileMetaData* file_metadata, int row_group_number);
    ~GroupReader() = default;

    Status init(const GroupReaderParam& _param);
    Status get_next(vectorized::ChunkPtr* chunk, size_t* row_count);
    void close();

private:
    using SlotIdExprContextsMap = std::unordered_map<int, std::vector<ExprContext*>>;

    Status _init_column_readers();
    Status _create_column_reader(const GroupReaderParam::Column& column);
    Status _set_io_ranges();
    // Extract dict filter columns and conjuncts
    void _process_columns_and_conjunct_ctxs();
    bool _can_using_dict_filter(const SlotDescriptor* slot, const SlotIdExprContextsMap& slot_conjunct_ctxs,
                                const tparquet::ColumnMetaData& column_metadata);
    // Returns true if all of the data pages in the column chunk are dict encoded
    bool _column_all_pages_dict_encoded(const tparquet::ColumnMetaData& column_metadata);
    Status _rewrite_dict_column_predicates();
    void _init_read_chunk();

    Status _read(size_t* row_count);
    void _dict_filter();
    Status _dict_decode(vectorized::ChunkPtr* chunk);

    int _chunk_size;

    RandomAccessFile* _file;
    SharedBufferedInputStream* _sb_stream;

    // parquet file meta
    FileMetaData* _file_metadata;

    // row group number in parquet file
    int _row_group_number;

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

    // dict filter column
    std::vector<GroupReaderParam::Column> _dict_filter_columns;
    // direct read conlumns
    std::vector<GroupReaderParam::Column> _direct_read_columns;

    // dict value is empty after conjunct eval, file group can be skipped
    bool _is_group_filtered = false;

    vectorized::ChunkPtr _read_chunk;
    vectorized::Buffer<uint8_t> _selection;

    // param for read row group
    GroupReaderParam _param;

    ObjectPool _obj_pool;

    ColumnReaderOptions _column_reader_opts;
};

} // namespace starrocks::parquet
