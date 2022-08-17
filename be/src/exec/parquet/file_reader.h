// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <cstdint>
#include <memory>

#include "column/chunk.h"
#include "common/status.h"
#include "exec/parquet/group_reader.h"
#include "gen_cpp/parquet_types.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace starrocks {
class RandomAccessFile;

namespace vectorized {
class HdfsScanStats;
class HdfsFileReaderParam;
} // namespace vectorized

} // namespace starrocks

namespace starrocks::parquet {

class FileMetaData;

class FileReader {
public:
    FileReader(int chunk_size, RandomAccessFile* file, uint64_t file_size);
    ~FileReader();

    Status init(const starrocks::vectorized::HdfsFileReaderParam& param);
    Status get_next(vectorized::ChunkPtr* chunk);

private:
    int _chunk_size;

    // parse footer of parquet file
    Status _parse_footer();

    // pre process schema columns, get the three type columns
    // (1) columns of direct read
    // (2) columns of convert typ
    // (3) columns of not exist in parquet file
    void _pre_process_schema();

    // 1. get conjuncts that column is not exist in file, are used to filter file.
    // 2. remove them from conjunct_ctxs_by_slot.
    void _pre_process_conjunct_ctxs();

    // filter file using not exist column conjuncts
    Status _filter_file();

    // create and inti group reader
    Status _create_and_init_group_reader(int row_group_number);

    // create row group reader
    std::shared_ptr<GroupReader> _row_group(int i);

    // init row group reader
    Status _init_group_reader();

    // filter row group by min/max conjuncts
    Status _filter_group(const tparquet::RowGroup& group, bool* is_filter);

    // get row group to read
    // if scan range conatain the first byte in the row group, will be read
    // TODO: later modify the larger block should be read
    bool _select_row_group(const tparquet::RowGroup& row_group);

    // make min/max chunk from stats of row group meta
    // exist=true: group meta contain statistics info
    Status _read_min_max_chunk(const tparquet::RowGroup& row_group, vectorized::ChunkPtr* min_chunk,
                               vectorized::ChunkPtr* max_chunk, bool* exist) const;

    Status _get_next_internal(vectorized::ChunkPtr* chunk);

    // only scan partition column + not exist column
    Status _exec_only_partition_scan(vectorized::ChunkPtr* chunk);

    // append not exist column
    void _append_not_exist_column_to_chunk(vectorized::ChunkPtr* chunk, size_t row_count);

    // append partition column
    void _append_partition_column_to_chunk(vectorized::ChunkPtr* chunk, size_t row_count);

    // get partition column idx in param.partition_columns
    int _get_partition_column_idx(const std::string& col_name) const;

    // check magic number of parquet file
    // current olny support "PAR1"
    static Status _check_magic(const uint8_t* file_magic);

    // decode min/max value from row group stats
    static Status _decode_min_max_column(const ParquetField& field, const std::string& timezone,
                                         const TypeDescriptor& type, const tparquet::ColumnMetaData& column_meta,
                                         const tparquet::ColumnOrder* column_order, vectorized::ColumnPtr* min_column,
                                         vectorized::ColumnPtr* max_column, bool* decode_ok);
    static bool _can_use_min_max_stats(const tparquet::ColumnMetaData& column_meta,
                                       const tparquet::ColumnOrder* column_order);
    // statistics.min_value max_value
    static bool _can_use_stats(const tparquet::Type::type& type, const tparquet::ColumnOrder* column_order);
    // statistics.min max
    static bool _can_use_deprecated_stats(const tparquet::Type::type& type, const tparquet::ColumnOrder* column_order);
    static bool _is_integer_type(const tparquet::Type::type& type);

    // find column meta according column name
    static const tparquet::ColumnMetaData* _get_column_meta(const tparquet::RowGroup& row_group,
                                                            const std::string& col_name);

    // get the data page start offset in parquet file
    static int64_t _get_row_group_start_offset(const tparquet::RowGroup& row_group);

    RandomAccessFile* _file;
    uint64_t _file_size;

    starrocks::vectorized::HdfsFileReaderParam _param;
    std::shared_ptr<FileMetaData> _file_metadata;
    vector<std::shared_ptr<GroupReader>> _row_group_readers;
    size_t _cur_row_group_idx = 0;
    size_t _row_group_size = 0;
    vectorized::Schema _schema;

    std::vector<GroupReaderParam::Column> _read_cols;

    std::vector<SlotId> _empty_chunk_slot_ids;
    size_t _total_row_count = 0;
    size_t _scan_row_count = 0;
    bool _is_only_partition_scan = false;

    // not exist column conjuncts eval false, file can be skipped
    bool _is_file_filtered = false;
    // conjuncts that column is not exist in file
    std::vector<ExprContext*> _not_exist_column_conjunct_ctxs;
};

} // namespace starrocks::parquet
