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

#include <cstdint>
#include <memory>

#include "column/chunk.h"
#include "common/status.h"
#include "formats/parquet/group_reader.h"
#include "formats/parquet/meta_helper.h"
#include "gen_cpp/parquet_types.h"
#include "runtime/runtime_state.h"
#include "util/buffered_stream.h"
#include "util/runtime_profile.h"

namespace starrocks {
class RandomAccessFile;

struct HdfsScannerContext;

} // namespace starrocks

namespace starrocks::parquet {

constexpr static const uint64_t FOOTER_BUFFER_SIZE = 16 * 1024;
constexpr static const char* PARQUET_MAGIC_NUMBER = "PAR1";

class FileMetaData;

class FileReader {
public:
    FileReader(int chunk_size, RandomAccessFile* file, uint64_t file_size);
    ~FileReader();

    Status init(HdfsScannerContext* scanner_ctx);

    Status get_next(ChunkPtr* chunk);

private:
    int _chunk_size;

    // parse footer of parquet file
    Status _parse_footer();

    void _prepare_read_columns();

    // init row group readers.
    Status _init_group_readers();

    // filter row group by min/max conjuncts
    StatusOr<bool> _filter_group(const tparquet::RowGroup& row_group);

    // get row group to read
    // if scan range conatain the first byte in the row group, will be read
    // TODO: later modify the larger block should be read
    bool _select_row_group(const tparquet::RowGroup& row_group);

    // make min/max chunk from stats of row group meta
    // exist=true: group meta contain statistics info
    Status _read_min_max_chunk(const tparquet::RowGroup& row_group, const std::vector<SlotDescriptor*>& slots,
                               ChunkPtr* min_chunk, ChunkPtr* max_chunk, bool* exist) const;

    Status _get_next_internal(ChunkPtr* chunk);

    // only scan partition column + not exist column
    Status _exec_only_partition_scan(ChunkPtr* chunk);

    // get partition column idx in param.partition_columns
    int32_t _get_partition_column_idx(const std::string& col_name) const;

    // check magic number of parquet file
    // current olny support "PAR1"
    static Status _check_magic(const uint8_t* file_magic);

    // decode min/max value from row group stats
    static Status _decode_min_max_column(const ParquetField& field, const std::string& timezone,
                                         const TypeDescriptor& type, const tparquet::ColumnMetaData& column_meta,
                                         const tparquet::ColumnOrder* column_order, ColumnPtr* min_column,
                                         ColumnPtr* max_column, bool* decode_ok);
    static bool _can_use_min_max_stats(const tparquet::ColumnMetaData& column_meta,
                                       const tparquet::ColumnOrder* column_order);
    // statistics.min_value max_value
    static bool _can_use_stats(const tparquet::Type::type& type, const tparquet::ColumnOrder* column_order);
    // statistics.min max
    static bool _can_use_deprecated_stats(const tparquet::Type::type& type, const tparquet::ColumnOrder* column_order);
    static bool _is_integer_type(const tparquet::Type::type& type);

    // get the data page start offset in parquet file
    static int64_t _get_row_group_start_offset(const tparquet::RowGroup& row_group);

    RandomAccessFile* _file = nullptr;
    uint64_t _file_size = 0;

    std::shared_ptr<FileMetaData> _file_metadata;
    std::vector<std::shared_ptr<GroupReader>> _row_group_readers;
    size_t _cur_row_group_idx = 0;
    size_t _row_group_size = 0;

    size_t _total_row_count = 0;
    size_t _scan_row_count = 0;
    bool _is_only_partition_scan = false;

    // not exist column conjuncts eval false, file can be skipped
    bool _is_file_filtered = false;
    HdfsScannerContext* _scanner_ctx = nullptr;
    std::shared_ptr<SharedBufferedInputStream> _sb_stream = nullptr;
    GroupReaderParam _group_reader_param;
    std::shared_ptr<MetaHelper> _meta_helper = nullptr;
};

} // namespace starrocks::parquet
