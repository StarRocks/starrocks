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
#include "fs/fs.h"
#include "gen_cpp/binlog.pb.h"
#include "storage/binlog_file_reader.h"
#include "storage/chunk_iterator.h"
#include "storage/rowset/rowset.h"

namespace starrocks {

class BinlogManager;

using BinlogFileMetaPBSharedPtr = std::shared_ptr<BinlogFileMetaPB>;
using BinlogFileReaderSharedPtr = std::shared_ptr<BinlogFileReader>;

struct BinlogReaderParams {
    // Chunk size to read from a segment at a time
    int chunk_size;
    // The schema of output from the reader
    vectorized::VectorizedSchema output_schema;
};

// Column names for metas
const string BINLOG_PREFIX = "_binlog";
const string BINLOG_OP = BINLOG_PREFIX + "_op";
const string BINLOG_VERSION = BINLOG_PREFIX + "_version";
const string BINLOG_SEQ_ID = BINLOG_PREFIX + "_seq_id";
const string BINLOG_TIMESTAMP = BINLOG_PREFIX + "_timestamp";

// Read binlog in a tablet. Binlog can be treated as a table with schema. The schema includes the
// data columns of base table and meta columns of binlog. The name and SQL data type of meta columns
// are as following.
//    +-------------------+---------------+----------------------------------------------------------------------+
//    | Column Name       | SQL Data Type | Description                                                          |
//    +===================+===============+======================================================================+
//    | _binlog_op        | TINYINT       | Operation of the change event in the binlog                          |
//    |                   |               | INSERT (0)                                                           |
//    |                   |               | UPDATE_BEFORE (1)                                                    |
//    |                   |               | UPDATE_AFTER (2)                                                     |
//    |                   |               | DELETE (3)                                                           |
//    +-------------------+---------------+----------------------------------------------------------------------+
//    | _binlog_version   | BIGINT        | The version of ingestion to generate the change event                |
//    +-------------------+---------------+----------------------------------------------------------------------+
//    | _binlog_seq_id    | BIGINT        | The sequence number of the change event in the version               |
//    +-------------------+---------------+----------------------------------------------------------------------+
//    | _binlog_timestamp | BIGINT        | The timestamp to generate the change event. The unit is microsecond. |
//    +-------------------+---------------+----------------------------------------------------------------------+
// BinlogReader will read binlog in chunks, and you can define the schema of the chunk via constructor's
// parameter *schema* which can contain both data columns and meta columns.
//
// How to use
//  std::shared_ptr<BinlogReader> binlog_reader;
//  // seek to the start position
//  Status st = binlog_reader->seek(start_version, start_seq_id);
//  while (st.ok()) {
//     st = binlog_reader->get_next();
//     // process chunk if there are rows in it
//  }
//  // get the next position <next_version, next_seq_id> of binlog
//  // to read, and you can save it as the binlog offset
//  binlog_reader->next_version()
//  binlog_reader->next_seq_id()
//
// Note that the binlog can only be read forward, and you can call seek() multiple
// times to skip to read some binlog, but the seek position must be no less than
// <binlog_reader->next_version(), binlog_reader->next_seq_id()> for each call
//
// TODO currently only support to read binlog from duplicate key table
class BinlogReader final {
public:
    // The last column of schema should include a column
    BinlogReader(std::shared_ptr<BinlogManager> binlog_manager, int64_t reader_id, BinlogReaderParams reader_params);

    ~BinlogReader() { close(); }

    Status init();

    // Seek to the position at <version, seq_id>. The position is inclusive.
    // Returns Status::OK() if find the change event, Status::NotFound() if
    // there is no such event, and other status if error happens
    Status seek(int64_t version, int64_t seq_id);

    // Get a chunk of change events less than the *max_version_exclusive*.
    // The schema of chunk should be the same with BinlogReaderParams#schema.
    // Return Status::OK() if there is at least one change event in the chunk
    // Return Status::EndOfFile() if there is no more change events, or the
    // version of left change events are no less than *max_version_exclusive*.
    // Return Status::NotFound if can't find the change event for the
    // <_next_version, _seq_id> before calling get_next(). This may happen when
    // the versions are not continuous in a tablet replica, and you may need
    // to get the data from ather replicas.
    // Return other status if error happens.
    Status get_next(vectorized::ChunkPtr* chunk, int64_t max_version_exclusive);

    // The version of next binlog to read, and will update after seek/get_next is called.
    int64_t next_version() { return _next_version; }

    // The sequence number of next change event to read, and will update after seek/get_next is called.
    int64_t next_seq_id() { return _next_seq_id; }

    void close();

private:
    Status _seek_binlog_file_reader(int64_t version, int64_t seq_id);
    Status _init_segment_iterator();
    void _release_segment_iterator(bool release_rowset);
    void _reset();
    void _swap_output_and_data_chunk(vectorized::Chunk* output_chunk);
    void _append_meta_column(vectorized::Chunk* output_chunk, int32_t num_rows, int64_t version, int64_t timestamp,
                             int64_t start_seq_id);

    std::shared_ptr<BinlogManager> _binlog_manager;
    int64_t _reader_id;
    BinlogReaderParams _reader_params;
    // Schema for data columns, used to read data from segments
    vectorized::VectorizedSchema _data_schema;
    // Index of each _data_schema column in the _reader_params#output_schema
    std::vector<uint32_t> _data_column_index;
    // Index of each meta column in the _reader_params#output_schema.
    // -1 if they are not in the output schema
    int32_t _binlog_op_column_index = -1;
    int32_t _binlog_version_column_index = -1;
    int32_t _binlog_seq_id_column_index = -1;
    int32_t _binlog_timestamp_column_index = -1;

    // current binlog file to read
    BinlogFileMetaPBSharedPtr _file_meta;
    BinlogFileReaderSharedPtr _binlog_file_reader;
    // owned by _binlog_file_reader
    LogEntryInfo* _log_entry_info;
    int64_t _next_version = -1;
    int64_t _next_seq_id = -1;

    OlapReaderStatistics _stats;
    RowsetSharedPtr _rowset;
    ChunkIteratorPtr _segment_iterator;
    // the chunk delivered to the segment iterator for get_next()
    vectorized::ChunkPtr _data_chunk;

    bool _closed = false;
};

using BinlogReaderSharedPtr = std::shared_ptr<BinlogReader>;

} // namespace starrocks
