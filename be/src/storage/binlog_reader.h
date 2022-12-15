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
#include "fs/fs.h"
#include "gen_cpp/binlog.pb.h"
#include "storage/binlog_file_reader.h"
#include "storage/chunk_iterator.h"
#include "storage/rowset/rowset.h"

namespace starrocks {

class BinlogManager;

using BinlogFileMetaPBSharedPtr = std::shared_ptr<BinlogFileMetaPB>;
using BinlogFileReaderSharedPtr = std::shared_ptr<BinlogFileReader>;

// Read binlog in a tablet. Binlog can be treated as a table with schema. The schema includes the
// data columns of base table and meta columns of binlog. The name and SQL data type of meta columns
// are as followings.
//    +-------------------+---------------+----------------------------------------------------------------------+
//    | Column Name       | SQL Data Type | Description                                                          |
//    +===================+===============+======================================================================+
//    | _binlog_op        | TINYINT       | Operation of the change event in the binlog                          |
//    |                   |               | INSERT (0)                                                           |
//    |                   |               | UPDATE_BEFORE (1)                                                    |
//    |                   |               | UPDATE_AFTER (2)                                                     |
//    |                   |               | DELETE (3)                                                           |
//    +-------------------+---------------+----------------------------------------------------------------------+
//    | _binlog_version   | BIGINT        | The version to generate the change event                             |
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
// <binlog_reader->next_version(), binlog_reader->next_seq_id()>
//
// TODO currently only support to read binlog from duplicate key table
class BinlogReader final {
public:
    // The last column of schema should include a column
    BinlogReader(int64_t reader_id, std::shared_ptr<BinlogManager> binlog_manager, vectorized::VectorizedSchema& schema,
                 int chunk_size);

    ~BinlogReader();

    // Seek to the position at <version, seq_id>. The position is inclusive.
    // Returns Status::OK() if find the change event, Status::NotFound() if
    // there is no such event, and other status if error happens
    Status seek(int64_t version, int64_t seq_id);

    // Get a chunk of change events less than the *max_version_exclusive*.
    // Return Status::OK() if there is at least one change event in the chunk,
    // Status::EndOfFile() if there is no more change events, and other status
    // if error happens
    Status get_next(vectorized::ChunkPtr* chunk, int64_t max_version_exclusive);

    // The version of next binlog to read, and will update after seek/get_next is called.
    int64_t next_version() { return _next_version; }

    // The sequence number of next change event to read, and will update after seek/get_next is called.
    int64_t next_seq_id() { return _next_seq_id; }

private:
    Status _seek_to_file_meta(int64_t version, int64_t seq_id);
    Status _seek_to_segment_row(int64_t seq_id);
    Status _init_segment_iterator(int32_t start_row_id);
    void _reset();

    uint64_t _reader_id;
    std::shared_ptr<BinlogManager> _binlog_manager;
    vectorized::VectorizedSchema _schema;
    int32_t _chunk_size;

    BinlogFileMetaPBSharedPtr _file_meta;
    BinlogFileReaderSharedPtr _binlog_file_reader;
    // owned by _binlog_file_reader
    LogEntryInfo* _log_entry_info;
    int64_t _next_version = -1;
    int64_t _next_seq_id = -1;
    RowsetSharedPtr _rowset;
    ChunkIteratorPtr _segment_iterator;
};

using BinlogReaderSharedPtr = std::shared_ptr<BinlogReader>;

} // namespace starrocks
