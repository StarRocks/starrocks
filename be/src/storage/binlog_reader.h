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

#pragma

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

class BinlogReader final {
public:
    // The last column of schema should include a column
    BinlogReader(std::shared_ptr<BinlogManager> binlog_manager, vectorized::Schema& schema, int64_t reader_id,
                 int64_t expire_time_in_ms, int chunk_size);

    ~BinlogReader();

    Status seek(int64_t version, int64_t changelog_id);

    // Get next chunk of binlog data which is less than the *max_version*.
    Status get_next(vectorized::ChunkPtr* chunk, int64_t max_version_exclusive);

    int64_t next_version() { return _next_version; }

    int64_t next_changelog_id() { return _next_changelog_id; }

    // If this reader has not been read for a given time, it will be expired
    bool is_expired() { return _expire_time_in_ms > 0 && _last_read_time_in_ms + _expire_time_in_ms > UnixMillis(); }

private:
    Status _seek_to_file_meta(int64_t version, int64_t changelog_id);
    Status _seek_to_segment_row(int64_t changelog_id);
    Status _init_segment_iterator(int32_t start_row_id);
    void _reset();

    std::shared_ptr<BinlogManager> _binlog_manager;
    vectorized::Schema _schema;
    uint64_t _reader_id;
    int32_t _chunk_size;
    int64_t _expire_time_in_ms;
    int64_t _last_read_time_in_ms;

    BinlogFileMetaPBSharedPtr _file_meta;
    BinlogFileReaderSharedPtr _binlog_file_reader;
    // owned by _binlog_file_reader
    LogEntryInfo* _log_entry_info;
    int64_t _next_version = -1;
    int64_t _next_changelog_id = -1;
    RowsetSharedPtr _rowset;
    ChunkIteratorPtr _segment_iterator;
};

using BinlogReaderSharedPtr = std::shared_ptr<BinlogReader>;

} // namespace starrocks
