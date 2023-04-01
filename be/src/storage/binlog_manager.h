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

#include <gutil/strings/substitute.h>

#include <cstdint>
#include <map>
#include <unordered_map>

#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/binlog.pb.h"
#include "storage/binlog_builder.h"
#include "storage/binlog_file_writer.h"
#include "storage/binlog_reader.h"
#include "util/blocking_queue.hpp"

namespace starrocks {

struct BinlogConfig {
    int64_t version;
    bool binlog_enable;
    int64_t binlog_ttl_second;
    int64_t binlog_max_size;

    void update(const BinlogConfig& new_config) {
        update(new_config.version, new_config.binlog_enable, new_config.binlog_ttl_second, new_config.binlog_max_size);
    }

    void update(const TBinlogConfig& new_config) {
        update(new_config.version, new_config.binlog_enable, new_config.binlog_ttl_second, new_config.binlog_max_size);
    }

    void update(const BinlogConfigPB& new_config) {
        update(new_config.version(), new_config.binlog_enable(), new_config.binlog_ttl_second(),
               new_config.binlog_max_size());
    }

    void update(int64_t new_version, bool new_binlog_enable, int64_t new_binlog_ttl_second,
                int64_t new_binlog_max_size) {
        version = new_version;
        binlog_enable = new_binlog_enable;
        binlog_ttl_second = new_binlog_ttl_second;
        binlog_max_size = new_binlog_max_size;
    }

    void to_pb(BinlogConfigPB* binlog_config_pb) {
        binlog_config_pb->set_version(version);
        binlog_config_pb->set_binlog_enable(binlog_enable);
        binlog_config_pb->set_binlog_ttl_second(binlog_ttl_second);
        binlog_config_pb->set_binlog_max_size(binlog_max_size);
    }

    std::string to_string() {
        return strings::Substitute(
                "BinlogConfig={version=$0, binlog_enable=$1, binlog_ttl_second=$2, binlog_max_size=$3}", version,
                binlog_enable, binlog_ttl_second, binlog_max_size);
    }
};

class Tablet;
class Rowset;

// Fetch rowsets
class RowsetFetcher {
public:
    virtual ~RowsetFetcher() = default;
    virtual std::shared_ptr<Rowset> get_rowset(int64_t rowset_id) = 0;
};

// Rowset fetcher for duplicate key table, and should be protected
// by Tablet#_meta_lock outside
class DupKeyRowsetFetcher : public RowsetFetcher {
public:
    DupKeyRowsetFetcher(Tablet& tablet);

    // Rowset id for duplicate key is version
    std::shared_ptr<Rowset> get_rowset(int64_t rowset_id) override;

private:
    Tablet& _tablet;
};

class BinlogRange {
public:
    BinlogRange(int64_t start_version, int64_t start_seq_id, int64_t end_version, int64_t end_seq_id)
            : _start_version(start_version),
              _start_seq_id(start_seq_id),
              _end_version(end_version),
              _end_seq_id(end_seq_id) {}

    bool is_empty() {
        return _start_version > _end_version || (_start_version == _end_version && _start_seq_id > _end_seq_id);
    }

    int64_t start_version() const { return _start_version; }
    int64_t start_seq_id() const { return _start_seq_id; }
    int64_t end_version() const { return _end_version; }
    int64_t end_seq_id() const { return _end_seq_id; }

    std::string debug_string() const {
        std::stringstream out;
        out << "BinlogRange(start_version=" << _start_version << ", start_seq_id=" << _start_seq_id
            << ", end_version=" << _end_version << ", end_seq_id=" << _end_seq_id << ")";
        return out.str();
    }

private:
    int64_t _start_version;
    int64_t _start_seq_id;
    int64_t _end_version;
    int64_t _end_seq_id;
};

// Hold a reference to the binlog file for read. The reference will be released
// automatically in the destructor
class BinlogFileReadHolder {
public:
    BinlogFileReadHolder(std::shared_ptr<std::atomic<int64_t>> _reader_count, BinlogFileMetaPBPtr file_meta)
            : _reader_count(_reader_count), _file_meta(file_meta) {
        _reader_count->fetch_add(1);
    }

    ~BinlogFileReadHolder() { _reader_count->fetch_sub(1); }

    BinlogFileMetaPBPtr& file_meta() { return _file_meta; }

private:
    std::shared_ptr<std::atomic<int64_t>> _reader_count;
    BinlogFileMetaPBPtr _file_meta;
};

using BinlogFileReadHolderPtr = std::shared_ptr<BinlogFileReadHolder>;

// Representation of a binlog file. It includes the file meta, and a
// reference count about how many readers are using it
class BinlogFile {
public:
    BinlogFile(BinlogFileMetaPBPtr file_meta) : _file_meta(file_meta) {
        _reader_count = std::make_shared<std::atomic<int64_t>>();
    }

    BinlogFileMetaPBPtr& file_meta() { return _file_meta; }

    void update_file_meta(BinlogFileMetaPBPtr& file_meta) { _file_meta = file_meta; }

    BinlogFileReadHolderPtr new_read_holder() {
        return std::make_shared<BinlogFileReadHolder>(_reader_count, _file_meta);
    }

    int64_t reader_count() { return _reader_count->load(); }

private:
    BinlogFileMetaPBPtr _file_meta;
    std::shared_ptr<std::atomic<int64_t>> _reader_count;
};
using BinlogFilePtr = std::shared_ptr<BinlogFile>;

// Manages the binlog metas and files, including generation, deletion and read.
class BinlogManager {
public:
    BinlogManager(int64_t tablet_id, std::string path, int64_t max_file_size, int32_t max_page_size,
                  CompressionTypePB compression_type, std::shared_ptr<RowsetFetcher> rowset_fetcher);

    ~BinlogManager();

    //  The process of an ingestion is as following, and protected by Tablet#_meta_lock to ensure there is
    //  no concurrent ingestion for duplicate key table
    //    +-------+  success to build binlog  +------------------------+ RowsetMetaPB persisted  +-----------------+
    //    | begin | ------------------------> | precommit(not visible) | --------------------->  | commit(visible) |
    //    +-------+  (binlog file persisted)  +------------------------+                         +-----------------+
    //      |                                       |
    //      | fail to build binlog                  | fail to persist RowsetMetaPB
    //      v                                       v
    //    +-------+                            +--------+
    //    | abort |                            | delete |
    //    +-------+                            +--------+

    // Begin to ingest binlog for a new version. Return a BinlogBuilderParams to be
    // used by BinlogBuilder. Status::AlreadyExist will be returned if this version
    // is no less than the current max version, this will happen if multiple ingestion
    // is published out of order. Note that even *enable_new_publish_mechanism* is
    // enabled, this still could happen. For example, there are 3 replicas r0, r1, r2,
    //   1. r0 and r1 publish version 2 successfully, but r3 has not run the publish task
    //   2. FE decides to publish version 3 because version 2 has two successful replicas
    ///  3. r3 receives the publish task for version 3, and run it successfully
    //   4. r3 runs the publish task for version 2 and is successful
    // As a result, version 2 and 3 are published out of order. We can ignore to generate
    // binlog for version 2, and can read it on other replicas.
    StatusOr<BinlogBuilderParamsPtr> begin_ingestion(int64_t version);

    // Pre-commit the result of BinlogBuilder, and binlog files are guaranteed to be persisted,
    // but it's not visible for reading. BinlogBuildResult includes the information of persisted
    // binlog files.
    void precommit_ingestion(int64_t version, BinlogBuildResultPtr result);

    // Abort the ingestion if error happens when building(persisting) binlog files.
    // The BinlogBuildResult includes the information of
    void abort_ingestion(int64_t version, BinlogBuildResultPtr result);

    // Delete the result of pre-commit. This can happen when pre-commit successes,
    // but fail to persist RowsetMetaPB in Tablet#add_inc_rowset
    void delete_ingestion(int64_t version);

    // Commit the result of pre-commit, and it's visible for reading
    void commit_ingestion(int64_t version);

    // Check expiration and capacity. It should be protected by Tablet#_meta_lock outside
    // because Tablet#_inc_rs_version_map may be visited. This method only updates metas
    // of binlog files that should be deleted, but not do the deletion which will be done
    // in delete_unused_binlog() later, so Tablet#_meta_lock will not be blocked too long.
    void check_expire_and_capacity(int64_t current_second, int64_t binlog_ttl_second, int64_t binlog_max_size);

    // Whether the rowset is used by the binlog.
    bool is_rowset_used(int64_t rowset_id);

    // Delete unused binlog files
    void delete_unused_binlog();

    // Delete all data, and only called in Tablet::delete_all_files currently
    void delete_all_binlog();

    // Register the reader, and return a unique id allocated for this reader.
    StatusOr<int64_t> register_reader(std::shared_ptr<BinlogReader> reader);

    // Unregister the reader with the given id.
    void unregister_reader(int64_t reader_id);

    // Find the binlog file which may contain the change event with given <version, seq_id>.
    // Return Status::NotFound if there is no such file.
    StatusOr<BinlogFileReadHolderPtr> find_binlog_file(int64_t version, int64_t seq_id);

    std::string get_binlog_file_path(int64_t file_id) { return BinlogUtil::binlog_file_path(_path, file_id); }

    BinlogRange current_binlog_range();

    // Following methods are for testing currently
    int64_t next_file_id() { return _next_file_id; }

    int64_t ingestion_version() { return _ingestion_version; }

    BinlogBuildResult* build_result() { return _build_result.get(); }

    BinlogFileWriter* active_binlog_writer() { return _active_binlog_writer.get(); }

    std::map<int128_t, BinlogFilePtr>& alive_binlog_files() { return _alive_binlog_files; }

    std::unordered_map<int64_t, int32_t>& alive_rowset_count_map() { return _alive_rowset_count_map; }

    int64_t total_alive_binlog_file_size() { return _total_alive_binlog_file_size; }

    int64_t total_alive_rowset_data_size() { return _total_alive_rowset_data_size; }

    std::deque<BinlogFilePtr>& wait_reader_binlog_files() { return _wait_reader_binlog_files; }

    std::unordered_map<int64_t, int32_t>& wait_reader_rowset_count_map() { return _wait_reader_rowset_count_map; }

    int64_t total_wait_reader_binlog_file_size() { return _total_wait_reader_binlog_file_size; }

    int64_t total_wait_reader_rowset_data_size() { return _total_wait_reader_rowset_data_size; }

    BlockingQueue<int64_t>& unused_binlog_file_ids() { return _unused_binlog_file_ids; }

    void close_active_writer();

private:
    void _apply_build_result(BinlogBuildResult* result);
    void _check_wait_reader_binlog_files();
    void _check_alive_binlog_files(int64_t current_second, int64_t binlog_ttl_second, int64_t binlog_max_size);

    int64_t _tablet_id;
    // binlog storage directory
    std::string _path;
    int64_t _max_file_size;
    int32_t _max_page_size;
    CompressionTypePB _compression_type;

    std::shared_ptr<RowsetFetcher> _rowset_fetcher;

    // file id for the next binlog file. Protected by Tablet#_meta_lock
    std::atomic<int64_t> _next_file_id = 0;
    // the version of running ingestion. -1 indicates no ingestion.
    // Protected by Tablet#_meta_lock
    int64_t _ingestion_version = -1;
    // The result after pre-commit ingestion. Protected by Tablet#_meta_lock
    BinlogBuildResultPtr _build_result;

    // protect following metas' read/write
    std::shared_mutex _meta_lock;

    // Alive binlog files (not expired and overcapacity), and can serve for read. Map from start
    // LSN(start_version, start_seq_id) of a binlog file to the file meta. A binlog file with a
    // smaller start LSN also has a smaller file id. The file with the biggest start LSN is the
    // meta of _active_binlog_writer if it's not null.
    std::map<int128_t, BinlogFilePtr> _alive_binlog_files;
    // Alive rowsets. Map from rowset id to the number of binlog files using it in _alive_binlog_files
    std::unordered_map<int64_t, int32_t> _alive_rowset_count_map;
    // Disk size for alive binlog files
    std::atomic<int64_t> _total_alive_binlog_file_size = 0;
    // Disk size for alive rowsets
    std::atomic<int64_t> _total_alive_rowset_data_size = 0;

    // the binlog file writer that can append data
    BinlogFileWriterPtr _active_binlog_writer;

    // Binlog files and rowsets that have been expired or overcapacity, but still used by some readers,
    // and can't be deleted immediately. Those binlog files can't serve for new read requests.
    std::deque<BinlogFilePtr> _wait_reader_binlog_files;
    std::unordered_map<int64_t, int32_t> _wait_reader_rowset_count_map;
    // Disk size for wait reader binlog files
    std::atomic<int64_t> _total_wait_reader_binlog_file_size = 0;
    // Disk size for wait reader rowsets
    std::atomic<int64_t> _total_wait_reader_rowset_data_size = 0;

    // ids of unused binlog files that can be deleted
    BlockingQueue<int64_t> _unused_binlog_file_ids;

    // Allocate an id for each binlog reader. Protected by _meta_lock
    int64_t _next_reader_id = 0;
    // Mapping from the reader id to the readers. Protected by _meta_lock
    std::unordered_map<int64_t, BinlogReaderSharedPtr> _binlog_readers;
};

} // namespace starrocks
