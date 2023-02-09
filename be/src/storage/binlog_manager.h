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

// Manages the binlog metas and files, including generation, deletion and read.
class BinlogManager : public std::enable_shared_from_this<BinlogManager> {
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

    // For testing
    int64_t next_file_id() { return _next_file_id; }

    int64_t ingestion_version() { return _ingestion_version; }

    BinlogBuildResult* build_result() { return _build_result.get(); }

    BinlogFileWriter* active_binlog_writer() { return _active_binlog_writer.get(); }

    std::map<int128_t, BinlogFileMetaPBPtr>& file_metas() { return _binlog_file_metas; }

    std::unordered_map<int64_t, int32_t>& rowset_count_map() { return _rowset_count_map; }

    int64_t total_binlog_file_disk_size() { return _total_binlog_file_disk_size; }

    int64_t total_rowset_disk_size() { return _total_rowset_disk_size; }

private:
    void _apply_build_result(BinlogBuildResult* result);

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
    // mapping from start LSN(start_version, start_seq_id) of a binlog file to the file meta.
    // A binlog file with a smaller start LSN also has a smaller file id. The file with the
    // biggest start LSN is the meta of _active_binlog_writer if it's not null.
    std::map<int128_t, BinlogFileMetaPBPtr> _binlog_file_metas;
    // the binlog file writer that can append data
    BinlogFileWriterPtr _active_binlog_writer;

    // mapping from rowset id to the number of binlog files using it
    std::unordered_map<int64_t, int32_t> _rowset_count_map;

    // statistics for disk usage
    int64_t _total_binlog_file_disk_size = 0;
    int64_t _total_rowset_disk_size = 0;
};

} // namespace starrocks
