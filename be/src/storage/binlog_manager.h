// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <gutil/strings/substitute.h>

#include <cstdint>
#include <memory>
#include <unordered_set>

#include "column/schema.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/binlog.pb.h"
#include "storage/binlog_file_writer.h"
#include "storage/binlog_reader.h"
#include "storage/rowset/rowset.h"

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

using BinlogFileMetaPBSharedPtr = std::shared_ptr<BinlogFileMetaPB>;
using BinlogReaderSharedPtr = std::shared_ptr<BinlogReader>;

class BinlogManager : public enable_shared_from_this<BinlogManager> {
public:
    static Status create_and_init(Tablet& tablet, std::shared_ptr<BinlogManager>* binlog_manager);

    BinlogManager(std::string path, int64_t max_file_size, int32_t max_page_size, CompressionTypePB compression_type);

    Status init(Tablet& tablet);

    Status add_insert_rowset(RowsetSharedPtr rowset);

    // Whether the rowset is used by the binlog. It will be called
    // for path_gc to check whether a rowset is used.
    bool is_rowset_used(const RowsetId& rowset_id);

    void delete_expired_binlog();

    std::shared_ptr<BinlogReader> create_reader(vectorized::Schema& schema, int64_t expire_time_in_ms, int chunk_size) {
        std::shared_lock lock(_meta_lock);
        int64_t reader_id = _next_reader_id++;
        return std::make_shared<BinlogReader>(shared_from_this(), schema, reader_id, expire_time_in_ms, chunk_size);
    }

    // Find the binlog file which may contain a given changelog
    StatusOr<BinlogFileMetaPBSharedPtr> seek_binlog_file(int64_t version, int64_t changelog_id);

private:
    friend class BinlogReader;

    int128_t _changelog_lsn(int64_t version, int64_t changelogid) { return (((int128_t)version) << 64) | changelogid; }

    std::string _binlog_file_name(int64_t file_id) { return strings::Substitute("$0/$1.binlog", _path, file_id); }

    StatusOr<std::shared_ptr<BinlogFileWriter>> _create_binlog_writer(int64_t file_id);

    Status _delete_binlog_files(std::vector<std::string>& file_names);

    void _update_metas_after_new_commit(std::vector<BinlogFileMetaPBSharedPtr> new_file_metas);

    void _convert_rowset_id_pb(const RowsetIdPB& rowset_id_pb, RowsetId* rowset_id) {
        rowset_id->hi = rowset_id_pb.hi();
        rowset_id->mi = rowset_id_pb.mi();
        rowset_id->lo = rowset_id_pb.lo();
    }

    // protect meta modify/read
    // 1. publish/apply will generate new binlog, and modify metas
    // 2. TTL and capacity control will delete binlog, and modify metas
    // 3. when the basic table remove a rowset, modify shared rowsets information
    // 4. binlog consumers will read metas
    // TODO more fine-grained concurrency control
    std::shared_mutex _meta_lock;

    // protect that there can be only one insert at the same time
    std::mutex _write_lock;

    // binlog storage directory
    std::string _path;
    int64_t _max_file_size;
    int32_t _max_page_size;
    CompressionTypePB _compression_type;

    // mapping from start LSN of a binlog file to the file meta. LSN is int128_t, and
    // composites of tablet_version(int64_t) and changelog_id(int64_t). A binlog file
    // with a smaller start LSN also has a smaller file id. The file with the biggest
    // start LSN is the meta of _active_binlog_writer if the writer is opening for write.
    std::map<int128_t, BinlogFileMetaPBSharedPtr> _binlog_file_metas;
    std::shared_ptr<BinlogFileWriter> _active_binlog_writer;

    std::unordered_map<RowsetId, int32_t, HashOfRowsetId> _rowset_count_map;

    // Allocate an id for each binlog reader
    int64_t _next_reader_id;
    // Mapping from the reader id to the readers.
    std::unordered_map<int64_t, BinlogReaderSharedPtr> _binlog_readers;
};

} // namespace starrocks
