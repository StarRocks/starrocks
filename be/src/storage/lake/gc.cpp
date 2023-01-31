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

#include "storage/lake/gc.h"

#include <rapidjson/error/en.h>
#include <rapidjson/reader.h>
#include <rapidjson/writer.h>

#include <algorithm>
#include <ctime>
#include <unordered_map>

#include "common/config.h"
#include "fs/fs.h"
#include "fs/rapidjson_stream_adapter.h"
#include "storage/lake/filenames.h"
#include "storage/lake/join_path.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/update_manager.h"
#include "storage/olap_common.h"
#include "util/raw_container.h"

namespace starrocks::lake {

static const char* const kOrphanSegmentKey = "orphan_segments";

namespace {
struct OrphanSegmentHandler {
    FileSystem* fs;
    std::string dir;
    std::string curr_key;
    int array_level = 0;

    bool Null() { return true; }
    bool Bool(bool b) { return true; }
    bool Int(int i) { return true; }
    bool Uint(unsigned u) { return true; }
    bool Int64(int64_t i) { return true; }
    bool Uint64(uint64_t u) { return true; }
    bool Double(double d) { return true; }
    bool RawNumber(const char* str, rapidjson::SizeType length, bool copy) { return true; }
    bool StartObject() { return true; }
    bool Key(const char* str, rapidjson::SizeType length, bool copy) {
        curr_key.assign(str, length);
        return true;
    }
    bool EndObject(rapidjson::SizeType memberCount) {
        curr_key.clear();
        return true;
    }
    bool StartArray() {
        ++array_level;
        return true;
    }
    bool EndArray(rapidjson::SizeType elementCount) {
        --array_level;
        return true;
    }
    bool String(const char* str, rapidjson::SizeType length, bool copy) {
        if (curr_key == kOrphanSegmentKey && array_level == 1) {
            std::string_view name(str, length);
            VLOG(2) << "Dropping disk cache of " << name;
            auto path = join_path(dir, name);
            auto st = fs->drop_local_cache(path);
            LOG_IF(ERROR, !st.ok() && !st.is_not_found()) << "Fail to drop disk cache of " << name << ": " << st;
        }
        return true;
    }
};
} // namespace

static Status write_orphan_list_file(const std::set<std::string>& orphans, WritableFile* file) {
    raw::RawVector<char> buffer(4096);
    RapidJSONWriteStreamAdapter os(file, buffer.data(), buffer.size());
    rapidjson::Writer<RapidJSONWriteStreamAdapter> writer(os);
    writer.StartObject();
    writer.Key(kOrphanSegmentKey);
    writer.StartArray();
    for (const auto& s : orphans) {
        writer.String(s.data(), s.size());
    }
    writer.EndArray();
    writer.EndObject();
    if (!os.status().ok()) {
        return os.status();
    }
    return file->close();
}

static Status delete_tablet_metadata(std::string_view root_location, const std::set<int64_t>& owned_tablets) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_location));
    const auto max_versions = config::lake_gc_metadata_max_versions;
    if (UNLIKELY(max_versions < 1)) {
        return Status::InternalError("invalid config 'lake_gc_metadata_max_versions': value must be no less than 1");
    }

    std::unordered_map<int64_t, std::vector<int64_t>> tablet_metadatas;
    std::unordered_map<int64_t, std::unordered_set<int64_t>> locked_tablet_metadatas;

    auto start_time = std::time(nullptr);
    auto metadata_root_location = join_path(root_location, kMetadataDirectoryName);
    auto iter_st = fs->iterate_dir(metadata_root_location, [&](std::string_view name) {
        if (is_tablet_metadata(name)) {
            auto [tablet_id, version] = parse_tablet_metadata_filename(name);
            if (owned_tablets.count(tablet_id) > 0) {
                tablet_metadatas[tablet_id].emplace_back(version);
            }
        }
        if (is_tablet_metadata_lock(name)) {
            auto [tablet_id, version, expire_time] = parse_tablet_metadata_lock_filename(name);
            if (start_time < expire_time && owned_tablets.count(tablet_id) > 0) {
                locked_tablet_metadatas[tablet_id].insert(version);
            }
        }
        return true;
    });

    if (!iter_st.ok()) {
        return iter_st;
    }

    for (auto& [tablet_id, versions] : tablet_metadatas) {
        if (versions.size() <= max_versions) {
            continue;
        }
        // TODO: batch delete
        // Keep the latest 10 versions.
        // If the tablet metadata is locked, the correspoding version will be kept.
        std::sort(versions.begin(), versions.end());
        for (size_t i = 0, sz = versions.size() - max_versions; i < sz; i++) {
            if (locked_tablet_metadatas.count(tablet_id)) {
                const auto& locked_tablet_metadata = locked_tablet_metadatas[tablet_id];
                if (locked_tablet_metadata.count(versions[i])) {
                    continue;
                }
            }
            auto path = join_path(metadata_root_location, tablet_metadata_filename(tablet_id, versions[i]));
            LOG(INFO) << "Deleting " << path;
            auto st = fs->delete_file(path);
            LOG_IF(WARNING, !st.ok() && !st.is_not_found()) << "Fail to delete " << path << ": " << st;
        }
    }
    return Status::OK();
}

static Status delete_txn_log(std::string_view root_location, const std::set<int64_t>& owned_tablets,
                             int64_t min_active_txn_id) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_location));
    auto txn_log_root_location = join_path(root_location, kTxnLogDirectoryName);
    auto iter_st = fs->iterate_dir(txn_log_root_location, [&](std::string_view name) {
        if (is_txn_log(name)) {
            auto [tablet_id, txn_id] = parse_txn_log_filename(name);
            if (txn_id < min_active_txn_id && owned_tablets.count(tablet_id) > 0) {
                VLOG(2) << "Deleting " << name;
                auto location = join_path(txn_log_root_location, name);
                auto st = fs->delete_file(location);
                LOG_IF(WARNING, !st.ok() && !st.is_not_found()) << "Fail to delete " << name << ": " << st;
            }
        }
        return true;
    });
    return iter_st.is_not_found() ? Status::OK() : iter_st;
}

static Status drop_disk_cache(std::string_view root_location) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_location));
    auto segment_root_location = join_path(root_location, kSegmentDirectoryName);
    auto orphan_list_location = join_path(root_location, kGCFileName);
    auto options = RandomAccessFileOptions{.skip_fill_local_cache = true};
    auto file_or = fs->new_random_access_file(options, orphan_list_location);
    if (file_or.status().is_not_found()) {
        return Status::OK();
    } else if (!file_or.ok()) {
        return file_or.status();
    }
    raw::RawVector<char> buffer(/*16MB=*/16L * 1024 * 1024);
    RapidJSONReadStreamAdapter is(file_or->get(), buffer.data(), buffer.size());
    OrphanSegmentHandler handler{.fs = fs.get(), .dir = segment_root_location};
    rapidjson::Reader reader;
    auto ok = reader.Parse(is, handler);
    LOG_IF(ERROR, !is.status().ok()) << "Fail to read orphan list file: " << is.status();
    if (!ok) {
        LOG(ERROR) << "Fail to parse json: " << rapidjson::GetParseError_En(ok.Code());
    }
    return Status::OK();
}

Status metadata_gc(std::string_view root_location, TabletManager* tablet_mgr, int64_t min_active_txn_id) {
    const auto owned_tablets = tablet_mgr->owned_tablets();
    Status ret;
    ret.update(delete_tablet_metadata(root_location, owned_tablets));
    ret.update(delete_txn_log(root_location, owned_tablets, min_active_txn_id));
    ret.update(drop_disk_cache(root_location));
    return ret;
}

static StatusOr<std::set<std::string>> find_orphan_datafiles(TabletManager* tablet_mgr, std::string_view root_location,
                                                             const std::vector<std::string>& tablet_metadatas,
                                                             const std::vector<std::string>& txn_logs) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_location));
    const auto metadata_root_location = join_path(root_location, kMetadataDirectoryName);
    const auto txn_log_root_location = join_path(root_location, kTxnLogDirectoryName);
    const auto segment_root_location = join_path(root_location, kSegmentDirectoryName);

    std::set<std::string> datafiles;

    // List segment
    auto iter_st = fs->iterate_dir(segment_root_location, [&](std::string_view name) {
        if (LIKELY(is_segment(name) || is_del(name) || is_delvec(name))) {
            datafiles.emplace(name);
        }
        return true;
    });
    if (!iter_st.ok() && !iter_st.is_not_found()) {
        return iter_st;
    }

    LOG(INFO) << "find_orphan_datafiles datafile cnt: " << datafiles.size();
    if (datafiles.empty()) {
        return datafiles;
    }

    auto check_rowset = [&](const RowsetMetadata& rowset) {
        for (const auto& seg : rowset.segments()) {
            datafiles.erase(seg);
        }
    };

    auto check_dels = [&](const TxnLogPB_OpWrite& opwrite) {
        for (const auto& del : opwrite.dels()) {
            datafiles.erase(del);
        }
    };

    auto check_delvecs = [&](int64_t tablet_id, const DelvecMetadataPB& delvec_meta) {
        for (const auto& delvec : delvec_meta.delvecs()) {
            std::string delvec_name = tablet_delvec_filename(tablet_id, delvec.page().version());
            datafiles.erase(delvec_name);
        }
    };

    auto check_rewrite_segments = [&](const TxnLogPB_OpWrite& opwrite) {
        for (const auto& seg : opwrite.rewrite_segments()) {
            datafiles.erase(seg);
        }
    };

    // record missed tsid range
    std::vector<TabletSegmentIdRange> missed_tsid_ranges;
    for (const auto& filename : tablet_metadatas) {
        auto location = join_path(metadata_root_location, filename);
        auto res = tablet_mgr->get_tablet_metadata(location, false);
        if (res.status().is_not_found()) {
            LOG(WARNING) << fmt::format("find_orphan_datafiles tablet meta {} not found", location);
            continue;
        } else if (!res.ok()) {
            return res.status();
        }

        auto metadata = std::move(res).value();
        for (const auto& rowset : metadata->rowsets()) {
            check_rowset(rowset);
        }
        check_delvecs(metadata->id(), metadata->delvec_meta());
        if (is_primary_key(metadata.get())) {
            // find missed tsid range, only used in pk table
            find_missed_tsid_range(metadata.get(), missed_tsid_ranges);
        }
    }

    for (const auto& filename : txn_logs) {
        auto location = join_path(txn_log_root_location, filename);
        auto res = tablet_mgr->get_txn_log(location, false);
        if (res.status().is_not_found()) {
            LOG(WARNING) << fmt::format("find_orphan_datafiles txnlog {} not found", location);
            continue;
        } else if (!res.ok()) {
            return res.status();
        }

        auto txn_log = std::move(res).value();
        if (txn_log->has_op_write()) {
            check_rowset(txn_log->op_write().rowset());
            check_dels(txn_log->op_write());
            check_rewrite_segments(txn_log->op_write());
        }
        if (txn_log->has_op_compaction()) {
            // No need to check input rowsets
            check_rowset(txn_log->op_compaction().output_rowset());
        }
        if (txn_log->has_op_schema_change()) {
            for (const auto& rowset : txn_log->op_schema_change().rowsets()) {
                check_rowset(rowset);
            }
            if (txn_log->op_schema_change().has_delvec_meta()) {
                check_delvecs(txn_log->tablet_id(), txn_log->op_schema_change().delvec_meta());
            }
        }
    }

    auto now = std::time(nullptr);

    for (auto it = datafiles.begin(); it != datafiles.end(); /**/) {
        auto location = join_path(segment_root_location, *it);
        auto res = fs->get_file_modified_time(location);
        if (!res.ok()) {
            LOG_IF(WARNING, !res.status().is_not_found())
                    << "Fail to get modified time of " << location << ": " << res.status();
            it = datafiles.erase(it);
        } else if (now < *res + config::lake_gc_segment_expire_seconds) {
            it = datafiles.erase(it);
        } else {
            ++it;
        }
    }

    return datafiles;
}

Status datafile_gc(std::string_view root_location, TabletManager* tablet_mgr) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_location));

    const auto owned_tablets = tablet_mgr->owned_tablets();
    const auto metadata_root_location = join_path(root_location, kMetadataDirectoryName);
    const auto txn_log_root_location = join_path(root_location, kTxnLogDirectoryName);
    const auto segment_root_location = join_path(root_location, kSegmentDirectoryName);

    int64_t min_tablet_id = INT64_MAX;
    std::vector<std::string> tablet_metadatas;

    // List tablet meatadata
    auto iter_st = fs->iterate_dir(metadata_root_location, [&](std::string_view name) {
        if (!is_tablet_metadata(name)) {
            return true;
        }
        auto [tablet_id, version] = parse_tablet_metadata_filename(name);
        min_tablet_id = std::min(min_tablet_id, tablet_id);
        tablet_metadatas.emplace_back(name);
        return true;
    });
    if (!iter_st.ok()) {
        return iter_st;
    }

    if (min_tablet_id == INT64_MAX) {
        LOG(INFO) << "Skiped datafile GC of " << root_location << " because there is no tablet metadata";
        return Status::OK();
    }

    if (owned_tablets.count(min_tablet_id) == 0) {
        // The tablet with the smallest ID is not managed by the current process, skip segment GC
        LOG(INFO) << "Skiped datafile GC of " << root_location
                  << " because the smallest ID is not managed by the current process";
        return Status::OK();
    }

    // List txn log
    std::vector<std::string> txn_logs;
    iter_st = fs->iterate_dir(txn_log_root_location, [&](std::string_view name) {
        txn_logs.emplace_back(name);
        return true;
    });
    if (!iter_st.ok() && !iter_st.is_not_found()) {
        return iter_st;
    }

    // Find orphan data files, include segment, del, and delvec
    ASSIGN_OR_RETURN(auto orphan_datafiles,
                     find_orphan_datafiles(tablet_mgr, root_location, tablet_metadatas, txn_logs));
    // Write orphan segment list file
    WritableFileOptions opts{
            .sync_on_close = false, .skip_fill_local_cache = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    ASSIGN_OR_RETURN(auto orphan_list_file, fs->new_writable_file(opts, join_path(root_location, kGCFileName)));
    RETURN_IF_ERROR(write_orphan_list_file(orphan_datafiles, orphan_list_file.get()));
    // Delete orphan segment files and del files
    for (auto& file : orphan_datafiles) {
        LOG(INFO) << "Deleting orphan data file: " << file;
        auto location = join_path(segment_root_location, file);
        auto st = fs->delete_file(location);
        LOG_IF(WARNING, !st.ok() && !st.is_not_found()) << "Fail to delete " << file << ": " << st;
    }
    return Status::OK();
}

} // namespace starrocks::lake
