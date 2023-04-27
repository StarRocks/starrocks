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
#include "gutil/stl_util.h"
#include "storage/lake/filenames.h"
#include "storage/lake/join_path.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/update_manager.h"
#include "storage/olap_common.h"
#include "testutil/sync_point.h"
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
            auto st = ignore_not_found(fs->drop_local_cache(path));
            LOG_IF(ERROR, !st.ok()) << "Fail to drop disk cache of " << name << ": " << st;
        }
        return true;
    }
};
} // namespace

static Status list_tablet_metadata(const std::string& metadir, std::set<std::string>* metas) {
    auto fs_or = FileSystem::CreateSharedFromString(metadir);
    if (!fs_or.ok()) {
        // Return NotFound only when the file or directory does not exist.
        if (fs_or.status().is_not_found()) {
            return Status::InternalError(fs_or.status().message());
        } else {
            return fs_or.status();
        }
    }

    return (*fs_or)->iterate_dir(metadir, [&](std::string_view name) {
        if (is_tablet_metadata(name)) {
            metas->emplace(name);
        }
        return true;
    });
}

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
    TEST_SYNC_POINT("CloudNative::GC::delete_tablet_metadata:enter");

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
            auto st = ignore_not_found(fs->delete_file(path));
            if (st.ok()) {
                LOG(INFO) << "Deleted " << path;
            } else {
                LOG(WARNING) << "Fail to delete " << path << ": " << st;
            }
        }
    }

    TEST_SYNC_POINT("CloudNative::GC::delete_tablet_metadata:return");

    return Status::OK();
}

static Status delete_txn_log(std::string_view root_location, const std::set<int64_t>& owned_tablets,
                             int64_t min_active_txn_id) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_location));
    auto txn_log_root_location = join_path(root_location, kTxnLogDirectoryName);
    return ignore_not_found(fs->iterate_dir(txn_log_root_location, [&](std::string_view name) {
        if (is_txn_log(name)) {
            auto [tablet_id, txn_id] = parse_txn_log_filename(name);
            if (txn_id < min_active_txn_id && owned_tablets.count(tablet_id) > 0) {
                auto location = join_path(txn_log_root_location, name);
                auto st = ignore_not_found(fs->delete_file(location));
                if (st.ok()) {
                    LOG(INFO) << "Deleted " << location << ". min_active_txn_id=" << min_active_txn_id;
                } else {
                    LOG(WARNING) << "Fail to delete " << name << ": " << st;
                }
            }
        }
        return true;
    }));
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

// To developers: |tablet_metadatas| must be a sorted container to use STLSetDifference
static StatusOr<std::set<std::string>> find_orphan_datafiles(TabletManager* tablet_mgr, std::string_view root_location,
                                                             std::set<std::string>* tablet_metadatas,
                                                             const std::vector<std::string>& txn_logs) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_location));
    const auto now = std::time(nullptr);
#ifndef BE_TEST
    const auto expire_seconds = std::max<int64_t>(config::lake_gc_segment_expire_seconds, 86400);
#else
    const auto expire_seconds = config::lake_gc_segment_expire_seconds;
#endif
    const auto metadata_root_location = join_path(root_location, kMetadataDirectoryName);
    const auto txn_log_root_location = join_path(root_location, kTxnLogDirectoryName);
    const auto segment_root_location = join_path(root_location, kSegmentDirectoryName);

    std::set<std::string> datafiles;

    bool need_check_modify_time = false;
    int64_t total_files = 0;
    // List segment
    auto st = ignore_not_found(fs->iterate_dir2(segment_root_location, [&](DirEntry entry) {
        total_files++;
        if (!is_segment(entry.name) && !is_del(entry.name) && !is_delvec(entry.name)) {
            LOG_EVERY_N(WARNING, 100) << "Unrecognized data file " << entry.name;
            return true;
        }
        if (!entry.mtime.has_value()) {
            // Need to check modify time again as long as there is a entry that does not have modify time.
            need_check_modify_time = true;
        }
        if (!(entry.mtime.has_value() && now < entry.mtime.value() + expire_seconds)) {
            datafiles.emplace(entry.name);
        }
        return true;
    }));
    if (!st.ok()) {
        return st;
    }

    TEST_SYNC_POINT("CloudNative::GC::find_orphan_datafiles:finished_list_meta");

    VLOG(4) << "Listed all data files. total files=" << total_files << " possible orphan files=" << datafiles.size();

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
            std::string delvec_name = tablet_delvec_filename(tablet_id, delvec.second.version());
            datafiles.erase(delvec_name);
        }
    };

    auto check_rewrite_segments = [&](const TxnLogPB_OpWrite& opwrite) {
        for (const auto& seg : opwrite.rewrite_segments()) {
            datafiles.erase(seg);
        }
    };

    TEST_SYNC_POINT("CloudNative::GC::find_orphan_datafiles:check_meta");

    std::set<std::string> processed_metadatas;
    int retries = 0;
    while (true) {
        bool has_deleted_metadata = false;
        for (const auto& filename : *tablet_metadatas) {
            auto location = join_path(metadata_root_location, filename);
            auto res = tablet_mgr->get_tablet_metadata(location, false);
            if (res.status().is_not_found()) { // This metadata file was deleted by another node
                has_deleted_metadata = true;
            } else if (!res.ok()) {
                return res.status();
            } else {
                auto metadata = std::move(res).value();
                for (const auto& rowset : metadata->rowsets()) {
                    check_rowset(rowset);
                }
                check_delvecs(metadata->id(), metadata->delvec_meta());
            }
        }

        if (has_deleted_metadata && ++retries <= config::experimental_lake_segment_gc_max_retries) {
            LOG(INFO) << "Some metadata files deleted by other nodes, retrying";
            std::set<std::string> new_metadatas;
            processed_metadatas.insert(tablet_metadatas->begin(), tablet_metadatas->end());
            RETURN_IF_ERROR(list_tablet_metadata(metadata_root_location, &new_metadatas));
            // Copies the elements from the set |new_metadatas| which are not found in set
            // |processed_metadatas| to the set |tablet_metadatas|.
            *tablet_metadatas = STLSetDifference(new_metadatas, processed_metadatas);
        } else if (has_deleted_metadata) {
            return Status::InternalError("Some metadata files deleted by other nodes");
        } else {
            break;
        }
    }

    for (const auto& filename : txn_logs) {
        auto location = join_path(txn_log_root_location, filename);
        auto res = tablet_mgr->get_txn_log(location, false);
        if (res.status().is_not_found()) {
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

    if (need_check_modify_time && !datafiles.empty()) {
        LOG(INFO) << "Checking modify time of " << datafiles.size() << " data files";
        for (auto it = datafiles.begin(); it != datafiles.end(); /**/) {
            auto location = join_path(segment_root_location, *it);
            auto res = fs->get_file_modified_time(location);
            if (!res.ok()) {
                LOG_IF(WARNING, !res.status().is_not_found())
                        << "Fail to get modified time of " << location << ": " << res.status();
                it = datafiles.erase(it);
            } else if (now < *res + expire_seconds) {
                it = datafiles.erase(it);
            } else {
                ++it;
            }
        }
    }
    VLOG(4) << "Found " << datafiles.size() << " orphan files";
    return datafiles;
}

Status datafile_gc(std::string_view root_location, TabletManager* tablet_mgr) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_location));

    const auto owned_tablets = tablet_mgr->owned_tablets();
    const auto metadata_root_location = join_path(root_location, kMetadataDirectoryName);
    const auto txn_log_root_location = join_path(root_location, kTxnLogDirectoryName);
    const auto segment_root_location = join_path(root_location, kSegmentDirectoryName);

    std::set<std::string> tablet_metadatas;
    RETURN_IF_ERROR(list_tablet_metadata(metadata_root_location, &tablet_metadatas));

    if (tablet_metadatas.empty()) {
        LOG(INFO) << "Skipped datafile GC of " << root_location << " because there is no tablet metadata";
        return Status::OK();
    }

    // Find the minimum tablet id.
    int64_t min_tablet_id = INT64_MAX;
    for (const auto& name : tablet_metadatas) {
        auto [tablet_id, version] = parse_tablet_metadata_filename(name);
        min_tablet_id = std::min(min_tablet_id, tablet_id);
        (void)version;
    }

    // Check if the minimum tablet id is managed by the current node.
    if (owned_tablets.count(min_tablet_id) == 0) {
        // The tablet with the smallest ID is not managed by the current process, skip segment GC
        LOG(INFO) << "Skiped datafile GC of " << root_location
                  << " because the smallest ID is not managed by the current process";
        return Status::OK();
    }

    // List txn log
    std::vector<std::string> txn_logs;
    RETURN_IF_ERROR(ignore_not_found(fs->iterate_dir(txn_log_root_location, [&](std::string_view name) {
        txn_logs.emplace_back(name);
        return true;
    })));

    // Find orphan data files, include segment, del, and delvec
    ASSIGN_OR_RETURN(auto orphan_datafiles,
                     find_orphan_datafiles(tablet_mgr, root_location, &tablet_metadatas, txn_logs));

    // Write orphan segment list file
    WritableFileOptions opts{
            .sync_on_close = false, .skip_fill_local_cache = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    ASSIGN_OR_RETURN(auto orphan_list_file, fs->new_writable_file(opts, join_path(root_location, kGCFileName)));
    RETURN_IF_ERROR(write_orphan_list_file(orphan_datafiles, orphan_list_file.get()));

    // Delete orphan segment files and del files
    for (auto& file : orphan_datafiles) {
        auto location = join_path(segment_root_location, file);
        auto st = ignore_not_found(fs->delete_file(location));
        if (st.ok()) {
            LOG(INFO) << "Deleted orphan data file: " << location;
        } else {
            LOG(WARNING) << "Fail to delete " << location << ": " << st;
        }
    }
    return Status::OK();
}

} // namespace starrocks::lake
