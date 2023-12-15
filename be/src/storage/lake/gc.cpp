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

static Status delete_file_with_log(FileSystem* fs, const std::string& path) {
    auto st = fs->delete_file(path);
    if (st.ok() && config::lake_print_delete_log) {
        LOG(INFO) << "Deleted " << path;
    } else if (!st.is_not_found()) {
        LOG(WARNING) << "Fail to delete " << path << ": " << st;
    } else {
        // Ignore Not Found
        st = Status::OK();
    }
    return st;
}

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

static Status delete_rowset_files(FileSystem* fs, std::string_view data_dir, const RowsetMetadataPB& rowset) {
    for (const auto& segment : rowset.segments()) {
        auto seg_path = join_path(data_dir, segment);
        RETURN_IF_ERROR(delete_file_with_log(fs, seg_path));
    }
    return Status::OK();
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

static Status delete_tablet_metadata(TabletManager* tablet_mgr, std::string_view root_location,
                                     const std::set<int64_t>& owned_tablets) {
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
    auto datafile_root_location = join_path(root_location, kSegmentDirectoryName);
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

        std::sort(versions.begin(), versions.end());
        versions.resize(versions.size() - max_versions);

        const bool enable_fast_gc = config::experimental_lake_enable_fast_gc;
        const bool has_lock_file = locked_tablet_metadatas.count(tablet_id) > 0;

        // Find metadata files that has garbage data files and delete all those files
        for (int64_t garbage_version = versions.back(); garbage_version >= versions[0]; /**/) {
            if (!enable_fast_gc) {
                break;
            }
            if (has_lock_file) {
                // It is not safe to delete compaction input files when there is a lock file, as it is unknown
                // whether these file are referenced by the locked metadata.
                LOG(INFO) << "Skipped copmaction metadata check due to the presence of lock file. location="
                          << root_location;
                break;
            }
            auto path = join_path(metadata_root_location, tablet_metadata_filename(tablet_id, garbage_version));
            auto res = tablet_mgr->get_tablet_metadata(path, false);
            if (res.status().is_not_found()) {
                break;
            } else if (!res.ok()) {
                LOG(ERROR) << "Fail to read " << path << ": " << res.status();
                return res.status();
            } else {
                auto metadata = std::move(res).value();
                for (const auto& rowset : metadata->compaction_inputs()) {
                    RETURN_IF_ERROR(delete_rowset_files(fs.get(), datafile_root_location, rowset));
                }
                for (const auto& file : metadata->orphan_files()) {
                    RETURN_IF_ERROR(delete_file_with_log(fs.get(), join_path(datafile_root_location, file.name())));
                }
                if (metadata->has_prev_garbage_version()) {
                    garbage_version = metadata->prev_garbage_version();
                } else {
                    break;
                }
            }
        }

        // TODO: batch delete
        // If the tablet metadata is locked, the correspoding version will be kept.
        for (auto version : versions) {
            if (has_lock_file) {
                const auto& locked_tablet_metadata = locked_tablet_metadatas[tablet_id];
                if (locked_tablet_metadata.count(version)) {
                    continue;
                }
            }
            auto path = join_path(metadata_root_location, tablet_metadata_filename(tablet_id, version));
            (void)delete_file_with_log(fs.get(), path);
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
                (void)delete_file_with_log(fs.get(), location);
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
    ret.update(delete_tablet_metadata(tablet_mgr, root_location, owned_tablets));
    ret.update(delete_txn_log(root_location, owned_tablets, min_active_txn_id));
    ret.update(drop_disk_cache(root_location));
    return ret;
}

// To developers: |tablet_metadatas| must be a sorted container to use STLSetDifference
static StatusOr<std::set<std::string>> find_orphan_datafiles(TabletManager* tablet_mgr, std::string_view root_location,
                                                             std::set<std::string>* tablet_metadatas,
                                                             const std::vector<std::string>& txn_logs,
                                                             int64_t min_active_txn_id) {
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

    int64_t total_files = 0;
    // List segment
    auto st = ignore_not_found(fs->iterate_dir2(segment_root_location, [&](DirEntry entry) {
        total_files++;
        if (!is_segment(entry.name) && !is_del(entry.name) && !is_delvec(entry.name)) {
            LOG_EVERY_N(WARNING, 100) << "Unrecognized data file " << entry.name;
            return true;
        }
        std::optional<int64_t> opt_txn_id = extract_txn_id_prefix(entry.name);
        if (opt_txn_id.has_value()) {
            // Using the transaction ID as the logical creation timestamp for the file, if the logical
            // creation time (txn_id) is smaller than the |min_active_txn_id|, it indicates that the
            // transaction that created this file has completed, and if the file is not referenced by
            // any metadata, it's safe to delete it.
            if (opt_txn_id.value() < min_active_txn_id) {
                datafiles.emplace(entry.name);
            } else {
                return true; // this file has not expired yet and cannot be deleted
            }
        } else if (entry.mtime.has_value()) {
            if (now >= entry.mtime.value() + expire_seconds) {
                datafiles.emplace(entry.name);
            } else {
                return true; // this file has not expired yet and cannot be deleted
            }
        } else {
            auto path = join_path(segment_root_location, entry.name);
            auto res = fs->get_file_modified_time(path);
            if (res.ok() && now >= *res + expire_seconds) {
                datafiles.emplace(entry.name);
            } else {
                auto st = ignore_not_found(res.status());
                LOG_IF(WARNING, !st.ok()) << "Fail to get modified time of " << path << ": " << st;
            }
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
        for (const auto& vd : delvec_meta.version_to_file()) {
            datafiles.erase(vd.second.name());
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

    VLOG(4) << "Found " << datafiles.size() << " orphan files";
    return datafiles;
}

Status datafile_gc(std::string_view root_location, TabletManager* tablet_mgr, int64_t min_active_txn_id) {
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
                     find_orphan_datafiles(tablet_mgr, root_location, &tablet_metadatas, txn_logs, min_active_txn_id));

    // Write orphan segment list file
    WritableFileOptions opts{
            .sync_on_close = false, .skip_fill_local_cache = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    ASSIGN_OR_RETURN(auto orphan_list_file, fs->new_writable_file(opts, join_path(root_location, kGCFileName)));
    RETURN_IF_ERROR(write_orphan_list_file(orphan_datafiles, orphan_list_file.get()));

    // Delete orphan segment files and del files
    for (auto& file : orphan_datafiles) {
        LOG(INFO) << "Deleting orphan data file: " << file;
        auto location = join_path(segment_root_location, file);
        (void)delete_file_with_log(fs.get(), location);
    }
    return Status::OK();
}

Status delete_garbage_files(TabletManager* tablet_mgr, int64_t tablet_id, int64_t version) {
    auto metadata_path = tablet_mgr->tablet_metadata_location(tablet_id, version);
    ASSIGN_OR_RETURN(auto metadata, tablet_mgr->get_tablet_metadata(metadata_path, false));
    auto data_dir = join_path(tablet_mgr->tablet_root_location(tablet_id), kSegmentDirectoryName);
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(data_dir));
    for (const auto& rowset : metadata->compaction_inputs()) {
        RETURN_IF_ERROR(delete_rowset_files(fs.get(), data_dir, rowset));
    }
    for (const auto& file : metadata->orphan_files()) {
        RETURN_IF_ERROR(delete_file_with_log(fs.get(), join_path(data_dir, file.name())));
    }
    return Status::OK();
}

} // namespace starrocks::lake
