// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/lake/gc.h"

#include <algorithm>
#include <ctime>
#include <unordered_map>

#include "common/config.h"
#include "fs/fs.h"
#include "storage/lake/filenames.h"
#include "storage/lake/join_path.h"
#include "storage/lake/location_provider.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_metadata.h"
#include "util/raw_container.h"

namespace starrocks::lake {

static Status write_orphan_list_file(const std::set<std::string>& orphans, WritableFile* file) {
    for (const auto& s : orphans) {
        RETURN_IF_ERROR(file->append(s));
        RETURN_IF_ERROR(file->append("\n"));
    }
    DCHECK_EQ((kSegmentFileNameLength + 1) * orphans.size(), file->size());
    return file->close();
}

static Status read_orphan_list_file(RandomAccessFile* file, std::vector<std::string>* orphans) {
    auto stream = file->stream();
    auto size_or = stream->get_size();
    if (size_or.status().is_not_found()) { // File does not exist
        return Status::OK();
    } else if (!size_or.ok()) {
        return size_or.status();
    }
    auto size = *size_or;
    DCHECK_EQ(0, size % (kSegmentFileNameLength + 1));
    raw::RawVector<char> buff;
    buff.resize(size); // TODO: streaming read
    orphans->reserve(size / (kSegmentFileNameLength + 1));
    for (auto offset = 0L; offset < size; offset += (kSegmentFileNameLength + 1)) {
        orphans->emplace_back(&buff[offset], kSegmentFileNameLength);
        DCHECK_EQ('\n', buff[offset + kSegmentFileNameLength + 1]);
    }
    return Status::OK();
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
    auto orphan_list_location = join_path(root_location, kOrphanListFileName);
    auto file_or = fs->new_random_access_file(orphan_list_location);
    if (file_or.status().is_not_found()) {
        return Status::OK();
    } else if (!file_or.ok()) {
        return file_or.status();
    }
    std::vector<std::string> orphan_segments;
    RETURN_IF_ERROR(read_orphan_list_file(file_or->get(), &orphan_segments));
    for (const auto& seg : orphan_segments) {
        VLOG(3) << "Dropping disk cache of " << seg;
        auto location = join_path(segment_root_location, seg);
        // TODO: Add a new interface for dropping disk cache
        auto st = fs->delete_file(location);
        LOG_IF(WARNING, !st.ok() && !st.is_not_found()) << "Fail to drop cache of " << seg << ": " << st;
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

static StatusOr<std::set<std::string>> find_orphan_segments(TabletManager* tablet_mgr, std::string_view root_location,
                                                            const std::vector<std::string>& tablet_metadatas,
                                                            const std::vector<std::string>& txn_logs) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_location));
    const auto metadata_root_location = join_path(root_location, kMetadataDirectoryName);
    const auto txn_log_root_location = join_path(root_location, kTxnLogDirectoryName);
    const auto segment_root_location = join_path(root_location, kSegmentDirectoryName);

    std::set<std::string> segments;

    // List segment
    auto iter_st = fs->iterate_dir(segment_root_location, [&](std::string_view name) {
        if (LIKELY(is_segment(name))) {
            segments.emplace(name);
        }
        return true;
    });
    if (!iter_st.ok() && !iter_st.is_not_found()) {
        return iter_st;
    }

    if (segments.empty()) {
        return segments;
    }

    auto check_rowset = [&](const RowsetMetadata& rowset) {
        for (const auto& seg : rowset.segments()) {
            segments.erase(seg);
        }
    };

    for (const auto& filename : tablet_metadatas) {
        auto location = join_path(metadata_root_location, filename);
        auto res = tablet_mgr->get_tablet_metadata(location, false);
        if (res.status().is_not_found()) {
            continue;
        } else if (!res.ok()) {
            return res.status();
        }

        auto metadata = std::move(res).value();
        for (const auto& rowset : metadata->rowsets()) {
            check_rowset(rowset);
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
        }
        if (txn_log->has_op_compaction()) {
            // No need to check input rowsets
            check_rowset(txn_log->op_compaction().output_rowset());
        }
        if (txn_log->has_op_schema_change()) {
            for (const auto& rowset : txn_log->op_schema_change().rowsets()) {
                check_rowset(rowset);
            }
        }
    }

    auto now = std::time(nullptr);

    for (auto it = segments.begin(); it != segments.end(); /**/) {
        auto location = join_path(segment_root_location, *it);
        auto res = fs->get_file_modified_time(location);
        if (!res.ok()) {
            LOG_IF(WARNING, !res.status().is_not_found())
                    << "Fail to get modified time of " << location << ": " << res.status();
            it = segments.erase(it);
        } else if (now < *res + config::lake_gc_segment_expire_seconds) {
            it = segments.erase(it);
        } else {
            ++it;
        }
    }

    return segments;
}

Status segment_gc(std::string_view root_location, TabletManager* tablet_mgr) {
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
        LOG(INFO) << "Skiped segment GC of " << root_location << " because there is no tablet metadata";
        return Status::OK();
    }

    if (owned_tablets.count(min_tablet_id) == 0) {
        // The tablet with the smallest ID is not managed by the current process, skip segment GC
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

    // Find orphan segments
    ASSIGN_OR_RETURN(auto orphan_segments, find_orphan_segments(tablet_mgr, root_location, tablet_metadatas, txn_logs));
    // Write orphan segment list file
    WritableFileOptions opts{.sync_on_close = false, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    ASSIGN_OR_RETURN(auto orphan_list_file, fs->new_writable_file(opts, join_path(root_location, kOrphanListFileName)));
    RETURN_IF_ERROR(write_orphan_list_file(orphan_segments, orphan_list_file.get()));
    // Delete orphan segment files
    for (auto& seg : orphan_segments) {
        LOG(INFO) << "Deleting orphan segment " << seg;
        auto location = join_path(segment_root_location, seg);
        auto st = fs->delete_file(location);
        LOG_IF(WARNING, !st.ok() && !st.is_not_found()) << "Fail to delete " << seg << ": " << st;
    }
    return Status::OK();
}

} // namespace starrocks::lake
