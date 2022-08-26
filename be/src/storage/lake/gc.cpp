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

namespace starrocks::lake {

static std::string format_time(time_t ts) {
    struct tm tm {};
    char buffer[32];
    // Format: 2009-06-06 20:20:00 UTC
    auto len = std::strftime(buffer, sizeof(buffer), "%Y.%m.%d %H:%M:%S UTC", gmtime_r(&ts, &tm));
    return {buffer, len};
}

// TODO: txn log GC
Status metadata_gc(std::string_view root_location, TabletManager* tablet_mgr) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_location));

    const auto max_versions = config::lake_gc_metadata_max_versions;
    if (UNLIKELY(max_versions < 1)) {
        return Status::InternalError("invalid config 'lake_gc_metadata_max_versions': value must be no less than 1");
    }

    std::unordered_map<int64_t, std::vector<int64_t>> tablet_metadatas;
    std::unordered_map<int64_t, std::unordered_set<int64_t>> locked_tablet_metadatas;

    auto start_time =
            std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch())
                    .count();

    auto iter_st = fs->iterate_dir(join_path(root_location, kMetadataDirectoryName), [&](std::string_view name) {
        if (is_tablet_metadata(name)) {
            auto [tablet_id, version] = parse_tablet_metadata_filename(name);
            tablet_metadatas[tablet_id].emplace_back(version);
        }
        if (is_tablet_metadata_lock(name)) {
            auto [tablet_id, version, expire_time] = parse_tablet_metadata_lock_filename(name);
            if (start_time < expire_time) {
                locked_tablet_metadatas[tablet_id].insert(version);
            }
        }
        return true;
    });

    if (iter_st.is_not_found()) {
        // ignore this error
        return Status::OK();
    }

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
            VLOG(5) << "Deleting " << tablet_metadata_filename(tablet_id, versions[i]);
            auto st = tablet_mgr->delete_tablet_metadata(tablet_id, versions[i]);
            LOG_IF(WARNING, !st.ok() && !st.is_not_found())
                    << "Fail to delete " << tablet_metadata_filename(tablet_id, versions[i]) << ": " << st;
        }
    }
    return Status::OK();
}

Status segment_gc(std::string_view root_location, TabletManager* tablet_mgr) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(root_location));

    auto now = std::time(nullptr);

    const auto metadata_root_location = join_path(root_location, kMetadataDirectoryName);
    const auto txn_log_root_location = join_path(root_location, kTxnLogDirectoryName);
    const auto segment_root_location = join_path(root_location, kSegmentDirectoryName);

    std::vector<std::string> tablet_metadatas;
    std::vector<std::string> txn_logs;
    std::unordered_set<std::string> segments;

    // List segment
    auto iter_st = fs->iterate_dir(segment_root_location, [&](std::string_view name) {
        segments.emplace(name);
        return true;
    });
    if (!iter_st.ok() && !iter_st.is_not_found()) {
        return iter_st;
    }

    // List tablet meatadata
    iter_st = fs->iterate_dir(metadata_root_location, [&](std::string_view name) {
        if (is_tablet_metadata(name)) {
            tablet_metadatas.emplace_back(name);
        }
        return true;
    });
    if (!iter_st.ok() && !iter_st.is_not_found()) {
        return iter_st;
    }

    // List txn log
    iter_st = fs->iterate_dir(txn_log_root_location, [&](std::string_view name) {
        txn_logs.emplace_back(name);
        return true;
    });
    if (!iter_st.ok() && !iter_st.is_not_found()) {
        return iter_st;
    }

    if (segments.empty()) {
        return Status::OK();
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

    for (auto& seg : segments) {
        auto location = join_path(segment_root_location, seg);
        auto res = fs->get_file_modified_time(location);
        if (!res.ok() && !res.status().is_not_found()) {
            LOG(WARNING) << "Fail to get modified time of " << location << ": " << res.status();
            continue;
        } else if (res.status().is_not_found() || now - *res < config::lake_gc_segment_expire_seconds) {
            continue;
        }

        LOG(INFO) << "Deleting orphan segment " << location << ". mtime=" << format_time(static_cast<time_t>(*res));

        auto st = fs->delete_file(location);
        LOG_IF(WARNING, !st.ok() && !st.is_not_found()) << "Fail to delete " << location << ": " << st;
    }
    return Status::OK();
}

} // namespace starrocks::lake