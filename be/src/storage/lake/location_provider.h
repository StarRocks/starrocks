// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <fmt/format.h>

#include <set>
#include <string>
#include <string_view>

#include "common/statusor.h"
#include "storage/lake/filenames.h"

namespace starrocks::lake {

static const char* const kMetadataDirectoryName = "meta";
static const char* const kTxnLogDirectoryName = "log";
static const char* const kSegmentDirectoryName = "data";

class LocationProvider {
public:
    virtual ~LocationProvider() = default;

    // The result should be guaranteed to not end with "/"
    virtual std::string root_location(int64_t tablet_id) const = 0;

    virtual Status list_root_locations(std::set<std::string>* groups) const = 0;

    std::string metadata_root_location(int64_t tablet_id) const {
        return join_path(root_location(tablet_id), kMetadataDirectoryName);
    }

    std::string txn_log_root_location(int64_t tablet_id) const {
        return join_path(root_location(tablet_id), kTxnLogDirectoryName);
    }

    std::string segment_root_location(int64_t tablet_id) const {
        return join_path(root_location(tablet_id), kSegmentDirectoryName);
    }

    std::string tablet_metadata_location(int64_t tablet_id, int64_t version) const {
        return join_path(metadata_root_location(tablet_id), tablet_metadata_filename(tablet_id, version));
    }

    std::string txn_log_location(int64_t tablet_id, int64_t txn_id) const {
        return join_path(txn_log_root_location(tablet_id), txn_log_filename(tablet_id, txn_id));
    }

    std::string txn_vlog_location(int64_t tablet_id, int64_t version) const {
        return join_path(txn_log_root_location(tablet_id), txn_vlog_filename(tablet_id, version));
    }

    std::string segment_location(int64_t tablet_id, std::string_view segment_name) const {
        return join_path(segment_root_location(tablet_id), segment_name);
    }

    std::string tablet_metadata_lock_location(int64_t tablet_id, int64_t version, int64_t expire_time) const {
        return join_path(metadata_root_location(tablet_id),
                         tablet_metadata_lock_filename(tablet_id, version, expire_time));
    }

private:
    static std::string join_path(std::string_view parent, std::string_view child) {
        return fmt::format("{}/{}", parent, child);
    }
};

} // namespace starrocks::lake
