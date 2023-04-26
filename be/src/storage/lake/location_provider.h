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

    // TODO: move this method to another class.
    virtual std::set<int64_t> owned_tablets() const = 0;

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

    std::string tablet_delvec_location(int64_t tablet_id, int64_t version) const {
        return join_path(segment_root_location(tablet_id), tablet_delvec_filename(tablet_id, version));
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

    std::string del_location(int64_t tablet_id, std::string_view del_name) const {
        return join_path(segment_root_location(tablet_id), del_name);
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
