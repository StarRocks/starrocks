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

#ifdef USE_STAROS
#include "storage/lake/starlet_location_provider.h"

#include <fmt/format.h>

#include <unordered_map>

#include "common/logging.h"
#include "fs/fs_starlet.h"
#include "gutil/strings/util.h"
#include "service/staros_worker.h"

namespace starrocks::lake {

std::string StarletLocationProvider::root_location(int64_t tablet_id) const {
    return build_starlet_uri(tablet_id, "");
}

StatusOr<std::string> StarletLocationProvider::real_location(const std::string& virtual_path) const {
    ASSIGN_OR_RETURN(auto path_and_id, parse_starlet_uri(virtual_path));
    auto info_or = g_worker->retrieve_shard_info(path_and_id.second);
    if (!info_or.ok()) {
        return to_status(info_or.status());
    }
    const auto& root_path = info_or->path_info.full_path();
    const auto& child_path = path_and_id.first;
    if (root_path.ends_with('/') || child_path.starts_with('/')) {
        return fmt::format("{}{}", info_or->path_info.full_path(), path_and_id.first);
    } else {
        return fmt::format("{}/{}", info_or->path_info.full_path(), path_and_id.first);
    }
}

} // namespace starrocks::lake
#endif // USE_STAROS
