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

#include "storage/lake/tablet_retain_info.h"

namespace starrocks::lake {

void TabletRetainInfo::init(const std::unordered_set<int64_t>& retain_versions) {
    _retain_versions = retain_versions;
}

bool TabletRetainInfo::retained_by_version(int64_t create_version, int64_t remove_version) const {
    for (auto version : _retain_versions) {
        if (create_version <= version && version < remove_version) {
            return true;
        }
    }
    return false;
}

bool TabletRetainInfo::contains_version(int64_t version) const {
    return _retain_versions.find(version) != _retain_versions.end();
}

} // namespace starrocks::lake
