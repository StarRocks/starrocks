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

#include <cstdint>
#include <string>
#include <unordered_set>
#include <vector>

namespace starrocks {
class Status;
}

namespace starrocks::lake {

class TabletManager;

/*
 * TabletRetainInfo is used to collect all files name, rowsets id for the specifed versions
 * that used in vacuum process to determine which files(data or meta) can be deleted.
*/
class TabletRetainInfo {
public:
    TabletRetainInfo() = default;
    ~TabletRetainInfo() = default;

    Status init(int64_t tablet_id, const std::vector<int64_t>& retain_versions, TabletManager* tablet_mgr);

    bool contains_file(const std::string& file_name) const;

    bool contains_version(int64_t version) const;

    bool contains_rowset(uint32_t rowset_id) const;

    int64_t tablet_id() const { return _tablet_id; }

private:
    int64_t _tablet_id;
    std::unordered_set<int64_t> _versions;
    std::unordered_set<std::string> _files;
    std::unordered_set<uint32_t> _rowset_ids;
};

} // namespace starrocks::lake
