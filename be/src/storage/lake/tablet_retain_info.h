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
#include <unordered_set>

namespace starrocks::lake {

/*
 * TabletRetainInfo carries the set of tablet metadata versions that lake vacuum must retain for a
 * partition (e.g. versions pinned by a cluster snapshot). It answers two questions used while
 * collecting garbage:
 *   - whether a tablet metadata file at a given version must be kept (contains_version);
 *   - whether a data file must be kept because it was live at a retained version
 *     (retained_by_version), decided purely from the file's own [create, remove) version interval.
 *
 * It deliberately reads no per-version tablet metadata: a retained version may predate the tablet
 * (e.g. a split/merge child asked to retain a pre-reshard version it never had), so reading it would
 * fail with NotFound. Keying on the file's own version instead makes the decision independent of
 * both that read and the per-file `shared` flag (which reshard does not set uniformly).
*/
class TabletRetainInfo {
public:
    TabletRetainInfo() = default;
    ~TabletRetainInfo() = default;

    void init(const std::unordered_set<int64_t>& retain_versions);

    // Whether a data file created at |create_version| and removed at |remove_version| was live at
    // some retained version, i.e. there exists a retained version V with
    // create_version <= V < remove_version. A |create_version| of 0 means "unknown" and is treated
    // as the earliest possible version, so the file is retained conservatively.
    bool retained_by_version(int64_t create_version, int64_t remove_version) const;

    bool contains_version(int64_t version) const;

private:
    std::unordered_set<int64_t> _retain_versions;
};

} // namespace starrocks::lake
