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

#include "storage/lake/index_delta_group_loader.h"

#include <unordered_set>

#include "storage/olap_common.h"

namespace starrocks::lake {

namespace {

// Pack (col_uid, index_type) into a 64-bit key for tombstone matching.
inline uint64_t pack_key(int32_t col_uid, IndexType type) {
    return (static_cast<uint64_t>(static_cast<uint32_t>(col_uid)) << 32) | static_cast<uint32_t>(type);
}

} // namespace

Status LakeIndexDeltaGroupLoader::load(const TabletSegmentId& tsid, int64_t query_version, IndexDeltaGroupList* out) {
    out->clear();
    if (_tablet_metadata == nullptr) {
        return Status::OK();
    }
    if (!_tablet_metadata->has_idg_meta()) {
        return Status::OK();
    }
    const auto& idg_meta = _tablet_metadata->idg_meta();
    auto it = idg_meta.idgs().find(tsid.segment_id);
    if (it == idg_meta.idgs().end()) {
        return Status::OK();
    }
    const auto& ver_pb = it->second;
    out->reserve(ver_pb.entries_size());
    for (const auto& entry_pb : ver_pb.entries()) {
        // Visibility filter: entries created after the query's snapshot are
        // not visible. Older snapshots see no IDG and fall back to footer.
        if (entry_pb.version() > query_version) {
            continue;
        }
        // Build tombstone set for this entry.
        std::unordered_set<uint64_t> dropped;
        dropped.reserve(entry_pb.dropped_keys_size());
        for (const auto& dk : entry_pb.dropped_keys()) {
            dropped.insert(pack_key(dk.col_unique_id(), dk.index_type()));
        }
        // Project active keys.
        IndexDeltaGroupEntry e;
        e.keys.reserve(entry_pb.keys_size());
        for (const auto& k : entry_pb.keys()) {
            if (dropped.count(pack_key(k.col_unique_id(), k.index_type())) == 0) {
                e.keys.push_back({k.col_unique_id(), k.index_type()});
            }
        }
        if (e.keys.empty()) {
            // Fully tombstoned: skip; the .idx file is logically dead and
            // will be physically removed by a future compaction or vacuum.
            continue;
        }
        e.index_file = entry_pb.index_file();
        e.version = entry_pb.version();
        e.encryption_meta = entry_pb.encryption_meta();
        e.shared_file = entry_pb.shared_file();
        e.file_size = entry_pb.file_size();
        out->push_back(std::move(e));
    }
    // Convention: return entries newest-first so callers can pick the highest
    // visible version with a single linear scan. apply_add_index inserts new
    // entries at the front of the proto list, so input is already newest-first;
    // sort here for safety in case future writers append at the back instead.
    std::sort(out->begin(), out->end(),
              [](const IndexDeltaGroupEntry& a, const IndexDeltaGroupEntry& b) { return a.version > b.version; });
    return Status::OK();
}

} // namespace starrocks::lake
