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

#include "common/runtime_profile.h"
#include "gen_cpp/PlanNodes_types.h"
#include "storage/tablet_scan_key_pruner.h"

namespace starrocks {

// Translates the plan-level scan key constraint into the storage-level struct, keeping thrift out of
// the storage layer. Shared by the local OLAP scan and the Lake data source so both paths agree on
// what a missing or unusable constraint means.
//
// Returns false when this scan range carries no usable HASH_BUCKET constraint, in which case the
// caller must run its pre-existing path untouched.
inline bool to_hash_bucket_constraint(const TInternalScanRange& scan_range, TabletHashBucketConstraint* out) {
    if (!scan_range.__isset.scan_key_constraint) {
        return false;
    }
    const TTabletScanKeyConstraint& constraint = scan_range.scan_key_constraint;
    if (constraint.type != TTabletScanKeyConstraintType::HASH_BUCKET) {
        return false;
    }

    if (!constraint.__isset.distribution_key_positions || !constraint.__isset.bucket_id ||
        !constraint.__isset.bucket_num) {
        return false;
    }

    out->distribution_key_positions = constraint.distribution_key_positions;
    out->bucket_id = constraint.bucket_id;
    out->bucket_num = constraint.bucket_num;
    // An older FE that predates the field means version 1; a newer FE sending a version this build
    // does not implement is rejected by the pruner rather than guessed at.
    out->hash_version = constraint.__isset.hash_version ? constraint.hash_version : 1;
    out->pruning_was_exact = constraint.__isset.pruning_was_exact && constraint.pruning_was_exact;
    return true;
}

// True when this scan range asks for range-distribution scan key pruning. Carries no payload: the BE
// compares each scan key against the range of the tablet version it opened, so there is nothing to
// translate beyond the enable signal itself.
inline bool wants_tablet_range_scan_key_prune(const TInternalScanRange& scan_range) {
    return scan_range.__isset.scan_key_constraint &&
           scan_range.scan_key_constraint.type == TTabletScanKeyConstraintType::RANGE;
}

// Reports what the prune did on this tablet. Shared so both scan paths emit the same counter names.
//
// ScanKeyPruneUnexpectedEmpty is the cheap always-on assertion: when FE's distribution pruning was
// exact, the tablet was selected because some value hashes to it, so at least one scan key must route
// here. A non-zero value therefore means FE and BE disagree about the hash contract (or data sits in
// the wrong bucket) and the feature should be turned off for that table.
inline void update_scan_key_prune_profile(RuntimeProfile* profile, const TabletHashBucketConstraint& constraint,
                                          const TabletScanKeyPruneResult& result, size_t original_range_count) {
    if (profile == nullptr) {
        return;
    }
    static const char* kPruneModeName = "TabletScanKeyPruneMode";
    profile->add_info_string(kPruneModeName, result.fallback ? "HASH_BUCKET_FALLBACK" : "HASH_BUCKET");

    COUNTER_UPDATE(ADD_COUNTER(profile, "ScanKeyRangesBeforeTabletPrune", TUnit::UNIT),
                   static_cast<int64_t>(original_range_count));
    COUNTER_UPDATE(ADD_COUNTER(profile, "ScanKeyRangesAfterTabletPrune", TUnit::UNIT),
                   static_cast<int64_t>(result.ranges.size()));
    COUNTER_UPDATE(ADD_COUNTER(profile, "ScanKeyRangesPrunedByTablet", TUnit::UNIT), result.pruned);
    COUNTER_UPDATE(ADD_COUNTER(profile, "ScanKeyPruneFallback", TUnit::UNIT), result.fallback ? 1 : 0);
    COUNTER_UPDATE(ADD_COUNTER(profile, "ScanKeyPrunedEmptyTablets", TUnit::UNIT), result.exact_empty ? 1 : 0);
    COUNTER_UPDATE(ADD_COUNTER(profile, "ScanKeyPruneUnexpectedEmpty", TUnit::UNIT),
                   (result.exact_empty && constraint.pruning_was_exact) ? 1 : 0);
}

} // namespace starrocks
