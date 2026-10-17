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

namespace starrocks::lake {

struct CacheOptions {
    bool fill_meta_cache = true;
    bool fill_data_cache = true;
    // When true, bypass the tablet-metadata metacache lookup and read straight from durable storage.
    // Lets a caller tell "durably persisted in remote storage" apart from "only in the metacache"
    // (cached during publish but not yet durably flushed).
    bool skip_meta_cache = false;
};

// Which of the two remote layouts a VERSION-1 tablet metadata read should try FIRST. An ordering
// preference, not an assertion about what storage holds: either order resolves the metadata, because
// whichever is tried first falls back to the other. Guessing wrong costs one extra read.
//
// Kept out of CacheOptions on purpose: this says nothing about what to cache.
enum class InitialMetadataOrder {
    // The tablet's own key first, the partition-shared object as fallback. Correct for every
    // partition, so the default -- and the only order an unhinted caller may use. See the ordering
    // invariant on TabletManager::get_tablet_metadata().
    kPerTabletFirst,

    // The partition-shared object first. Reserved for a caller holding FE's
    // prefer_shared_initial_metadata hint, which FE sends only for a partition it has confirmed holds
    // a single index. Worth the reversal because a partition created with
    // TCreateTabletReq::enable_tablet_creation_optimization never writes a per-tablet version-1 key
    // at all, so kPerTabletFirst is a guaranteed NotFound for every one of its tablets.
    //
    // Never infer this from process-local state. On a partition that also holds a rollup or
    // schema-change shadow index, the shared object belongs to the OTHER index, and because it
    // exists the read succeeds and returns the wrong schema with nothing to signal it.
    kSharedFirst,
};

} // namespace starrocks::lake
