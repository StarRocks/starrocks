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
#include <vector>

#include "common/status.h"
#include "storage/lake/tablet_metadata.h"

namespace starrocks::lake {

// The input contract of a tablet merge. A merge takes over every file of its sources by reference, never
// rewriting one, so it admits a group of sources only when:
//   - every source declares the key range it covers;
//   - no source still shares a data file with another tablet (segments, del files, primary-key index sstables,
//     .cols and .idx files; delete-vector files are rewritten by the merge and do not count);
//   - no file is declared by two sources, whatever their shared flags say;
//   - every primary-key index sstable declares a key range, and that range lies inside its own source's range;
//   - rowsets that share a uid (pieces of one logical rowset) claim pairwise-disjoint segment indices;
//   - the sources agree on table-level configuration the merged tablet inherits from one of them.
// Together these make the key sets of the sources disjoint, which is what lets the merge concatenate them.
// Reads metadata only; performs no I/O.
Status admit_merge_sources(const std::vector<TabletMetadataPtr>& sources);

// Whether |sstable| declares a key range that lies inside |source|'s range: the part of the admission check a
// merge also applies to an sstable its source flush writes after admission.
Status check_sstable_range(const TabletMetadataPB& source, const PersistentIndexSstablePB& sstable);

// Turns |status|, a deterministic refusal to merge |sources| into |target_tablet_id| found before anything was
// written, into the error the publish returns: same status code, message prefixed with "MERGE_REJECTED: " so the
// prefix and the diagnostic lead whatever an operator reads it in. Also logs one WARNING naming the tablets.
Status merge_rejection(const Status& status, const std::vector<TabletMetadataPtr>& sources, int64_t target_tablet_id);

} // namespace starrocks::lake
