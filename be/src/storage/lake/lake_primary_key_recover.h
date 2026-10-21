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

#include <vector>

#include "common/status.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/types_fwd.h"
#include "storage/primary_index.h"

namespace starrocks::lake {

class Tablet;
class MetaFileBuilder;

// Recovers a shared-data primary key tablet whose primary key index disagrees with its data: a key live
// twice, an index entry pointing at a deleted row, or at a segment that does not exist.
//
// The delete vectors of the base version are the source of truth. Publish put into them everything a replay
// of the rowsets could not reconstruct -- rows that lost a conditional update, auto-increment conflicts,
// in-segment duplicates of an ORDER BY != PK load, deletes applied before a compaction -- so they are kept as
// they are, and del files are not replayed. Recover scans every live row in rowset order and, wherever a key
// is live more than once, deletes all but the newest row. The new deletes are merged into the existing delete
// vectors. The persistent index is then discarded, to be rebuilt from the rowsets and the merged delete
// vectors by the publish that runs next.
//
// Recover only appends to |builder| and changes |metadata|; nothing is written until the publish finalizes.
class LakePrimaryKeyRecover {
public:
    // |metadata| is the publish's copy of the base version, |base_version| the version it was read at.
    LakePrimaryKeyRecover(MetaFileBuilder* builder, Tablet* tablet, MutableTabletMetadataPtr metadata,
                          int64_t base_version)
            : _builder(builder), _tablet(tablet), _metadata(std::move(metadata)), _base_version(base_version) {}

    Status recover();

    // Number of rows recover deleted because a newer row has the same key.
    size_t num_duplicate_rows() const { return _num_duplicate_rows; }

    // Sort rowsets in the order their rows were written. A compaction output takes the place of its newest
    // input, so loads and compactions that ran concurrently stay in order.
    static Status sort_rowsets(std::vector<RowsetPtr>* rowsets);

private:
    // Scan every live row and collect the rows superseded by a newer row with the same key.
    Status find_duplicates(PrimaryIndex::DeletesMap* duplicates);
    // Merge |duplicates| into the delete vectors and the rowsets' delete counts.
    Status delete_duplicates(const PrimaryIndex::DeletesMap& duplicates);
    // Drop the persistent index so that the publish rebuilds it.
    Status discard_index();

    MetaFileBuilder* _builder;
    Tablet* _tablet;
    MutableTabletMetadataPtr _metadata;
    int64_t _base_version;
    size_t _num_duplicate_rows = 0;
};

} // namespace starrocks::lake
