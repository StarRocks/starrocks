// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset_factory.h"

namespace starrocks {

namespace vectorized {

struct MergeConfig {
    size_t chunk_size;
};

// heap based rowset merger used for updatable tablet's compaction

Status compaction_merge_rowsets(Tablet& tablet, int64_t version, const vector<RowsetSharedPtr>& rowsets,
                                RowsetWriter* writer, const MergeConfig& cfg);

} // namespace vectorized

} // namespace starrocks
