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

#include <memory>

#include "gen_cpp/lake_types.pb.h"

namespace starrocks {

using TabletMetadata = TabletMetadataPB;
using TabletMetadataPtr = std::shared_ptr<const TabletMetadata>;
using MutableTabletMetadataPtr = std::shared_ptr<TabletMetadata>;
using BundleTabletMetadataPtr = std::shared_ptr<BundleTabletMetadataPB>;
using TabletMetadataPtrs = std::vector<TabletMetadataPtr>;

// Sum a tablet's aggregate stats from rowset metadata WITHOUT any delvec I/O.
// For PK tablets, subtracts the stored (approximate) num_dels; never reads delete
// vectors (unlike get_tablet_stats's accurate/old-metadata fallback branches).
// Used on the publish hot path where extra I/O is unacceptable; the periodic
// TabletStatMgr scan remains responsible for accurate live-row counts.
inline void compute_tablet_stats(const TabletMetadataPB& metadata, int64_t* num_rows, int64_t* data_size) {
    const bool is_pk = metadata.schema().keys_type() == PRIMARY_KEYS;
    int64_t rows = 0;
    int64_t size = 0;
    for (const auto& rowset : metadata.rowsets()) {
        const int64_t num_deletes = (is_pk && rowset.has_num_dels()) ? rowset.num_dels() : 0;
        const int64_t live_rows = rowset.num_rows() - num_deletes;
        rows += (live_rows > 0 ? live_rows : 0);
        size += rowset.data_size();
    }
    for (const auto& [_, file] : metadata.delvec_meta().version_to_file()) {
        size += file.size();
    }
    *num_rows = rows;
    *data_size = size;
}

} // namespace starrocks
