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

#include "storage/segment_footer_collector.h"

#include "fs/fs.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/segment.h"
#include "storage/tablet.h"

namespace starrocks {

std::vector<VisibleSegmentFooter> collect_visible_segment_footers(const std::shared_ptr<Tablet>& tablet) {
    std::vector<VisibleSegmentFooter> footers;
    if (tablet == nullptr) {
        return footers;
    }
    std::vector<RowsetSharedPtr> rowsets;
    if (Status st = tablet->capture_consistent_rowsets(tablet->max_version(), &rowsets); !st.ok()) {
        return footers;
    }

    auto* fs = FileSystem::Default();
    for (const auto& rs : rowsets) {
        if (rs == nullptr) {
            continue;
        }
        const int64_t num_segments = rs->num_segments();
        for (int64_t seg_id = 0; seg_id < num_segments; ++seg_id) {
            std::string seg_path = Rowset::segment_file_path(rs->rowset_path(), rs->rowset_id(), seg_id);
            auto file_or = fs->new_random_access_file(seg_path);
            if (!file_or.ok()) {
                // Encrypted, bundled, or already vacuumed: skip rather than fail,
                // so a metadata query degrades to fewer rows.
                continue;
            }
            SegmentFooterPB footer;
            if (auto parsed = Segment::parse_segment_footer(file_or.value().get(), &footer, nullptr, nullptr);
                !parsed.ok()) {
                continue;
            }
            footers.emplace_back(VisibleSegmentFooter{rs->rowset_id().to_string(), seg_id, std::move(footer)});
        }
    }
    return footers;
}

} // namespace starrocks
