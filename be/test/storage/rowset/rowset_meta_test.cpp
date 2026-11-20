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

#include "storage/rowset/rowset_meta.h"

#include <gtest/gtest.h>

namespace starrocks {

TEST(RowsetMetaTest, explicit_overlap_survives_version_overwrite_and_reload) {
    RowsetMetaPB pb;
    pb.set_deprecated_rowset_id(1);
    pb.set_start_version(2);
    pb.set_end_version(2);
    pb.set_num_segments(9);
    pb.set_segments_overlap_pb(OVERLAPPING);
    RowsetMeta meta(pb);
    ASSERT_TRUE(meta.is_segments_overlapping());
    meta.set_version(Version(0, 2));
    EXPECT_TRUE(meta.is_segments_overlapping());
    EXPECT_EQ(9, meta.get_compaction_score());

    pb.set_start_version(0);
    RowsetMetaPB restored_pb;
    ASSERT_TRUE(restored_pb.ParseFromString(pb.SerializeAsString()));
    RowsetMeta restored(restored_pb);
    EXPECT_TRUE(restored.is_segments_overlapping());
    EXPECT_EQ(9, restored.get_compaction_score());
}

TEST(RowsetMetaTest, overlap_flags_and_legacy_metadata) {
    for (int num_segments : {0, 1, 3}) {
        for (const auto& version : {Version(2, 2), Version(0, 2), Version(-1, -1)}) {
            for (int flag : {-1, static_cast<int>(OVERLAP_UNKNOWN), static_cast<int>(OVERLAPPING),
                             static_cast<int>(NONOVERLAPPING)}) {
                SCOPED_TRACE(testing::Message() << num_segments << "," << version.first << "," << flag);
                RowsetMetaPB pb;
                pb.set_deprecated_rowset_id(1);
                pb.set_num_segments(num_segments);
                if (version.first >= 0) {
                    pb.set_start_version(version.first);
                    pb.set_end_version(version.second);
                }
                if (flag >= 0) pb.set_segments_overlap_pb(static_cast<SegmentsOverlapPB>(flag));
                RowsetMeta meta(pb);
                bool expected = false;
                if (num_segments > 1) {
                    if (flag == OVERLAPPING) expected = true;
                    if (flag == -1 || flag == OVERLAP_UNKNOWN)
                        expected = version.first >= 0 && version.first == version.second;
                }
                EXPECT_EQ(expected, meta.is_segments_overlapping());
                EXPECT_EQ(expected ? num_segments : 1, meta.get_compaction_score());
            }
        }
    }
}

} // namespace starrocks
