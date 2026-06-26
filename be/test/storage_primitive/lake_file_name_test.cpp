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

#include "storage/primitive/lake_file_name.h"

#include <gtest/gtest.h>

namespace starrocks::lake {

TEST(LakeFileNameTest, test_file_type_predicates) {
    EXPECT_TRUE(is_segment("0000000000000001_abc.dat"));
    EXPECT_TRUE(is_del("0000000000000001_abc.del"));
    EXPECT_TRUE(is_delvec("0000000000000001_abc.delvec"));
    EXPECT_TRUE(is_txn_log("0000000000000001_0000000000000002.log"));
    EXPECT_TRUE(is_txn_slog("0000000000000001_0000000000000002.slog"));
    EXPECT_TRUE(is_txn_vlog("0000000000000001_0000000000000002.vlog"));
    EXPECT_TRUE(is_tablet_metadata("0000000000000001_0000000000000002.meta"));
    EXPECT_TRUE(is_tablet_initial_metadata("0000000000000000_0000000000000001.meta"));
    EXPECT_TRUE(is_tablet_metadata_lock("0000000000000001_0000000000000002_0000000000000003.lock"));
    EXPECT_TRUE(is_sst("abc.sst"));
    EXPECT_TRUE(is_cols("abc.cols"));
    EXPECT_TRUE(is_idx("abc.idx"));
    EXPECT_TRUE(is_lcrm("abc.lcrm"));
    EXPECT_TRUE(is_combined_txn_log("0000000000000001.logs"));
    EXPECT_TRUE(is_vector_index("abc.vi"));

    EXPECT_FALSE(is_segment("abc.meta"));
    EXPECT_FALSE(is_tablet_metadata("abc.dat"));
    EXPECT_FALSE(is_txn_log("abc.logs"));
    EXPECT_FALSE(is_combined_txn_log("abc.log"));
}

} // namespace starrocks::lake
