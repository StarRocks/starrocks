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

#include "storage/lake/tablet_retain_info.h"

#include <gtest/gtest.h>

namespace starrocks::lake {

// A data file created at |create| and removed at |remove| is retained iff some retained version V
// satisfies create <= V < remove (i.e. the file was live at V).
TEST(LakeTabletRetainInfoTest, retained_by_version_interval) {
    TabletRetainInfo info;
    info.init(/*retain_versions=*/{5});

    EXPECT_TRUE(info.retained_by_version(/*create=*/5, /*remove=*/6));   // create == V, V < remove
    EXPECT_TRUE(info.retained_by_version(/*create=*/3, /*remove=*/6));   // create < V < remove
    EXPECT_TRUE(info.retained_by_version(/*create=*/5, /*remove=*/100)); // wide live interval

    EXPECT_FALSE(info.retained_by_version(/*create=*/6, /*remove=*/10)); // created after V
    EXPECT_FALSE(info.retained_by_version(/*create=*/3, /*remove=*/5));  // removed at V (V not < remove)
    EXPECT_FALSE(info.retained_by_version(/*create=*/3, /*remove=*/4));  // interval entirely below V
}

// An unset/0 creation version means "unknown, assume earliest": retained whenever some pinned
// version is below the removal version (safe conservative retain, never a false delete).
TEST(LakeTabletRetainInfoTest, retained_by_version_unknown_create_version) {
    TabletRetainInfo info;
    info.init({5});

    EXPECT_TRUE(info.retained_by_version(/*create=*/0, /*remove=*/6));  // some pinned V (5) < remove
    EXPECT_FALSE(info.retained_by_version(/*create=*/0, /*remove=*/5)); // no pinned V < remove(==5)
    EXPECT_FALSE(info.retained_by_version(/*create=*/0, /*remove=*/4)); // no pinned V < remove
}

TEST(LakeTabletRetainInfoTest, retained_by_version_multiple_versions) {
    TabletRetainInfo info;
    info.init({5, 20});

    EXPECT_TRUE(info.retained_by_version(0, 6));   // covers V=5
    EXPECT_TRUE(info.retained_by_version(10, 21)); // covers V=20
    EXPECT_TRUE(info.retained_by_version(0, 100)); // covers both
    EXPECT_FALSE(info.retained_by_version(6, 20)); // between: 5<6, and 20 is not < 20
}

TEST(LakeTabletRetainInfoTest, retained_by_version_empty_retain_set) {
    TabletRetainInfo info;
    info.init({});

    EXPECT_FALSE(info.retained_by_version(0, 1000));
    EXPECT_FALSE(info.retained_by_version(1, 2));
}

TEST(LakeTabletRetainInfoTest, contains_version) {
    TabletRetainInfo info;
    info.init({2, 5});

    EXPECT_TRUE(info.contains_version(2));
    EXPECT_TRUE(info.contains_version(5));
    EXPECT_FALSE(info.contains_version(1));
    EXPECT_FALSE(info.contains_version(3));
}

} // namespace starrocks::lake
