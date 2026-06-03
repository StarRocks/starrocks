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

#include "exec/pipeline/scan/footer_prefetch_state.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace starrocks::pipeline {

static FooterPrefetchItem make_item(const std::string& key) {
    FooterPrefetchItem item;
    item.key = key;
    item.path = key;
    item.file_size = 1024;
    return item;
}

static std::vector<FooterPrefetchItem> make_items(int n) {
    std::vector<FooterPrefetchItem> v;
    v.reserve(n);
    for (int i = 0; i < n; ++i) {
        v.push_back(make_item("f" + std::to_string(i)));
    }
    return v;
}

// Window is (frontier, frontier + lead_distance]; in-flight is capped.
TEST(FooterPrefetchStateTest, TakesUpToLeadDistanceAndInFlightCap) {
    FooterPrefetchState st(make_items(100), /*lead_distance=*/4, /*max_in_flight=*/2, /*metacache_on=*/true,
                           /*datacache_populate_on=*/false);
    FooterPrefetchItem out;
    EXPECT_TRUE(st.try_take_next(&out));
    EXPECT_EQ("f0", out.key);
    EXPECT_TRUE(st.try_take_next(&out));
    EXPECT_EQ("f1", out.key);
    EXPECT_FALSE(st.try_take_next(&out)); // in-flight cap (2) reached

    st.on_task_done();
    EXPECT_TRUE(st.try_take_next(&out));
    EXPECT_EQ("f2", out.key);
    st.on_task_done();
    EXPECT_TRUE(st.try_take_next(&out));
    EXPECT_EQ("f3", out.key);
    st.on_task_done();
    EXPECT_FALSE(st.try_take_next(&out)); // window (0, 4] exhausted, frontier has not advanced
}

TEST(FooterPrefetchStateTest, FrontierAdvancesWindow) {
    FooterPrefetchState st(make_items(100), /*lead_distance=*/2, /*max_in_flight=*/8, true, false);
    FooterPrefetchItem out;
    EXPECT_TRUE(st.try_take_next(&out));
    EXPECT_EQ("f0", out.key);
    EXPECT_TRUE(st.try_take_next(&out));
    EXPECT_EQ("f1", out.key);
    EXPECT_FALSE(st.try_take_next(&out)); // window (0, 2] exhausted
    st.on_task_done();
    st.on_task_done();

    st.mark_started("f0"); // frontier -> 1, window (1, 3]
    EXPECT_TRUE(st.try_take_next(&out));
    EXPECT_EQ("f2", out.key);
}

// If the real scan overtook the prefetcher during an unblock, the submit cursor jumps to the
// frontier and already-started files are skipped -- never re-warmed.
TEST(FooterPrefetchStateTest, SkipsStartedAndNeverFallsBehindFrontier) {
    FooterPrefetchState st(make_items(100), /*lead_distance=*/10, /*max_in_flight=*/8, true, false);
    for (int i = 0; i < 5; ++i) {
        st.mark_started("f" + std::to_string(i)); // frontier -> 5
    }
    FooterPrefetchItem out;
    EXPECT_TRUE(st.try_take_next(&out));
    EXPECT_EQ("f5", out.key); // not f0..f4
}

TEST(FooterPrefetchStateTest, CancelStopsTaking) {
    FooterPrefetchState st(make_items(10), 4, 4, true, false);
    st.cancel();
    EXPECT_TRUE(st.cancelled());
    FooterPrefetchItem out;
    EXPECT_FALSE(st.try_take_next(&out));
}

TEST(FooterPrefetchStateTest, AppendExtendsList) {
    FooterPrefetchState st(make_items(2), /*lead_distance=*/100, /*max_in_flight=*/100, true, false);
    FooterPrefetchItem out;
    EXPECT_TRUE(st.try_take_next(&out));
    EXPECT_EQ("f0", out.key);
    EXPECT_TRUE(st.try_take_next(&out));
    EXPECT_EQ("f1", out.key);
    EXPECT_FALSE(st.try_take_next(&out)); // only 2 files

    std::vector<FooterPrefetchItem> more;
    more.push_back(make_item("g0"));
    st.append(std::move(more));
    EXPECT_TRUE(st.try_take_next(&out));
    EXPECT_EQ("g0", out.key);
}

// Duplicate keys (offset splits of one path / re-delivered ranges) collapse to one entry, so
// mark_started always resolves and the contiguous frontier never freezes on an un-markable dup.
TEST(FooterPrefetchStateTest, DedupsDuplicateKeys) {
    std::vector<FooterPrefetchItem> items;
    items.push_back(make_item("f0"));
    items.push_back(make_item("f0")); // duplicate of the same path
    items.push_back(make_item("f1"));
    FooterPrefetchState st(std::move(items), /*lead_distance=*/100, /*max_in_flight=*/100, true, false);

    FooterPrefetchItem out;
    EXPECT_TRUE(st.try_take_next(&out));
    EXPECT_EQ("f0", out.key);
    EXPECT_TRUE(st.try_take_next(&out));
    EXPECT_EQ("f1", out.key);             // not a second f0
    EXPECT_FALSE(st.try_take_next(&out)); // only 2 distinct files

    // Marking both started advances the frontier to the end (it would freeze at the duplicate
    // index if dedup were missing); a fresh append is then takeable, a re-duplicate is dropped.
    st.mark_started("f0");
    st.mark_started("f1");
    std::vector<FooterPrefetchItem> more;
    more.push_back(make_item("f0")); // dup again -> dropped
    more.push_back(make_item("g0"));
    st.append(std::move(more));
    EXPECT_TRUE(st.try_take_next(&out));
    EXPECT_EQ("g0", out.key);
    EXPECT_FALSE(st.try_take_next(&out));
}

TEST(FooterPrefetchStateTest, WarmableFlags) {
    FooterPrefetchState neither(make_items(1), 4, 4, /*metacache_on=*/false, /*datacache_populate_on=*/false);
    EXPECT_FALSE(neither.warmable());

    FooterPrefetchState meta(make_items(1), 4, 4, /*metacache_on=*/true, /*datacache_populate_on=*/false);
    EXPECT_TRUE(meta.warmable());
    EXPECT_TRUE(meta.metacache_on());
    EXPECT_FALSE(meta.datacache_populate_on());

    FooterPrefetchState block(make_items(1), 4, 4, /*metacache_on=*/false, /*datacache_populate_on=*/true);
    EXPECT_TRUE(block.warmable());
    EXPECT_TRUE(block.datacache_populate_on());
}

} // namespace starrocks::pipeline
