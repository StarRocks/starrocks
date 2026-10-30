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

#include "storage/lake/index_delta_group_loader.h"

#include <gtest/gtest.h>

#include <memory>

#include "base/testutil/assert.h"
#include "gen_cpp/lake_types.pb.h"
#include "gen_cpp/types.pb.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/olap_common.h"

namespace starrocks::lake {

// Minimal unit coverage for the reader-side IDG filter: version visibility,
// tombstone application, newest-first ordering. No FileSystem required.
class IndexDeltaGroupLoaderTest : public ::testing::Test {
protected:
    static IndexDeltaGroupEntryPB* append_entry(IndexDeltaGroupVerPB* ver, const std::string& file, int64_t version,
                                                int32_t col_uid, IndexType type) {
        auto* e = ver->add_entries();
        auto* k = e->add_keys();
        k->set_col_unique_id(col_uid);
        k->set_index_type(type);
        e->set_index_file(file);
        e->set_version(version);
        return e;
    }
};

TEST_F(IndexDeltaGroupLoaderTest, EmptyMetadataReturnsEmpty) {
    auto md = std::make_shared<TabletMetadataPB>();
    md->set_id(1);
    LakeIndexDeltaGroupLoader loader(md);
    IndexDeltaGroupList out;
    ASSERT_OK(loader.load(TabletSegmentId(1, 42), /*query_version=*/100, &out));
    EXPECT_TRUE(out.empty());
}

TEST_F(IndexDeltaGroupLoaderTest, VersionVisibilityFilter) {
    auto md = std::make_shared<TabletMetadataPB>();
    md->set_id(1);
    auto& ver = (*md->mutable_idg_meta()->mutable_idgs())[42];
    append_entry(&ver, "old.idx", /*version=*/5, 7, BITMAP);
    append_entry(&ver, "new.idx", /*version=*/20, 7, BITMAP);

    LakeIndexDeltaGroupLoader loader(md);

    // Snapshot older than the newer entry sees only the older entry.
    IndexDeltaGroupList out;
    ASSERT_OK(loader.load(TabletSegmentId(1, 42), 10, &out));
    ASSERT_EQ(1u, out.size());
    EXPECT_EQ("old.idx", out[0].index_file);

    // Snapshot equal to the newer entry's version sees both, newest first.
    out.clear();
    ASSERT_OK(loader.load(TabletSegmentId(1, 42), 20, &out));
    ASSERT_EQ(2u, out.size());
    EXPECT_EQ("new.idx", out[0].index_file);
    EXPECT_EQ("old.idx", out[1].index_file);
}

TEST_F(IndexDeltaGroupLoaderTest, TombstonedKeysStripped) {
    auto md = std::make_shared<TabletMetadataPB>();
    md->set_id(1);
    auto& ver = (*md->mutable_idg_meta()->mutable_idgs())[42];
    auto* e = append_entry(&ver, "payload.idx", 10, 7, BITMAP);
    // Add a second active key on the same entry.
    auto* k2 = e->add_keys();
    k2->set_col_unique_id(8);
    k2->set_index_type(BITMAP);
    // Tombstone the (7, BITMAP) key only.
    auto* d = e->add_dropped_keys();
    d->set_col_unique_id(7);
    d->set_index_type(BITMAP);

    LakeIndexDeltaGroupLoader loader(md);
    IndexDeltaGroupList out;
    ASSERT_OK(loader.load(TabletSegmentId(1, 42), 100, &out));
    ASSERT_EQ(1u, out.size());
    ASSERT_EQ(1u, out[0].keys.size());
    EXPECT_EQ(8, out[0].keys[0].col_unique_id);
    EXPECT_EQ(BITMAP, out[0].keys[0].index_type);
}

TEST_F(IndexDeltaGroupLoaderTest, EmptyIndexFileSkipped) {
    // A malformed IDG entry whose index_file is unset/empty would make
    // downstream open() fail with a confusing error. The loader skips it
    // defensively and continues with well-formed entries.
    auto md = std::make_shared<TabletMetadataPB>();
    md->set_id(1);
    auto& ver = (*md->mutable_idg_meta()->mutable_idgs())[42];
    // Entry 0: malformed, no index_file.
    {
        auto* e = ver.add_entries();
        auto* k = e->add_keys();
        k->set_col_unique_id(7);
        k->set_index_type(BITMAP);
        e->set_version(5);
        // intentionally no set_index_file
    }
    // Entry 1: well-formed, should still be returned.
    append_entry(&ver, "good.idx", /*version=*/4, /*col_uid=*/7, BITMAP);

    LakeIndexDeltaGroupLoader loader(md);
    IndexDeltaGroupList out;
    ASSERT_OK(loader.load(TabletSegmentId(1, 42), /*query_version=*/100, &out));
    ASSERT_EQ(1u, out.size());
    EXPECT_EQ("good.idx", out[0].index_file);
}

TEST_F(IndexDeltaGroupLoaderTest, FullyTombstonedEntryOmitted) {
    auto md = std::make_shared<TabletMetadataPB>();
    md->set_id(1);
    auto& ver = (*md->mutable_idg_meta()->mutable_idgs())[42];
    auto* e = append_entry(&ver, "dead.idx", 10, 7, BITMAP);
    // Tombstone the only key.
    auto* d = e->add_dropped_keys();
    d->set_col_unique_id(7);
    d->set_index_type(BITMAP);

    LakeIndexDeltaGroupLoader loader(md);
    IndexDeltaGroupList out;
    ASSERT_OK(loader.load(TabletSegmentId(1, 42), 100, &out));
    EXPECT_TRUE(out.empty());
}

} // namespace starrocks::lake
