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

#include <gtest/gtest.h>

#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "common/status.h"
#include "storage/chunk_helper.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/tablet_reshard.h"
#include "storage/lake/transactions.h"
#include "storage/lake/txn_log.h"
#include "storage/lake/update_manager.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/tablet_schema.h"
#include "test_util.h"

namespace starrocks::lake {

using VSchema = starrocks::Schema;
using VChunk = starrocks::Chunk;

// Build a shadow-rewrite txn info: a normal txn info re-keyed with txn_type == TXN_SHADOW_REWRITE and
// carrying the watershed W in shadow_rewrite_alter_version. The shadow tablet is then published via an
// ordinary PUBLISH_NORMAL PublishTabletInfo; publish_version dispatches on the txn type.
static TxnInfoPB TEST_shadow_rewrite_txn_info(int64_t txn_id, int64_t alter_version, int64_t commit_time) {
    TxnInfoPB info = TEST_txn_info(txn_id, commit_time);
    info.set_txn_type(TXN_SHADOW_REWRITE);
    info.set_shadow_rewrite_alter_version(alter_version);
    return info;
}

// Behavior tests for the SHADOW_REWRITE early-interception branch in publish_version:
// the historical-rewrite source op_write@rewriteTxn is anchored as an op_schema_change@W
// during the flip publish (present => carries the rowset; absent => empty), then the
// post-watershed double-write vlogs W+1..commitVersion replay as plain op_write.
//
// The flip publishes the shadow tablet under the partition's own rewriteTxnId (the publish
// txn id is free because vlog replay is version-keyed), base_version=1, new_version=commitVersion.

// DUP_KEYS fixture: c0 (key INT), c1 (value INT). Exercises cases (a) present, (b) absent+vlogs,
// (c) absent+no-vlogs. Modeled on SchemaChangeFlipReplayTest.
class ShadowRewritePublishDupTest : public TestBase {
public:
    ShadowRewritePublishDupTest() : TestBase(kTestDir) {}

    void SetUp() override {
        clear_and_init_test_dir();

        _tablet_metadata = std::make_shared<TabletMetadata>();
        _tablet_metadata->set_id(next_id());
        _tablet_metadata->set_version(1);
        _tablet_metadata->set_next_rowset_id(1);
        auto schema = _tablet_metadata->mutable_schema();
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        schema->set_keys_type(DUP_KEYS);
        schema->set_num_rows_per_row_block(65535);
        {
            auto c0 = schema->add_column();
            c0->set_unique_id(next_id());
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        {
            auto c1 = schema->add_column();
            c1->set_unique_id(next_id());
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(false);
            c1->set_is_nullable(false);
            c1->set_aggregation("NONE");
        }
        _tablet_schema = TabletSchema::create(*schema);
        _schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(_tablet_schema));
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

protected:
    constexpr static const char* const kTestDir = "test_lake_shadow_rewrite_publish_dup";

    // Write one (c0, c1) row to |tablet_id| under |txn_id| as a plain op_write (no publish).
    void write_row(int64_t tablet_id, int64_t txn_id, int32_t c0_val, int32_t c1_val) {
        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        c0->append_datum(Datum(c0_val));
        c1->append_datum(Datum(c1_val));
        VChunk chunk({std::move(c0), std::move(c1)}, _schema);
        uint32_t indexes[1] = {0};

        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk, indexes, sizeof(indexes) / sizeof(indexes[0])));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
    }

    static ChunkPtr read(const VersionedTablet& tablet) {
        auto metadata = tablet.metadata();
        auto schema = tablet.get_schema();
        auto reader = std::make_shared<TabletReader>(tablet.tablet_manager(), metadata, *(schema->schema()));
        CHECK_OK(reader->prepare());
        TabletReaderParams params;
        CHECK_OK(reader->open(params));
        auto chunk = ChunkFactory::new_chunk(*(schema->schema()), 16);
        while (true) {
            auto tmp = ChunkFactory::new_chunk(*(schema->schema()), 16);
            auto st = reader->get_next(tmp.get());
            if (st.ok()) {
                chunk->append(*tmp);
            } else if (st.is_end_of_file()) {
                break;
            } else {
                CHECK(false) << st;
            }
        }
        return chunk;
    }

    std::map<int32_t, int32_t> read_rows(int64_t tablet_id, int64_t version) {
        auto tablet = _tablet_mgr->get_tablet(tablet_id, version).value();
        auto chunk = read(tablet);
        std::map<int32_t, int32_t> rows;
        for (size_t i = 0; i < chunk->num_rows(); ++i) {
            rows[chunk->get(i)[0].get_int32()] = chunk->get(i)[1].get_int32();
        }
        return rows;
    }

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<VSchema> _schema;
    int64_t _partition_id = 100;
};

// Case (a): present source op_write@rewriteTxn, no vlogs.
// Result @ commitVersion carries the historical rowset, anchored at W.
TEST_F(ShadowRewritePublishDupTest, PresentSourceNoVlogs) {
    const int64_t tablet_id = _tablet_metadata->id();
    const int64_t W = 2; // present source => W >= 2 so the source load runs (not the W==1 skip)
    const int64_t rewrite_txn_id = 20001;
    const int64_t new_version = W + 1; // commitVersion: anchor only, no double-writes

    write_row(tablet_id, rewrite_txn_id, /*c0=*/0, /*c1=*/100);

    auto txn_info = TEST_shadow_rewrite_txn_info(rewrite_txn_id, W, time(nullptr));
    ASSIGN_OR_ABORT(auto new_metadata,
                    publish_version(_tablet_mgr.get(), PublishTabletInfo(tablet_id),
                                    /*base_version=*/1, new_version, std::span<const TxnInfoPB>(&txn_info, 1), false));
    ASSERT_EQ(new_version, new_metadata->version());

    auto rows = read_rows(tablet_id, new_version);
    ASSERT_EQ(1, rows.size());
    EXPECT_EQ(100, rows[0]);
}

// Case (b): at W > 1 every shadow tablet MUST have a source op_write -- the rewrite INSERT opens and
// finishes a delta writer for every shadow tablet (LakeTabletsChannel::_create_delta_writers +
// finish-all-on-EOS), so even a 0-row shadow tablet gets an (empty) op_write. A missing source op_write at
// W > 1 with the target version unpublished therefore means the source was lost: the load's not-found
// propagates and the publish FAILS (so it retries) rather than synthesizing an empty op_schema_change@W --
// which would silently drop the partition's pre-watershed data while still replaying the post-watershed
// vlogs (a partial, corrupt result).
TEST_F(ShadowRewritePublishDupTest, AbsentSourceAtWGreaterThanOneFails) {
    const int64_t tablet_id = _tablet_metadata->id();
    const int64_t W = 2; // W > 1: a missing source op_write is a lost source, not a legitimate empty tablet
    const int64_t rewrite_txn_id = 20001;

    // Post-watershed double-write vlogs exist, but the source op_write@rewriteTxn is absent.
    const int64_t post_txn_1 = 30001;
    write_row(tablet_id, post_txn_1, /*c0=*/1, /*c1=*/200);
    ASSERT_OK(publish_single_log_version(tablet_id, post_txn_1, /*log_version=*/W + 1));

    const int64_t new_version = W + 2; // commitVersion (unpublished)
    auto txn_info = TEST_shadow_rewrite_txn_info(rewrite_txn_id, W, time(nullptr));
    auto res = publish_version(_tablet_mgr.get(), PublishTabletInfo(tablet_id),
                               /*base_version=*/1, new_version, std::span<const TxnInfoPB>(&txn_info, 1), false);
    ASSERT_FALSE(res.ok()) << "W>1 with a missing source op_write must fail, not synthesize empty";
    EXPECT_TRUE(res.status().is_not_found()) << res.status();
}

// Case (c): absent source op_write, no vlogs, W == 1 (empty partition at the watershed).
// W == 1 means the base partition was empty: BE skips the source load entirely and synthesizes an
// empty op_schema_change@1, so the version advances empty with no rowsets and no source load occurs.
TEST_F(ShadowRewritePublishDupTest, AbsentSourceNoVlogsEmptyAdvance) {
    const int64_t tablet_id = _tablet_metadata->id();
    const int64_t W = 1; // W == 1 => skip-load empty path
    const int64_t rewrite_txn_id = 20001;
    const int64_t new_version = W + 1; // commitVersion: empty advance

    auto txn_info = TEST_shadow_rewrite_txn_info(rewrite_txn_id, W, time(nullptr));
    ASSIGN_OR_ABORT(auto new_metadata,
                    publish_version(_tablet_mgr.get(), PublishTabletInfo(tablet_id),
                                    /*base_version=*/1, new_version, std::span<const TxnInfoPB>(&txn_info, 1), false));
    ASSERT_EQ(new_version, new_metadata->version());
    EXPECT_EQ(0, new_metadata->rowsets_size());

    auto rows = read_rows(tablet_id, new_version);
    EXPECT_EQ(0, rows.size());
}

// W == 1 skip-load path: with an injected load_txn_log error point active, a W == 1 shadow publish must
// still succeed because the source load is skipped entirely (never 404s, never hits the error point).
// The tablet advances 1 -> 2 empty, matching AbsentSourceNoVlogsEmptyAdvance but proving no load ran.
TEST_F(ShadowRewritePublishDupTest, EmptyAtW1SkipsLoad) {
    const int64_t tablet_id = _tablet_metadata->id();
    const int64_t W = 1; // W == 1 => skip-load empty path
    const int64_t rewrite_txn_id = 20002;
    const int64_t new_version = W + 1; // commitVersion: empty advance

    TEST_ENABLE_ERROR_POINT("TabletManager::load_txn_log",
                            Status::IOError("load_txn_log must not run for W==1 shadow rewrite"));
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        TEST_DISABLE_ERROR_POINT("TabletManager::load_txn_log");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    auto txn_info = TEST_shadow_rewrite_txn_info(rewrite_txn_id, W, time(nullptr));
    ASSIGN_OR_ABORT(auto new_metadata,
                    publish_version(_tablet_mgr.get(), PublishTabletInfo(tablet_id),
                                    /*base_version=*/1, new_version, std::span<const TxnInfoPB>(&txn_info, 1), false));
    ASSERT_EQ(new_version, new_metadata->version());
    EXPECT_EQ(0, new_metadata->rowsets_size());
}

// PRIMARY_KEYS fixture: case (d) newer-wins. Modeled on LakePrimaryKeyPublishTest.
// Present source rows at keys K, a vlog at W+1 overwriting one K; after publish the newer
// (vlog) value wins. FAILS before the early-interception branch if _base_version is not
// anchored to W (PK replay mis-orders / publish errors on the missing primary log).
class ShadowRewritePublishPkTest : public TestBase {
public:
    ShadowRewritePublishPkTest() : TestBase(kTestDir) {
        _tablet_metadata = generate_simple_tablet_metadata(PRIMARY_KEYS);
        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(_tablet_schema));
        _slot_cid_map.emplace(0, 0);
        _slot_cid_map.emplace(1, 1);
        _slot_cid_map.emplace(2, 2);
    }

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

protected:
    constexpr static const char* const kTestDir = "test_lake_shadow_rewrite_publish_pk";

    // Build a (c0, c1, __op=UPSERT) chunk for the given key/value pairs.
    ChunkPtr gen_chunk(const std::vector<int32_t>& keys, const std::vector<int32_t>& vals) {
        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        auto c2 = Int8Column::create();
        for (size_t i = 0; i < keys.size(); ++i) {
            c0->append_datum(Datum(keys[i]));
            c1->append_datum(Datum(vals[i]));
            c2->append_datum(Datum((int8_t)TOpType::UPSERT));
        }
        return std::make_shared<VChunk>(Columns{std::move(c0), std::move(c1), std::move(c2)}, _slot_cid_map);
    }

    // Write a PK op_write (no publish) under |txn_id|.
    void write_pk(int64_t tablet_id, int64_t txn_id, const std::vector<int32_t>& keys,
                  const std::vector<int32_t>& vals) {
        auto chunk = gen_chunk(keys, vals);
        std::vector<uint32_t> indexes(chunk->num_rows());
        for (uint32_t i = 0; i < chunk->num_rows(); ++i) indexes[i] = i;
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(*chunk, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
    }

    std::map<int32_t, int32_t> read_rows(int64_t tablet_id, int64_t version) {
        auto metadata = _tablet_mgr->get_tablet_metadata(tablet_id, version).value();
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));
        std::map<int32_t, int32_t> rows;
        while (true) {
            auto tmp = ChunkFactory::new_chunk(*_schema, 64);
            auto st = reader->get_next(tmp.get());
            if (st.is_end_of_file()) break;
            CHECK_OK(st);
            for (size_t i = 0; i < tmp->num_rows(); ++i) {
                rows[tmp->get(i)[0].get_int32()] = tmp->get(i)[1].get_int32();
            }
        }
        return rows;
    }

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<VSchema> _schema;
    int64_t _partition_id = next_id();
    Chunk::SlotHashMap _slot_cid_map;
};

// Case (d): PK present source (keys 0,1 => 100,101), vlog at W+1 overwriting key 1 => 999.
// After publish the newer (vlog) value for key 1 wins; key 0 keeps the anchored base value.
TEST_F(ShadowRewritePublishPkTest, NewerWinsUnderVlog) {
    const int64_t tablet_id = _tablet_metadata->id();
    const int64_t W = 2; // present source => W >= 2 so the source load runs (not the W==1 skip)
    const int64_t rewrite_txn_id = 20001;

    // 1) Historical source snapshot keyed by the rewrite txn id (op_write, no publish).
    write_pk(tablet_id, rewrite_txn_id, {0, 1}, {100, 101});

    // 2) A post-watershed double-write overwrites key 1 -> 999, landing as a vtxn log at W+1.
    const int64_t post_txn_1 = 30001;
    write_pk(tablet_id, post_txn_1, {1}, {999});
    ASSERT_OK(publish_single_log_version(tablet_id, post_txn_1, /*log_version=*/W + 1));

    // 3) Flip publish under the rewrite txn id; commitVersion = W + 2.
    const int64_t new_version = W + 2;
    auto txn_info = TEST_shadow_rewrite_txn_info(rewrite_txn_id, W, time(nullptr));
    ASSIGN_OR_ABORT(auto new_metadata,
                    publish_version(_tablet_mgr.get(), PublishTabletInfo(tablet_id),
                                    /*base_version=*/1, new_version, std::span<const TxnInfoPB>(&txn_info, 1), false));
    ASSERT_EQ(new_version, new_metadata->version());

    auto rows = read_rows(tablet_id, new_version);
    ASSERT_EQ(2, rows.size());
    EXPECT_EQ(100, rows[0]); // anchored base@W
    EXPECT_EQ(999, rows[1]); // newer vlog @W+1 wins
}

} // namespace starrocks::lake
