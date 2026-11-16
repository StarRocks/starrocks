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

#include <algorithm>

#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "common/config_lake_fwd.h"
#include "common/config_primary_key_fwd.h"
#include "common/status.h"
#include "fs/bundle_file.h"
#include "fs/fs_util.h"
#include "storage/chunk_helper.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/metacache.h"
#include "storage/lake/primary_key_manager.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/tablet_reshard.h"
#include "storage/lake/transactions.h"
#include "storage/lake/txn_log.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/tablet_schema.h"
#include "test_util.h"
#include "testutil/column_test_helper.h"

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
    void write_row(int64_t tablet_id, int64_t txn_id, int32_t c0_val, int32_t c1_val,
                   const PUniqueId* load_id = nullptr) {
        auto c0 = ColumnTestHelper::build_column<int32_t>({c0_val});
        auto c1 = ColumnTestHelper::build_column<int32_t>({c1_val});
        VChunk chunk({std::move(c0), std::move(c1)}, _schema);
        uint32_t indexes[1] = {0};

        DeltaWriterBuilder builder;
        builder.set_tablet_manager(_tablet_mgr.get())
                .set_tablet_id(tablet_id)
                .set_txn_id(txn_id)
                .set_partition_id(_partition_id)
                .set_mem_tracker(_mem_tracker.get())
                .set_schema_id(_tablet_schema->id());
        if (load_id != nullptr) {
            builder.set_load_id(*load_id).set_is_multi_statements_txn(true).set_profile(&_multi_statement_profile);
        }
        ASSIGN_OR_ABORT(auto delta_writer, builder.build());
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
    RuntimeProfile _multi_statement_profile{"shadow_rewrite_dup_multi_statement"};
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

// DUP_KEYS keeps both statements even when they insert the same key. The grouped shadow replay
// must append both segments with distinct rowset-local indices rather than overwrite either row.
TEST_F(ShadowRewritePublishDupTest, MultiStatementVlogRetainsDuplicateKeysAndSegmentIndices) {
    ConfigResetGuard group_write_guard(&config::lake_enable_txn_vlog_group, true);
    const auto tablet_id = _tablet_metadata->id();
    const int64_t watershed = 2;
    const auto rewrite_txn_id = next_id();
    write_row(tablet_id, rewrite_txn_id, 0, 100);

    const auto txn_id = next_id();
    PUniqueId first_load;
    first_load.set_hi(66273);
    first_load.set_lo(11);
    PUniqueId second_load;
    second_load.set_hi(66273);
    second_load.set_lo(12);
    write_row(tablet_id, txn_id, 1, 200, &first_load);
    write_row(tablet_id, txn_id, 1, 999, &second_load);

    auto info = TEST_txn_info(txn_id, time(nullptr));
    info.add_load_ids()->CopyFrom(first_load);
    info.add_load_ids()->CopyFrom(second_load);
    const int64_t log_version = watershed + 1;
    ASSERT_OK(publish_log_version(_tablet_mgr.get(), tablet_id, std::span<const TxnInfoPB>(&info, 1), &log_version));

    config::lake_enable_txn_vlog_group = false;

    const int64_t flip_version = watershed + 2;
    auto flip_info = TEST_shadow_rewrite_txn_info(rewrite_txn_id, watershed, time(nullptr));
    ASSIGN_OR_ABORT(auto metadata, publish_version(_tablet_mgr.get(), PublishTabletInfo(tablet_id), 1, flip_version,
                                                   std::span<const TxnInfoPB>(&flip_info, 1), false, std::nullopt));
    ASSERT_EQ(2, metadata->rowsets_size());
    const auto& merged = metadata->rowsets(1);
    ASSERT_EQ(2, merged.segment_metas_size());
    EXPECT_EQ(2, merged.num_rows());
    ASSERT_TRUE(merged.segment_metas(0).has_segment_idx());
    ASSERT_TRUE(merged.segment_metas(1).has_segment_idx());
    EXPECT_NE(merged.segment_metas(0).segment_idx(), merged.segment_metas(1).segment_idx());

    _tablet_mgr->metacache()->prune();
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id, flip_version));
    auto chunk = read(tablet);
    ASSERT_EQ(3, chunk->num_rows());
    std::vector<std::pair<int32_t, int32_t>> rows;
    for (size_t i = 0; i < chunk->num_rows(); ++i) {
        rows.emplace_back(chunk->get(i)[0].get_int32(), chunk->get(i)[1].get_int32());
    }
    std::sort(rows.begin(), rows.end());
    const std::vector<std::pair<int32_t, int32_t>> expected{{0, 100}, {1, 200}, {1, 999}};
    EXPECT_EQ(expected, rows);
}

// FE log-publishes a multi-table transaction (BEGIN; INSERT INTO t1; INSERT INTO t2; COMMIT) to t1's shadow
// tablets with the load ids of every statement, but the statement that loaded t2 wrote no log for them. The
// statements that did must be materialized and replayed at the flip, not fail the log publish forever and leave
// the transaction COMMITTED and the alter unable to flip.
TEST_F(ShadowRewritePublishDupTest, MultiStatementVlogSkipsStatementsOfOtherTables) {
    for (bool group_writes : {false, true}) {
        SCOPED_TRACE(group_writes ? "group writes" : "legacy writes");
        ConfigResetGuard group_write_guard(&config::lake_enable_txn_vlog_group, group_writes);
        auto tablet_metadata = std::make_shared<TabletMetadata>(*_tablet_metadata);
        tablet_metadata->set_id(next_id());
        ASSERT_OK(_tablet_mgr->put_tablet_metadata(*tablet_metadata));
        const auto tablet_id = tablet_metadata->id();
        const int64_t watershed = 2;
        const auto rewrite_txn_id = next_id();
        write_row(tablet_id, rewrite_txn_id, 0, 100);

        const auto txn_id = next_id();
        PUniqueId first_load;
        first_load.set_hi(66313);
        first_load.set_lo(1);
        PUniqueId other_table_load;
        other_table_load.set_hi(66313);
        other_table_load.set_lo(2);
        PUniqueId last_load;
        last_load.set_hi(66313);
        last_load.set_lo(3);
        write_row(tablet_id, txn_id, 1, 200, &first_load);
        write_row(tablet_id, txn_id, 3, 400, &last_load);

        auto info = TEST_txn_info(txn_id, time(nullptr));
        for (const auto* load_id : {&first_load, &other_table_load, &last_load}) {
            info.add_load_ids()->CopyFrom(*load_id);
        }
        const int64_t log_version = watershed + 1;
        ASSERT_OK(
                publish_log_version(_tablet_mgr.get(), tablet_id, std::span<const TxnInfoPB>(&info, 1), &log_version));
        // A retry after the sources were swept finds the materialized log.
        for (const auto* load_id : {&first_load, &last_load}) {
            auto st = fs::delete_file(_tablet_mgr->txn_log_location(tablet_id, txn_id, *load_id));
            ASSERT_TRUE(st.ok() || st.is_not_found()) << st;
        }
        _tablet_mgr->metacache()->prune();
        ASSERT_OK(
                publish_log_version(_tablet_mgr.get(), tablet_id, std::span<const TxnInfoPB>(&info, 1), &log_version));

        const int64_t flip_version = watershed + 2;
        auto flip_info = TEST_shadow_rewrite_txn_info(rewrite_txn_id, watershed, time(nullptr));
        ASSERT_OK(publish_version(_tablet_mgr.get(), PublishTabletInfo(tablet_id), 1, flip_version,
                                  std::span<const TxnInfoPB>(&flip_info, 1), false, std::nullopt)
                          .status());
        _tablet_mgr->metacache()->prune();
        const std::map<int32_t, int32_t> expected{{0, 100}, {1, 200}, {3, 400}};
        EXPECT_EQ(expected, read_rows(tablet_id, flip_version));
    }
}

// PRIMARY_KEYS fixture: case (d) newer-wins. Modeled on LakePrimaryKeyPublishTest.
// Present source rows at keys K, a vlog at W+1 overwriting one K; after publish the newer
// (vlog) value wins. FAILS before the early-interception branch if _base_version is not
// anchored to W (PK replay mis-orders / publish errors on the missing primary log).
class ShadowRewritePublishPkTest : public TestBase {
public:
    ShadowRewritePublishPkTest() : ShadowRewritePublishPkTest(kTestDir) {}

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

protected:
    // A suite derived from this fixture passes a directory of its own. gtest-parallel runs different suites at
    // the same time, and with one shared directory the SetUp or TearDown of one suite deletes another's files.
    explicit ShadowRewritePublishPkTest(const char* test_dir) : TestBase(test_dir) {
        _tablet_metadata = generate_simple_tablet_metadata(PRIMARY_KEYS);
        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(_tablet_schema));
        _slot_cid_map.emplace(0, 0);
        _slot_cid_map.emplace(1, 1);
        _slot_cid_map.emplace(2, 2);
    }

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

class ShadowRewriteMultiStatementPkTest : public ShadowRewritePublishPkTest, public testing::WithParamInterface<bool> {
public:
    ShadowRewriteMultiStatementPkTest() : ShadowRewriteMultiStatementPkTest(kMultiStatementTestDir) {}

protected:
    explicit ShadowRewriteMultiStatementPkTest(const char* test_dir) : ShadowRewritePublishPkTest(test_dir) {
        _tablet_metadata->set_enable_persistent_index(true);
        _tablet_metadata->set_persistent_index_type(PersistentIndexTypePB::CLOUD_NATIVE);
    }

    constexpr static const char* const kMultiStatementTestDir = "test_lake_shadow_rewrite_publish_pk_multi_stmt";

    TxnLogPtr write_statement(int64_t txn_id, const PUniqueId& load_id, bool delete_only, int32_t value) {
        ChunkPtr chunk;
        std::vector<SlotDescriptor> slots;
        std::vector<SlotDescriptor*> slot_pointers;
        if (delete_only) {
            // SQL DELETE writes only the primary key and __op. The real writer emits an empty
            // partial-update segment as the delete's ordering slot, plus txn_meta and rewrite meta.
            slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
            slots.emplace_back(2, "__op", TypeDescriptor{LogicalType::TYPE_INT});
            for (auto& slot : slots) slot_pointers.push_back(&slot);
            auto keys = Int32Column::create();
            auto ops = Int8Column::create();
            keys->append_datum(Datum(int32_t{1}));
            ops->append_datum(Datum(static_cast<int8_t>(TOpType::DELETE)));
            Chunk::SlotHashMap slot_map{{0, 0}, {2, 1}};
            chunk = std::make_shared<Chunk>(Columns{std::move(keys), std::move(ops)}, slot_map);
        } else {
            chunk = gen_chunk({1}, {value});
        }
        DeltaWriterBuilder builder;
        builder.set_tablet_manager(_tablet_mgr.get())
                .set_tablet_id(_tablet_metadata->id())
                .set_txn_id(txn_id)
                .set_partition_id(_partition_id)
                .set_mem_tracker(_mem_tracker.get())
                .set_schema_id(_tablet_schema->id())
                .set_profile(&_profile)
                .set_load_id(load_id)
                .set_is_multi_statements_txn(true)
                .set_partial_update_mode(PartialUpdateMode::ROW_MODE);
        if (delete_only) builder.set_slot_descriptors(&slot_pointers);
        ASSIGN_OR_ABORT(auto writer, builder.build());
        CHECK_OK(writer->open());
        const uint32_t index = 0;
        CHECK_OK(writer->write(*chunk, &index, 1));
        ASSIGN_OR_ABORT(auto log, writer->finish_with_txnlog());
        writer->close();
        return log;
    }

    static PUniqueId make_load_id(int64_t lo) {
        PUniqueId id;
        id.set_hi(66273);
        id.set_lo(lo);
        return id;
    }

    // A vector deliberately keeps duplicate primary keys visible to the assertions.
    std::vector<std::pair<int32_t, int32_t>> read_visible_rows(int64_t version) {
        _tablet_mgr->metacache()->prune();
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));
        std::vector<std::pair<int32_t, int32_t>> rows;
        while (true) {
            auto chunk = ChunkFactory::new_chunk(*_schema, 64);
            auto st = reader->get_next(chunk.get());
            if (st.is_end_of_file()) break;
            CHECK_OK(st);
            for (size_t i = 0; i < chunk->num_rows(); ++i) {
                rows.emplace_back(chunk->get(i)[0].get_int32(), chunk->get(i)[1].get_int32());
            }
        }
        std::sort(rows.begin(), rows.end());
        return rows;
    }

    RuntimeProfile _profile{"shadow_multi_statement"};
};

// The multi-statement .vlog must retain each statement's update semantics and ordering. Merging
// DELETE's partial-update metadata into INSERT's full segment makes rewrite meta shorter than the
// segment list, so the unmodified implementation fails at RowsetUpdateState::_rewrite_segment.
TEST_P(ShadowRewriteMultiStatementPkTest, PartialDeleteThenInsertSurvivesReplayAndRebuild) {
    ConfigResetGuard group_write_guard(&config::lake_enable_txn_vlog_group, true);
    ConfigResetGuard preserve_order_guard(&config::lake_enable_pk_preserve_txn_delete_order, GetParam());
    const auto tablet_id = _tablet_metadata->id();
    const int64_t watershed = 2;
    const auto rewrite_txn_id = next_id();
    write_pk(tablet_id, rewrite_txn_id, {0, 1}, {100, 101});

    const auto txn_id = next_id();
    const auto delete_load = make_load_id(1);
    const auto insert_load = make_load_id(2);
    auto delete_log = write_statement(txn_id, delete_load, true, 0);
    auto insert_log = write_statement(txn_id, insert_load, false, 999);
    ASSERT_TRUE(delete_log->op_write().has_txn_meta());
    ASSERT_EQ(1, delete_log->op_write().rowset().segment_metas_size());
    ASSERT_EQ(0, delete_log->op_write().rowset().segment_metas(0).num_rows());
    ASSERT_EQ(1, delete_log->op_write().rewrite_segments_meta_size());
    ASSERT_EQ(1, delete_log->op_write().dels_meta_size());
    ASSERT_FALSE(insert_log->op_write().has_txn_meta());
    ASSERT_EQ(1, insert_log->op_write().rowset().segment_metas_size());
    ASSERT_EQ(0, insert_log->op_write().rewrite_segments_meta_size());

    auto info = TEST_txn_info(txn_id, time(nullptr));
    info.add_load_ids()->CopyFrom(delete_load);
    info.add_load_ids()->CopyFrom(insert_load);
    const int64_t log_version = watershed + 1;
    ASSERT_OK(publish_log_version(_tablet_mgr.get(), tablet_id, std::span<const TxnInfoPB>(&info, 1), &log_version));
    ASSERT_TRUE(fs::path_exist(_tablet_mgr->txn_vlog_group_location(tablet_id, log_version)));
    ASSERT_FALSE(fs::path_exist(_tablet_mgr->txn_vlog_location(tablet_id, log_version)));

    config::lake_enable_txn_vlog_group = false;

    // Retry materialization from its persisted .vlog, after the per-statement source logs vanish.
    for (const auto& load_id : {delete_load, insert_load}) {
        auto st = fs::delete_file(_tablet_mgr->txn_log_location(tablet_id, txn_id, load_id));
        ASSERT_TRUE(st.ok() || st.is_not_found()) << st;
    }
    _tablet_mgr->metacache()->prune();
    ASSERT_OK(publish_log_version(_tablet_mgr.get(), tablet_id, std::span<const TxnInfoPB>(&info, 1), &log_version));

    const int64_t flip_version = watershed + 2;
    auto flip_info = TEST_shadow_rewrite_txn_info(rewrite_txn_id, watershed, time(nullptr));
    ASSERT_OK(publish_version(_tablet_mgr.get(), PublishTabletInfo(tablet_id), 1, flip_version,
                              std::span<const TxnInfoPB>(&flip_info, 1), false, std::nullopt)
                      .status());
    const std::vector<std::pair<int32_t, int32_t>> replay_rows{{0, 100}, {1, 999}};
    EXPECT_EQ(replay_rows, read_visible_rows(flip_version));
    // The replay retires the file it read.
    StorageEngine::instance()->wait_storage_cleanup_tasks();
    EXPECT_FALSE(fs::path_exist(_tablet_mgr->txn_vlog_group_location(tablet_id, log_version)));

    // A subsequent upsert must replace the reinserted row rather than expose duplicate live keys.
    const auto next_txn = next_id();
    write_pk(tablet_id, next_txn, {1}, {1000});
    ASSERT_OK(publish_single_version(tablet_id, flip_version + 1, next_txn).status());
    const std::vector<std::pair<int32_t, int32_t>> next_rows{{0, 100}, {1, 1000}};
    EXPECT_EQ(next_rows, read_visible_rows(flip_version + 1));

    // Discard caches, then force an index rebuild before another upsert. Replaying persisted del
    // files must agree with the original publish about which statement's row wins.
    ASSERT_TRUE(_primary_key_mgr->try_remove_primary_index_cache(tablet_id));
    _tablet_mgr->metacache()->prune();
    const auto rebuild_txn = next_id();
    write_pk(tablet_id, rebuild_txn, {1}, {1001});
    ASSERT_OK(publish_single_version(tablet_id, flip_version + 2, rebuild_txn, /*rebuild_pindex=*/true).status());
    const std::vector<std::pair<int32_t, int32_t>> rebuilt_rows{{0, 100}, {1, 1001}};
    EXPECT_EQ(rebuilt_rows, read_visible_rows(flip_version + 2));
}

TEST_P(ShadowRewriteMultiStatementPkTest, FullInsertThenInsertPreservesLastStatement) {
    ConfigResetGuard group_write_guard(&config::lake_enable_txn_vlog_group, true);
    ConfigResetGuard preserve_order_guard(&config::lake_enable_pk_preserve_txn_delete_order, GetParam());
    const auto tablet_id = _tablet_metadata->id();
    const int64_t watershed = 2;
    const auto rewrite_txn_id = next_id();
    write_pk(tablet_id, rewrite_txn_id, {0, 1}, {100, 101});

    const auto txn_id = next_id();
    const auto first_load = make_load_id(3);
    const auto second_load = make_load_id(4);
    write_statement(txn_id, first_load, false, 200);
    write_statement(txn_id, second_load, false, 999);
    auto info = TEST_txn_info(txn_id, time(nullptr));
    info.add_load_ids()->CopyFrom(first_load);
    info.add_load_ids()->CopyFrom(second_load);
    const int64_t log_version = watershed + 1;
    ASSERT_OK(publish_log_version(_tablet_mgr.get(), tablet_id, std::span<const TxnInfoPB>(&info, 1), &log_version));

    config::lake_enable_txn_vlog_group = false;

    const int64_t flip_version = watershed + 2;
    auto flip_info = TEST_shadow_rewrite_txn_info(rewrite_txn_id, watershed, time(nullptr));
    ASSERT_OK(publish_version(_tablet_mgr.get(), PublishTabletInfo(tablet_id), 1, flip_version,
                              std::span<const TxnInfoPB>(&flip_info, 1), false, std::nullopt)
                      .status());
    const std::vector<std::pair<int32_t, int32_t>> replay_rows{{0, 100}, {1, 999}};
    EXPECT_EQ(replay_rows, read_visible_rows(flip_version));

    ASSERT_TRUE(_primary_key_mgr->try_remove_primary_index_cache(tablet_id));
    _tablet_mgr->metacache()->prune();
    const auto rebuild_txn = next_id();
    write_pk(tablet_id, rebuild_txn, {1}, {1000});
    ASSERT_OK(publish_single_version(tablet_id, flip_version + 1, rebuild_txn, /*rebuild_pindex=*/true).status());
    const std::vector<std::pair<int32_t, int32_t>> rebuilt_rows{{0, 100}, {1, 1000}};
    EXPECT_EQ(rebuilt_rows, read_visible_rows(flip_version + 1));
}

// A statement group replays through the batch applier, which loads the primary index first. An inconsistency
// found by that load must recover and load again, as a merged .vlog on the single-log path did; otherwise the
// flip publish fails the same way on every retry.
TEST_P(ShadowRewriteMultiStatementPkTest, GroupReplayRecoversFromIndexLoadFailure) {
    ConfigResetGuard group_write_guard(&config::lake_enable_txn_vlog_group, true);
    ConfigResetGuard preserve_order_guard(&config::lake_enable_pk_preserve_txn_delete_order, GetParam());
    ConfigResetGuard recover_guard(&config::enable_primary_key_recover, true);
    const auto tablet_id = _tablet_metadata->id();
    const int64_t watershed = 2;
    const auto rewrite_txn_id = next_id();
    write_pk(tablet_id, rewrite_txn_id, {0, 1}, {100, 101});

    const auto txn_id = next_id();
    const auto delete_load = make_load_id(5);
    const auto insert_load = make_load_id(6);
    write_statement(txn_id, delete_load, true, 0);
    write_statement(txn_id, insert_load, false, 999);
    auto info = TEST_txn_info(txn_id, time(nullptr));
    info.add_load_ids()->CopyFrom(delete_load);
    info.add_load_ids()->CopyFrom(insert_load);
    const int64_t log_version = watershed + 1;
    ASSERT_OK(publish_log_version(_tablet_mgr.get(), tablet_id, std::span<const TxnInfoPB>(&info, 1), &log_version));
    ASSIGN_OR_ABORT(auto group, _tablet_mgr->get_txn_vlog_group(tablet_id, log_version));
    ASSERT_EQ(2, group.size());

    // Fail the first index load the way a duplicate key does.
    int index_loads = 0;
    SyncPoint::GetInstance()->SetCallBack("lake_index_load.1", [&](void* arg) {
        if (index_loads++ == 0) {
            *(Status*)arg = Status::AlreadyExist("ut_test");
        }
    });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp clear_sync_point([]() {
        SyncPoint::GetInstance()->ClearCallBack("lake_index_load.1");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    const int64_t flip_version = watershed + 2;
    auto flip_info = TEST_shadow_rewrite_txn_info(rewrite_txn_id, watershed, time(nullptr));
    ASSERT_OK(publish_version(_tablet_mgr.get(), PublishTabletInfo(tablet_id), 1, flip_version,
                              std::span<const TxnInfoPB>(&flip_info, 1), false, std::nullopt)
                      .status());
    // The failed load and the one after the recover.
    EXPECT_GE(index_loads, 2);
    const std::vector<std::pair<int32_t, int32_t>> replay_rows{{0, 100}, {1, 999}};
    EXPECT_EQ(replay_rows, read_visible_rows(flip_version));
}

INSTANTIATE_TEST_SUITE_P(DeleteOrderModes, ShadowRewriteMultiStatementPkTest, testing::Bool());

class ShadowRewriteBundledMultiStatementPkTest : public ShadowRewriteMultiStatementPkTest {
public:
    ShadowRewriteBundledMultiStatementPkTest() : ShadowRewriteMultiStatementPkTest(kBundledTestDir) {
        // A third column makes the first statement a nonempty partial update: c2 must be filled
        // from the historical row, so publish must physically rewrite that statement's segment.
        auto* schema = _tablet_metadata->mutable_schema();
        schema->set_id(next_id());
        auto* c2 = schema->add_column();
        c2->set_unique_id(next_id());
        c2->set_name("c2");
        c2->set_type("INT");
        c2->set_is_key(false);
        c2->set_is_nullable(false);
        c2->set_aggregation("REPLACE");
        _tablet_schema = TabletSchema::create(*schema);
        _schema = std::make_shared<VSchema>(ChunkHelper::convert_schema(_tablet_schema));
        _partial_slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
        _partial_slots.emplace_back(1, "c1", TypeDescriptor{LogicalType::TYPE_INT});
        for (auto& slot : _partial_slots) _partial_slot_pointers.push_back(&slot);
    }

    void SetUp() override {
        ShadowRewriteMultiStatementPkTest::SetUp();
        CHECK_OK(_tablet_mgr->create_schema_file(_tablet_metadata->id(), _tablet_metadata->schema()));
        auto other = std::make_shared<TabletMetadata>(*_tablet_metadata);
        other->set_id(next_id());
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*other));
        CHECK_OK(_tablet_mgr->create_schema_file(other->id(), other->schema()));
        _tablets = {_tablet_metadata->id(), other->id()};
    }

protected:
    constexpr static const char* const kBundledTestDir = "test_lake_shadow_rewrite_publish_pk_bundled";

    std::unique_ptr<DeltaWriter> new_writer(int64_t tablet_id, int64_t txn_id, const PUniqueId* load_id, bool partial,
                                            BundleWritableFileContext* bundle) {
        DeltaWriterBuilder builder;
        builder.set_tablet_manager(_tablet_mgr.get())
                .set_tablet_id(tablet_id)
                .set_txn_id(txn_id)
                .set_partition_id(_partition_id)
                .set_mem_tracker(_mem_tracker.get())
                .set_schema_id(_tablet_schema->id())
                .set_profile(&_profile)
                .set_bundle_writable_file_context(bundle);
        if (partial) {
            builder.set_slot_descriptors(&_partial_slot_pointers).set_partial_update_mode(PartialUpdateMode::ROW_MODE);
        }
        if (load_id != nullptr) builder.set_load_id(*load_id).set_is_multi_statements_txn(true);
        ASSIGN_OR_ABORT(auto writer, builder.build());
        CHECK_OK(writer->open());
        return writer;
    }

    void write_row(DeltaWriter* writer, int32_t key, int32_t value, int32_t preserved, bool partial) {
        auto c0 = ColumnTestHelper::build_column<int32_t>({key});
        auto c1 = ColumnTestHelper::build_column<int32_t>({value});
        Columns columns{std::move(c0), std::move(c1)};
        Chunk::SlotHashMap slots{{0, 0}, {1, 1}};
        if (!partial) {
            columns.push_back(ColumnTestHelper::build_column<int32_t>({preserved}));
            slots[2] = 2;
        }
        Chunk chunk(std::move(columns), slots);
        const uint32_t index = 0;
        ASSERT_OK(writer->write(chunk, &index, 1));
    }

    void write_bundled_statement(int64_t txn_id, const PUniqueId& load_id, bool partial) {
        BundleWritableFileContext bundle;
        std::vector<std::unique_ptr<DeltaWriter>> writers;
        // Keep both writers open until they finish, just as the partition's load channel does.
        for (int64_t tablet_id : _tablets) {
            writers.push_back(new_writer(tablet_id, txn_id, &load_id, partial, &bundle));
        }
        for (size_t i = 0; i < _tablets.size(); ++i) {
            const int32_t key = static_cast<int32_t>(1000 * i) + (partial ? 1 : 2);
            ASSERT_NO_FATAL_FAILURE(write_row(writers[i].get(), key, partial ? 200 : 300, 301, partial));
            ASSERT_OK(writers[i]->finish_with_txnlog());
            writers[i]->close();
        }
    }

    std::vector<std::vector<int32_t>> read_all_columns(int64_t tablet_id, int64_t version) {
        _tablet_mgr->metacache()->prune();
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));
        std::vector<std::vector<int32_t>> rows;
        while (true) {
            auto chunk = ChunkFactory::new_chunk(*_schema, 64);
            auto st = reader->get_next(chunk.get());
            if (st.is_end_of_file()) break;
            CHECK_OK(st);
            for (size_t i = 0; i < chunk->num_rows(); ++i) {
                rows.push_back(
                        {chunk->get(i)[0].get_int32(), chunk->get(i)[1].get_int32(), chunk->get(i)[2].get_int32()});
            }
        }
        std::sort(rows.begin(), rows.end());
        return rows;
    }

    std::vector<SlotDescriptor> _partial_slots;
    std::vector<SlotDescriptor*> _partial_slot_pointers;
    std::vector<int64_t> _tablets;
};

TEST_P(ShadowRewriteBundledMultiStatementPkTest, PartialThenFullKeepsBundleSliceThroughReplayAndRebuild) {
    ConfigResetGuard group_write_guard(&config::lake_enable_txn_vlog_group, true);
    ConfigResetGuard preserve_order_guard(&config::lake_enable_pk_preserve_txn_delete_order, GetParam());
    const int64_t watershed = 2;
    const int64_t flip_version = watershed + 2;
    const int64_t rewrite_txn = next_id();
    for (size_t i = 0; i < _tablets.size(); ++i) {
        auto writer = new_writer(_tablets[i], rewrite_txn, nullptr, false, nullptr);
        ASSERT_NO_FATAL_FAILURE(write_row(writer.get(), static_cast<int32_t>(1000 * i) + 1, 100, 101, false));
        ASSERT_OK(writer->finish_with_txnlog());
        writer->close();
    }

    const int64_t txn_id = next_id();
    const auto partial_load = make_load_id(7);
    const auto full_load = make_load_id(8);
    ASSERT_NO_FATAL_FAILURE(write_bundled_statement(txn_id, partial_load, true));
    ASSERT_NO_FATAL_FAILURE(write_bundled_statement(txn_id, full_load, false));
    auto info = TEST_txn_info(txn_id, time(nullptr));
    info.add_load_ids()->CopyFrom(partial_load);
    info.add_load_ids()->CopyFrom(full_load);
    const int64_t log_version = watershed + 1;

    std::vector<SegmentMetadataPB> full_segments;
    std::vector<std::string> partial_filenames;
    for (int64_t tablet_id : _tablets) {
        ASSIGN_OR_ABORT(auto partial, _tablet_mgr->get_txn_log(tablet_id, txn_id, partial_load));
        ASSERT_TRUE(partial->op_write().has_txn_meta());
        ASSERT_EQ(1, partial->op_write().rowset().segment_metas_size());
        ASSERT_EQ(1, partial->op_write().rowset().segment_metas(0).num_rows());
        ASSERT_EQ(1, partial->op_write().rewrite_segments_meta_size());
        partial_filenames.push_back(partial->op_write().rowset().segment_metas(0).filename());
        ASSIGN_OR_ABORT(auto full, _tablet_mgr->get_txn_log(tablet_id, txn_id, full_load));
        ASSERT_EQ(1, full->op_write().rowset().segment_metas_size());
        full_segments.push_back(full->op_write().rowset().segment_metas(0));
        ASSERT_TRUE(full_segments.back().has_bundle_file_offset());
        ASSERT_OK(
                publish_log_version(_tablet_mgr.get(), tablet_id, std::span<const TxnInfoPB>(&info, 1), &log_version));
        ASSIGN_OR_ABORT(auto group, _tablet_mgr->get_txn_vlog_group(tablet_id, log_version));
        ASSERT_EQ(2, group.size());
        ASSERT_TRUE(fs::path_exist(_tablet_mgr->txn_vlog_group_location(tablet_id, log_version)));
    }
    // Different tablet keys make a lost slice offset observable. These are real slices of the same file.
    ASSERT_EQ(full_segments[0].filename(), full_segments[1].filename());
    ASSERT_GT(full_segments[1].bundle_file_offset(), 0);
    config::lake_enable_txn_vlog_group = false;

    for (size_t i = 0; i < _tablets.size(); ++i) {
        const int64_t tablet_id = _tablets[i];
        const int32_t key = static_cast<int32_t>(1000 * i);
        auto flip_info = TEST_shadow_rewrite_txn_info(rewrite_txn, watershed, time(nullptr));
        ASSIGN_OR_ABORT(auto metadata, publish_version(_tablet_mgr.get(), PublishTabletInfo(tablet_id), 1, flip_version,
                                                       std::span<const TxnInfoPB>(&flip_info, 1), false, std::nullopt));
        ASSERT_EQ(2, metadata->rowsets_size());
        const auto& replayed = metadata->rowsets(1);
        ASSERT_EQ(2, replayed.segment_metas_size());
        const auto& rewritten = replayed.segment_metas(0);
        EXPECT_NE(partial_filenames[i], rewritten.filename()) << "the partial segment must actually be rewritten";
        ASSERT_TRUE(rewritten.has_bundle_file_offset());
        EXPECT_EQ(0, rewritten.bundle_file_offset());
        const auto& untouched = replayed.segment_metas(1);
        EXPECT_EQ(full_segments[i].filename(), untouched.filename());
        ASSERT_TRUE(untouched.has_bundle_file_offset());
        EXPECT_EQ(full_segments[i].bundle_file_offset(), untouched.bundle_file_offset());
        const std::vector<std::vector<int32_t>> expected{{key + 1, 200, 101}, {key + 2, 300, 301}};
        EXPECT_EQ(expected, read_all_columns(tablet_id, flip_version));

        // Rebuild from persisted files, then overwrite both keys. A wrong rssid or slice would leave
        // duplicate live keys or read another tablet's row; the vector assertion retains duplicates.
        ASSERT_TRUE(_primary_key_mgr->try_remove_primary_index_cache(tablet_id));
        _tablet_mgr->metacache()->prune();
        const int64_t next_txn = next_id();
        auto writer = new_writer(tablet_id, next_txn, nullptr, false, nullptr);
        ASSERT_NO_FATAL_FAILURE(write_row(writer.get(), key + 1, 400, 401, false));
        ASSERT_NO_FATAL_FAILURE(write_row(writer.get(), key + 2, 500, 501, false));
        ASSERT_OK(writer->finish_with_txnlog());
        writer->close();
        ASSERT_OK(publish_single_version(tablet_id, flip_version + 1, next_txn, true).status());
        const std::vector<std::vector<int32_t>> rebuilt{{key + 1, 400, 401}, {key + 2, 500, 501}};
        EXPECT_EQ(rebuilt, read_all_columns(tablet_id, flip_version + 1));
    }
}

INSTANTIATE_TEST_SUITE_P(DeleteOrderModes, ShadowRewriteBundledMultiStatementPkTest, testing::Bool());

} // namespace starrocks::lake
