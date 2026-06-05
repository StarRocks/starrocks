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

// Integration tests for the Sparse Delta Column Group (SDCG) lake/shared-data write+read path.
// All behavior is gated by config::enable_sparse_dcg (default false). With the flag off the
// column-mode partial update path is byte-identical to today (always dense `.cols`).
//
// These tests use a LARGE base segment (M rows) and a SMALL partial-update batch (K rows) so the
// density decision K/M < sdcg_dense_threshold takes the sparse `.spcols` path. The small-base
// LakePartialUpdateTest cases always have K==M (dense) and therefore never exercise SDCG.

#include <gtest/gtest.h>

#include <vector>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "common/config_primary_key_fwd.h"
#include "storage/chunk_helper.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/filenames.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/test_util.h"
#include "storage/tablet_schema.h"

namespace starrocks::lake {

// Base table: c0 (INT key), c1 (INT value), c2 (INT value, default 10). Same shape as
// LakePartialUpdateTestBase, but driven with a configurable base row count.
class LakeSparseDcgTest : public TestBase {
public:
    LakeSparseDcgTest() : TestBase(kTestDirectory) {
        _tablet_metadata = std::make_shared<TabletMetadata>();
        _tablet_metadata->set_id(next_id());
        _tablet_metadata->set_version(1);
        _tablet_metadata->set_next_rowset_id(1);
        auto schema = _tablet_metadata->mutable_schema();
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        schema->set_keys_type(PRIMARY_KEYS);
        schema->set_num_rows_per_row_block(65535);
        auto c0 = schema->add_column();
        {
            c0->set_unique_id(next_id());
            c0->set_name("c0");
            c0->set_type("INT");
            c0->set_is_key(true);
            c0->set_is_nullable(false);
        }
        auto c1 = schema->add_column();
        {
            c1->set_unique_id(next_id());
            c1->set_name("c1");
            c1->set_type("INT");
            c1->set_is_key(false);
            c1->set_is_nullable(true);
            c1->set_aggregation("REPLACE");
        }
        auto c2 = schema->add_column();
        {
            c2->set_unique_id(next_id());
            c2->set_name("c2");
            c2->set_type("INT");
            c2->set_is_key(false);
            c2->set_is_nullable(true);
            c2->set_aggregation("REPLACE");
            c2->set_default_value("10");
        }

        // Partial-update slots: c0 (key) + c1 (the single updated value column).
        _slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
        _slots.emplace_back(1, "c1", TypeDescriptor{LogicalType::TYPE_INT});
        _slot_pointers.emplace_back(&_slots[0]);
        _slot_pointers.emplace_back(&_slots[1]);

        _slot_cid_map.emplace(0, 0);
        _slot_cid_map.emplace(1, 1);
        _slot_cid_map.emplace(2, 2);

        _tablet_schema = TabletSchema::create(*schema);
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
        CHECK_OK(_tablet_mgr->create_schema_file(_tablet_metadata->id(), _tablet_metadata->schema()));
    }

    void TearDown() override { remove_test_dir_or_die(); }

    // Full-row chunk for keys [0, n): c0=i, c1=i*3, c2=i*4.
    Chunk full_chunk(int n) {
        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        auto c2 = Int32Column::create();
        for (int i = 0; i < n; ++i) {
            c0->append(i);
            c1->append(i * 3);
            c2->append(i * 4);
        }
        return Chunk({std::move(c0), std::move(c1), std::move(c2)}, _slot_cid_map);
    }

    // Column-mode partial chunk touching the given keys: (c0=key, c1=new_c1_value(key)).
    Chunk partial_chunk(const std::vector<int>& keys, const std::function<int(int)>& new_c1) {
        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        for (int k : keys) {
            c0->append(k);
            c1->append(new_c1(k));
        }
        return Chunk({std::move(c0), std::move(c1)}, _slot_cid_map);
    }

    // Write a full base segment of `n` rows, returns the new version.
    int64_t write_base(int n, int64_t* version) {
        auto chunk = full_chunk(n);
        std::vector<uint32_t> indexes(n);
        for (int i = 0; i < n; ++i) indexes[i] = i;
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(_tablet_metadata->id())
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        CHECK_OK(delta_writer->open());
        CHECK_OK(delta_writer->write(chunk, indexes.data(), indexes.size()));
        CHECK_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        CHECK_OK(publish_single_version(_tablet_metadata->id(), *version + 1, txn_id).status());
        ++(*version);
        return txn_id;
    }

    // Column-mode partial update of the given keys' c1 value; returns the txn id.
    int64_t column_update(const std::vector<int>& keys, const std::function<int(int)>& new_c1, int64_t* version) {
        auto chunk = partial_chunk(keys, new_c1);
        std::vector<uint32_t> indexes(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) indexes[i] = static_cast<uint32_t>(i);
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(_tablet_metadata->id())
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(PartialUpdateMode::COLUMN_UPDATE_MODE)
                                                   .build());
        CHECK_OK(delta_writer->open());
        CHECK_OK(delta_writer->write(chunk, indexes.data(), indexes.size()));
        CHECK_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        CHECK_OK(publish_single_version(_tablet_metadata->id(), *version + 1, txn_id).status());
        ++(*version);
        return txn_id;
    }

    // Read the whole table at `version` into per-key (c1,c2) maps. Returns row count.
    int read_table(int64_t version, std::map<int, int>* c1_by_key, std::map<int, int>* c2_by_key) {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));
        auto chunk = ChunkFactory::new_chunk(*_schema, 256);
        int rows = 0;
        while (true) {
            auto st = reader->get_next(chunk.get());
            if (st.is_end_of_file()) break;
            CHECK_OK(st);
            auto cols = chunk->columns();
            for (int i = 0; i < chunk->num_rows(); ++i) {
                int k = cols[0]->get(i).get_int32();
                (*c1_by_key)[k] = cols[1]->get(i).get_int32();
                (*c2_by_key)[k] = cols[2]->get(i).get_int32();
            }
            rows += chunk->num_rows();
            chunk->reset();
        }
        return rows;
    }

    // Count, across all DCG entries of all rssids in `metadata`, the files of each kind, and the
    // total recorded sparse K. Returns {dense_count, sparse_count, total_sparse_K}.
    struct DcgFileStats {
        int dense = 0;
        int sparse = 0;
        int64_t total_sparse_k = 0;
        int64_t max_source_num_rows = 0;
        bool any_spcols_filename = false;
    };
    DcgFileStats collect_dcg_stats(const TabletMetadataPtr& metadata) {
        DcgFileStats s;
        for (const auto& [rssid, dcg_ver] : metadata->dcg_meta().dcgs()) {
            for (int i = 0; i < dcg_ver.column_files_size(); ++i) {
                const auto kind = i < dcg_ver.file_kinds_size() ? dcg_ver.file_kinds(i) : DENSE_COLS;
                if (kind == SPARSE_PERCOL) {
                    ++s.sparse;
                    if (i < dcg_ver.sparse_row_counts_size()) s.total_sparse_k += dcg_ver.sparse_row_counts(i);
                    EXPECT_TRUE(is_spcols(dcg_ver.column_files(i)))
                            << "SPARSE entry must reference a .spcols file: " << dcg_ver.column_files(i);
                    s.any_spcols_filename = true;
                } else {
                    ++s.dense;
                }
            }
            if (dcg_ver.has_source_segment_num_rows()) {
                s.max_source_num_rows = std::max(s.max_source_num_rows, dcg_ver.source_segment_num_rows());
            }
        }
        return s;
    }

protected:
    constexpr static const char* const kTestDirectory = "test_lake_sparse_dcg";

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    int64_t _partition_id = 4562;
    std::vector<SlotDescriptor> _slots;
    std::vector<SlotDescriptor*> _slot_pointers;
    Chunk::SlotHashMap _slot_cid_map;
};

// (a) Sparse write+read roundtrip: small K over a larger base -> .spcols produced (assert file
// kind / K in meta) -> read back full table, assert updated + untouched values.
TEST_F(LakeSparseDcgTest, test_sparse_write_read_roundtrip) {
    ConfigResetGuard<bool> g_enable(&config::enable_sparse_dcg, true);

    constexpr int M = 2000;
    int64_t version = 1;
    write_base(M, &version);

    // Update c1 of 10 keys to key*1000 (K=10, M=2000 => K/M=0.005 < 0.3 -> sparse).
    std::vector<int> keys = {5, 17, 42, 100, 333, 777, 999, 1234, 1500, 1999};
    column_update(
            keys, [](int k) { return k * 1000; }, &version);

    ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
    auto stats = collect_dcg_stats(metadata);
    EXPECT_EQ(1, stats.sparse) << "expected exactly one .spcols overlay file";
    EXPECT_EQ(0, stats.dense) << "no dense .cols file should be produced for a sparse update";
    EXPECT_EQ(static_cast<int64_t>(keys.size()), stats.total_sparse_k);
    EXPECT_EQ(M, stats.max_source_num_rows);
    EXPECT_TRUE(stats.any_spcols_filename);

    std::map<int, int> c1, c2;
    int rows = read_table(version, &c1, &c2);
    EXPECT_EQ(M, rows);
    std::set<int> updated(keys.begin(), keys.end());
    for (int k = 0; k < M; ++k) {
        ASSERT_TRUE(c1.count(k)) << "missing key " << k;
        if (updated.count(k)) {
            EXPECT_EQ(k * 1000, c1[k]) << "updated key " << k;
        } else {
            EXPECT_EQ(k * 3, c1[k]) << "untouched key " << k;
        }
        // c2 is never updated.
        EXPECT_EQ(k * 4, c2[k]) << "c2 key " << k;
    }
}

// (b) Multi-version same row: 3 sparse updates of one row -> newest wins (last-write-wins via
// version-ascending overlay apply).
TEST_F(LakeSparseDcgTest, test_multi_version_same_row_newest_wins) {
    ConfigResetGuard<bool> g_enable(&config::enable_sparse_dcg, true);

    constexpr int M = 1000;
    int64_t version = 1;
    write_base(M, &version);

    const int target = 500;
    column_update(
            {target}, [](int) { return 111; }, &version);
    column_update(
            {target}, [](int) { return 222; }, &version);
    column_update(
            {target}, [](int) { return 333; }, &version);

    ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
    auto stats = collect_dcg_stats(metadata);
    // Three independent sparse overlays of the same column are CHAINED (not stripped), so all
    // three .spcols files survive in the DCG entry list.
    EXPECT_EQ(3, stats.sparse);
    EXPECT_EQ(0, stats.dense);
    EXPECT_EQ(3, stats.total_sparse_k); // K=1 each

    std::map<int, int> c1, c2;
    int rows = read_table(version, &c1, &c2);
    EXPECT_EQ(M, rows);
    EXPECT_EQ(333, c1[target]) << "newest sparse overlay must win";
    // Spot check a neighbor is untouched.
    EXPECT_EQ(499 * 3, c1[499]);
    EXPECT_EQ(501 * 3, c1[501]);
}

// (c) Sparse then dense same column -> dense supersedes, older sparse orphaned in meta.
// Force the second update to take the DENSE path by lowering the density threshold so the second
// (larger) batch's K/M exceeds it.
TEST_F(LakeSparseDcgTest, test_sparse_then_dense_supersedes_and_orphans) {
    ConfigResetGuard<bool> g_enable(&config::enable_sparse_dcg, true);

    constexpr int M = 1000;
    int64_t version = 1;
    write_base(M, &version);

    // First update: K=5 -> sparse (0.005 < 0.3).
    std::vector<int> sparse_keys = {10, 20, 30, 40, 50};
    column_update(
            sparse_keys, [](int k) { return k * 1000; }, &version);
    {
        ASSIGN_OR_ABORT(auto md, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
        auto s = collect_dcg_stats(md);
        EXPECT_EQ(1, s.sparse);
        EXPECT_EQ(0, s.dense);
    }

    // Second update of the SAME column c1: force dense by setting the threshold to 0 so any K/M
    // ratio (>0) takes the dense path. K=300 here.
    ConfigResetGuard<double> g_thresh(&config::sdcg_dense_threshold, 0.0);
    std::vector<int> dense_keys;
    for (int k = 0; k < 300; ++k) dense_keys.push_back(k);
    int64_t before_orphans;
    {
        ASSIGN_OR_ABORT(auto md, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
        before_orphans = md->orphan_files_size();
    }
    column_update(
            dense_keys, [](int k) { return k * 7; }, &version);

    ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
    auto stats = collect_dcg_stats(metadata);
    // The dense .cols rewrite supersedes c1; the older sparse .spcols is orphaned, leaving a
    // single dense entry for c1 in the live DCG list.
    EXPECT_EQ(0, stats.sparse) << "older sparse overlay must be superseded by the dense rewrite";
    EXPECT_EQ(1, stats.dense);
    EXPECT_GT(metadata->orphan_files_size(), before_orphans) << "old .spcols must be orphaned";

    // Read back: dense-updated keys win, sparse-only keys (10..50, not in dense batch) fall back to
    // the BASE value because the dense rewrite is the new row-complete base (the sparse overlay
    // that previously updated them is now orphaned). This is the documented dense-supersedes rule.
    std::map<int, int> c1, c2;
    int rows = read_table(version, &c1, &c2);
    EXPECT_EQ(M, rows);
    for (int k = 0; k < 300; ++k) {
        EXPECT_EQ(k * 7, c1[k]) << "dense-updated key " << k;
    }
    // keys 10,20,30,40,50 are within [0,300) so they are covered by the dense batch (k*7 wins).
    // Check a sparse-only key would have reverted: pick none outside since all sparse keys < 300.
    EXPECT_EQ(700 * 3, c1[700]) << "untouched key reverts to base";
}

// (d) Flag off -> no .spcols ever (kinds empty), behavior == dense.
TEST_F(LakeSparseDcgTest, test_flag_off_is_dense) {
    // enable_sparse_dcg stays false (default).
    ASSERT_FALSE(config::enable_sparse_dcg);

    constexpr int M = 2000;
    int64_t version = 1;
    write_base(M, &version);

    std::vector<int> keys = {5, 17, 42, 100, 333, 777, 999, 1234, 1500, 1999};
    column_update(
            keys, [](int k) { return k * 1000; }, &version);

    ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
    auto stats = collect_dcg_stats(metadata);
    EXPECT_EQ(0, stats.sparse) << "flag off must never produce a .spcols file";
    EXPECT_EQ(1, stats.dense) << "flag off => dense .cols path";
    EXPECT_FALSE(stats.any_spcols_filename);
    // Dense-only meta must carry NO SDCG arrays (byte-identical to pre-SDCG): file_kinds absent.
    for (const auto& [rssid, dcg_ver] : metadata->dcg_meta().dcgs()) {
        EXPECT_EQ(0, dcg_ver.file_kinds_size()) << "dense-only meta must not emit file_kinds";
        EXPECT_EQ(0, dcg_ver.sparse_row_counts_size()) << "dense-only meta must not emit sparse_row_counts";
        EXPECT_FALSE(dcg_ver.has_source_segment_num_rows()) << "dense-only meta must not record source rows";
    }

    // Read back: the dense path produces the same logical result.
    std::map<int, int> c1, c2;
    int rows = read_table(version, &c1, &c2);
    EXPECT_EQ(M, rows);
    std::set<int> updated(keys.begin(), keys.end());
    for (int k = 0; k < M; ++k) {
        if (updated.count(k)) {
            EXPECT_EQ(k * 1000, c1[k]);
        } else {
            EXPECT_EQ(k * 3, c1[k]);
        }
        EXPECT_EQ(k * 4, c2[k]);
    }
}

} // namespace starrocks::lake
