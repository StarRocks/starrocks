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

#include <algorithm>
#include <functional>
#include <map>
#include <set>
#include <vector>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
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

    // Column-mode partial chunk where keys[i] is set to NULL when is_null(keys[i]) is true; otherwise
    // to new_c1(keys[i]). c1 is a nullable column, so a NULL update must read back as NULL.
    Chunk partial_chunk_nullable(const std::vector<int>& keys, const std::function<bool(int)>& is_null,
                                 const std::function<int(int)>& new_c1) {
        auto c0 = Int32Column::create();
        auto c1_data = Int32Column::create();
        auto c1_null = NullColumn::create();
        for (int k : keys) {
            c0->append(k);
            if (is_null(k)) {
                c1_data->append(0);
                c1_null->append(1);
            } else {
                c1_data->append(new_c1(k));
                c1_null->append(0);
            }
        }
        auto c1 = NullableColumn::create(std::move(c1_data), std::move(c1_null));
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

    // Column-mode partial update from a prebuilt (c0,c1) chunk; writes every physical row in chunk
    // order (so duplicate keys within one chunk exercise the last-write-wins path). Returns txn id.
    int64_t column_update_chunk(Chunk& chunk, int64_t* version) {
        const size_t n = chunk.num_rows();
        std::vector<uint32_t> indexes(n);
        for (size_t i = 0; i < n; ++i) indexes[i] = static_cast<uint32_t>(i);
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

    // Column-mode partial update of the given keys' c1 value; returns the txn id.
    int64_t column_update(const std::vector<int>& keys, const std::function<int(int)>& new_c1, int64_t* version) {
        auto chunk = partial_chunk(keys, new_c1);
        return column_update_chunk(chunk, version);
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

    // Like read_table but also records, for c1, which keys read back as SQL NULL. A key present in
    // `c1_null_keys` had a NULL c1; the matching `c1_by_key` entry is left at its raw datum value
    // (don't trust it when the key is in the null set).
    int read_table_nullable(int64_t version, std::map<int, int>* c1_by_key, std::set<int>* c1_null_keys) {
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
                auto datum = cols[1]->get(i);
                if (datum.is_null()) {
                    c1_null_keys->insert(k);
                    (*c1_by_key)[k] = INT32_MIN; // sentinel: must not be read as a value
                } else {
                    (*c1_by_key)[k] = datum.get_int32();
                }
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
        // Presence cross-checks (only meaningful when sparse > 0): every SPARSE entry must carry a
        // 1:1 SparsePresencePB whose row_count == its sparse_row_count and whose [min,max] is a
        // valid, ordered base-segment ordinal range. DENSE entries must carry an empty (unknown)
        // presence so the array stays 1:1 with column_files.
        bool all_sparse_presences_known = true;
        bool all_dense_presences_unknown = true;
        bool presence_row_count_matches_k = true;
        bool presence_range_ordered_and_in_bounds = true;
    };
    DcgFileStats collect_dcg_stats(const TabletMetadataPtr& metadata) {
        DcgFileStats s;
        for (const auto& [rssid, dcg_ver] : metadata->dcg_meta().dcgs()) {
            const int64_t m = dcg_ver.has_source_segment_num_rows() ? dcg_ver.source_segment_num_rows() : 0;
            for (int i = 0; i < dcg_ver.column_files_size(); ++i) {
                const auto kind = i < dcg_ver.file_kinds_size() ? dcg_ver.file_kinds(i) : DENSE_COLS;
                const bool has_presence = i < dcg_ver.presences_size();
                const auto& p = has_presence ? dcg_ver.presences(i) : SparsePresencePB();
                if (kind == SPARSE_PERCOL) {
                    ++s.sparse;
                    const int64_t k = i < dcg_ver.sparse_row_counts_size() ? dcg_ver.sparse_row_counts(i) : 0;
                    s.total_sparse_k += k;
                    EXPECT_TRUE(is_spcols(dcg_ver.column_files(i)))
                            << "SPARSE entry must reference a .spcols file: " << dcg_ver.column_files(i);
                    s.any_spcols_filename = true;
                    // A sparse entry's presence must be fully known.
                    if (!(has_presence && p.has_min_source_rowid() && p.has_max_source_rowid() && p.has_row_count())) {
                        s.all_sparse_presences_known = false;
                    } else {
                        if (p.row_count() != k) s.presence_row_count_matches_k = false;
                        if (p.min_source_rowid() > p.max_source_rowid()) {
                            s.presence_range_ordered_and_in_bounds = false;
                        }
                        if (p.min_source_rowid() < 0 || (m > 0 && p.max_source_rowid() >= m)) {
                            s.presence_range_ordered_and_in_bounds = false;
                        }
                    }
                } else {
                    ++s.dense;
                    // A dense entry, when a presences array is emitted at all, must be padded-empty.
                    if (has_presence && (p.has_min_source_rowid() || p.has_max_source_rowid() || p.has_row_count())) {
                        s.all_dense_presences_unknown = false;
                    }
                }
            }
            if (dcg_ver.has_source_segment_num_rows()) {
                s.max_source_num_rows = std::max(s.max_source_num_rows, dcg_ver.source_segment_num_rows());
            }
            // When a presences array is emitted it must be strictly 1:1 with column_files.
            if (dcg_ver.presences_size() > 0) {
                EXPECT_EQ(dcg_ver.column_files_size(), dcg_ver.presences_size())
                        << "presences must be 1:1 with column_files when emitted";
            }
        }
        return s;
    }

    // Number of orphaned files recorded in the metadata (grows when a dense rewrite supersedes a
    // sparse chain during in-place promotion).
    int64_t orphan_count(const TabletMetadataPtr& metadata) { return metadata->orphan_files_size(); }

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
    // Explicitly force the flag off (other tests may have mutated the global config;
    // never depend on the default still being in effect).
    ConfigResetGuard<bool> flag_guard(&config::enable_sparse_dcg, false);

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
        EXPECT_EQ(0, dcg_ver.presences_size()) << "dense-only meta must not emit presences";
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

// (e) Presence summary is emitted on the sparse write and loads back 1:1: the single .spcols entry
// carries a SparsePresencePB whose [min,max] equals the min/max touched base rowid and whose
// row_count equals K. collect_dcg_stats cross-checks the invariants; here we pin the exact bounds.
TEST_F(LakeSparseDcgTest, test_presence_emitted_and_bounds_exact) {
    ConfigResetGuard<bool> g_enable(&config::enable_sparse_dcg, true);

    constexpr int M = 2000;
    int64_t version = 1;
    write_base(M, &version);

    // Touched keys == base rowids (key i lives at ordinal i in the single base segment). min=7,
    // max=1900, K=6.
    std::vector<int> keys = {7, 42, 500, 999, 1234, 1900};
    column_update(
            keys, [](int k) { return k * 1000; }, &version);

    ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
    auto stats = collect_dcg_stats(metadata);
    EXPECT_EQ(1, stats.sparse);
    EXPECT_EQ(0, stats.dense);
    EXPECT_TRUE(stats.all_sparse_presences_known) << "the sparse entry must carry a fully-known presence";
    EXPECT_TRUE(stats.all_dense_presences_unknown);
    EXPECT_TRUE(stats.presence_row_count_matches_k);
    EXPECT_TRUE(stats.presence_range_ordered_and_in_bounds);

    // Pin the exact emitted bounds.
    bool found = false;
    for (const auto& [rssid, dcg_ver] : metadata->dcg_meta().dcgs()) {
        for (int i = 0; i < dcg_ver.column_files_size(); ++i) {
            const auto kind = i < dcg_ver.file_kinds_size() ? dcg_ver.file_kinds(i) : DENSE_COLS;
            if (kind != SPARSE_PERCOL) continue;
            ASSERT_LT(i, dcg_ver.presences_size());
            const auto& p = dcg_ver.presences(i);
            EXPECT_EQ(7, p.min_source_rowid());
            EXPECT_EQ(1900, p.max_source_rowid());
            EXPECT_EQ(static_cast<int64_t>(keys.size()), p.row_count());
            found = true;
        }
    }
    EXPECT_TRUE(found) << "expected one sparse presence entry";
}

// (f) K exactly at sdcg_sparse_max_rows boundary => dense; K just under => sparse. The density-ratio
// branch is taken out of the picture by giving M a huge value so K/M stays well below
// sdcg_dense_threshold; the only deciding factor is the absolute K < sdcg_sparse_max_rows cap.
TEST_F(LakeSparseDcgTest, test_sparse_max_rows_boundary) {
    ConfigResetGuard<bool> g_enable(&config::enable_sparse_dcg, true);
    // Pin the cap to a small value so the boundary is cheap to reach. cap=64: K=64 => dense, K=63 =>
    // sparse. Keep the density ratio low by making the base far larger than the cap.
    ConfigResetGuard<int64_t> g_cap(&config::sdcg_sparse_max_rows, 64);

    constexpr int M = 4000; // K/M for K<=64 stays << 0.3
    int64_t version = 1;
    write_base(M, &version);

    // K = 63 (< cap) => sparse.
    {
        std::vector<int> keys;
        for (int k = 0; k < 63; ++k) keys.push_back(k);
        column_update(
                keys, [](int k) { return k + 100000; }, &version);
        ASSIGN_OR_ABORT(auto md, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
        auto s = collect_dcg_stats(md);
        EXPECT_EQ(1, s.sparse) << "K=63 < cap=64 must take the sparse path";
        EXPECT_EQ(0, s.dense);
    }

    // K = 64 (== cap) => dense (the cap is exclusive: K < cap required for sparse). Use a disjoint key
    // range so this is a fresh rssid-relative decision.
    {
        std::vector<int> keys;
        for (int k = 1000; k < 1064; ++k) keys.push_back(k);
        column_update(
                keys, [](int k) { return k + 200000; }, &version);
        ASSIGN_OR_ABORT(auto md, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
        auto s = collect_dcg_stats(md);
        // The K=63 sparse overlay from the prior step survives (different columns? no -- same c1, but
        // the K=64 dense rewrite of c1 supersedes ALL prior sparse c1 overlays). So after the dense
        // write there are zero sparse entries and one dense entry for c1.
        EXPECT_EQ(0, s.sparse) << "K=64 == cap must take the dense path (and supersede the prior sparse)";
        EXPECT_EQ(1, s.dense);
    }

    // Final read-back: the K=64 dense rewrite is the new base for c1, so keys 1000..1063 win, keys
    // 0..62 (sparse-only, now orphaned) revert to base, and everything else is base.
    std::map<int, int> c1, c2;
    int rows = read_table(version, &c1, &c2);
    EXPECT_EQ(M, rows);
    for (int k = 1000; k < 1064; ++k) EXPECT_EQ(k + 200000, c1[k]) << "dense-updated key " << k;
    for (int k = 0; k < 63; ++k) EXPECT_EQ(k * 3, c1[k]) << "orphaned sparse key reverts to base " << k;
}

// (g) In-batch duplicate PK: the same key appears twice in one partial-update chunk. The dense path's
// rule is last-write-wins by physical order; the sparse path must match (the .spcols source_rowids
// stay strictly unique -- one local ordinal per base rowid -- and carry the LAST value).
TEST_F(LakeSparseDcgTest, test_in_batch_duplicate_pk_last_wins) {
    ConfigResetGuard<bool> g_enable(&config::enable_sparse_dcg, true);

    constexpr int M = 1000;
    int64_t version = 1;
    write_base(M, &version);

    // Chunk physical rows: (key=300, c1=11), (key=300, c1=22), (key=301, c1=33). Key 300 appears
    // twice; the LAST physical row (c1=22) must win.
    auto c0 = Int32Column::create();
    auto c1 = Int32Column::create();
    c0->append(300);
    c1->append(11);
    c0->append(300);
    c1->append(22);
    c0->append(301);
    c1->append(33);
    Chunk chunk({std::move(c0), std::move(c1)}, _slot_cid_map);
    column_update_chunk(chunk, &version);

    ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
    auto stats = collect_dcg_stats(metadata);
    EXPECT_EQ(1, stats.sparse);
    EXPECT_EQ(0, stats.dense);
    // K counts DISTINCT base rowids: 300 and 301 => K=2, NOT 3. The duplicate collapses to one row.
    EXPECT_EQ(2, stats.total_sparse_k) << ".spcols source_rowids must be strictly unique (dup collapses)";

    std::map<int, int> rc1, rc2;
    int rows = read_table(version, &rc1, &rc2);
    EXPECT_EQ(M, rows);
    EXPECT_EQ(22, rc1[300]) << "in-batch duplicate: last physical row wins";
    EXPECT_EQ(33, rc1[301]);
    EXPECT_EQ(299 * 3, rc1[299]) << "neighbor untouched";
}

// (h) Explicit NULL update via the sparse path: set c1 of some keys to NULL, leave others updated to
// a real value, leave the rest untouched. Read back: NULL keys are SQL NULL, real-value keys win,
// untouched keys keep the base value.
TEST_F(LakeSparseDcgTest, test_sparse_explicit_null_update) {
    ConfigResetGuard<bool> g_enable(&config::enable_sparse_dcg, true);

    constexpr int M = 1000;
    int64_t version = 1;
    write_base(M, &version);

    // keys 10,20 -> NULL ; keys 30,40 -> real value key*1000 ; everything else untouched.
    std::vector<int> keys = {10, 20, 30, 40};
    std::set<int> null_keys = {10, 20};
    auto chunk = partial_chunk_nullable(
            keys, [&](int k) { return null_keys.count(k) > 0; }, [](int k) { return k * 1000; });
    column_update_chunk(chunk, &version);

    ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
    auto stats = collect_dcg_stats(metadata);
    EXPECT_EQ(1, stats.sparse) << "K=4 over M=1000 must be sparse";
    EXPECT_EQ(4, stats.total_sparse_k);

    std::map<int, int> rc1;
    std::set<int> read_null;
    int rows = read_table_nullable(version, &rc1, &read_null);
    EXPECT_EQ(M, rows);
    EXPECT_TRUE(read_null.count(10)) << "key 10 must read back NULL";
    EXPECT_TRUE(read_null.count(20)) << "key 20 must read back NULL";
    EXPECT_FALSE(read_null.count(30)) << "key 30 has a real value";
    EXPECT_EQ(30 * 1000, rc1[30]);
    EXPECT_EQ(40 * 1000, rc1[40]);
    // Untouched neighbor keeps base value and is NOT null.
    EXPECT_FALSE(read_null.count(11));
    EXPECT_EQ(11 * 3, rc1[11]);
}

// (i) Deep chain: 20 single-row sparse updates of the SAME row, ascending values. The newest overlay
// (version-ascending apply) must win, and all 20 .spcols files survive (chained, not stripped). This
// is deeper than the PoC's 3-version case and stays under sdcg_promotion_hard_count (=16... wait, 20
// exceeds the hard cap). To keep all 20 layers sparse (no promotion), raise the hard cap and the
// threshold for this test so the convergence guard never fires.
TEST_F(LakeSparseDcgTest, test_deep_sparse_chain_20_newest_wins) {
    ConfigResetGuard<bool> g_enable(&config::enable_sparse_dcg, true);
    // Disable the convergence guard so all 20 single-row overlays stay sparse: raise the hard cap
    // well above 20 and the threshold above 1.0 (cum_K/M can never reach it).
    ConfigResetGuard<int32_t> g_hard(&config::sdcg_promotion_hard_count, 1000);
    ConfigResetGuard<double> g_thresh(&config::sdcg_promotion_threshold, 2.0);

    constexpr int M = 1000;
    int64_t version = 1;
    write_base(M, &version);

    const int target = 500;
    constexpr int kChain = 20;
    for (int v = 1; v <= kChain; ++v) {
        const int value = 1000 + v; // strictly ascending so the newest is unambiguous
        column_update(
                {target}, [value](int) { return value; }, &version);
    }

    ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
    auto stats = collect_dcg_stats(metadata);
    EXPECT_EQ(kChain, stats.sparse) << "all 20 single-row overlays must survive as a sparse chain";
    EXPECT_EQ(0, stats.dense);
    EXPECT_EQ(kChain, stats.total_sparse_k); // K=1 each

    std::map<int, int> c1, c2;
    int rows = read_table(version, &c1, &c2);
    EXPECT_EQ(M, rows);
    EXPECT_EQ(1000 + kChain, c1[target]) << "newest of 20 sparse overlays must win";
    EXPECT_EQ(499 * 3, c1[499]) << "neighbor untouched";
}

// (j) In-place promotion / convergence by HARD COUNT: write sdcg_promotion_hard_count sparse overlays
// touching the same single row (each stays sparse), then one more. The trigger is
// (chain_len + 1) > sdcg_promotion_hard_count: after `cap` sparse layers exist, chain_len == cap and
// the next batch has chain_len + 1 == cap + 1 > cap, forcing the dense path. The dense rewrite
// materializes and supersedes the whole chain, collapsing the live DCG to a single dense entry and
// growing the orphan list.
TEST_F(LakeSparseDcgTest, test_promotion_hard_count_collapses_chain) {
    ConfigResetGuard<bool> g_enable(&config::enable_sparse_dcg, true);
    // Small cap so the test is cheap. cap=4 => the 5th same-column batch promotes.
    ConfigResetGuard<int32_t> g_hard(&config::sdcg_promotion_hard_count, 4);
    // Keep the threshold trigger out of the way (raise above any reachable cum_K/M).
    ConfigResetGuard<double> g_thresh(&config::sdcg_promotion_threshold, 2.0);

    constexpr int M = 1000;
    int64_t version = 1;
    write_base(M, &version);

    const int target = 500;
    // Write exactly `cap` sparse overlays (chain grows 0->1->2->3->4). Each must stay sparse.
    for (int v = 1; v <= 4; ++v) {
        const int value = 7000 + v;
        column_update(
                {target}, [value](int) { return value; }, &version);
        ASSIGN_OR_ABORT(auto md, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
        auto s = collect_dcg_stats(md);
        EXPECT_EQ(v, s.sparse) << "batch " << v << " (<= cap) must stay sparse, chain depth " << v;
        EXPECT_EQ(0, s.dense);
    }

    int64_t before_orphans;
    {
        ASSIGN_OR_ABORT(auto md, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
        before_orphans = orphan_count(md);
    }

    // The (cap+1)th batch: chain_len == 4, 4 + 1 == 5 > cap(4) => promotion forces dense.
    const int promoted_value = 99999;
    column_update(
            {target}, [](int) { return promoted_value; }, &version);

    ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
    auto stats = collect_dcg_stats(metadata);
    EXPECT_EQ(0, stats.sparse) << "promotion must supersede the entire sparse chain";
    EXPECT_EQ(1, stats.dense) << "the chain collapses to a single dense entry";
    EXPECT_GT(orphan_count(metadata), before_orphans) << "the promoted-away sparse files must be orphaned";

    // The promoted dense value is the newest; it must win on read.
    std::map<int, int> c1, c2;
    int rows = read_table(version, &c1, &c2);
    EXPECT_EQ(M, rows);
    EXPECT_EQ(promoted_value, c1[target]) << "the dense rewrite carries the newest value";
    EXPECT_EQ(499 * 3, c1[499]) << "neighbor untouched";
}

// (k) In-place promotion / convergence by CUM-K/M THRESHOLD: with a tiny sdcg_promotion_threshold,
// even a shallow chain promotes once the accumulated touched rows (cum_K + K) reach
// sdcg_promotion_threshold * M. Hard count is raised out of the way so the threshold is the only
// trigger.
TEST_F(LakeSparseDcgTest, test_promotion_threshold_collapses_chain) {
    ConfigResetGuard<bool> g_enable(&config::enable_sparse_dcg, true);
    // Hard count out of the way.
    ConfigResetGuard<int32_t> g_hard(&config::sdcg_promotion_hard_count, 1000);
    // Threshold tiny: with M=1000, threshold*M = 30. The density decision still requires K/M < 0.3
    // (K < 300) and K < sdcg_sparse_max_rows for each individual batch, so use K=20 batches: the
    // first stays sparse (cum_K=0, 0+20=20 < 30), the second promotes (cum_K=20, 20+20=40 >= 30).
    ConfigResetGuard<double> g_thresh(&config::sdcg_promotion_threshold, 0.03);

    constexpr int M = 1000;
    int64_t version = 1;
    write_base(M, &version);

    // Batch 1: keys 0..19 (K=20). cum_K before = 0, 0+20=20 < 30 => sparse.
    {
        std::vector<int> keys;
        for (int k = 0; k < 20; ++k) keys.push_back(k);
        column_update(
                keys, [](int k) { return k + 500000; }, &version);
        ASSIGN_OR_ABORT(auto md, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
        auto s = collect_dcg_stats(md);
        EXPECT_EQ(1, s.sparse) << "first batch under threshold stays sparse";
        EXPECT_EQ(0, s.dense);
    }

    int64_t before_orphans;
    {
        ASSIGN_OR_ABORT(auto md, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
        before_orphans = orphan_count(md);
    }

    // Batch 2: keys 100..119 (K=20, distinct from batch 1 to keep K/M low). cum_K=20 (from batch 1's
    // overlay covering c1), 20+20=40 >= 30 => promote to dense.
    {
        std::vector<int> keys;
        for (int k = 100; k < 120; ++k) keys.push_back(k);
        column_update(
                keys, [](int k) { return k + 600000; }, &version);
    }

    ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
    auto stats = collect_dcg_stats(metadata);
    EXPECT_EQ(0, stats.sparse) << "the threshold trigger must promote to dense and supersede the chain";
    EXPECT_EQ(1, stats.dense);
    EXPECT_GT(orphan_count(metadata), before_orphans);

    // Read back: the dense rewrite is the new base for c1. Batch-2 keys (100..119) win; batch-1 keys
    // (0..19) were sparse-only and are now orphaned, so they revert to base. (This mirrors the
    // documented dense-supersedes semantics already covered by test (c).)
    std::map<int, int> c1, c2;
    int rows = read_table(version, &c1, &c2);
    EXPECT_EQ(M, rows);
    for (int k = 100; k < 120; ++k) EXPECT_EQ(k + 600000, c1[k]) << "promoted dense key " << k;
    for (int k = 0; k < 20; ++k) EXPECT_EQ(k * 3, c1[k]) << "orphaned sparse key reverts to base " << k;
}

} // namespace starrocks::lake

// ===========================================================================================
// VARCHAR sparse-update roundtrip. Uses its own fixture because the shared LakeSparseDcgTest is
// fixed to three INT columns. A nullable VARCHAR value column exercises the .spcols binary-column
// gather/overlay path (variable-width data), which the INT cases never touch.
// ===========================================================================================
namespace starrocks::lake {

class LakeSparseDcgVarcharTest : public TestBase {
public:
    LakeSparseDcgVarcharTest() : TestBase(kTestDirectory) {
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
            c1->set_type("VARCHAR");
            c1->set_length(128);
            c1->set_is_key(false);
            c1->set_is_nullable(true);
            c1->set_aggregation("REPLACE");
        }

        _slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
        _slots.emplace_back(1, "c1", TypeDescriptor::create_varchar_type(128));
        _slot_pointers.emplace_back(&_slots[0]);
        _slot_pointers.emplace_back(&_slots[1]);
        _slot_cid_map.emplace(0, 0);
        _slot_cid_map.emplace(1, 1);

        _tablet_schema = TabletSchema::create(*schema);
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
        CHECK_OK(_tablet_mgr->create_schema_file(_tablet_metadata->id(), _tablet_metadata->schema()));
    }
    void TearDown() override { remove_test_dir_or_die(); }

    static std::string base_str(int i) { return fmt::format("base_{}", i); }

    int64_t write_base(int n, int64_t* version) {
        auto c0 = Int32Column::create();
        auto c1 = BinaryColumn::create();
        for (int i = 0; i < n; ++i) {
            c0->append(i);
            c1->append(base_str(i));
        }
        Chunk chunk({std::move(c0), std::move(c1)}, _slot_cid_map);
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

    int64_t column_update(const std::vector<int>& keys, const std::function<std::string(int)>& new_c1,
                          int64_t* version) {
        auto c0 = Int32Column::create();
        auto c1 = BinaryColumn::create();
        for (int k : keys) {
            c0->append(k);
            c1->append(new_c1(k));
        }
        Chunk chunk({std::move(c0), std::move(c1)}, _slot_cid_map);
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

    int read_table(int64_t version, std::map<int, std::string>* c1_by_key) {
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
                (*c1_by_key)[k] = std::string(cols[1]->get(i).get_slice());
            }
            rows += chunk->num_rows();
            chunk->reset();
        }
        return rows;
    }

protected:
    constexpr static const char* const kTestDirectory = "test_lake_sparse_dcg_varchar";
    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    int64_t _partition_id = 4563;
    std::vector<SlotDescriptor> _slots;
    std::vector<SlotDescriptor*> _slot_pointers;
    Chunk::SlotHashMap _slot_cid_map;
};

// (l) VARCHAR sparse update roundtrip: a small batch of variable-width string updates over a larger
// base must take the sparse path, and read back the new strings for updated keys / base strings for
// untouched keys. Also chain two sparse string updates of the same key (newest wins).
TEST_F(LakeSparseDcgVarcharTest, test_varchar_sparse_roundtrip) {
    ConfigResetGuard<bool> g_enable(&config::enable_sparse_dcg, true);

    constexpr int M = 2000;
    int64_t version = 1;
    write_base(M, &version);

    std::vector<int> keys = {3, 77, 512, 1999};
    column_update(
            keys, [](int k) { return fmt::format("updated_value_for_key_{}_xyz", k); }, &version);

    {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
        // The sparse path must have been taken (a .spcols overlay with K rows).
        int sparse = 0, dense = 0;
        int64_t total_k = 0;
        for (const auto& [rssid, dcg_ver] : metadata->dcg_meta().dcgs()) {
            for (int i = 0; i < dcg_ver.column_files_size(); ++i) {
                const auto kind = i < dcg_ver.file_kinds_size() ? dcg_ver.file_kinds(i) : DENSE_COLS;
                if (kind == SPARSE_PERCOL) {
                    ++sparse;
                    if (i < dcg_ver.sparse_row_counts_size()) total_k += dcg_ver.sparse_row_counts(i);
                    EXPECT_TRUE(is_spcols(dcg_ver.column_files(i)));
                } else {
                    ++dense;
                }
            }
        }
        EXPECT_EQ(1, sparse);
        EXPECT_EQ(0, dense);
        EXPECT_EQ(static_cast<int64_t>(keys.size()), total_k);
    }

    {
        std::map<int, std::string> c1;
        int rows = read_table(version, &c1);
        EXPECT_EQ(M, rows);
        std::set<int> updated(keys.begin(), keys.end());
        for (int k = 0; k < M; ++k) {
            ASSERT_TRUE(c1.count(k)) << "missing key " << k;
            if (updated.count(k)) {
                EXPECT_EQ(fmt::format("updated_value_for_key_{}_xyz", k), c1[k]) << "updated key " << k;
            } else {
                EXPECT_EQ(base_str(k), c1[k]) << "untouched key " << k;
            }
        }
    }

    // Chain a second sparse string update of one of the same keys: newest wins.
    column_update(
            {512}, [](int) { return std::string("second_overlay_wins"); }, &version);
    {
        std::map<int, std::string> c1;
        int rows = read_table(version, &c1);
        EXPECT_EQ(M, rows);
        EXPECT_EQ("second_overlay_wins", c1[512]) << "newest string overlay must win";
        EXPECT_EQ(fmt::format("updated_value_for_key_{}_xyz", 77), c1[77]) << "first overlay still applies to key 77";
        EXPECT_EQ(base_str(1000), c1[1000]) << "untouched key keeps base string";
    }
}

} // namespace starrocks::lake
