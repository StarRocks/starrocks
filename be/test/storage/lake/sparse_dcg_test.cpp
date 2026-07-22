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
#include <array>
#include <functional>
#include <map>
#include <set>
#include <string>
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
#include "common/config_compaction_fwd.h"
#include "common/config_primary_key_fwd.h"
#include "common/flexible_partial_update.h"
#include "storage/chunk_helper.h"
#include "storage/lake/compaction_task.h"
#include "storage/lake/compaction_task_context.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/filenames.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/test_util.h"
#include "storage/storage_metrics.h"
#include "storage/tablet_schema.h"
#include "storage/types.h"
#include "storage_primitive/column_predicate_factory.h"
#include "storage_primitive/predicate_tree/predicate_tree.hpp"

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
        // These unit tests use small bases (M ~ 1-2k rows) to exercise the sparse `.spcols` code path.
        // The production cost model only picks sparse for large segments (sdcg_sparse_min_segment_rows,
        // default 65536); lower it here so unit-scale bases qualify. Restored in TearDown so it does not
        // leak into other test fixtures sharing the process. The K/M and K-cap gates still apply.
        _orig_sparse_min_segment_rows = config::sdcg_sparse_min_segment_rows;
        config::sdcg_sparse_min_segment_rows = 1;
    }

    void TearDown() override {
        config::sdcg_sparse_min_segment_rows = _orig_sparse_min_segment_rows;
        remove_test_dir_or_die();
    }

    int64_t _orig_sparse_min_segment_rows = 65536;

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

    // ROW-mode partial update that writes c2 but OMITS c1. The masked full-row rewrite must READ the
    // omitted c1 from the source -- and when c1 carries a sparse DCG overlay, that read goes through
    // UpdateManager::get_column_values' overlay merge (dense base + newer sparse layers).
    int64_t row_mode_update_c2(const std::vector<int>& keys, const std::function<int(int)>& new_c2, int64_t* version) {
        std::vector<SlotDescriptor> slots;
        slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
        slots.emplace_back(2, "c2", TypeDescriptor{LogicalType::TYPE_INT});
        std::vector<SlotDescriptor*> slot_ptrs{&slots[0], &slots[1]};
        Chunk::SlotHashMap cid_map; // maps slot_id -> chunk column index
        cid_map.emplace(0, 0);      // slot c0 (id 0) -> chunk col 0
        cid_map.emplace(2, 1);      // slot c2 (id 2) -> chunk col 1
        auto c0 = Int32Column::create();
        auto c2 = Int32Column::create();
        for (int k : keys) {
            c0->append(k);
            c2->append(new_c2(k));
        }
        Chunk chunk({std::move(c0), std::move(c2)}, cid_map);
        std::vector<uint32_t> idx(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) idx[i] = static_cast<uint32_t>(i);
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto w, DeltaWriterBuilder()
                                        .set_tablet_manager(_tablet_mgr.get())
                                        .set_tablet_id(_tablet_metadata->id())
                                        .set_txn_id(txn_id)
                                        .set_partition_id(_partition_id)
                                        .set_mem_tracker(_mem_tracker.get())
                                        .set_schema_id(_tablet_schema->id())
                                        .set_slot_descriptors(&slot_ptrs)
                                        .set_partial_update_mode(PartialUpdateMode::ROW_MODE)
                                        .build());
        CHECK_OK(w->open());
        CHECK_OK(w->write(chunk, idx.data(), idx.size()));
        CHECK_OK(w->finish_with_txnlog());
        w->close();
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

// (b2) PARTIAL OVERLAP: several sparse overlays in ONE converged DCG (same segment), each touching a
// DIFFERENT row set for the SAME column, with deliberate overlap. The collector must gather ALL files
// and resolve per row by version (last-write-wins). The old first-hit collector (get_column_idx) picked
// exactly ONE file and silently dropped the rest -> rows whose newest value lives in a non-selected
// file reverted to base (silent data loss). test_multi_version_same_row_newest_wins touches ONE row so
// "newest wins" coincidentally masked this; here different rows expose it per-row.
TEST_F(LakeSparseDcgTest, test_partial_overlap_multi_file_one_dcg_value_exact) {
    ConfigResetGuard<bool> g_enable(&config::enable_sparse_dcg, true);

    constexpr int M = 1000;
    int64_t version = 1;
    write_base(M, &version); // base: c1[k] == k*3

    //   v1 (oldest): rows {10,20,30} -> c1 = 1000+k
    //   v2 (mid):    rows {20,40}    -> c1 = 2000+k   (row 20 overwrites v1)
    //   v3 (newest): rows {30,50}    -> c1 = 3000+k   (row 30 overwrites v1)
    column_update(
            {10, 20, 30}, [](int k) { return 1000 + k; }, &version);
    column_update(
            {20, 40}, [](int k) { return 2000 + k; }, &version);
    column_update(
            {30, 50}, [](int k) { return 3000 + k; }, &version);

    // Three sparse .spcols must coexist in ONE segment's converged DCG (the multi-file-per-DCG topology).
    ASSIGN_OR_ABORT(auto md, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
    auto stats = collect_dcg_stats(md);
    EXPECT_EQ(3, stats.sparse);
    EXPECT_EQ(0, stats.dense);

    std::map<int, int> c1, c2;
    int rows = read_table(version, &c1, &c2);
    EXPECT_EQ(M, rows);
    EXPECT_EQ(1000 + 10, c1[10]) << "row 10: only v1 -> 1010";
    EXPECT_EQ(2000 + 20, c1[20]) << "row 20: v2 over v1 -> 2020 (lost if v2's file is the un-collected one)";
    EXPECT_EQ(3000 + 30, c1[30]) << "row 30: v3 over v1 -> 3030 (lost if v3's file is the un-collected one)";
    EXPECT_EQ(2000 + 40, c1[40]) << "row 40: only v2 -> 2040";
    EXPECT_EQ(3000 + 50, c1[50]) << "row 50: only v3 -> 3050";
    EXPECT_EQ(11 * 3, c1[11]) << "untouched neighbor reads base";
    EXPECT_EQ(999 * 3, c1[999]) << "untouched tail reads base";
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

// Reads must be METADATA-driven: sparse layers written while the flag was on must remain
// readable after the flag is turned off (regression test for the bitshuffle "invalid pos"
// crash where flag-off reads fell into the legacy first-hit dense path).
TEST_F(LakeSparseDcgTest, test_flag_off_reads_existing_sparse) {
    constexpr int M = 2000;
    int64_t version = 1;
    write_base(M, &version);

    {
        ConfigResetGuard<bool> g_enable(&config::enable_sparse_dcg, true);
        std::vector<int> keys = {909};
        column_update(
                keys, [](int k) { return k * 2 + 7; }, &version);
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
        ASSERT_EQ(1, collect_dcg_stats(metadata).sparse) << "precondition: a sparse layer exists";
    }

    // Flag is back to false here; the sparse layer must still be readable via the overlay.
    ConfigResetGuard<bool> g_off(&config::enable_sparse_dcg, false);
    {
        std::map<int, int> c1, c2;
        int rows = read_table(version, &c1, &c2);
        ASSERT_EQ(M, rows);
        ASSERT_EQ(909 * 2 + 7, c1[909]) << "sparse layer must be readable with the flag off";
        ASSERT_EQ(5 * 3, c1[5]) << "untouched row keeps base value";
    }

    // And a flag-off (dense) write over the same column must succeed and supersede the chain
    // (its source read goes through the overlay).
    std::vector<int> keys2 = {909};
    column_update(
            keys2, [](int k) { return k * 5 + 1; }, &version);
    std::map<int, int> c1b, c2b;
    ASSERT_EQ(M, read_table(version, &c1b, &c2b));
    ASSERT_EQ(909 * 5 + 1, c1b[909]);
    ASSIGN_OR_ABORT(auto metadata2, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
    auto stats2 = collect_dcg_stats(metadata2);
    ASSERT_EQ(0, stats2.sparse) << "dense supersede collapses the sparse layer";
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
        // The K=64 dense write is a ROW-COMPLETE rewrite of c1 that reads THROUGH the current overlay
        // (including the prior K=63 sparse), so it supersedes and drops the prior sparse entry from the
        // DCG -- leaving zero sparse and one dense entry for c1.
        EXPECT_EQ(0, s.sparse) << "K=64 == cap takes the dense path and supersedes the prior sparse";
        EXPECT_EQ(1, s.dense);
    }

    // Final read-back: the K=64 dense rewrite baked the CURRENT column value into the new base, so
    // keys 1000..1063 take the dense-updated value, keys 0..62 keep their sparse-updated value (which
    // the dense read through and preserved -- NOT reverted to the original base), and everything else
    // is the original base.
    std::map<int, int> c1, c2;
    int rows = read_table(version, &c1, &c2);
    EXPECT_EQ(M, rows);
    for (int k = 1000; k < 1064; ++k) EXPECT_EQ(k + 200000, c1[k]) << "dense-updated key " << k;
    for (int k = 0; k < 63; ++k)
        EXPECT_EQ(k + 100000, c1[k]) << "sparse-updated key preserved via dense read-through " << k;
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
    // sdcg_promotion_threshold is DEPRECATED: it no longer drives a synchronous cum_K/M promotion to
    // dense (convergence is now background-compaction-driven). So the second sparse batch stays sparse
    // -- no dense rewrite, no chain collapse, no new orphans on the write path.
    EXPECT_GE(stats.sparse, 1) << "chain is NOT threshold-promoted to dense; sparse overlays persist";
    EXPECT_EQ(0, stats.dense);
    EXPECT_EQ(before_orphans, orphan_count(metadata)) << "no synchronous promotion => no new orphans";

    // Read back (layered merge): both sparse batches are preserved -- batch-2 keys (100..119) and
    // batch-1 keys (0..19) each keep their own updated value; nothing reverts to base.
    std::map<int, int> c1, c2;
    int rows = read_table(version, &c1, &c2);
    EXPECT_EQ(M, rows);
    for (int k = 100; k < 120; ++k) EXPECT_EQ(k + 600000, c1[k]) << "batch-2 sparse key " << k;
    for (int k = 0; k < 20; ++k) EXPECT_EQ(k + 500000, c1[k]) << "batch-1 sparse key preserved " << k;
}

// ---- Compaction-conflict REPLAY (enable_sdcg_compaction_conflict_replay) ------------------------
// A full PK compaction is computed at compact_version V; a column-mode partial update then lands a
// SPARSE_PERCOL overlay at V+1 (> V) on an input segment; the compaction is published at V+2. With
// replay ON the racing overlay is re-applied (by PK, via the .lcrm rows-mapper) onto the KEPT
// compaction output; with replay OFF the whole compaction output is discarded (historical behavior).
// Both must read back correctly; the difference is whether the conflict was replayed or discarded.

// Shared race driver: writes 3 full rowsets (real compaction inputs), a pre-compaction sparse overlay
// (folded by the compaction), starts+executes a compaction at V, lands a RACING sparse overlay at V+1,
// then publishes the compaction at V+2. Fills the expected c1 map. Returns nothing (asserts on the way).
TEST_F(LakeSparseDcgTest, test_compaction_conflict_replays_racing_sparse) {
    ConfigResetGuard<bool> g_sparse(&config::enable_sparse_dcg, true);
    ConfigResetGuard<bool> g_replay(&config::enable_sdcg_compaction_conflict_replay, true);
    // pick_rowsets() only compacts once the tablet has >= lake_pk_compaction_min_input_segments segments
    // (default 5). This test builds 3 base rowsets, so lower the gate or the compaction is a no-op
    // (0 input rowsets, no .lcrm) and the racing overlay is never classified/replayed.
    ConfigResetGuard<int64_t> g_min_seg(&config::lake_pk_compaction_min_input_segments, 2);

    constexpr int M = 2000;
    const auto tablet_id = _tablet_metadata->id();
    int64_t version = 1;
    write_base(M, &version); // v2
    write_base(M, &version); // v3
    write_base(M, &version); // v4 -- 3 fully-overlapping rowsets => compaction has real inputs to merge

    // Pre-compaction sparse overlay (chain to fold); K=4 over M=2000 => sparse .spcols.
    std::vector<int> pre_keys = {10, 250, 1000, 1999};
    column_update(
            pre_keys, [](int k) { return k * 1000; }, &version); // v5

    // Compute (but do NOT publish) a full compaction at compact_version = v5.
    const int64_t compact_version = version;
    auto compaction_txn = next_id();
    auto ctx =
            std::make_unique<CompactionTaskContext>(compaction_txn, tablet_id, compact_version, false, false, nullptr);
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(ctx.get()));
    ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));

    // RACE: a second sparse overlay lands at v6 (> compact_version) on an input segment. Key 10 overlaps
    // pre_keys, so the racing (newest) value must win after replay.
    std::vector<int> racing_keys = {10, 777, 1500};
    column_update(
            racing_keys, [](int k) { return k + 7000000; }, &version); // v6

    const int64_t replayed_before = StorageMetrics::instance()->sdcg_compaction_conflict_replay_executed_total.value();
    const int64_t discard_before = StorageMetrics::instance()->sdcg_compaction_conflict_discard_total.value();

    // Publish the compaction on top of the racing version -> conflict is classified REPLAYABLE_DCG and replayed.
    ASSERT_OK(publish_single_version(tablet_id, version + 1, compaction_txn).status());
    ++version;

    EXPECT_EQ(replayed_before + 1, StorageMetrics::instance()->sdcg_compaction_conflict_replay_executed_total.value())
            << "the racing sparse overlay must be REPLAYED onto the kept compaction output";
    EXPECT_EQ(discard_before, StorageMetrics::instance()->sdcg_compaction_conflict_discard_total.value())
            << "a replayable conflict must not increment the must-discard counter";

    std::map<int, int> c1, c2;
    int rows = read_table(version, &c1, &c2);
    EXPECT_EQ(M, rows) << "no row loss / duplication after replay";
    std::set<int> racing(racing_keys.begin(), racing_keys.end());
    std::set<int> pre(pre_keys.begin(), pre_keys.end());
    for (int k = 0; k < M; ++k) {
        ASSERT_TRUE(c1.count(k)) << "missing key " << k;
        if (racing.count(k)) {
            EXPECT_EQ(k + 7000000, c1[k]) << "racing key " << k << " must read the replayed (newest) value";
        } else if (pre.count(k)) {
            EXPECT_EQ(k * 1000, c1[k]) << "pre-compaction sparse value must survive compaction+replay for key " << k;
        } else {
            EXPECT_EQ(k * 3, c1[k]) << "untouched key " << k;
        }
        EXPECT_EQ(k * 4, c2[k]) << "c2 is never updated, key " << k;
    }
}

// (s) Compaction-conflict replay with a DENSE racing layer: the racing overlay is a row-complete DENSE
// `.cols` (K/M >= dense_threshold) rather than sparse. Replay must read it through the DENSE_COLS
// branch (new_dcg_segment, by base ordinal) and re-apply onto the kept compaction output.
TEST_F(LakeSparseDcgTest, test_compaction_conflict_replays_racing_dense) {
    ConfigResetGuard<bool> g_sparse(&config::enable_sparse_dcg, true);
    ConfigResetGuard<bool> g_replay(&config::enable_sdcg_compaction_conflict_replay, true);
    ConfigResetGuard<int64_t> g_min_seg(&config::lake_pk_compaction_min_input_segments, 2);

    constexpr int M = 2000;
    const auto tablet_id = _tablet_metadata->id();
    int64_t version = 1;
    write_base(M, &version);
    write_base(M, &version);
    write_base(M, &version);

    const std::vector<int> pre_keys = {10, 250, 1000, 1999};
    column_update(
            pre_keys, [](int k) { return k * 1000; }, &version); // v5 sparse pre-overlay (folded)

    const int64_t compact_version = version;
    auto compaction_txn = next_id();
    auto ctx =
            std::make_unique<CompactionTaskContext>(compaction_txn, tablet_id, compact_version, false, false, nullptr);
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(ctx.get()));
    ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));

    // RACING layer: K=700 of M=2000 => K/M=0.35 >= dense_threshold(0.3) => a DENSE `.cols` overlay.
    std::vector<int> racing_keys;
    for (int k = 0; k < 700; ++k) racing_keys.push_back(k);
    column_update(
            racing_keys, [](int k) { return k + 7000000; }, &version); // v6 dense racing

    const int64_t replayed_before = StorageMetrics::instance()->sdcg_compaction_conflict_replay_executed_total.value();
    ASSERT_OK(publish_single_version(tablet_id, version + 1, compaction_txn).status());
    ++version;
    EXPECT_EQ(replayed_before + 1, StorageMetrics::instance()->sdcg_compaction_conflict_replay_executed_total.value())
            << "the DENSE racing overlay must be REPLAYED onto the kept compaction output";

    std::map<int, int> c1, c2;
    int rows = read_table(version, &c1, &c2);
    EXPECT_EQ(M, rows) << "no row loss / duplication after dense replay";
    std::set<int> racing(racing_keys.begin(), racing_keys.end());
    std::set<int> pre(pre_keys.begin(), pre_keys.end());
    for (int k = 0; k < M; ++k) {
        if (racing.count(k)) {
            EXPECT_EQ(k + 7000000, c1[k]) << "dense racing key wins, " << k;
        } else if (pre.count(k)) {
            EXPECT_EQ(k * 1000, c1[k]) << "pre-overlay survives (not raced), " << k;
        } else {
            EXPECT_EQ(k * 3, c1[k]) << "base, " << k;
        }
        EXPECT_EQ(k * 4, c2[k]) << "c2 never updated, " << k;
    }
}

// (t) AUTO partial_update_mode: the delta_writer's L1 (per-load) width selector runs and, for a NARROW
// (single value column) update, resolves to COLUMN mode. Exercises the auto-resolve branch.
TEST_F(LakeSparseDcgTest, test_auto_mode_narrow_resolves_column) {
    ConfigResetGuard<bool> g_sparse(&config::enable_sparse_dcg, true);

    constexpr int M = 2000;
    int64_t version = 1;
    write_base(M, &version); // c1 = k*3

    auto ch = partial_chunk({10, 20, 30, 40}, [](int k) { return k + 100000; });
    const size_t n = ch.num_rows();
    std::vector<uint32_t> idx(n);
    for (size_t i = 0; i < n; ++i) idx[i] = static_cast<uint32_t>(i);
    auto txn_id = next_id();
    ASSIGN_OR_ABORT(auto w, DeltaWriterBuilder()
                                    .set_tablet_manager(_tablet_mgr.get())
                                    .set_tablet_id(_tablet_metadata->id())
                                    .set_txn_id(txn_id)
                                    .set_partition_id(_partition_id)
                                    .set_mem_tracker(_mem_tracker.get())
                                    .set_schema_id(_tablet_schema->id())
                                    .set_slot_descriptors(&_slot_pointers)
                                    .set_partial_update_mode(PartialUpdateMode::AUTO_MODE)
                                    .build());
    CHECK_OK(w->open());
    CHECK_OK(w->write(ch, idx.data(), n));
    CHECK_OK(w->finish_with_txnlog());
    w->close();
    CHECK_OK(publish_single_version(_tablet_metadata->id(), version + 1, txn_id).status());
    ++version;

    std::map<int, int> c1, c2;
    int rows = read_table(version, &c1, &c2);
    EXPECT_EQ(M, rows);
    for (int k : {10, 20, 30, 40}) EXPECT_EQ(k + 100000, c1[k]) << "auto-narrow applied, key " << k;
    EXPECT_EQ(500 * 3, c1[500]) << "untouched base";
}

// (u) DCG overlay merge with a DENSE layer in the foldable range: the merge must DEFER the whole
// segment (a sparse-only fold cannot represent an interleaved row-complete dense rewrite). Exercises
// the has_dense_below defer branch.
TEST_F(LakeSparseDcgTest, test_dcg_overlay_merge_defers_on_dense_below) {
    ConfigResetGuard<bool> g_sparse(&config::enable_sparse_dcg, true);
    ConfigResetGuard<int64_t> g_ramp(&config::sdcg_read_amp_budget, 2); // fold trigger = 3

    constexpr int M = 2000;
    int64_t version = 1;
    write_base(M, &version);

    const std::vector<int> ck = {5, 100, 900, 1500};
    column_update(
            ck, [](int k) { return k + 100; }, &version); // sparse layer 1
    column_update(
            ck, [](int k) { return k + 200; }, &version); // sparse layer 2
    // A DENSE layer (K=700 => K/M>=0.3) interleaved into the chain.
    std::vector<int> dk;
    for (int k = 0; k < 700; ++k) dk.push_back(k);
    column_update(
            dk, [](int k) { return k + 300000; }, &version); // DENSE layer (below compact_version)
    column_update(
            ck, [](int k) { return k + 400; }, &version); // sparse layer 3

    auto compaction_txn = next_id();
    auto ctx = std::make_unique<CompactionTaskContext>(compaction_txn, _tablet_metadata->id(), version, false, false,
                                                       nullptr);
    ASSIGN_OR_ABORT(auto vt, _tablet_mgr->get_tablet(_tablet_metadata->id(), version));
    auto rowsets = vt.get_rowsets();
    ASSERT_EQ(1u, rowsets.size());
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(ctx.get(), rowsets));
    ASSERT_OK(task->execute(CompactionTask::kNoCancelFn)); // dense-below => segment deferred, no fold
    ASSERT_OK(publish_single_version(_tablet_metadata->id(), version + 1, compaction_txn).status());
    ++version;

    // Data must remain correct regardless of the defer: ck newest = k+400; dk (not ck) = k+300000; else base.
    std::map<int, int> c1, c2;
    int rows = read_table(version, &c1, &c2);
    EXPECT_EQ(M, rows);
    std::set<int> cset(ck.begin(), ck.end()), dset(dk.begin(), dk.end());
    for (int k = 0; k < M; ++k) {
        if (cset.count(k)) {
            EXPECT_EQ(k + 400, c1[k]) << "newest sparse, key " << k;
        } else if (dset.count(k)) {
            EXPECT_EQ(k + 300000, c1[k]) << "dense layer value, key " << k;
        } else {
            EXPECT_EQ(k * 3, c1[k]) << "base, key " << k;
        }
    }
}

// (n) Background DCG overlay-chain merge: a single non-overlapped base rowset accrues a deep sparse
// chain; a base compaction folds the SPARSE_PERCOL layers (version <= compact_version) into ONE packed
// `.spcols` via try_execute_dcg_overlay_merge (op_dcg_compaction), collapsing the chain while
// preserving last-write-wins data. Exercises the background convergence path (no racing layer here).
TEST_F(LakeSparseDcgTest, test_dcg_overlay_merge_collapses_chain) {
    ConfigResetGuard<bool> g_sparse(&config::enable_sparse_dcg, true);
    // read_amp_budget=2 makes the effective fold trigger = max(2, R_max+1) = 3 (narrow INT columns would
    // otherwise use the depth-10 default, never firing at unit scale). budget>0 is NOT read-strict, so the
    // updates still take the sparse path.
    ConfigResetGuard<int64_t> g_ramp(&config::sdcg_read_amp_budget, 2);

    constexpr int M = 2000;
    const auto tablet_id = _tablet_metadata->id();
    int64_t version = 1;
    write_base(M, &version); // v2: one non-overlapped base rowset (single segment)

    // Build a sparse chain of depth 3 on the single base segment (overlapping keys exercise LWW).
    std::vector<int> keys = {5, 100, 777, 1500, 1999};
    for (int round = 0; round < 3; ++round) {
        column_update(
                keys, [round](int k) { return k + 1000 * (round + 1); }, &version); // v3, v4, v5
    }
    {
        ASSIGN_OR_ABORT(auto md, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        EXPECT_GE(collect_dcg_stats(md).sparse, 3) << "precondition: a >=3 deep sparse chain on the base segment";
    }

    // Compact the single base rowset (explicit input -> HORIZONTAL_COMPACTION, whose execute() first tries
    // try_execute_dcg_overlay_merge; auto-pick on one already-compacted base rowset would instead route to
    // a no-op cloud-native-index task and never reach the merge).
    auto compaction_txn = next_id();
    auto ctx = std::make_unique<CompactionTaskContext>(compaction_txn, tablet_id, version,
                                                       /*force_base_compaction=*/false, /*skip_write_txnlog=*/false,
                                                       nullptr);
    ASSIGN_OR_ABORT(auto vtablet, _tablet_mgr->get_tablet(tablet_id, version));
    auto input_rowsets = vtablet.get_rowsets();
    ASSERT_EQ(1u, input_rowsets.size()) << "single non-overlapped base rowset feeds the overlay merge";
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(ctx.get(), input_rowsets));
    ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
    ASSERT_OK(publish_single_version(tablet_id, version + 1, compaction_txn).status());
    ++version;

    // The >=3 sparse layers folded into a single merged .spcols; no dense layer is produced.
    {
        ASSIGN_OR_ABORT(auto md, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        auto s = collect_dcg_stats(md);
        EXPECT_EQ(1, s.sparse) << "the sparse chain must collapse to one merged .spcols";
        EXPECT_EQ(0, s.dense);
    }

    // Data correctness after the merge: updated keys read the NEWEST value (round 2 => k + 3000);
    // untouched keys keep base c1 (k*3); c2 is never updated (k*4).
    std::map<int, int> c1, c2;
    int rows = read_table(version, &c1, &c2);
    EXPECT_EQ(M, rows);
    std::set<int> updated(keys.begin(), keys.end());
    for (int k = 0; k < M; ++k) {
        ASSERT_TRUE(c1.count(k)) << "missing key " << k;
        if (updated.count(k)) {
            EXPECT_EQ(k + 3000, c1[k]) << "merged updated key " << k;
        } else {
            EXPECT_EQ(k * 3, c1[k]) << "untouched key " << k;
        }
        EXPECT_EQ(k * 4, c2[k]) << "c2 untouched, key " << k;
    }
}

// (o) Filtered read over a sparse-overlay column: a predicate on the overlaid column exercises the
// LayeredOverlayColumnIterator zone-map / value-could-match / contiguous-apply paths that a full
// unpredicated scan never reaches.
TEST_F(LakeSparseDcgTest, test_sparse_overlay_predicate_filtered_read) {
    ConfigResetGuard<bool> g_sparse(&config::enable_sparse_dcg, true);

    constexpr int M = 2000;
    int64_t version = 1;
    write_base(M, &version);

    // 3-deep sparse chain on c1; the newest layer sets all target keys to the SAME value so a
    // c1 == value predicate selects exactly them (newest-wins merged across the layers).
    const std::vector<int> keys = {10, 50, 900, 1500};
    column_update(
            keys, [](int k) { return k + 100; }, &version);
    column_update(
            keys, [](int k) { return k + 200; }, &version);
    column_update(
            keys, [](int) { return 7000020; }, &version);

    ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
    auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
    CHECK_OK(reader->prepare());
    TabletReaderParams params;
    std::unique_ptr<ColumnPredicate> pred(new_column_eq_predicate(get_type_info(TYPE_INT), 1, "7000020"));
    PredicateAndNode and_node;
    and_node.add_child(PredicateColumnNode(pred.get()));
    params.pred_tree = PredicateTree::create(std::move(and_node));
    CHECK_OK(reader->open(params));

    auto chunk = ChunkFactory::new_chunk(*_schema, 256);
    std::set<int> got;
    while (true) {
        auto st = reader->get_next(chunk.get());
        if (st.is_end_of_file()) break;
        CHECK_OK(st);
        auto cols = chunk->columns();
        for (int i = 0; i < chunk->num_rows(); ++i) {
            EXPECT_EQ(7000020, cols[1]->get(i).get_int32());
            got.insert(cols[0]->get(i).get_int32());
        }
        chunk->reset();
    }
    EXPECT_EQ(std::set<int>(keys.begin(), keys.end()), got) << "predicate must select exactly the overlaid keys";
}

// (p) Filtered reads with range predicates + NULL overlays: exercises the overlay iterator's range
// zone-map path, value-could-match (incl. NULL), and contiguous apply across many base ranges.
TEST_F(LakeSparseDcgTest, test_sparse_overlay_filtered_reads_ranges_and_nulls) {
    ConfigResetGuard<bool> g_sparse(&config::enable_sparse_dcg, true);

    constexpr int M = 2000;
    int64_t version = 1;
    write_base(M, &version); // base c1 = k*3

    const std::vector<int> ks = {10, 20, 30, 40};
    column_update(
            ks, [](int k) { return k + 900000; }, &version); // layer 1 (all large)
    {
        // layer 2: keys 10,30 -> NULL ; 20,40 -> k+800000 (newest wins)
        auto ch = partial_chunk_nullable(
                ks, [](int k) { return k == 10 || k == 30; }, [](int k) { return k + 800000; });
        column_update_chunk(ch, &version);
    }

    ASSIGN_OR_ABORT(auto md, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
    auto filtered = [&](ColumnPredicate* p) {
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), md, *_schema);
        CHECK_OK(reader->prepare());
        TabletReaderParams params;
        PredicateAndNode node;
        node.add_child(PredicateColumnNode(p));
        params.pred_tree = PredicateTree::create(std::move(node));
        CHECK_OK(reader->open(params));
        auto chunk = ChunkFactory::new_chunk(*_schema, 256);
        std::set<int> got;
        while (true) {
            auto st = reader->get_next(chunk.get());
            if (st.is_end_of_file()) break;
            CHECK_OK(st);
            auto cols = chunk->columns();
            for (int i = 0; i < chunk->num_rows(); ++i) got.insert(cols[0]->get(i).get_int32());
            chunk->reset();
        }
        return got;
    };
    auto ti = get_type_info(TYPE_INT);
    // c1 >= 800000 selects the two overlaid NON-NULL keys {20,40}; NULL rows (10,30) are excluded.
    {
        std::unique_ptr<ColumnPredicate> p(new_column_ge_predicate(ti, 1, "800000"));
        EXPECT_EQ((std::set<int>{20, 40}), filtered(p.get())) << "range predicate over overlay (nulls excluded)";
    }
    // c1 == 3000 selects the single untouched base key 1000 (1000*3), exercising base match + overlay skip.
    {
        std::unique_ptr<ColumnPredicate> p(new_column_eq_predicate(ti, 1, "3000"));
        EXPECT_EQ((std::set<int>{1000}), filtered(p.get())) << "base-value predicate on an untouched row";
    }
}

// (q) ROW-mode update reading THROUGH a sparse overlay: build a sparse c1 chain, then a ROW-mode
// update of c2 (omitting c1) must read c1 via the UpdateManager::get_column_values overlay merge and
// preserve it in the masked full-row rewrite. Verifies data correctness across the read-merge.
TEST_F(LakeSparseDcgTest, test_row_mode_reads_through_sparse_overlay) {
    ConfigResetGuard<bool> g_sparse(&config::enable_sparse_dcg, true);

    constexpr int M = 2000;
    int64_t version = 1;
    write_base(M, &version); // c1 = k*3, c2 = k*4

    const std::vector<int> ck = {5, 100, 900, 1500};
    column_update(
            ck, [](int k) { return k + 500000; }, &version);
    column_update(
            ck, [](int k) { return k + 600000; }, &version); // newest c1 for ck = k+600000

    // ROW-mode c2 update over an overlapping+extended key set, omitting c1.
    const std::vector<int> rk = {5, 100, 900, 1500, 250, 1750};
    row_mode_update_c2(
            rk, [](int k) { return k + 7000000; }, &version);

    std::map<int, int> c1, c2;
    int rows = read_table(version, &c1, &c2);
    EXPECT_EQ(M, rows);
    std::set<int> cset(ck.begin(), ck.end()), rset(rk.begin(), rk.end());
    for (int k = 0; k < M; ++k) {
        ASSERT_TRUE(c1.count(k)) << "missing key " << k;
        if (cset.count(k)) {
            EXPECT_EQ(k + 600000, c1[k]) << "c1 sparse overlay must survive the row-mode read-merge, key " << k;
        } else {
            EXPECT_EQ(k * 3, c1[k]) << "c1 base, key " << k;
        }
        if (rset.count(k)) {
            EXPECT_EQ(k + 7000000, c2[k]) << "c2 row-updated, key " << k;
        } else {
            EXPECT_EQ(k * 4, c2[k]) << "c2 base, key " << k;
        }
    }
}

// (q2) ROW-mode update reading through a DENSE overlay: build a DENSE c1 `.cols` (K/M >= threshold),
// then a ROW-mode c2 update (omitting c1) must read c1 via the get_column_values DENSE-base branch and
// preserve it. Complements the sparse-overlay read-merge test.
TEST_F(LakeSparseDcgTest, test_row_mode_reads_through_dense_overlay) {
    ConfigResetGuard<bool> g_sparse(&config::enable_sparse_dcg, true);

    constexpr int M = 2000;
    int64_t version = 1;
    write_base(M, &version); // c1 = k*3, c2 = k*4

    // DENSE c1 overlay: K=700 of M=2000 => K/M=0.35 => a row-complete dense `.cols`.
    std::vector<int> dk;
    for (int k = 0; k < 700; ++k) dk.push_back(k);
    column_update(
            dk, [](int k) { return k + 400000; }, &version);

    // ROW-mode c2 update (omits c1) -> reads c1 through the dense overlay.
    const std::vector<int> rk = {5, 100, 650, 900};
    row_mode_update_c2(
            rk, [](int k) { return k + 7000000; }, &version);

    std::map<int, int> c1, c2;
    int rows = read_table(version, &c1, &c2);
    EXPECT_EQ(M, rows);
    std::set<int> dset(dk.begin(), dk.end()), rset(rk.begin(), rk.end());
    for (int k = 0; k < M; ++k) {
        ASSERT_TRUE(c1.count(k)) << "missing key " << k;
        if (dset.count(k)) {
            EXPECT_EQ(k + 400000, c1[k]) << "dense c1 overlay must survive the row-mode read-merge, key " << k;
        } else {
            EXPECT_EQ(k * 3, c1[k]) << "c1 base, key " << k;
        }
        if (rset.count(k)) {
            EXPECT_EQ(k + 7000000, c2[k]) << "c2 row-updated, key " << k;
        } else {
            EXPECT_EQ(k * 4, c2[k]) << "c2 base, key " << k;
        }
    }
}

// (q3) ROW-mode read through a MIXED overlay (dense base + newer sparse on top): get_column_values
// must select the dense file as the row-complete base and let only STRICTLY-NEWER sparse overlays win.
TEST_F(LakeSparseDcgTest, test_row_mode_reads_through_mixed_overlay) {
    ConfigResetGuard<bool> g_sparse(&config::enable_sparse_dcg, true);

    constexpr int M = 2000;
    int64_t version = 1;
    write_base(M, &version); // c1 = k*3

    // 1) DENSE c1 overlay (K=700 => dense) as the row-complete base.
    std::vector<int> dk;
    for (int k = 0; k < 700; ++k) dk.push_back(k);
    column_update(
            dk, [](int k) { return k + 400000; }, &version);
    // 2) SPARSE c1 overlay ON TOP (K=3 => sparse) -- strictly newer, must win over the dense base.
    const std::vector<int> sk = {5, 100, 1500};
    column_update(
            sk, [](int k) { return k + 500000; }, &version);

    // ROW-mode c2 update (omits c1) -> reads c1 through the dense-base + newer-sparse merge.
    const std::vector<int> rk = {5, 100, 650, 900, 1500};
    row_mode_update_c2(
            rk, [](int k) { return k + 7000000; }, &version);

    std::map<int, int> c1, c2;
    int rows = read_table(version, &c1, &c2);
    EXPECT_EQ(M, rows);
    std::set<int> dset(dk.begin(), dk.end()), sset(sk.begin(), sk.end()), rset(rk.begin(), rk.end());
    for (int k = 0; k < M; ++k) {
        ASSERT_TRUE(c1.count(k)) << "missing key " << k;
        if (sset.count(k)) {
            EXPECT_EQ(k + 500000, c1[k]) << "newer sparse wins over dense base, key " << k;
        } else if (dset.count(k)) {
            EXPECT_EQ(k + 400000, c1[k]) << "dense base, key " << k;
        } else {
            EXPECT_EQ(k * 3, c1[k]) << "original base, key " << k;
        }
        if (rset.count(k)) {
            EXPECT_EQ(k + 7000000, c2[k]) << "c2 row-updated, key " << k;
        } else {
            EXPECT_EQ(k * 4, c2[k]) << "c2 base, key " << k;
        }
    }
}

// (r) Column-mode CONDITIONAL update (merge_condition on c1): a row is applied only when its new c1
// exceeds the existing c1. Exercises the handler condition path (_resolve_condition_cid,
// _locate_condition_idx_in_partial_schema, inline compare_at). merge_condition disables the sparse
// path, so this is a dense column-mode apply.
TEST_F(LakeSparseDcgTest, test_column_mode_conditional_update) {
    ConfigResetGuard<bool> g_sparse(&config::enable_sparse_dcg, true);

    constexpr int M = 2000;
    int64_t version = 1;
    write_base(M, &version); // c1 = k*3

    // keys 100,300 get a HIGHER c1 (applied); keys 200,400 get a LOWER c1 (condition fails -> kept).
    const std::vector<int> keys = {100, 200, 300, 400};
    auto ch = partial_chunk(keys, [](int k) { return (k == 200 || k == 400) ? 1 : k + 100000; });
    const size_t n = ch.num_rows();
    std::vector<uint32_t> idx(n);
    for (size_t i = 0; i < n; ++i) idx[i] = static_cast<uint32_t>(i);
    auto txn_id = next_id();
    ASSIGN_OR_ABORT(auto w, DeltaWriterBuilder()
                                    .set_tablet_manager(_tablet_mgr.get())
                                    .set_tablet_id(_tablet_metadata->id())
                                    .set_txn_id(txn_id)
                                    .set_partition_id(_partition_id)
                                    .set_mem_tracker(_mem_tracker.get())
                                    .set_schema_id(_tablet_schema->id())
                                    .set_slot_descriptors(&_slot_pointers)
                                    .set_merge_condition("c1")
                                    .set_partial_update_mode(PartialUpdateMode::COLUMN_UPDATE_MODE)
                                    .build());
    CHECK_OK(w->open());
    CHECK_OK(w->write(ch, idx.data(), n));
    CHECK_OK(w->finish_with_txnlog());
    w->close();
    CHECK_OK(publish_single_version(_tablet_metadata->id(), version + 1, txn_id).status());
    ++version;

    std::map<int, int> c1, c2;
    int rows = read_table(version, &c1, &c2);
    EXPECT_EQ(M, rows);
    EXPECT_EQ(100 + 100000, c1[100]) << "condition met (new>old): applied";
    EXPECT_EQ(300 + 100000, c1[300]) << "condition met (new>old): applied";
    EXPECT_EQ(200 * 3, c1[200]) << "condition failed (new<old): kept base";
    EXPECT_EQ(400 * 3, c1[400]) << "condition failed (new<old): kept base";
}

// With replay disabled, the same racing conflict must DISCARD the compaction output (input rowsets +
// racing overlay stay live), so reads are still correct and the replay counter does NOT advance.
TEST_F(LakeSparseDcgTest, test_compaction_conflict_discards_when_replay_disabled) {
    ConfigResetGuard<bool> g_sparse(&config::enable_sparse_dcg, true);
    ConfigResetGuard<bool> g_replay(&config::enable_sdcg_compaction_conflict_replay, false);

    constexpr int M = 2000;
    const auto tablet_id = _tablet_metadata->id();
    int64_t version = 1;
    write_base(M, &version); // v2
    write_base(M, &version); // v3
    write_base(M, &version); // v4

    std::vector<int> pre_keys = {10, 250, 1000, 1999};
    column_update(
            pre_keys, [](int k) { return k * 1000; }, &version); // v5

    const int64_t compact_version = version;
    auto compaction_txn = next_id();
    auto ctx =
            std::make_unique<CompactionTaskContext>(compaction_txn, tablet_id, compact_version, false, false, nullptr);
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(ctx.get()));
    ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));

    std::vector<int> racing_keys = {10, 777, 1500};
    column_update(
            racing_keys, [](int k) { return k + 7000000; }, &version); // v6

    const int64_t replayed_before = StorageMetrics::instance()->sdcg_compaction_conflict_replay_executed_total.value();

    ASSERT_OK(publish_single_version(tablet_id, version + 1, compaction_txn).status());
    ++version;

    EXPECT_EQ(replayed_before, StorageMetrics::instance()->sdcg_compaction_conflict_replay_executed_total.value())
            << "with replay disabled the conflict must be discarded, not replayed";

    // Discard preserves the input rowsets + both overlays, so the read is still correct (newest wins).
    std::map<int, int> c1, c2;
    int rows = read_table(version, &c1, &c2);
    EXPECT_EQ(M, rows) << "no row loss / duplication after discard";
    std::set<int> racing(racing_keys.begin(), racing_keys.end());
    std::set<int> pre(pre_keys.begin(), pre_keys.end());
    for (int k = 0; k < M; ++k) {
        ASSERT_TRUE(c1.count(k)) << "missing key " << k;
        if (racing.count(k)) {
            EXPECT_EQ(k + 7000000, c1[k]) << "racing key " << k;
        } else if (pre.count(k)) {
            EXPECT_EQ(k * 1000, c1[k]) << "pre key " << k;
        } else {
            EXPECT_EQ(k * 3, c1[k]) << "untouched key " << k;
        }
        EXPECT_EQ(k * 4, c2[k]) << "c2 key " << k;
    }
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
        // A second value column so that updating only c1 is a genuine COLUMN-mode partial update
        // (c2 omitted). Without it, writing c0+c1 covers every column => a full upsert, which never
        // takes the column-mode / sparse path.
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

        // Partial-update slots: c0 (key) + c1 (the single updated varchar value column); c2 is omitted.
        _slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
        _slots.emplace_back(1, "c1", TypeDescriptor::create_varchar_type(128));
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
        // Unit-scale bases; lower the large-segment gate so sparse is exercised (see LakeSparseDcgTest).
        _orig_sparse_min_segment_rows = config::sdcg_sparse_min_segment_rows;
        config::sdcg_sparse_min_segment_rows = 1;
    }
    void TearDown() override {
        config::sdcg_sparse_min_segment_rows = _orig_sparse_min_segment_rows;
        remove_test_dir_or_die();
    }
    int64_t _orig_sparse_min_segment_rows = 65536;

    static std::string base_str(int i) { return fmt::format("base_{}", i); }

    int64_t write_base(int n, int64_t* version) {
        auto c0 = Int32Column::create();
        auto c1 = BinaryColumn::create();
        auto c2 = Int32Column::create();
        for (int i = 0; i < n; ++i) {
            c0->append(i);
            c1->append(base_str(i));
            c2->append(i * 7);
        }
        Chunk chunk({std::move(c0), std::move(c1), std::move(c2)}, _slot_cid_map);
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

// ============================================================================================
// SDCG FLEXIBLE (per-row heterogeneous) PACKED partial update.
//
// Different rows of one load update different column subsets. FE injects a hidden "__cset__"
// SMALLINT set-id column (here we inject it directly as the last value slot, since the UT has no
// "__op" column). The rowset writer folds the per-load set-id dictionary
// (FlexiblePartialUpdateRegistry, keyed by txn_id) into RowsetTxnMetaPB.distinct_column_sets. At
// apply, the handler reads "__cset__" from the .upt, decodes each row's set-id into a column-uid
// mask, groups columns into equivalence classes, and PACKS all sparse classes of a (segment,batch)
// into ONE union .spcols (rows = union of class rowids; each value column NULL-padded where it does
// not cover the union) with PER-COLUMN presence (exact roaring). The reader applies a column ONLY at
// its covered rows (placeholders never applied).
//
// Table here: c0 (INT key), c1 (INT value), c2 (INT value). Two distinct column-sets exercised:
// {c1} and {c2}, plus {c1,c2}.
// ============================================================================================
class LakeFlexiblePackedDcgTest : public TestBase {
public:
    LakeFlexiblePackedDcgTest() : TestBase(kTestDirectory) {
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
            c1->set_default_value("0");
        }
        auto c2 = schema->add_column();
        {
            c2->set_unique_id(next_id());
            c2->set_name("c2");
            c2->set_type("INT");
            c2->set_is_key(false);
            c2->set_is_nullable(true);
            c2->set_aggregation("REPLACE");
            c2->set_default_value("0");
        }

        // Flexible partial-update slots: c0 (key) + c1 + c2 + "__cset__" set-id column LAST (no
        // "__op" in this UT, so the writer detects flexible from the trailing "__cset__" slot).
        _slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
        _slots.emplace_back(1, "c1", TypeDescriptor{LogicalType::TYPE_INT});
        _slots.emplace_back(2, "c2", TypeDescriptor{LogicalType::TYPE_INT});
        _slots.emplace_back(3, LOAD_CSET_COLUMN, TypeDescriptor{LogicalType::TYPE_SMALLINT});
        for (auto& s : _slots) {
            _slot_pointers.emplace_back(&s);
        }

        _slot_cid_map.emplace(0, 0);
        _slot_cid_map.emplace(1, 1);
        _slot_cid_map.emplace(2, 2);
        _slot_cid_map.emplace(3, 3); // __cset__ chunk column index

        _tablet_schema = TabletSchema::create(*schema);
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
        CHECK_OK(_tablet_mgr->create_schema_file(_tablet_metadata->id(), _tablet_metadata->schema()));
        // These unit tests use small bases (M ~ 1-2k rows) to exercise the sparse `.spcols` code path.
        // The production cost model only picks sparse for large segments (sdcg_sparse_min_segment_rows,
        // default 65536); lower it here so unit-scale bases qualify. Restored in TearDown so it does not
        // leak into other test fixtures sharing the process. The K/M and K-cap gates still apply.
        _orig_sparse_min_segment_rows = config::sdcg_sparse_min_segment_rows;
        config::sdcg_sparse_min_segment_rows = 1;
    }

    void TearDown() override {
        config::sdcg_sparse_min_segment_rows = _orig_sparse_min_segment_rows;
        remove_test_dir_or_die();
    }

    int64_t _orig_sparse_min_segment_rows = 65536;

    // Full-row base chunk for keys [0, n): c0=i, c1=i*3, c2=i*4.
    Chunk full_chunk(int n) {
        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        auto c2 = Int32Column::create();
        for (int i = 0; i < n; ++i) {
            c0->append(i);
            c1->append(i * 3);
            c2->append(i * 4);
        }
        Chunk::SlotHashMap base_map;
        base_map.emplace(0, 0);
        base_map.emplace(1, 1);
        base_map.emplace(2, 2);
        return Chunk({std::move(c0), std::move(c1), std::move(c2)}, base_map);
    }

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

    // One flexible row's intent: which columns it updates (a subset of {"c1","c2"}) and the new
    // values. A column not in `cols` is NOT touched for that key (it keeps its base value).
    struct FlexRow {
        int key;
        std::vector<std::string> cols; // present value columns (the row's set)
        int c1;                        // ignored unless "c1" in cols
        int c2;                        // ignored unless "c2" in cols
    };

    // Flexible packed partial update. Builds the [c0,c1,c2,__cset__] chunk: every row physically
    // carries c1 and c2 (omitted columns are placeholders, never applied at the right rows), plus the
    // per-row set-id in __cset__. The dictionary is interned into the registry under the writer's
    // txn_id (mirroring what the JSON scanner would do), so the rowset writer can fold it into
    // RowsetTxnMetaPB.distinct_column_sets. Returns the txn id.
    int64_t flexible_update(const std::vector<FlexRow>& rows, int64_t* version) {
        auto txn_id = next_id();
        // Intern each row's column-set into the per-load dictionary keyed by txn_id (the scanner's job).
        auto dict = FlexiblePartialUpdateRegistry::instance()->get_or_create(txn_id);
        auto c0 = Int32Column::create();
        auto c1_data = Int32Column::create();
        auto c1_null = NullColumn::create();
        auto c2_data = Int32Column::create();
        auto c2_null = NullColumn::create();
        auto cset = Int16Column::create();
        for (const auto& r : rows) {
            c0->append(r.key);
            const bool has_c1 = std::find(r.cols.begin(), r.cols.end(), "c1") != r.cols.end();
            const bool has_c2 = std::find(r.cols.begin(), r.cols.end(), "c2") != r.cols.end();
            // Present columns carry the real value; omitted columns carry a placeholder. The packed
            // apply must NEVER write the placeholder at this row for the omitted column.
            c1_data->append(has_c1 ? r.c1 : -999);
            c1_null->append(0);
            c2_data->append(has_c2 ? r.c2 : -999);
            c2_null->append(0);
            ColumnSetId sid = dict->intern(r.cols);
            cset->append(static_cast<int16_t>(sid));
        }
        auto c1 = NullableColumn::create(std::move(c1_data), std::move(c1_null));
        auto c2 = NullableColumn::create(std::move(c2_data), std::move(c2_null));
        Chunk chunk({std::move(c0), std::move(c1), std::move(c2), std::move(cset)}, _slot_cid_map);

        const size_t n = chunk.num_rows();
        std::vector<uint32_t> indexes(n);
        for (size_t i = 0; i < n; ++i) indexes[i] = static_cast<uint32_t>(i);
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
        FlexiblePartialUpdateRegistry::instance()->erase(txn_id);
        return txn_id;
    }

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

    // Count packed (SPARSE_PERCOL) files and assert per-column presence lists are emitted 1:1.
    struct FlexStats {
        int sparse_files = 0;
        int dense_files = 0;
        int total_presence_list_entries = 0;
        bool presence_lists_present = false;
        bool presence_lists_1to1 = true;
        std::map<uint32_t, int64_t> col_uid_to_count; // per-column covered-row count across packed files
    };
    FlexStats collect_flex_stats(const TabletMetadataPtr& metadata) {
        FlexStats s;
        for (const auto& [rssid, dcg_ver] : metadata->dcg_meta().dcgs()) {
            if (dcg_ver.column_presence_lists_size() > 0) {
                s.presence_lists_present = true;
                if (dcg_ver.column_presence_lists_size() != dcg_ver.column_files_size()) {
                    s.presence_lists_1to1 = false;
                }
            }
            for (int i = 0; i < dcg_ver.column_files_size(); ++i) {
                const auto kind = i < dcg_ver.file_kinds_size() ? dcg_ver.file_kinds(i) : DENSE_COLS;
                if (kind == SPARSE_PERCOL) {
                    ++s.sparse_files;
                } else {
                    ++s.dense_files;
                }
                if (i < dcg_ver.column_presence_lists_size()) {
                    const auto& list = dcg_ver.column_presence_lists(i);
                    s.total_presence_list_entries += list.entries_size();
                    for (const auto& e : list.entries()) {
                        s.col_uid_to_count[e.column_uid()] += e.count();
                    }
                }
            }
        }
        return s;
    }

    uint32_t uid_of(const std::string& name) {
        for (size_t i = 0; i < _tablet_schema->num_columns(); ++i) {
            if (_tablet_schema->column(i).name() == name) return _tablet_schema->column(i).unique_id();
        }
        return 0;
    }

protected:
    constexpr static const char* const kTestDirectory = "test_lake_flexible_packed_dcg";

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    int64_t _partition_id = 7731;
    std::vector<SlotDescriptor> _slots;
    std::vector<SlotDescriptor*> _slot_pointers;
    Chunk::SlotHashMap _slot_cid_map;
};

// (1) Heterogeneous pack roundtrip: two column sets ({c1} and {c2}) over OVERLAPPING base rowids in
// ONE apply must produce per-rssid packing, with each column applied only at its rows; untouched rows
// keep their base values.
TEST_F(LakeFlexiblePackedDcgTest, test_hetero_pack_roundtrip) {
    ConfigResetGuard<bool> g_enable(&config::enable_sparse_dcg, true);

    constexpr int M = 2000;
    int64_t version = 1;
    write_base(M, &version);

    // Rows interleave the two sets over overlapping keys:
    //   key 100: update c1 only -> c1=100000, c2 keeps base (400)
    //   key 200: update c2 only -> c2=200000, c1 keeps base (600)
    //   key 300: update both    -> c1=300001, c2=300002
    //   key 500: update c1 only -> c1=500000
    //   key 700: update c2 only -> c2=700000
    std::vector<FlexRow> rows = {
            {100, {"c1"}, 100000, 0}, {200, {"c2"}, 0, 200000}, {300, {"c1", "c2"}, 300001, 300002},
            {500, {"c1"}, 500000, 0}, {700, {"c2"}, 0, 700000},
    };
    flexible_update(rows, &version);

    ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
    auto fs = collect_flex_stats(metadata);
    EXPECT_GE(fs.sparse_files, 1) << "flexible heterogeneous update must produce >=1 packed .spcols";
    EXPECT_TRUE(fs.presence_lists_present) << "packed files must emit per-column presence lists";
    EXPECT_TRUE(fs.presence_lists_1to1) << "column_presence_lists must be 1:1 with column_files when present";
    // c1 covered keys: {100,300,500} = 3 ; c2 covered keys: {200,300,700} = 3.
    EXPECT_EQ(3, fs.col_uid_to_count[uid_of("c1")]) << "c1 must cover exactly its 3 rows";
    EXPECT_EQ(3, fs.col_uid_to_count[uid_of("c2")]) << "c2 must cover exactly its 3 rows";

    std::map<int, int> c1, c2;
    int total = read_table(version, &c1, &c2);
    EXPECT_EQ(M, total);
    for (int k = 0; k < M; ++k) {
        ASSERT_TRUE(c1.count(k)) << "missing key " << k;
        // c1 expectations
        if (k == 100) {
            EXPECT_EQ(100000, c1[k]);
        } else if (k == 300) {
            EXPECT_EQ(300001, c1[k]);
        } else if (k == 500) {
            EXPECT_EQ(500000, c1[k]);
        } else {
            EXPECT_EQ(k * 3, c1[k]) << "c1 untouched key " << k << " must keep base";
        }
        // c2 expectations
        if (k == 200) {
            EXPECT_EQ(200000, c2[k]);
        } else if (k == 300) {
            EXPECT_EQ(300002, c2[k]);
        } else if (k == 700) {
            EXPECT_EQ(700000, c2[k]);
        } else {
            EXPECT_EQ(k * 4, c2[k]) << "c2 untouched key " << k << " must keep base";
        }
    }
}

// (1b) Flexible MASKED-DENSE roundtrip: with a zero read-amp budget (read-strict), a flexible per-row
// heterogeneous update must NOT take the packed `.spcols` path; it falls to a row-complete masked-dense
// `.cols` (flex_column) written via _update_source_chunk_by_upt_flexible. Data must be identical to the
// packed case: each column applied only at its rows, omitted columns keep base.
TEST_F(LakeFlexiblePackedDcgTest, test_flexible_masked_dense_roundtrip) {
    ConfigResetGuard<bool> g_enable(&config::enable_sparse_dcg, true);
    // Zero read-amp budget => read-strict => flexible resolves to masked-dense (flex_column), exercising
    // the dense flexible apply (_update_source_chunk_by_upt_flexible) instead of the packed builder.
    ConfigResetGuard<int64_t> g_ramp(&config::sdcg_read_amp_budget, 0);

    constexpr int M = 2000;
    int64_t version = 1;
    write_base(M, &version);

    std::vector<FlexRow> rows = {
            {100, {"c1"}, 100000, 0}, {200, {"c2"}, 0, 200000}, {300, {"c1", "c2"}, 300001, 300002},
            {500, {"c1"}, 500000, 0}, {700, {"c2"}, 0, 700000},
    };
    flexible_update(rows, &version);

    ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
    auto fs = collect_flex_stats(metadata);
    EXPECT_EQ(0, fs.sparse_files) << "read-strict flexible must NOT write a packed .spcols";
    EXPECT_GE(fs.dense_files, 1) << "read-strict flexible must write a masked-dense .cols";

    std::map<int, int> c1, c2;
    int total = read_table(version, &c1, &c2);
    EXPECT_EQ(M, total);
    for (int k = 0; k < M; ++k) {
        ASSERT_TRUE(c1.count(k)) << "missing key " << k;
        if (k == 100) {
            EXPECT_EQ(100000, c1[k]);
        } else if (k == 300) {
            EXPECT_EQ(300001, c1[k]);
        } else if (k == 500) {
            EXPECT_EQ(500000, c1[k]);
        } else {
            EXPECT_EQ(k * 3, c1[k]) << "c1 untouched key " << k;
        }
        if (k == 200) {
            EXPECT_EQ(200000, c2[k]);
        } else if (k == 300) {
            EXPECT_EQ(300002, c2[k]);
        } else if (k == 700) {
            EXPECT_EQ(700000, c2[k]);
        } else {
            EXPECT_EQ(k * 4, c2[k]) << "c2 untouched key " << k;
        }
    }
}

// (1c) Background merge of a PACKED (heterogeneous) overlay chain: a deep chain of packed `.spcols`
// layers is folded by try_execute_dcg_overlay_merge, which must DECOMPOSE each packed layer per-column
// (via its presence-list roaring) before merging. Exercises the packed decomposition path in the merge.
TEST_F(LakeFlexiblePackedDcgTest, test_dcg_overlay_merge_folds_packed_chain) {
    ConfigResetGuard<bool> g_enable(&config::enable_sparse_dcg, true);
    ConfigResetGuard<int64_t> g_ramp(&config::sdcg_read_amp_budget, 2); // effective fold trigger = 3

    constexpr int M = 2000;
    int64_t version = 1;
    write_base(M, &version); // c1 = k*3, c2 = k*4

    // 3 heterogeneous (packed) layers over overlapping keys: {c1}, {c2}, {c1,c2} sets in each round.
    for (int r = 0; r < 3; ++r) {
        std::vector<FlexRow> rows = {
                {50, {"c1"}, 50 + r, 0},
                {100, {"c2"}, 0, 100 + r},
                {900, {"c1", "c2"}, 900 + r, 9000 + r},
        };
        flexible_update(rows, &version);
    }

    auto compaction_txn = next_id();
    auto ctx = std::make_unique<CompactionTaskContext>(compaction_txn, _tablet_metadata->id(), version,
                                                       /*force_base_compaction=*/false,
                                                       /*skip_write_txnlog=*/false, nullptr);
    ASSIGN_OR_ABORT(auto vt, _tablet_mgr->get_tablet(_tablet_metadata->id(), version));
    auto rowsets = vt.get_rowsets();
    ASSERT_EQ(1u, rowsets.size());
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(ctx.get(), rowsets));
    ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
    ASSERT_OK(publish_single_version(_tablet_metadata->id(), version + 1, compaction_txn).status());
    ++version;

    // Data must survive the packed fold: newest per (key,col); untouched cols/keys keep base.
    std::map<int, int> c1, c2;
    int rows = read_table(version, &c1, &c2);
    EXPECT_EQ(M, rows);
    for (int k = 0; k < M; ++k) {
        int e1 = k * 3, e2 = k * 4;
        if (k == 50) e1 = 52;   // last c1 update (r=2)
        if (k == 100) e2 = 102; // last c2 update
        if (k == 900) {
            e1 = 902;
            e2 = 9002;
        } // both updated
        EXPECT_EQ(e1, c1[k]) << "c1 key " << k;
        EXPECT_EQ(e2, c2[k]) << "c2 key " << k;
    }
}

// (2) Placeholder-not-applied: a key covered by c1 but NOT c2 must keep its base c2 (the union .spcols
// physically holds a c2 placeholder at that union ordinal; the per-column roaring must gate it out).
TEST_F(LakeFlexiblePackedDcgTest, test_placeholder_not_applied) {
    ConfigResetGuard<bool> g_enable(&config::enable_sparse_dcg, true);

    constexpr int M = 1000;
    int64_t version = 1;
    write_base(M, &version);

    // key 10 updates ONLY c1; key 11 updates ONLY c2. They are adjacent in the union, so a range-only
    // gate (min/max) would wrongly apply each column's placeholder to the other key. The roaring gate
    // must prevent that.
    std::vector<FlexRow> rows = {
            {10, {"c1"}, 1010, 0},
            {11, {"c2"}, 0, 1111},
    };
    flexible_update(rows, &version);

    std::map<int, int> c1, c2;
    int total = read_table(version, &c1, &c2);
    EXPECT_EQ(M, total);
    // key 10: c1 updated, c2 MUST keep base (40). key 11: c2 updated, c1 MUST keep base (33).
    EXPECT_EQ(1010, c1[10]) << "key 10 c1 updated";
    EXPECT_EQ(10 * 4, c2[10]) << "key 10 c2 MUST keep base (placeholder must not be applied)";
    EXPECT_EQ(11 * 3, c1[11]) << "key 11 c1 MUST keep base (placeholder must not be applied)";
    EXPECT_EQ(1111, c2[11]) << "key 11 c2 updated";
    // a totally untouched key keeps both base values.
    EXPECT_EQ(500 * 3, c1[500]);
    EXPECT_EQ(500 * 4, c2[500]);
}

// (3) Homogeneous degrade: when EVERY row uses the same single column-set ({c1}), the flexible path
// collapses to a single equivalence class. Verify correctness: c1 updated only where touched, c2 never
// touched, untouched c1 keeps base.
TEST_F(LakeFlexiblePackedDcgTest, test_homogeneous_single_set) {
    ConfigResetGuard<bool> g_enable(&config::enable_sparse_dcg, true);

    constexpr int M = 2000;
    int64_t version = 1;
    write_base(M, &version);

    std::vector<FlexRow> rows;
    for (int k : {3, 50, 123, 800, 1999}) {
        rows.push_back({k, {"c1"}, k * 1000, 0});
    }
    flexible_update(rows, &version);

    std::map<int, int> c1, c2;
    int total = read_table(version, &c1, &c2);
    EXPECT_EQ(M, total);
    std::set<int> updated = {3, 50, 123, 800, 1999};
    for (int k = 0; k < M; ++k) {
        if (updated.count(k)) {
            EXPECT_EQ(k * 1000, c1[k]) << "updated key " << k;
        } else {
            EXPECT_EQ(k * 3, c1[k]) << "untouched key " << k;
        }
        // c2 is never in any row's set: must stay base for ALL keys.
        EXPECT_EQ(k * 4, c2[k]) << "c2 must be untouched for key " << k;
    }
}

// ============================================================================================
// FLEXIBLE-on-ROW (ROW_MODE masked full-row rewrite) end-to-end test.
//
// Same hidden-"__cset__" + distinct_column_sets contract as the COLUMN/SDCG flexible path, but the
// load is written with PartialUpdateMode::ROW_MODE so the lake apply takes
// RowsetUpdateState::rewrite_segment's flexible branch -> SegmentRewriter::rewrite_full_row_lake
// (NOT the column-mode handler, NOT rewrite_partial_update). The CORRECTNESS LANDMINE under test:
// the .upt is a DENSE UNION with placeholders for omitted cells; the per-ROW covered-column mask
// must take the .upt value ONLY where the row's set-id covers the column, else keep base/default.
//
// Table: c0 (INT key) + c1..c4 (4 INT value columns, nullable, default 0).
// ============================================================================================
class LakeFlexibleRowModeTest : public TestBase {
public:
    LakeFlexibleRowModeTest() : TestBase(kTestDirectory) {
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
        for (int ci = 1; ci <= 4; ++ci) {
            auto c = schema->add_column();
            c->set_unique_id(next_id());
            c->set_name("c" + std::to_string(ci));
            c->set_type("INT");
            c->set_is_key(false);
            c->set_is_nullable(true);
            c->set_aggregation("REPLACE");
            c->set_default_value("0");
        }

        // Flexible slots: c0 (key) + c1..c4 + trailing "__cset__" set-id slot (no "__op" in this UT,
        // so the writer detects flexible from the trailing "__cset__" slot).
        _slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
        for (int ci = 1; ci <= 4; ++ci) {
            _slots.emplace_back(ci, "c" + std::to_string(ci), TypeDescriptor{LogicalType::TYPE_INT});
        }
        _slots.emplace_back(5, LOAD_CSET_COLUMN, TypeDescriptor{LogicalType::TYPE_SMALLINT});
        for (auto& s : _slots) {
            _slot_pointers.emplace_back(&s);
        }
        for (int i = 0; i <= 5; ++i) {
            _slot_cid_map.emplace(i, i);
        }

        _tablet_schema = TabletSchema::create(*schema);
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
        CHECK_OK(_tablet_mgr->create_schema_file(_tablet_metadata->id(), _tablet_metadata->schema()));
        // These unit tests use small bases (M ~ 1-2k rows) to exercise the sparse `.spcols` code path.
        // The production cost model only picks sparse for large segments (sdcg_sparse_min_segment_rows,
        // default 65536); lower it here so unit-scale bases qualify. Restored in TearDown so it does not
        // leak into other test fixtures sharing the process. The K/M and K-cap gates still apply.
        _orig_sparse_min_segment_rows = config::sdcg_sparse_min_segment_rows;
        config::sdcg_sparse_min_segment_rows = 1;
    }

    void TearDown() override {
        config::sdcg_sparse_min_segment_rows = _orig_sparse_min_segment_rows;
        remove_test_dir_or_die();
    }

    int64_t _orig_sparse_min_segment_rows = 65536;

    // Full-row base chunk for keys [0, n): c0=i, cX = i*(10+X). Distinct per-column base values so a
    // mis-merge (taking the wrong column / a placeholder) is detectable.
    Chunk full_chunk(int n) {
        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        auto c2 = Int32Column::create();
        auto c3 = Int32Column::create();
        auto c4 = Int32Column::create();
        for (int i = 0; i < n; ++i) {
            c0->append(i);
            c1->append(i * 11);
            c2->append(i * 12);
            c3->append(i * 13);
            c4->append(i * 14);
        }
        Chunk::SlotHashMap base_map;
        for (int i = 0; i <= 4; ++i) base_map.emplace(i, i);
        return Chunk({std::move(c0), std::move(c1), std::move(c2), std::move(c3), std::move(c4)}, base_map);
    }

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

    // One flexible row's intent. `cols` is the row's column-set (subset of {c1,c2,c3,c4}); `vals`
    // gives the new value for each of c1..c4 (only those in `cols` are applied). A brand-new key
    // (not in the base) takes column DEFAULT for omitted columns.
    struct FlexRow {
        int key;
        std::vector<std::string> cols;
        int vals[4]; // index 0..3 -> c1..c4
    };

    // FLEXIBLE-on-ROW partial update written with ROW_MODE. Builds the [c0,c1,c2,c3,c4,__cset__]
    // chunk: every row physically carries all 4 value columns (omitted ones are placeholders that the
    // masked merge MUST NOT apply), plus the per-row set-id. Returns the txn id.
    int64_t flexible_row_update(const std::vector<FlexRow>& rows, int64_t* version) {
        auto txn_id = next_id();
        auto dict = FlexiblePartialUpdateRegistry::instance()->get_or_create(txn_id);
        auto c0 = Int32Column::create();
        auto c1_data = Int32Column::create();
        auto c1_null = NullColumn::create();
        auto c2_data = Int32Column::create();
        auto c2_null = NullColumn::create();
        auto c3_data = Int32Column::create();
        auto c3_null = NullColumn::create();
        auto c4_data = Int32Column::create();
        auto c4_null = NullColumn::create();
        auto cset = Int16Column::create();
        auto has_col = [](const FlexRow& r, const char* name) {
            return std::find(r.cols.begin(), r.cols.end(), name) != r.cols.end();
        };
        for (const auto& r : rows) {
            c0->append(r.key);
            // Omitted columns carry a poison placeholder that must NEVER reach the table.
            c1_data->append(has_col(r, "c1") ? r.vals[0] : -777777);
            c1_null->append(0);
            c2_data->append(has_col(r, "c2") ? r.vals[1] : -777777);
            c2_null->append(0);
            c3_data->append(has_col(r, "c3") ? r.vals[2] : -777777);
            c3_null->append(0);
            c4_data->append(has_col(r, "c4") ? r.vals[3] : -777777);
            c4_null->append(0);
            ColumnSetId sid = dict->intern(r.cols);
            cset->append(static_cast<int16_t>(sid));
        }
        auto c1 = NullableColumn::create(std::move(c1_data), std::move(c1_null));
        auto c2 = NullableColumn::create(std::move(c2_data), std::move(c2_null));
        auto c3 = NullableColumn::create(std::move(c3_data), std::move(c3_null));
        auto c4 = NullableColumn::create(std::move(c4_data), std::move(c4_null));
        Chunk chunk({std::move(c0), std::move(c1), std::move(c2), std::move(c3), std::move(c4), std::move(cset)},
                    _slot_cid_map);

        const size_t n = chunk.num_rows();
        std::vector<uint32_t> indexes(n);
        for (size_t i = 0; i < n; ++i) indexes[i] = static_cast<uint32_t>(i);
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(_tablet_metadata->id())
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   // The whole point: ROW_MODE, not COLUMN_UPDATE_MODE.
                                                   .set_partial_update_mode(PartialUpdateMode::ROW_MODE)
                                                   .build());
        CHECK_OK(delta_writer->open());
        CHECK_OK(delta_writer->write(chunk, indexes.data(), indexes.size()));
        CHECK_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        CHECK_OK(publish_single_version(_tablet_metadata->id(), *version + 1, txn_id).status());
        ++(*version);
        FlexiblePartialUpdateRegistry::instance()->erase(txn_id);
        return txn_id;
    }

    // Read the whole table into per-column key->value maps.
    int read_table(int64_t version, std::array<std::map<int, int>, 4>* by_col) {
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
                for (int j = 0; j < 4; ++j) {
                    (*by_col)[j][k] = cols[j + 1]->get(i).get_int32();
                }
            }
            rows += chunk->num_rows();
            chunk->reset();
        }
        return rows;
    }

protected:
    constexpr static const char* const kTestDirectory = "test_lake_flexible_row_mode";

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    int64_t _partition_id = 7741;
    std::vector<SlotDescriptor> _slots;
    std::vector<SlotDescriptor*> _slot_pointers;
    Chunk::SlotHashMap _slot_cid_map;
};

// Heterogeneous masked full-row rewrite: 3 rows over distinct column-sets {c1}, {c2,c3}, {c1,c3}
// over a 4-value-col table + one brand-new key. Each cell must equal the update value IFF the row's
// set covers that column, else the base value (DEFAULT for the new key). c4 is touched by NO row and
// must stay base everywhere; omitted cells must NEVER become the poison placeholder (-777777) or NULL.
TEST_F(LakeFlexibleRowModeTest, test_masked_full_row_rewrite) {
    constexpr int M = 64;
    int64_t version = 1;
    write_base(M, &version);

    // key 5  : set {c1}      -> c1=5000 ; c2,c3,c4 keep base
    // key 9  : set {c2,c3}   -> c2=9200,c3=9300 ; c1,c4 keep base
    // key 20 : set {c1,c3}   -> c1=20100,c3=20300 ; c2,c4 keep base
    // key 100: NEW key {c2}  -> c2=100200 ; c1,c3,c4 take column DEFAULT (0)
    std::vector<FlexRow> rows = {
            {5, {"c1"}, {5000, 0, 0, 0}},
            {9, {"c2", "c3"}, {0, 9200, 9300, 0}},
            {20, {"c1", "c3"}, {20100, 0, 20300, 0}},
            {100, {"c2"}, {0, 100200, 0, 0}},
    };
    flexible_row_update(rows, &version);

    std::array<std::map<int, int>, 4> col; // col[0]=c1 .. col[3]=c4
    int total = read_table(version, &col);
    EXPECT_EQ(M + 1, total) << "the new key 100 adds one row";

    auto base = [](int k, int which /*1..4*/) { return k * (10 + which); };

    for (int k = 0; k < M; ++k) {
        // c1
        if (k == 5) {
            EXPECT_EQ(5000, col[0][k]);
        } else if (k == 20) {
            EXPECT_EQ(20100, col[0][k]);
        } else {
            EXPECT_EQ(base(k, 1), col[0][k]) << "c1 untouched key " << k << " must keep base";
        }
        // c2
        if (k == 9) {
            EXPECT_EQ(9200, col[1][k]);
        } else {
            EXPECT_EQ(base(k, 2), col[1][k]) << "c2 untouched key " << k << " must keep base";
        }
        // c3
        if (k == 9) {
            EXPECT_EQ(9300, col[2][k]);
        } else if (k == 20) {
            EXPECT_EQ(20300, col[2][k]);
        } else {
            EXPECT_EQ(base(k, 3), col[2][k]) << "c3 untouched key " << k << " must keep base";
        }
        // c4 is in NO row's set: base everywhere, and never the poison placeholder.
        EXPECT_EQ(base(k, 4), col[3][k]) << "c4 must be untouched (base) for key " << k;
        // No cell may ever be the poison placeholder.
        EXPECT_NE(-777777, col[0][k]);
        EXPECT_NE(-777777, col[1][k]);
        EXPECT_NE(-777777, col[2][k]);
        EXPECT_NE(-777777, col[3][k]);
    }

    // Brand-new key 100: c2 updated; c1,c3,c4 take column DEFAULT (0), NOT the placeholder.
    ASSERT_TRUE(col[1].count(100));
    EXPECT_EQ(100200, col[1][100]);
    EXPECT_EQ(0, col[0][100]) << "new-key omitted c1 must be DEFAULT";
    EXPECT_EQ(0, col[2][100]) << "new-key omitted c3 must be DEFAULT";
    EXPECT_EQ(0, col[3][100]) << "new-key omitted c4 must be DEFAULT";
}

} // namespace starrocks::lake
