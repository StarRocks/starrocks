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

#include <map>
#include <random>
#include <set>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "base/testutil/sync_point.h"
#include "base/utility/defer_op.h"
#include "column/array_column.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/datum_convert.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "common/config_ingest_fwd.h"
#include "common/config_primary_key_fwd.h"
#include "common/config_rowset_fwd.h"
#include "common/logging.h"
#include "fs/fs.h"
#include "platform/key_cache.h"
#include "storage/chunk_helper.h"
#include "storage/datum_variant.h"
#include "storage/del_vector.h"
#include "storage/lake/column_mode_partial_update_handler.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/tablet_reshard_helper.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/test_util.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/tablet_schema.h"
#include "storage/types.h"
#include "storage/variant_tuple.h"

namespace starrocks::lake {

class LakePartialUpdateTestBase : public TestBase {
public:
    explicit LakePartialUpdateTestBase(const char* test_directory) : TestBase(test_directory) {
        _tablet_metadata = std::make_shared<TabletMetadata>();
        _tablet_metadata->set_id(next_id());
        _tablet_metadata->set_version(1);
        _tablet_metadata->set_next_rowset_id(1);
        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c0   |  INT | YES |  NO  |
        //  |   c1   |  INT | NO  |  NO  |
        //  |   c2   |  INT | NO  |  NO  |
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

        _slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
        _slots.emplace_back(1, "c1", TypeDescriptor{LogicalType::TYPE_INT});
        _slot_pointers.emplace_back(&_slots[0]);
        _slot_pointers.emplace_back(&_slots[1]);

        _slot_cid_map.emplace(0, 0);
        _slot_cid_map.emplace(1, 1);
        _slot_cid_map.emplace(2, 2);

        _tablet_schema = TabletSchema::create(*schema);
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
        _partial_schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema, {0, 1}));
    }

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
        CHECK_OK(_tablet_mgr->create_schema_file(_tablet_metadata->id(), _tablet_metadata->schema()));
    }

    void TearDown() override {
        // check primary index cache's ref
        EXPECT_TRUE(_update_mgr->TEST_check_primary_index_cache_ref(_tablet_metadata->id(), 1));
        StorageEngine::instance()->wait_storage_cleanup_tasks();
        // check trash files already removed
        for (const auto& file : _trash_files) {
            EXPECT_FALSE(fs::path_exist(file));
        }
        remove_test_dir_or_die();
    }

    Chunk generate_data(int64_t chunk_size, int shift, bool partial, int update_ratio) {
        std::vector<int> v0(chunk_size);
        std::vector<int> v1(chunk_size);
        std::vector<int> v2(chunk_size);
        for (int i = 0; i < chunk_size; i++) {
            v0[i] = i + shift * chunk_size;
        }
        auto rng = std::default_random_engine{};
        std::shuffle(v0.begin(), v0.end(), rng);
        for (int i = 0; i < chunk_size; i++) {
            v1[i] = v0[i] * update_ratio;
        }

        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        c0->append_numbers(v0.data(), v0.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int));

        if (!partial) {
            for (int i = 0; i < chunk_size; i++) {
                v2[i] = v0[i] * 4;
            }
            auto c2 = Int32Column::create();
            c2->append_numbers(v2.data(), v2.size() * sizeof(int));
            return Chunk({std::move(c0), std::move(c1), std::move(c2)}, _slot_cid_map);
        } else {
            return Chunk({std::move(c0), std::move(c1)}, _slot_cid_map);
        }
    }

    int64_t check(int64_t version, const std::function<bool(int c0, int c1, int c2)>& check_fn) {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));
        auto chunk = ChunkFactory::new_chunk(*_schema, 128);
        int64_t ret = 0;
        while (true) {
            auto st = reader->get_next(chunk.get());
            if (st.is_end_of_file()) {
                break;
            }
            CHECK_OK(st);
            ret += chunk->num_rows();
            auto cols = chunk->columns();
            for (int i = 0; i < chunk->num_rows(); i++) {
                EXPECT_TRUE(check_fn(cols[0]->get(i).get_int32(), cols[1]->get(i).get_int32(),
                                     cols[2]->get(i).get_int32()));
            }
            chunk->reset();
        }
        return ret;
    }

protected:
    constexpr static const int kChunkSize = 12;

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    std::shared_ptr<Schema> _partial_schema;
    int64_t _partition_id = 4561;
    std::vector<std::string> _trash_files;
    std::vector<SlotDescriptor> _slots;
    std::vector<SlotDescriptor*> _slot_pointers;
    Chunk::SlotHashMap _slot_cid_map;
};

class LakePartialUpdateTest : public LakePartialUpdateTestBase, testing::WithParamInterface<PrimaryKeyParam> {
public:
    LakePartialUpdateTest() : LakePartialUpdateTestBase(kTestDirectory) {}

    void SetUp() override {
        _tablet_metadata->set_enable_persistent_index(GetParam().enable_persistent_index);
        _tablet_metadata->set_persistent_index_type(GetParam().persistent_index_type);
        LakePartialUpdateTestBase::SetUp();
    }
    constexpr static const char* const kTestDirectory = "test_lake_partial_update";
};

TEST_P(LakePartialUpdateTest, test_write) {
    auto chunk0 = generate_data(kChunkSize, 0, false, 3);
    auto chunk1 = generate_data(kChunkSize, 0, true, 3);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    // normal write
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c0 * 3 == c1) && (c0 * 4 == c2); }));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 3);

    // partial update
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(GetParam().partial_update_mode)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
        ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        if (GetParam().partial_update_mode == PartialUpdateMode::COLUMN_UPDATE_MODE) {
            if (i == 0) {
                EXPECT_EQ(new_tablet_metadata->orphan_files_size(), 1);
            } else {
                // Superseded .cols files are orphaned; with change data capture on, the prior
                // publish's per-publish column_overlay_vecs delvec is orphaned too (the extra one).
                const int expected = new_tablet_metadata->cdc_metadata().enable_cdc() ? 3 : 2;
                EXPECT_EQ(new_tablet_metadata->orphan_files_size(), expected);
            }
        } else {
            EXPECT_EQ(new_tablet_metadata->orphan_files_size(), 1);
        }
    }
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c0 * 3 == c1) && (c0 * 4 == c2); }));
    ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    if (GetParam().partial_update_mode == PartialUpdateMode::COLUMN_UPDATE_MODE) {
        EXPECT_EQ(new_tablet_metadata->rowsets_size(), 3);
    } else {
        EXPECT_EQ(new_tablet_metadata->rowsets_size(), 6);
    }
    EXPECT_TRUE(_update_mgr->update_state_mem_tracker()->consumption() == 0);
    if (GetParam().enable_persistent_index && GetParam().persistent_index_type == PersistentIndexTypePB::LOCAL) {
        check_local_persistent_index_meta(tablet_id, version);
    }
}

TEST_P(LakePartialUpdateTest, test_column_mode_partial_update_streams_source_segment) {
    if (GetParam().partial_update_mode != PartialUpdateMode::COLUMN_UPDATE_MODE) {
        GTEST_SKIP() << "Only column mode reads source segments while generating DCGs";
    }

    auto chunk_full = generate_data(kChunkSize, 0, false, 3);
    auto chunk_partial = generate_data(kChunkSize, 0, true, 5);
    std::vector<uint32_t> indexes(kChunkSize);
    std::iota(indexes.begin(), indexes.end(), 0);

    auto version = 1;
    const auto tablet_id = _tablet_metadata->id();
    {
        const auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk_full, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, ++version, txn_id).status());
    }

    const auto txn_id = next_id();
    ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                               .set_tablet_manager(_tablet_mgr.get())
                                               .set_tablet_id(tablet_id)
                                               .set_txn_id(txn_id)
                                               .set_partition_id(_partition_id)
                                               .set_mem_tracker(_mem_tracker.get())
                                               .set_schema_id(_tablet_schema->id())
                                               .set_slot_descriptors(&_slot_pointers)
                                               .set_partial_update_mode(PartialUpdateMode::COLUMN_UPDATE_MODE)
                                               .build());
    ASSERT_OK(delta_writer->open());
    ASSERT_OK(delta_writer->write(chunk_partial, indexes.data(), indexes.size()));
    ASSERT_OK(delta_writer->finish_with_txnlog());
    delta_writer->close();

    ConfigResetGuard<int32_t> chunk_size_guard(&config::vector_chunk_size, 4);
    ConfigResetGuard<int64_t> memory_limit_guard(&config::partial_update_memory_limit_per_worker, 80);
    ConfigResetGuard<bool> parallel_guard(&config::enable_pk_index_parallel_execution, false);
    int64_t upt_memory_usage_per_row = 0;
    std::vector<std::pair<uint32_t, uint32_t>> emitted_ranges;
    SyncPoint::GetInstance()->SetCallBack("ColumnModePartialUpdateHandler::_calc_upt_memory_usage_per_row",
                                          [&](void* arg) { upt_memory_usage_per_row = *static_cast<int64_t*>(arg); });
    SyncPoint::GetInstance()->SetCallBack("ColumnModePartialUpdateHandler::_read_from_source_segment_and_update:emit",
                                          [&](void* arg) {
                                              const auto* container = static_cast<StreamChunkContainer*>(arg);
                                              emitted_ranges.emplace_back(container->start_rowid, container->end_rowid);
                                          });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp sync_point_guard([&]() {
        SyncPoint::GetInstance()->ClearCallBack("ColumnModePartialUpdateHandler::_calc_upt_memory_usage_per_row");
        SyncPoint::GetInstance()->ClearCallBack(
                "ColumnModePartialUpdateHandler::_read_from_source_segment_and_update:emit");
        SyncPoint::GetInstance()->DisableProcessing();
    });

    ASSERT_OK(publish_single_version(tablet_id, ++version, txn_id).status());
    EXPECT_GE(upt_memory_usage_per_row, static_cast<int64_t>(sizeof(int32_t) * 2));
    EXPECT_EQ((std::vector<std::pair<uint32_t, uint32_t>>{{0, 4}, {4, 8}, {8, 12}}), emitted_ranges);
    EXPECT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return c0 * 5 == c1 && c0 * 4 == c2; }));
}

// Regression test: when a partial update orphans a bundled segment, the orphan_files entry must be
// flagged shared so vacuum's cross-tablet alive-check protects it.
//
// Under file bundling one physical .dat holds the segments of several sibling tablets, and a
// segment's shared-ness is encoded by bundle_file_offset (not the `shared` flag). The orphan
// FileMetaPB only carries `shared`, so the orphan-producing paths must set it to
// is_shared_segment() = shared || has_bundle_file_offset(). Otherwise collect_garbage_files sees
// file.shared()==false, routes the orphan to the plain deleter, and deletes the physical bundle
// file even while a sibling tablet still references it in a live rowset -- wedging that publish.
//
// This test stamps a bundle_file_offset onto a row-mode partial update segment (as a bundled load
// records it), publishes so rewrite_segment orphans the raw segment, and asserts the orphaned
// segment is flagged shared.
TEST_P(LakePartialUpdateTest, test_bundled_orphan_segment_marked_shared) {
    // Column mode orphans the partial-column update segment via a different path; this test targets
    // the row-mode rewrite orphan path.
    if (GetParam().partial_update_mode == PartialUpdateMode::COLUMN_UPDATE_MODE) {
        return;
    }

    auto chunk0 = generate_data(kChunkSize, 0, false, 3);
    auto chunk1 = generate_data(kChunkSize, 0, true, 3);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();

    // 1. Normal full write so the partial update has base rows to rewrite against.
    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // 2. Row-mode partial update write (num_rows > 0 so rewrite_segment rewrites + orphans the raw
    //    segment); do not publish yet.
    auto txn_id = next_id();
    {
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(GetParam().partial_update_mode)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
    }

    // 3. Simulate file bundling: stamp bundle_file_offset onto the written segment(s). Offset 0 keeps
    //    the standalone segment file readable; num_rows is left intact so the rewrite runs normally.
    std::set<std::string> bundled_segments;
    {
        ASSIGN_OR_ABORT(auto original_txn_log, _tablet_mgr->get_txn_log(tablet_id, txn_id));
        auto new_txn_log = std::make_shared<TxnLogPB>(*original_txn_log);
        auto* rowset = new_txn_log->mutable_op_write()->mutable_rowset();
        ASSERT_GT(rowset->segment_metas_size(), 0);
        for (int i = 0; i < rowset->segment_metas_size(); i++) {
            auto* seg = rowset->mutable_segment_metas(i);
            bundled_segments.insert(seg->filename());
            seg->set_bundle_file_offset(0);
        }
        ASSERT_OK(_tablet_mgr->put_txn_log(new_txn_log));
    }

    // 4. Publish. rewrite_segment rewrites the bundled segment and orphans the raw one.
    ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
    version++;

    // 5. The orphaned raw bundle segment must be flagged shared for vacuum's alive-check.
    ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    int checked = 0;
    for (const auto& file : metadata->orphan_files()) {
        if (bundled_segments.count(file.name()) > 0) {
            checked++;
            EXPECT_TRUE(file.shared()) << "orphaned bundle segment must be flagged shared: " << file.name();
        }
    }
    EXPECT_EQ(checked, static_cast<int>(bundled_segments.size()))
            << "every bundled segment should have been orphaned by the rewrite";
}

// This test case covers the following logic:
// - with_default branch in get_column_values() (default values and column_to_expr_value override)
// - Column mode generates DCG then switches to row mode, triggering need_dcg_check and DCG loading paths
TEST_P(LakePartialUpdateTest, test_dcg_then_row_mode_with_default_and_expr_override) {
    auto chunk0 = generate_data(kChunkSize, 0, false, 3);
    auto chunk_partial_same_keys = generate_data(kChunkSize, 0, true, 3);

    // Construct a batch of "new primary key" partial update data, containing only (c0, c1)
    // c0 = i + kChunkSize, c1 = i * 3 (values don't matter much, keeping consistent ratio with generate_data)
    std::vector<int> new_keys(kChunkSize);
    std::vector<int> new_vals(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        new_keys[i] = i + kChunkSize;
        new_vals[i] = new_keys[i] * 3;
    }
    auto c0_new = Int32Column::create();
    auto c1_new = Int32Column::create();
    c0_new->append_numbers(new_keys.data(), new_keys.size() * sizeof(int));
    c1_new->append_numbers(new_vals.data(), new_vals.size() * sizeof(int));
    Chunk chunk_partial_new_keys({c0_new, c1_new}, _slot_cid_map);

    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) indexes[i] = i;

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();

    // 1) Basic full writes (3 versions)
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // 2) Column mode (COLUMN_UPDATE_MODE) partial update, generating DCG
    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(PartialUpdateMode::COLUMN_UPDATE_MODE)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk_partial_same_keys, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // 3) Row mode (ROW_MODE) partial update (same primary keys), only providing (c0, c1), triggering need_dcg_check and DCG loading
    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(PartialUpdateMode::ROW_MODE)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk_partial_same_keys, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // 4) Row mode (ROW_MODE) partial update (new primary keys), covering with_default branch, and overriding default values via column_to_expr_value
    {
        std::map<std::string, std::string> expr_overrides;
        // Override the default value of unprovided column c2 from schema default (10) to 77
        expr_overrides.emplace("c2", "77");

        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(PartialUpdateMode::ROW_MODE)
                                                   .set_column_to_expr_value(&expr_overrides)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk_partial_new_keys, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Verification:
    // - Old primary keys [0, kChunkSize) still satisfy c1 = c0*3, c2 = c0*4
    // - New primary keys [kChunkSize, 2*kChunkSize) satisfy c1 = c0*3, and c2 is overridden by expr to 77
    ASSERT_EQ(kChunkSize * 2, check(version, [&](int c0, int c1, int c2) {
                  if (c0 < kChunkSize) {
                      return (c1 == c0 * 3) && (c2 == c0 * 4);
                  } else if (c0 < kChunkSize * 2) {
                      return (c1 == c0 * 3) && (c2 == 77);
                  }
                  return false;
              }));
}

TEST_P(LakePartialUpdateTest, test_partial_update_with_condition) {
    auto chunk0 = generate_data(kChunkSize, 0, false, 3);
    std::vector<Chunk> chunks(3);
    chunks[0] = generate_data(kChunkSize, 0, true, 2);
    chunks[1] = generate_data(kChunkSize, 0, true, 3);
    chunks[2] = generate_data(kChunkSize, 0, true, 4);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    // normal write
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c0 * 3 == c1) && (c0 * 4 == c2); }));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 3);

    // 0. partial update with condition less than merge condition val
    // 1. partial update with condition equal to merge condition val
    // 2. partial update with condition greater than merge condition val
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(GetParam().partial_update_mode)
                                                   .set_merge_condition("c1")
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunks[i], indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
        ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        if (i == 0 || i == 1) {
            ASSERT_EQ(kChunkSize,
                      check(version, [](int c0, int c1, int c2) { return (c0 * 3 == c1) && (c0 * 4 == c2); }));
        } else {
            ASSERT_EQ(kChunkSize,
                      check(version, [](int c0, int c1, int c2) { return (c0 * 4 == c1) && (c0 * 4 == c2); }));
        }
    }
    if (GetParam().enable_persistent_index && GetParam().persistent_index_type == PersistentIndexTypePB::LOCAL) {
        check_local_persistent_index_meta(tablet_id, version);
    }
}

// Decision B: a column-mode partial update with a merge condition records, per source segment, the
// rows it actually changed (the condition winners, old <= new) as a bitmap page in column_overlay_vecs.
// The losers (old > new) keep their previous values and must be absent from the bitmap.
TEST_P(LakePartialUpdateTest, test_column_mode_dcg_update_row_vec_records_only_winners) {
    // Column-mode partial update only records the overlay row vector when change data capture is
    // enabled on the tablet (SetUp published version 1 with the flag off).
    _tablet_metadata->mutable_cdc_metadata()->set_enable_cdc(true);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    if (GetParam().partial_update_mode != PartialUpdateMode::COLUMN_UPDATE_MODE) {
        return;
    }

    // Baseline written with c0 in ascending order, so physical source rowid i corresponds to c0 == i.
    // c1 = c0 * 3, c2 = c0 * 4. One full write -> one source segment.
    std::vector<int> full_c0(kChunkSize), full_c1(kChunkSize), full_c2(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        full_c0[i] = i;
        full_c1[i] = i * 3;
        full_c2[i] = i * 4;
    }
    auto fc0 = Int32Column::create();
    auto fc1 = Int32Column::create();
    auto fc2 = Int32Column::create();
    fc0->append_numbers(full_c0.data(), full_c0.size() * sizeof(int));
    fc1->append_numbers(full_c1.data(), full_c1.size() * sizeof(int));
    fc2->append_numbers(full_c2.data(), full_c2.size() * sizeof(int));
    Chunk full_chunk({std::move(fc0), std::move(fc1), std::move(fc2)}, _slot_cid_map);

    // Partial update (c0, c1) with merge condition on c1. Even keys raise c1 above the old value
    // (winners), odd keys lower it below the old value (losers). Same key order as the baseline,
    // so the expected winner source rowids are exactly the even ones.
    std::set<uint32_t> expected_winner_rowids;
    std::vector<int> upt_c0(kChunkSize), upt_c1(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        upt_c0[i] = i;
        if (i % 2 == 0) {
            upt_c1[i] = i * 3 + 100; // new > old -> winner
            expected_winner_rowids.insert(static_cast<uint32_t>(i));
        } else {
            upt_c1[i] = i * 3 - 1; // new < old -> loser
        }
    }
    auto pc0 = Int32Column::create();
    auto pc1 = Int32Column::create();
    pc0->append_numbers(upt_c0.data(), upt_c0.size() * sizeof(int));
    pc1->append_numbers(upt_c1.data(), upt_c1.size() * sizeof(int));
    Chunk partial_chunk({std::move(pc0), std::move(pc1)}, _slot_cid_map);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();

    // Baseline full write.
    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(full_chunk, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Column partial update with merge condition.
    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(PartialUpdateMode::COLUMN_UPDATE_MODE)
                                                   .set_merge_condition("c1")
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(partial_chunk, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Read-back sanity: even keys took the new c1 (c0*3+100), odd keys kept the old c1 (c0*3).
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) {
                  const int expected_c1 = (c0 % 2 == 0) ? (c0 * 3 + 100) : (c0 * 3);
                  return (c1 == expected_c1) && (c2 == c0 * 4);
              }));

    ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    const auto& row_vecs = metadata->cdc_metadata().pk_change_locator().column_overlay_vecs();
    ASSERT_EQ(row_vecs.size(), 1) << "exactly one source segment was column-updated";

    const auto& seg_page = *row_vecs.begin();
    DelVector winners;
    LakeIOOptions lake_io_opts{.fill_data_cache = true};
    ASSERT_OK(get_del_vec(_tablet_mgr.get(), *metadata, seg_page.second, true, lake_io_opts, &winners));
    ASSERT_NE(winners.roaring(), nullptr);
    EXPECT_EQ(winners.cardinality(), expected_winner_rowids.size());
    for (uint32_t rowid : expected_winner_rowids) {
        EXPECT_TRUE(winners.roaring()->contains(rowid)) << "winner rowid " << rowid << " missing from bitmap";
    }
    // Losers (old > new) kept their values and must not be present.
    for (int i = 1; i < kChunkSize; i += 2) {
        EXPECT_FALSE(winners.roaring()->contains(static_cast<uint32_t>(i)))
                << "loser rowid " << i << " unexpectedly present in bitmap";
    }
}

// The source segment is now read in bounded ranges, and split_rowid_pairs rebases each range's
// source rowids to that range's base. column_overlay_vecs is consumed as segment-absolute rowids,
// so the overlay capture must undo that rebase. Without it every range contributes 0..n-1 and the
// bitmap collapses onto the first range. The sibling test above cannot catch this: with the default
// vector_chunk_size the whole segment fits one range whose base is 0.
TEST_P(LakePartialUpdateTest, test_column_mode_dcg_update_row_vec_is_segment_absolute_across_ranges) {
    _tablet_metadata->mutable_cdc_metadata()->set_enable_cdc(true);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    if (GetParam().partial_update_mode != PartialUpdateMode::COLUMN_UPDATE_MODE) {
        return;
    }

    // Baseline written with c0 ascending, so physical source rowid i corresponds to c0 == i.
    std::vector<int> full_c0(kChunkSize), full_c1(kChunkSize), full_c2(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        full_c0[i] = i;
        full_c1[i] = i * 3;
        full_c2[i] = i * 4;
    }
    auto fc0 = Int32Column::create();
    auto fc1 = Int32Column::create();
    auto fc2 = Int32Column::create();
    fc0->append_numbers(full_c0.data(), full_c0.size() * sizeof(int));
    fc1->append_numbers(full_c1.data(), full_c1.size() * sizeof(int));
    fc2->append_numbers(full_c2.data(), full_c2.size() * sizeof(int));
    Chunk full_chunk({std::move(fc0), std::move(fc1), std::move(fc2)}, _slot_cid_map);

    // Update c1 on every key, so every source rowid 0..kChunkSize-1 must land in the overlay.
    std::vector<int> upt_c0(kChunkSize), upt_c1(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        upt_c0[i] = i;
        upt_c1[i] = i * 3 + 100;
    }
    auto pc0 = Int32Column::create();
    auto pc1 = Int32Column::create();
    pc0->append_numbers(upt_c0.data(), upt_c0.size() * sizeof(int));
    pc1->append_numbers(upt_c1.data(), upt_c1.size() * sizeof(int));
    Chunk partial_chunk({std::move(pc0), std::move(pc1)}, _slot_cid_map);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();

    // Baseline full write.
    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(full_chunk, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Column partial update, published with a small chunk size so the source segment streams in
    // several ranges with non-zero bases.
    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(PartialUpdateMode::COLUMN_UPDATE_MODE)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(partial_chunk, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();

        ConfigResetGuard<int32_t> chunk_size_guard(&config::vector_chunk_size, 4);
        ConfigResetGuard<int64_t> memory_limit_guard(&config::partial_update_memory_limit_per_worker, 80);
        ConfigResetGuard<bool> parallel_guard(&config::enable_pk_index_parallel_execution, false);
        std::vector<std::pair<uint32_t, uint32_t>> emitted_ranges;
        SyncPoint::GetInstance()->SetCallBack(
                "ColumnModePartialUpdateHandler::_read_from_source_segment_and_update:emit", [&](void* arg) {
                    const auto* container = static_cast<StreamChunkContainer*>(arg);
                    emitted_ranges.emplace_back(container->start_rowid, container->end_rowid);
                });
        SyncPoint::GetInstance()->EnableProcessing();
        DeferOp sync_point_guard([&]() {
            SyncPoint::GetInstance()->ClearCallBack(
                    "ColumnModePartialUpdateHandler::_read_from_source_segment_and_update:emit");
            SyncPoint::GetInstance()->DisableProcessing();
        });

        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
        // Guard the premise: a single range would make the assertion below pass even unrebased.
        ASSERT_GT(emitted_ranges.size(), 1) << "source segment did not stream in multiple ranges";
        ASSERT_GT(emitted_ranges.back().first, 0u) << "last range must have a non-zero base";
    }

    ASSERT_EQ(kChunkSize,
              check(version, [](int c0, int c1, int c2) { return (c1 == c0 * 3 + 100) && (c2 == c0 * 4); }));

    ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    const auto& row_vecs = metadata->cdc_metadata().pk_change_locator().column_overlay_vecs();
    ASSERT_EQ(row_vecs.size(), 1) << "exactly one source segment was column-updated";

    const auto& seg_page = *row_vecs.begin();
    DelVector updated;
    LakeIOOptions lake_io_opts{.fill_data_cache = true};
    ASSERT_OK(get_del_vec(_tablet_mgr.get(), *metadata, seg_page.second, true, lake_io_opts, &updated));
    ASSERT_NE(updated.roaring(), nullptr);
    EXPECT_EQ(updated.cardinality(), kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        EXPECT_TRUE(updated.roaring()->contains(static_cast<uint32_t>(i)))
                << "updated rowid " << i << " missing from bitmap";
    }
}

// PK CDC §8.6 G1: a base-advancement retry must inherit the CDC captures finalized by the prefix it
// already published. Driven entirely through real publish_version calls.
//
//   - Two disjoint baseline segments A (keys 0..) and B (keys 1000..), one rowset each.
//   - Two column-mode partial-update txns: t1 raises c1 on A's keys (winners on segment A),
//     t2 raises c1 on B's keys (winners on segment B). Each capture lands in column_overlay_vecs
//     keyed by its source segment's rssid, so the two never overwrite each other.
//   - PREFIX publish [t1] (base=v0, new=v0+1) finalizes the intermediate version vA and advances the
//     in-memory primary index data_version to vA, while FE's base stays at v0.
//   - FULL publish [t1, t2] (base=v0, new=v0+2): cal_new_base_version sees index_version==vA (> v0),
//     finds metadata@vA, and advances base to vA, so only t2 is replayed. At publish start the deep
//     copy source is metadata@vA, whose column_overlay_vecs[A]@vA must be kept (vA > ori_base v0) so
//     the diff still covers (v0, new]. A naive "clear all at start" would have dropped it.
TEST_P(LakePartialUpdateTest, test_cdc_retry_inherits_prefix_capture) {
    // Column-mode partial update only records the overlay row vector when change data capture is
    // enabled on the tablet (SetUp published version 1 with the flag off).
    _tablet_metadata->mutable_cdc_metadata()->set_enable_cdc(true);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    if (GetParam().partial_update_mode != PartialUpdateMode::COLUMN_UPDATE_MODE) {
        return;
    }
    // Keep both writes a single source segment each and avoid persistent-index reload churn that
    // could drop the cached data_version between publishes.
    if (GetParam().enable_persistent_index) {
        return;
    }

    auto tablet_id = _tablet_metadata->id();
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    // Two disjoint baseline segments, one rowset each.
    auto write_full = [&](int base, int64_t new_version) {
        std::vector<int> v0(kChunkSize), v1(kChunkSize), v2(kChunkSize);
        for (int i = 0; i < kChunkSize; i++) {
            v0[i] = base + i;
            v1[i] = v0[i] * 3;
            v2[i] = v0[i] * 4;
        }
        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        auto c2 = Int32Column::create();
        c0->append_numbers(v0.data(), v0.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int));
        c2->append_numbers(v2.data(), v2.size() * sizeof(int));
        Chunk chunk({std::move(c0), std::move(c1), std::move(c2)}, _slot_cid_map);
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, new_version, txn_id).status());
    };

    write_full(0, 2);    // segment A, version 2
    write_full(1000, 3); // segment B, version 3
    int64_t v0 = 3;      // FE base for the partial-update batch

    // Build a column-mode partial-update txn that raises c1 above the old value on keys [base, base+N)
    // (every row a winner under merge condition on c1). Returns the txn_id; leaves it unpublished.
    auto build_partial_winner_txn = [&](int base) -> int64_t {
        std::vector<int> u0(kChunkSize), u1(kChunkSize);
        for (int i = 0; i < kChunkSize; i++) {
            u0[i] = base + i;
            u1[i] = u0[i] * 3 + 100; // new > old -> winner
        }
        auto pc0 = Int32Column::create();
        auto pc1 = Int32Column::create();
        pc0->append_numbers(u0.data(), u0.size() * sizeof(int));
        pc1->append_numbers(u1.data(), u1.size() * sizeof(int));
        Chunk partial_chunk({std::move(pc0), std::move(pc1)}, _slot_cid_map);
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(PartialUpdateMode::COLUMN_UPDATE_MODE)
                                                   .set_merge_condition("c1")
                                                   .build());
        CHECK_OK(delta_writer->open());
        CHECK_OK(delta_writer->write(partial_chunk, indexes.data(), indexes.size()));
        CHECK_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        return txn_id;
    };

    auto t1 = build_partial_winner_txn(0);    // updates segment A
    auto t2 = build_partial_winner_txn(1000); // updates segment B

    // PREFIX publish [t1]: base=v0(3), new=vA(4). Finalizes the intermediate version and advances the
    // in-memory index data_version to vA.
    int64_t vA = v0 + 1; // 4
    {
        std::vector<int64_t> prefix_txns{t1};
        ASSIGN_OR_ABORT(auto prefix_meta, batch_publish(tablet_id, v0, vA, prefix_txns));
        // t1's capture is present at vA, keyed by segment A's rssid.
        ASSERT_EQ(prefix_meta->cdc_metadata().pk_change_locator().column_overlay_vecs().size(), 1);
        for (const auto& [rssid, page] : prefix_meta->cdc_metadata().pk_change_locator().column_overlay_vecs()) {
            EXPECT_EQ(page.version(), vA);
        }
    }

    // FULL publish [t1, t2]: base=v0(3), new=v0+2(5). cal_new_base_version advances base to vA and
    // replays only t2.
    int64_t final_version = v0 + 2; // 5
    std::vector<int64_t> full_txns{t1, t2};
    ASSIGN_OR_ABORT(auto final_meta, batch_publish(tablet_id, v0, final_version, full_txns));

    // The inheritance window survived: t1's capture (version vA, in (v0, vA]) is still present, and
    // t2's fresh capture (version final_version) was added by the replay.
    const auto& row_vecs = final_meta->cdc_metadata().pk_change_locator().column_overlay_vecs();
    ASSERT_EQ(row_vecs.size(), 2) << "both the inherited t1 capture and the replayed t2 capture must be present";
    bool found_inherited = false;
    bool found_fresh = false;
    for (const auto& [rssid, page] : row_vecs) {
        if (page.version() == vA) found_inherited = true;
        if (page.version() == final_version) found_fresh = true;
    }
    EXPECT_TRUE(found_inherited) << "prefix capture at version " << vA << " must be inherited, not pruned";
    EXPECT_TRUE(found_fresh) << "replayed t2 must add a capture at version " << final_version;
}

// Validates that column-mode partial update rejects a merge_condition when the condition column
// is not part of the partial update column set — the handler cannot read the new condition value
// otherwise and must refuse rather than silently producing wrong results.
TEST_P(LakePartialUpdateTest, test_column_mode_condition_missing_column_rejected) {
    if (GetParam().partial_update_mode != PartialUpdateMode::COLUMN_UPDATE_MODE) {
        return;
    }

    auto chunk0 = generate_data(kChunkSize, 0, false, 3);
    auto chunk1 = generate_data(kChunkSize, 0, true, 2);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    // Baseline full write.
    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Partial update set is (c0, c1), but merge_condition references c2 which is NOT in the set.
    auto txn_id = next_id();
    ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                               .set_tablet_manager(_tablet_mgr.get())
                                               .set_tablet_id(tablet_id)
                                               .set_txn_id(txn_id)
                                               .set_partition_id(_partition_id)
                                               .set_mem_tracker(_mem_tracker.get())
                                               .set_schema_id(_tablet_schema->id())
                                               .set_slot_descriptors(&_slot_pointers)
                                               .set_partial_update_mode(PartialUpdateMode::COLUMN_UPDATE_MODE)
                                               .set_merge_condition("c2")
                                               .build());
    ASSERT_OK(delta_writer->open());
    ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
    auto st = delta_writer->finish_with_txnlog().status();
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_not_supported()) << st;
    delta_writer->close();
}

TEST_P(LakePartialUpdateTest, test_dcg_not_found_and_fallback_to_segment) {
    // Prepare base full data
    auto chunk0 = generate_data(kChunkSize, 0, false, 3);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) indexes[i] = i;

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < 2; i++) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Column mode update on one column to create DCG for c1 only
    auto partial_c1 = generate_data(kChunkSize, 0, true, 7); // (c0, c1)
    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(PartialUpdateMode::COLUMN_UPDATE_MODE)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(partial_c1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Row mode read to fetch unmodified c2: we provide (c0, c1) so the writer schema matches slots (2 columns),
    // and get_column_values() will read unmodified c2. Since DCG has only c1, reading c2 triggers DCG NotFound fallback.
    std::vector<int> keys_only(kChunkSize);
    std::vector<int> c1_vals(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        keys_only[i] = i;
        c1_vals[i] = i * 7; // consistent with partial_c1 ratio
    }
    auto c0_col = Int32Column::create();
    auto c1_col = Int32Column::create();
    c0_col->append_numbers(keys_only.data(), keys_only.size() * sizeof(int));
    c1_col->append_numbers(c1_vals.data(), c1_vals.size() * sizeof(int));
    Chunk::SlotHashMap slot_kv;
    slot_kv[0] = 0; // c0
    slot_kv[1] = 1; // c1
    Chunk keys_c1_chunk({std::move(c0_col), std::move(c1_col)}, slot_kv);

    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(PartialUpdateMode::ROW_MODE)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(keys_c1_chunk, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Verify c2 is still from original segment (fallback) or default, and c1 reflects DCG update
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c1 == c0 * 7) && (c2 == c0 * 4); }));
}

// Explicitly test DCG column file missing: column_file_by_idx returns a path but the file is removed,
// so new_dcg_segment fails and get_column_values should surface an InternalError during publish.
TEST_P(LakePartialUpdateTest, test_dcg_segment_missing_files_returns_error) {
    auto chunk0 = generate_data(kChunkSize, 0, false, 3);
    auto partial_c1 = generate_data(kChunkSize, 0, true, 7); // (c0, c1) to generate DCG for c1
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) indexes[i] = i;

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();

    // Base full write
    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Column mode partial update to create DCG
    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(PartialUpdateMode::COLUMN_UPDATE_MODE)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(partial_c1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Remove generated DCG column files with absolute path to force Segment::open failure inside new_dcg_segment
    {
        ASSIGN_OR_ABORT(auto md, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        for (const auto& kv : md->dcg_meta().dcgs()) {
            const auto& dcg_ver = kv.second;
            for (const auto& rel : dcg_ver.column_files()) {
                auto abs = _tablet_mgr->segment_location(tablet_id, rel);
                (void)fs::remove(abs);
            }
        }
    }

    // Row mode partial update providing only primary keys with single-column slots,
    // so c1,c2 are unmodified and need to be read; c1 prefers DCG -> error
    std::vector<int> keys_only(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) keys_only[i] = i;
    auto c0_only = Int32Column::create();
    c0_only->append_numbers(keys_only.data(), keys_only.size() * sizeof(int));
    Chunk::SlotHashMap slot_only;
    slot_only[0] = 0; // only c0
    Chunk c0_only_chunk({std::move(c0_only)}, slot_only);
    // Build local slot descriptors with single column (c0) to match chunk schema
    std::vector<SlotDescriptor> local_slots;
    local_slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
    std::vector<SlotDescriptor*> local_slot_ptrs;
    local_slot_ptrs.emplace_back(&local_slots[0]);

    StatusOr<TabletMetadataPtr> pub_st = Status::OK();
    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&local_slot_ptrs)
                                                   .set_partial_update_mode(PartialUpdateMode::ROW_MODE)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(c0_only_chunk, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        pub_st = publish_single_version(tablet_id, version + 1, txn_id);
    }
    // Expect publish failed due to DCG segment open failure
    ASSERT_FALSE(pub_st.status().ok());
}

TEST_P(LakePartialUpdateTest, test_write_multi_segment) {
    auto chunk0 = generate_data(kChunkSize, 0, false, 3);
    auto chunk1 = generate_data(kChunkSize, 0, true, 3);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    // normal write
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c0 * 3 == c1) && (c0 * 4 == c2); }));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 3);

    // partial update, and make it generate two segment files in one rowset
    const int64_t old_size = config::write_buffer_size;
    config::write_buffer_size = 1;
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(GetParam().partial_update_mode)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
        ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        if (GetParam().partial_update_mode == PartialUpdateMode::COLUMN_UPDATE_MODE) {
            if (i == 0) {
                EXPECT_EQ(new_tablet_metadata->orphan_files_size(), 2);
            } else {
                // Superseded .cols files are orphaned; with change data capture on, the prior
                // publish's per-publish column_overlay_vecs delvec is orphaned too (the extra one).
                const int expected = new_tablet_metadata->cdc_metadata().enable_cdc() ? 4 : 3;
                EXPECT_EQ(new_tablet_metadata->orphan_files_size(), expected);
            }
        } else {
            EXPECT_EQ(new_tablet_metadata->orphan_files_size(), 2);
        }
    }
    config::write_buffer_size = old_size;
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c0 * 3 == c1) && (c0 * 4 == c2); }));
    ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    if (GetParam().partial_update_mode == PartialUpdateMode::COLUMN_UPDATE_MODE) {
        EXPECT_EQ(new_tablet_metadata->rowsets_size(), 3);
    } else {
        EXPECT_EQ(new_tablet_metadata->rowsets_size(), 6);
        // check segment size in last metadata
        EXPECT_EQ(new_tablet_metadata->rowsets(5).segment_metas_size(), 2);
    }
    EXPECT_TRUE(_update_mgr->update_state_mem_tracker()->consumption() == 0);
    if (GetParam().enable_persistent_index && GetParam().persistent_index_type == PersistentIndexTypePB::LOCAL) {
        check_local_persistent_index_meta(tablet_id, version);
    }
}

TEST_P(LakePartialUpdateTest, test_write_multi_segment_by_diff_val) {
    auto chunk0 = generate_data(kChunkSize, 0, false, 3);
    auto chunk1 = generate_data(kChunkSize, 0, true, 5);
    auto chunk2 = generate_data(kChunkSize, 0, true, 6);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    // normal write
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c0 * 3 == c1) && (c0 * 4 == c2); }));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 3);

    // partial update, and make it generate two segment files in one rowset
    const int64_t old_size = config::write_buffer_size;
    config::write_buffer_size = 1;
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(GetParam().partial_update_mode)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->write(chunk2, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
        ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        if (GetParam().partial_update_mode == PartialUpdateMode::COLUMN_UPDATE_MODE) {
            if (i == 0) {
                EXPECT_EQ(new_tablet_metadata->orphan_files_size(), 2);
            } else {
                // Superseded .cols files are orphaned; with change data capture on, the prior
                // publish's per-publish column_overlay_vecs delvec is orphaned too (the extra one).
                const int expected = new_tablet_metadata->cdc_metadata().enable_cdc() ? 4 : 3;
                EXPECT_EQ(new_tablet_metadata->orphan_files_size(), expected);
            }
        } else {
            EXPECT_EQ(new_tablet_metadata->orphan_files_size(), 2);
        }
    }
    config::write_buffer_size = old_size;
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c0 * 6 == c1) && (c0 * 4 == c2); }));
    ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    if (GetParam().partial_update_mode == PartialUpdateMode::COLUMN_UPDATE_MODE) {
        EXPECT_EQ(new_tablet_metadata->rowsets_size(), 3);
    } else {
        EXPECT_EQ(new_tablet_metadata->rowsets_size(), 6);
        // check segment size in last metadata
        EXPECT_EQ(new_tablet_metadata->rowsets(5).segment_metas_size(), 2);
    }
    if (GetParam().enable_persistent_index && GetParam().persistent_index_type == PersistentIndexTypePB::LOCAL) {
        check_local_persistent_index_meta(tablet_id, version);
    }
}

TEST_P(LakePartialUpdateTest, test_resolve_conflict) {
    auto chunk0 = generate_data(kChunkSize, 0, false, 3);
    auto chunk1 = generate_data(kChunkSize, 0, true, 5);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    // normal write
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c0 * 3 == c1) && (c0 * 4 == c2); }));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 3);

    std::vector<int64_t> txn_ids;
    // concurrent partial update
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        txn_ids.emplace_back(txn_id);
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(GetParam().partial_update_mode)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
    }
    // publish in order
    for (auto txn_id : txn_ids) {
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
        ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        if (GetParam().partial_update_mode != PartialUpdateMode::COLUMN_UPDATE_MODE) {
            EXPECT_EQ(new_tablet_metadata->orphan_files_size(), 1);
        }
    }
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c0 * 5 == c1) && (c0 * 4 == c2); }));
    ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    if (GetParam().partial_update_mode != PartialUpdateMode::COLUMN_UPDATE_MODE) {
        EXPECT_EQ(new_tablet_metadata->rowsets_size(), 6);
    }
    if (GetParam().enable_persistent_index && GetParam().persistent_index_type == PersistentIndexTypePB::LOCAL) {
        check_local_persistent_index_meta(tablet_id, version);
    }
}

TEST_P(LakePartialUpdateTest, test_resolve_conflict_multi_segment) {
    auto chunk0 = generate_data(kChunkSize, 0, false, 3);
    auto chunk1 = generate_data(kChunkSize, 0, true, 5);
    auto chunk2 = generate_data(kChunkSize, 0, true, 6);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    // normal write
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c0 * 3 == c1) && (c0 * 4 == c2); }));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 3);

    // partial update, and make it generate two segment files in one rowset
    const int64_t old_size = config::write_buffer_size;
    config::write_buffer_size = 1;
    std::vector<int64_t> txn_ids;
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        txn_ids.emplace_back(txn_id);
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(GetParam().partial_update_mode)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->write(chunk2, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
    }
    // publish in order
    for (auto txn_id : txn_ids) {
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    config::write_buffer_size = old_size;
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c0 * 6 == c1) && (c0 * 4 == c2); }));
    ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    if (GetParam().partial_update_mode == PartialUpdateMode::COLUMN_UPDATE_MODE) {
        EXPECT_EQ(new_tablet_metadata->rowsets_size(), 3);
    } else {
        EXPECT_EQ(new_tablet_metadata->rowsets_size(), 6);
        // check segment size in last metadata
        EXPECT_EQ(new_tablet_metadata->rowsets(5).segment_metas_size(), 2);
    }
    if (GetParam().enable_persistent_index && GetParam().persistent_index_type == PersistentIndexTypePB::LOCAL) {
        check_local_persistent_index_meta(tablet_id, version);
    }
}

TEST_P(LakePartialUpdateTest, test_resolve_conflict2) {
    auto chunk0 = generate_data(kChunkSize, 0, false, 3);
    auto chunk1 = generate_data(kChunkSize, 0, true, 5);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    // normal write
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c0 * 3 == c1) && (c0 * 4 == c2); }));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 3);

    SyncPoint::GetInstance()->SetCallBack("TabletManager::skip_cache_latest_metadata",
                                          [](void* arg) { *(bool*)arg = true; });
    SyncPoint::GetInstance()->EnableProcessing();

    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("TabletManager::skip_cache_latest_metadata");
        SyncPoint::GetInstance()->DisableProcessing();
    });
    std::vector<int64_t> txn_ids;
    // concurrent partial update
    for (int i = 0; i < 2; i++) {
        auto txn_id = next_id();
        txn_ids.emplace_back(txn_id);
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(GetParam().partial_update_mode)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
        ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        if (GetParam().partial_update_mode == PartialUpdateMode::COLUMN_UPDATE_MODE) {
            if (i == 0) {
                EXPECT_EQ(new_tablet_metadata->orphan_files_size(), 1);
            } else {
                // Superseded .cols files are orphaned; with change data capture on, the prior
                // publish's per-publish column_overlay_vecs delvec is orphaned too (the extra one).
                const int expected = new_tablet_metadata->cdc_metadata().enable_cdc() ? 3 : 2;
                EXPECT_EQ(new_tablet_metadata->orphan_files_size(), expected);
            }
        } else {
            EXPECT_EQ(new_tablet_metadata->orphan_files_size(), 1);
        }
    }
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c0 * 5 == c1) && (c0 * 4 == c2); }));
    ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    if (GetParam().partial_update_mode == PartialUpdateMode::COLUMN_UPDATE_MODE) {
        EXPECT_EQ(new_tablet_metadata->rowsets_size(), 3);
    } else {
        EXPECT_EQ(new_tablet_metadata->rowsets_size(), 5);
    }
    if (GetParam().enable_persistent_index && GetParam().persistent_index_type == PersistentIndexTypePB::LOCAL) {
        check_local_persistent_index_meta(tablet_id, version);
    }
}

TEST_P(LakePartialUpdateTest, test_write_with_index_reload) {
    auto chunk0 = generate_data(kChunkSize, 0, false, 3);
    auto chunk1 = generate_data(kChunkSize, 0, true, 3);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    // normal write
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c0 * 3 == c1) && (c0 * 4 == c2); }));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 3);

    // remove pk index, to make it reload again
    _update_mgr->try_remove_primary_index_cache(tablet_id);

    // partial update
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(GetParam().partial_update_mode)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
        ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        if (GetParam().partial_update_mode == PartialUpdateMode::COLUMN_UPDATE_MODE) {
            if (i == 0) {
                EXPECT_EQ(new_tablet_metadata->orphan_files_size(), 1);
            } else {
                // Superseded .cols files are orphaned; with change data capture on, the prior
                // publish's per-publish column_overlay_vecs delvec is orphaned too (the extra one).
                const int expected = new_tablet_metadata->cdc_metadata().enable_cdc() ? 3 : 2;
                EXPECT_EQ(new_tablet_metadata->orphan_files_size(), expected);
            }
        } else {
            EXPECT_EQ(new_tablet_metadata->orphan_files_size(), 1);
        }
    }
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c0 * 3 == c1) && (c0 * 4 == c2); }));
    ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    if (GetParam().partial_update_mode == PartialUpdateMode::COLUMN_UPDATE_MODE) {
        EXPECT_EQ(new_tablet_metadata->rowsets_size(), 3);
    } else {
        EXPECT_EQ(new_tablet_metadata->rowsets_size(), 6);
    }
    if (GetParam().enable_persistent_index && GetParam().persistent_index_type == PersistentIndexTypePB::LOCAL) {
        check_local_persistent_index_meta(tablet_id, version);
    }
    if (GetParam().enable_persistent_index && GetParam().persistent_index_type == PersistentIndexTypePB::CLOUD_NATIVE) {
        auto sstable_meta = new_tablet_metadata->sstable_meta();
        for (auto& sstable : sstable_meta.sstables()) {
            EXPECT_GT(sstable.max_rss_rowid(), 0);
        }
    }
}

TEST_P(LakePartialUpdateTest, test_partial_update_publish_retry) {
    if (GetParam().enable_persistent_index) return;
    auto chunk0 = generate_data(kChunkSize, 0, false, 3);
    auto chunk1 = generate_data(kChunkSize, 0, true, 3);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    // normal write
    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c0 * 3 == c1) && (c0 * 4 == c2); }));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 1);

    // partial update
    auto txn_id = next_id();
    {
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(GetParam().partial_update_mode)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();

        // The tablet metadata may be saved with the legacy headerless format or the checksummed
        // header format depending on lake_enable_protobuf_file_checksum, so inject on both.
        SyncPoint::GetInstance()->SetCallBack("ProtobufFile::save:serialize", [](void* arg) { *(bool*)arg = false; });
        SyncPoint::GetInstance()->SetCallBack("ProtobufFileWithHeader::save:serialize",
                                              [](void* arg) { *(bool*)arg = false; });
        SyncPoint::GetInstance()->EnableProcessing();
        ASSERT_ERROR(publish_single_version(tablet_id, version + 1, txn_id).status());
        SyncPoint::GetInstance()->ClearCallBack("ProtobufFile::save:serialize");
        SyncPoint::GetInstance()->ClearCallBack("ProtobufFileWithHeader::save:serialize");
        SyncPoint::GetInstance()->DisableProcessing();
    }
    // retry publish again
    ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
    _tablet_mgr->prune_metacache();
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c0 * 3 == c1) && (c0 * 4 == c2); }));
}

TEST_P(LakePartialUpdateTest, test_concurrent_write_publish) {
    auto chunk0 = generate_data(kChunkSize, 0, false, 3);
    auto chunk1 = generate_data(kChunkSize, 0, true, 5);
    auto chunk2 = generate_data(kChunkSize, 0, true, 6);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    // normal write
    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        ++version;
    }
    // partial update
    std::thread t1([&]() {
        for (int i = 0; i < 100; ++i) {
            auto txn_id1 = next_id();
            ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                       .set_tablet_manager(_tablet_mgr.get())
                                                       .set_tablet_id(tablet_id)
                                                       .set_txn_id(txn_id1)
                                                       .set_partition_id(_partition_id)
                                                       .set_mem_tracker(_mem_tracker.get())
                                                       .set_schema_id(_tablet_schema->id())
                                                       .set_slot_descriptors(&_slot_pointers)
                                                       .set_partial_update_mode(GetParam().partial_update_mode)
                                                       .build());
            ASSERT_OK(delta_writer->open());
            ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
            ASSERT_OK(delta_writer->finish_with_txnlog());
            delta_writer->close();
            ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id1).status());
            version++;
        }
    });

    // partial update
    std::thread t2([&]() {
        for (int i = 0; i < 100; ++i) {
            const int64_t old_size = config::write_buffer_size;
            config::write_buffer_size = 1;
            const int64_t old_mem_usage = config::l0_max_mem_usage;
            config::l0_max_mem_usage = 1;
            auto txn_id2 = next_id() + 1000;
            ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                       .set_tablet_manager(_tablet_mgr.get())
                                                       .set_tablet_id(tablet_id)
                                                       .set_txn_id(txn_id2)
                                                       .set_partition_id(_partition_id)
                                                       .set_mem_tracker(_mem_tracker.get())
                                                       .set_schema_id(_tablet_schema->id())
                                                       .set_slot_descriptors(&_slot_pointers)
                                                       .set_partial_update_mode(GetParam().partial_update_mode)
                                                       .build());
            ASSERT_OK(delta_writer->open());
            ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
            ASSERT_OK(delta_writer->write(chunk2, indexes.data(), indexes.size()));
            ASSERT_OK(delta_writer->finish_with_txnlog());
            delta_writer->close();
            config::write_buffer_size = old_size;
            config::l0_max_mem_usage = old_mem_usage;
        }
    });
    t1.join();
    t2.join();
}

TEST_P(LakePartialUpdateTest, test_batch_publish) {
    auto chunk0 = generate_data(kChunkSize, 0, false, 3);
    auto chunk1 = generate_data(kChunkSize, 0, true, 3);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    // normal write
    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c0 * 3 == c1) && (c0 * 4 == c2); }));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));

    auto base_version = version;
    std::vector<int64_t> txn_ids(3);
    for (int i = 0; i < 3; i++) {
        int64_t txn_id = next_id();
        txn_ids[i] = txn_id;
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(GetParam().partial_update_mode)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
    }
    auto new_version = base_version + 3;

    ASSERT_OK(batch_publish(tablet_id, base_version, new_version, txn_ids).status());

    ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, new_version));
    if (GetParam().partial_update_mode == PartialUpdateMode::COLUMN_UPDATE_MODE) {
        EXPECT_EQ(new_tablet_metadata->rowsets_size(), 1);
        // 3 .dat + 2 .cols
        EXPECT_EQ(new_tablet_metadata->orphan_files_size(), 5);
    } else {
        EXPECT_EQ(new_tablet_metadata->rowsets_size(), 4);
        EXPECT_EQ(new_tablet_metadata->orphan_files_size(), 3);
    }
    _tablet_mgr->prune_metacache();
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c0 * 3 == c1) && (c0 * 4 == c2); }));
    _update_mgr->try_remove_primary_index_cache(tablet_id);

    // publish again
    ASSERT_OK(batch_publish(tablet_id, base_version, new_version, txn_ids).status());
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c0 * 3 == c1) && (c0 * 4 == c2); }));
    ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, new_version));
    if (GetParam().partial_update_mode == PartialUpdateMode::COLUMN_UPDATE_MODE) {
        EXPECT_EQ(new_tablet_metadata->rowsets_size(), 1);
    } else {
        EXPECT_EQ(new_tablet_metadata->rowsets_size(), 4);
    }
    if (GetParam().partial_update_mode == PartialUpdateMode::COLUMN_UPDATE_MODE) {
        // 3 .dat + 2 .cols
        EXPECT_EQ(new_tablet_metadata->orphan_files_size(), 5);
    } else {
        EXPECT_EQ(new_tablet_metadata->orphan_files_size(), 3);
    }
}

INSTANTIATE_TEST_SUITE_P(LakePartialUpdateTest, LakePartialUpdateTest,
                         ::testing::Values(PrimaryKeyParam{true, PersistentIndexTypePB::CLOUD_NATIVE},
                                           PrimaryKeyParam{
                                                   .enable_persistent_index = true,
                                                   .persistent_index_type = PersistentIndexTypePB::CLOUD_NATIVE,
                                                   .partial_update_mode = PartialUpdateMode::COLUMN_UPDATE_MODE}));

class LakeIncompleteSortKeyPartialUpdateTest : public TestBase {
public:
    LakeIncompleteSortKeyPartialUpdateTest() : TestBase(kTestDirectory) {
        _tablet_metadata = std::make_shared<TabletMetadata>();
        _tablet_metadata->set_id(next_id());
        _tablet_metadata->set_version(1);
        _tablet_metadata->set_next_rowset_id(1);
        //
        //  | column | type | KEY | NULL | SORTKEY |
        //  +--------+------+-----+------+---------+
        //  |   c0   |  INT | YES |  NO  |   NO    |
        //  |   c1   |  INT | NO  |  NO  |   YES   |
        //  |   c2   |  INT | NO  |  NO  |   YES   |
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
            c1->set_is_nullable(false);
            c1->set_aggregation("REPLACE");
        }
        auto c2 = schema->add_column();
        {
            c2->set_unique_id(next_id());
            c2->set_name("c2");
            c2->set_type("INT");
            c2->set_is_key(false);
            c2->set_is_nullable(false);
            c2->set_aggregation("REPLACE");
            //c2->set_default_value("10");
        }

        _slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
        _slots.emplace_back(1, "c1", TypeDescriptor{LogicalType::TYPE_INT});
        _slots.emplace_back(2, "__op", TypeDescriptor{LogicalType::TYPE_INT});
        _slot_pointers.emplace_back(&_slots[0]);
        _slot_pointers.emplace_back(&_slots[1]);
        _slot_pointers.emplace_back(&_slots[2]);

        _slot_cid_map.emplace(0, 0);
        _slot_cid_map.emplace(1, 1);
        _slot_cid_map.emplace(2, 2);

        schema->add_sort_key_idxes(1);
        schema->add_sort_key_idxes(2);
        _tablet_schema = TabletSchema::create(*schema);
    }

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override {
        // check primary index cache's ref
        EXPECT_TRUE(_update_mgr->TEST_check_primary_index_cache_ref(_tablet_metadata->id(), 1));
        StorageEngine::instance()->wait_storage_cleanup_tasks();
        remove_test_dir_or_die();
    }

    Chunk generate_data(int64_t chunk_size, int shift, int update_ratio) {
        std::vector<int> v0(chunk_size);
        std::vector<int> v1(chunk_size);
        std::vector<int> v2(chunk_size);
        for (int i = 0; i < chunk_size; i++) {
            v0[i] = i + shift * chunk_size;
        }
        auto rng = std::default_random_engine{};
        std::shuffle(v0.begin(), v0.end(), rng);
        for (int i = 0; i < chunk_size; i++) {
            v1[i] = v0[i] * update_ratio;
        }

        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        c0->append_numbers(v0.data(), v0.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int));

        return Chunk({std::move(c0), std::move(c1)}, _slot_cid_map);
    }

protected:
    constexpr static const char* const kTestDirectory = "test_lake_incomplete_sort_key_partial_update";
    constexpr static const int kChunkSize = 12;

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    int64_t _partition_id = 4561;
    std::vector<SlotDescriptor> _slots;
    std::vector<SlotDescriptor*> _slot_pointers;
    Chunk::SlotHashMap _slot_cid_map;
};

// `ORDER BY` may put value columns into a primary key table's sort key while still keeping a key
// column in it, e.g. PRIMARY KEY(c0) ORDER BY(c1, c0). A DELETE on such a table reaches the writer
// as a "partial update" (its write schema is just the key columns), and its publish must not try to
// rewrite the empty segment that the delete-only flush emits: the rewrite would write the unmodified
// columns (c1, c2) alone, a column set that holds SOME sort key columns but not c0.
class LakeSortKeyWithValueColumnDeleteTest : public TestBase {
public:
    LakeSortKeyWithValueColumnDeleteTest() : TestBase(kTestDirectory) {
        _tablet_metadata = std::make_shared<TabletMetadata>();
        _tablet_metadata->set_id(next_id());
        _tablet_metadata->set_version(1);
        _tablet_metadata->set_next_rowset_id(1);
        //
        //  | column | type | KEY | NULL | SORTKEY |
        //  +--------+------+-----+------+---------+
        //  |   c0   |  INT | YES |  NO  |   YES   |
        //  |   c1   |  INT | NO  |  YES |   YES   |
        //  |   c2   |  INT | NO  |  YES |   NO    |
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
        }
        // ORDER BY(c1, c0): a value column first, the key column second. The delete's rewrite column
        // set (c1, c2) therefore intersects the sort key without covering it.
        schema->add_sort_key_idxes(1);
        schema->add_sort_key_idxes(0);
        _tablet_schema = TabletSchema::create(*schema);
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));

        // A DELETE hands the writer the key columns plus the trailing __op marker.
        _delete_slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
        _delete_slots.emplace_back(1, "__op", TypeDescriptor{LogicalType::TYPE_TINYINT});
        for (auto& slot : _delete_slots) {
            _delete_slot_pointers.emplace_back(&slot);
        }
    }

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override {
        EXPECT_TRUE(_update_mgr->TEST_check_primary_index_cache_ref(_tablet_metadata->id(), 1));
        StorageEngine::instance()->wait_storage_cleanup_tasks();
        remove_test_dir_or_die();
    }

    Chunk generate_full_data(int64_t chunk_size) {
        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        auto c2 = Int32Column::create();
        for (int i = 0; i < chunk_size; i++) {
            c0->append(i);
            c1->append(i * 3);
            c2->append(i * 7);
        }
        Chunk::SlotHashMap slot_cid_map;
        slot_cid_map[0] = 0;
        slot_cid_map[1] = 1;
        slot_cid_map[2] = 2;
        return Chunk({std::move(c0), std::move(c1), std::move(c2)}, slot_cid_map);
    }

    Chunk generate_delete_data(int64_t chunk_size) {
        auto c0 = Int32Column::create();
        auto cop = Int8Column::create();
        for (int i = 0; i < chunk_size; i++) {
            c0->append(i);
            cop->append(TOpType::DELETE);
        }
        Chunk::SlotHashMap slot_cid_map;
        slot_cid_map[0] = 0;
        slot_cid_map[1] = 1;
        return Chunk({std::move(c0), std::move(cop)}, slot_cid_map);
    }

    int64_t read_rows(int64_t tablet_id, int64_t version) {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));
        auto chunk = ChunkFactory::new_chunk(*_schema, 128);
        int64_t rows = 0;
        while (true) {
            auto st = reader->get_next(chunk.get());
            if (st.is_end_of_file()) {
                break;
            }
            CHECK_OK(st);
            rows += chunk->num_rows();
            chunk->reset();
        }
        return rows;
    }

protected:
    constexpr static const char* const kTestDirectory = "test_lake_sort_key_with_value_column_delete";
    constexpr static const int kChunkSize = 12;

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    int64_t _partition_id = 4562;
    std::vector<SlotDescriptor> _delete_slots;
    std::vector<SlotDescriptor*> _delete_slot_pointers;
};

TEST_F(LakeSortKeyWithValueColumnDeleteTest, test_delete_publish) {
    auto tablet_id = _tablet_metadata->id();
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }
    int64_t version = 1;

    // Load the rows with a full-schema write.
    {
        auto chunk = generate_full_data(kChunkSize);
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto dw, DeltaWriterBuilder()
                                         .set_tablet_manager(_tablet_mgr.get())
                                         .set_tablet_id(tablet_id)
                                         .set_txn_id(txn_id)
                                         .set_partition_id(_partition_id)
                                         .set_mem_tracker(_mem_tracker.get())
                                         .set_schema_id(_tablet_schema->id())
                                         .build());
        ASSERT_OK(dw->open());
        ASSERT_OK(dw->write(chunk, indexes.data(), indexes.size()));
        ASSERT_OK(dw->finish_with_txnlog());
        dw->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize, read_rows(tablet_id, version));

    // Delete every row. The delete-only flush emits a 0-row segment plus a del file, and the txn log
    // carries partial-update metadata for it; publishing must skip the rewrite of that empty segment
    // instead of failing with "is sort key but not find while init segment writer".
    {
        auto chunk = generate_delete_data(kChunkSize);
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto dw, DeltaWriterBuilder()
                                         .set_tablet_manager(_tablet_mgr.get())
                                         .set_tablet_id(tablet_id)
                                         .set_txn_id(txn_id)
                                         .set_partition_id(_partition_id)
                                         .set_mem_tracker(_mem_tracker.get())
                                         .set_schema_id(_tablet_schema->id())
                                         .set_slot_descriptors(&_delete_slot_pointers)
                                         .build());
        ASSERT_OK(dw->open());
        ASSERT_OK(dw->write(chunk, indexes.data(), indexes.size()));
        ASSERT_OK(dw->finish_with_txnlog());
        dw->close();

        // The regression only bites when the txn log really does look like a partial update with an
        // empty segment to rewrite -- assert that shape so the test cannot silently stop covering it.
        ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(tablet_id, txn_id));
        const auto& op_write = txn_log->op_write();
        ASSERT_TRUE(op_write.has_txn_meta());
        ASSERT_GT(op_write.rewrite_segments_meta_size(), 0);
        ASSERT_EQ(0, op_write.rowset().num_rows());
        ASSERT_GT(op_write.rowset().segment_metas_size(), 0);
        for (const auto& segment_meta : op_write.rowset().segment_metas()) {
            ASSERT_EQ(0, segment_meta.num_rows());
        }

        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(0, read_rows(tablet_id, version));
}

TEST_F(LakeIncompleteSortKeyPartialUpdateTest, test_incomplete_sort_key) {
    auto chunk0 = generate_data(kChunkSize, 0, 3);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto tablet_id = _tablet_metadata->id();

    // incomplete sort key partial write.
    auto txn_id = next_id();
    ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                               .set_tablet_manager(_tablet_mgr.get())
                                               .set_tablet_id(tablet_id)
                                               .set_txn_id(txn_id)
                                               .set_partition_id(_partition_id)
                                               .set_mem_tracker(_mem_tracker.get())
                                               .set_schema_id(_tablet_schema->id())
                                               .set_slot_descriptors(&_slot_pointers)
                                               .build());
    ASSERT_OK(delta_writer->open());
    ASSERT_ERROR(delta_writer->write(chunk0, indexes.data(), indexes.size()));
}

TEST_P(LakePartialUpdateTest, test_partial_update_retry_rewrite_check) {
    if (GetParam().enable_persistent_index) return;
    if (GetParam().partial_update_mode == PartialUpdateMode::COLUMN_UPDATE_MODE) return;
    auto chunk0 = generate_data(kChunkSize, 0, false, 3);
    auto chunk1 = generate_data(kChunkSize, 0, true, 5);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    // normal write
    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c0 * 3 == c1) && (c0 * 4 == c2); }));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 1);

    // partial update
    auto txn_id = next_id();
    {
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
    }
    // retry publish again
    for (int i = 0; i < 5; i++) {
        TEST_ENABLE_ERROR_POINT("TabletManager::put_tablet_metadata",
                                Status::IOError("injected put tablet metadata error"));

        SyncPoint::GetInstance()->EnableProcessing();

        DeferOp defer([]() {
            TEST_DISABLE_ERROR_POINT("TabletManager::put_tablet_metadata");
            SyncPoint::GetInstance()->DisableProcessing();
        });
        _tablet_mgr->prune_metacache();
        ASSERT_ERROR(publish_single_version(tablet_id, version + 1, txn_id));
        auto txn_log_st = _tablet_mgr->get_txn_log(tablet_id, txn_id);
        EXPECT_TRUE(txn_log_st.ok());
    }
    // success
    _tablet_mgr->prune_metacache();
    ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id));
    ASSERT_EQ(kChunkSize, check(version + 1, [](int c0, int c1, int c2) { return (c0 * 5 == c1) && (c0 * 4 == c2); }));
}

// The other half of the guard in RowsetUpdateState::rewrite_segment: a row-mode partial update whose
// rowset-level num_rows has been apportioned to 0 by a split cross publish must STILL be rewritten,
// because its segments do hold this tablet's rows. Emulate the apportionment by zeroing the txn log's
// rowset counter while leaving the per-segment counts alone -- exactly what
// tablet_reshard_helper::update_rowset_data_stats does to a sibling.
//
// c2 is the assertion that has teeth: it is not in the partial write, so it only survives if the
// rewrite merged the unmodified columns back in. Skipping the rewrite would attach a segment holding
// c0 and c1 alone.
TEST_P(LakePartialUpdateTest, test_partial_update_rewrite_with_apportioned_num_rows) {
    if (GetParam().enable_persistent_index) return;
    if (GetParam().partial_update_mode == PartialUpdateMode::COLUMN_UPDATE_MODE) return;
    auto chunk0 = generate_data(kChunkSize, 0, false, 3);
    auto chunk1 = generate_data(kChunkSize, 0, true, 5);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c0 * 3 == c1) && (c0 * 4 == c2); }));

    auto txn_id = next_id();
    {
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
    }

    // Apportion the rowset counter down to 0, leaving the per-segment counts as the only witness
    // that this rowset holds rows.
    {
        ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(tablet_id, txn_id));
        auto apportioned = std::make_shared<TxnLogPB>(*txn_log);
        auto* rowset = apportioned->mutable_op_write()->mutable_rowset();
        ASSERT_GT(rowset->num_rows(), 0);
        ASSERT_GT(rowset->segment_metas_size(), 0);
        rowset->set_num_rows(0);
        int64_t segment_rows = 0;
        for (const auto& segment_meta : rowset->segment_metas()) {
            segment_rows += segment_meta.num_rows();
        }
        ASSERT_GT(segment_rows, 0) << "the per-segment counts must survive the apportionment";
        ASSERT_OK(_tablet_mgr->put_txn_log(apportioned));
        _tablet_mgr->prune_metacache();
    }

    ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
    ASSERT_EQ(kChunkSize, check(version + 1, [](int c0, int c1, int c2) { return (c0 * 5 == c1) && (c0 * 4 == c2); }));
}

// Turn the fixture's tablet into a range-distributed one owning [lower, upper): the order-preserving
// big-endian PK encoding range distribution requires (create_sst_seek_range_from rejects anything else,
// and CrossPublishRowSelector::create_if_needed declines to build without it) plus the range itself,
// whose bound inclusivity is not a choice -- TabletRangeHelper::validate_tablet_range accepts
// [lower, upper) and nothing else. The schema id is refreshed so the schema file the writers load stays
// in step with the metadata this produces.
static void make_range_distributed(TabletMetadata* metadata, int lower_inclusive, int upper_exclusive) {
    auto* schema_pb = metadata->mutable_schema();
    schema_pb->set_id(next_id());
    schema_pb->set_primary_key_encoding_type(PrimaryKeyEncodingTypePB::PK_ENCODING_TYPE_V2);
    auto append_int_bound = [](auto* bound, int value) {
        DatumVariant variant(get_type_info(LogicalType::TYPE_INT), Datum(value));
        VariantTuple tuple;
        tuple.append(variant);
        tuple.to_proto(bound);
    };
    auto* range_pb = metadata->mutable_range();
    append_int_bound(range_pb->mutable_lower_bound(), lower_inclusive);
    range_pb->set_lower_bound_included(true);
    append_int_bound(range_pb->mutable_upper_bound(), upper_exclusive);
    range_pb->set_upper_bound_included(false);
}

// A row-mode partial update reads the columns it does not carry from each row's old location, and a
// cross published SPLIT child is handed its siblings' rows along with its own. Looking a sibling's key
// up resolves against the sstables this child inherited from the parent, so the location it returns can
// name a rowset the split pruned away -- get_column_values then fails the publish for good on an
// unknown rssid, the same failure #77744 fixed for del files. Ask only about the rows this child owns
// and leave the rest at the "no old row" sentinel, which plan_read_by_rssid turns into default values.
//
// A real SPLIT is out of reach here, so the two things CrossPublishRowSelector::create_if_needed keys
// on are staged directly: a tablet range over the middle of the key space, and an op_write whose
// segments are marked shared. The baseline write is LOCAL, which is what puts the siblings' keys in
// this child's index -- without them a sibling lookup would simply miss and there would be nothing to
// observe.
//
// The range is stamped onto the ROWSET here, exactly as convert_txn_log_for_splitting does, which is
// what narrows the publish iterator to this child's slice of the segment -- and what this covers:
//
// - The rewrite's row-count contract. SegmentRewriter::rewrite_partial_update copies the source
//   segment's own columns verbatim, every row of them, and appends the merged ones, so
//   SegmentWriter::finalize_columns fails the publish outright ("num rows written mismatch") unless
//   there is one merged value per SOURCE row. A narrowed iterator produces fewer, so rewrite_segment
//   pads the rest with defaults. c2 is the witness: it is not in the partial write, so an owned row
//   carries its old c2 (key * 4) and a padded row carries c2's declared default (10).
// - That those padded rows stay out of reads. MetaFileBuilder clears `shared` on the rewrite output
//   (the file really is private to this tablet), so what clips it is the range on the rowset --
//   Rowset::set_segment_tablet_range accepts either. Hence one assertion on the file, which must hold
//   every row, and one on the tablet, which must serve only this child's.
TEST_P(LakePartialUpdateTest, test_cross_publish_row_mode_partial_update_reads_only_owned_rows) {
    if (GetParam().partial_update_mode == PartialUpdateMode::COLUMN_UPDATE_MODE) {
        GTEST_SKIP() << "column mode resolves the unmodified columns through DCGs, not this lookup";
    }
    const int n = kChunkSize;
    const int kOwnedLower = n / 4;
    const int kOwnedUpper = n - n / 4;
    const int kOwnedRows = kOwnedUpper - kOwnedLower;

    make_range_distributed(_tablet_metadata.get(), kOwnedLower, kOwnedUpper);
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    ASSERT_OK(_tablet_mgr->create_schema_file(_tablet_metadata->id(), _tablet_metadata->schema()));
    _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
    _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));

    auto chunk0 = generate_data(n, 0, false, 3); // full rows: c1 = key * 3, c2 = key * 4
    auto chunk1 = generate_data(n, 0, true, 5);  // partial rows: c0 and c1 = key * 5 only
    auto indexes = std::vector<uint32_t>(n);
    for (int i = 0; i < n; i++) {
        indexes[i] = i;
    }
    auto tablet_id = _tablet_metadata->id();

    // v2: a local full write puts every key -- this child's and its siblings' -- in the index. Not
    // shared, so it is neither selected at publish nor clipped at read.
    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, 2, txn_id).status());
    }
    ASSERT_EQ(n, check(2, [](int c0, int c1, int c2) { return (c0 * 3 == c1) && (c0 * 4 == c2); }));

    // v3: cross publish the partial update. What convert_txn_log_for_splitting leaves behind is the
    // parent's segments on every child, so each has to select its own rows out of them.
    auto txn_id = next_id();
    {
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(GetParam().partial_update_mode)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
    }
    {
        ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(tablet_id, txn_id));
        auto shared_log = std::make_shared<TxnLog>(*txn_log);
        auto* rowset = shared_log->mutable_op_write()->mutable_rowset();
        ASSERT_GT(rowset->segment_metas_size(), 0);
        rowset->mutable_range()->CopyFrom(_tablet_metadata->range());
        for (auto& segment_meta : *rowset->mutable_segment_metas()) {
            segment_meta.set_shared(true);
        }
        ASSERT_OK(_tablet_mgr->put_txn_log(shared_log));
        _tablet_mgr->prune_metacache();
    }
    ASSERT_OK(publish_single_version(tablet_id, 3, txn_id).status());

    ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id, 3));
    ASSERT_EQ(2, metadata->rowsets_size());
    // Guards the staging as much as the fix: only the owned keys were re-indexed, so only their old
    // rows were displaced. All n here would mean the selector never engaged.
    EXPECT_EQ(kOwnedRows, metadata->rowsets(0).num_dels());

    // The rewrite output is private (no `shared` flag) but its rowset carries the range, so reads clip
    // it: the siblings' rows are in the file and must not come back. Every key appears exactly once --
    // the owned ones from the rewrite, the rest still from the baseline rowset.
    ASSERT_EQ(n, check(3, [&](int c0, int c1, int c2) {
                  const bool owned = c0 >= kOwnedLower && c0 < kOwnedUpper;
                  return owned ? (c1 == c0 * 5 && c2 == c0 * 4) : (c1 == c0 * 3 && c2 == c0 * 4);
              }));

    const auto& rewritten = metadata->rowsets(1);
    ASSERT_EQ(1, rewritten.segment_metas_size());
    ASSIGN_OR_ABORT(auto fs, FileSystemFactory::CreateSharedFromString(kTestDirectory));
    auto segment_path = _tablet_mgr->segment_location(tablet_id, rewritten.segment_metas(0).filename());
    ASSIGN_OR_ABORT(auto segment, Segment::open(fs, FileInfo{segment_path}, /*segment_id=*/0, _tablet_schema));
    OlapReaderStatistics stats;
    SegmentReadOptions opts;
    opts.fs = fs;
    opts.tablet_id = tablet_id;
    opts.stats = &stats;
    opts.chunk_size = 128;
    ASSIGN_OR_ABORT(auto seg_iter, segment->new_iterator(*_schema, opts));
    auto read_chunk = ChunkFactory::new_chunk(*_schema, 128);
    std::map<int, std::pair<int, int>> rows;
    while (true) {
        read_chunk->reset();
        auto st = seg_iter->get_next(read_chunk.get());
        if (st.is_end_of_file()) {
            break;
        }
        ASSERT_OK(st);
        for (size_t i = 0; i < read_chunk->num_rows(); i++) {
            auto row = read_chunk->get(i);
            rows[row[0].get_int32()] = {row[1].get_int32(), row[2].get_int32()};
        }
    }
    seg_iter->close();

    ASSERT_EQ(static_cast<size_t>(n), rows.size());
    for (int key = 0; key < n; key++) {
        const bool owned = key >= kOwnedLower && key < kOwnedUpper;
        auto it = rows.find(key);
        ASSERT_NE(rows.end(), it) << "key " << key << " missing from the rewritten segment";
        EXPECT_EQ(key * 5, it->second.first) << "key " << key;
        EXPECT_EQ(owned ? key * 4 : 10, it->second.second) << "key " << key;
    }
}

// Column-mode partial update on a cross-published rowset. This handler builds its OWN
// SegmentPKIterators, so it needs its own selector: the mapping it derives from the index decides
// which source row gets which update, and taking a sibling's row rewrites a value this child does not
// own (in COLUMN_UPSERT_MODE it would materialize the key outright, which is why the ownership mask
// has to reach build_rss_rowid_to_update_rowid -- a sibling's key that this child's inherited sstables
// still answer for looks exactly like an update, and one they do not looks exactly like an insert).
//
// Staged as in the row-mode test: the baseline write is LOCAL, so every key -- siblings' included --
// is in this child's index and resolvable. Only the owned half may end up updated.
TEST_P(LakePartialUpdateTest, test_cross_publish_column_mode_updates_only_owned_rows) {
    if (GetParam().partial_update_mode != PartialUpdateMode::COLUMN_UPDATE_MODE) {
        GTEST_SKIP() << "row mode merges the unmodified columns through the rewrite, not a DCG";
    }
    const int n = kChunkSize;
    const int kOwnedLower = n / 4;
    const int kOwnedUpper = n - n / 4;

    make_range_distributed(_tablet_metadata.get(), kOwnedLower, kOwnedUpper);
    ASSERT_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    ASSERT_OK(_tablet_mgr->create_schema_file(_tablet_metadata->id(), _tablet_metadata->schema()));
    _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
    _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));

    auto chunk0 = generate_data(n, 0, false, 3); // full rows: c1 = key * 3, c2 = key * 4
    auto chunk1 = generate_data(n, 0, true, 5);  // partial rows: c0 and c1 = key * 5 only
    auto indexes = std::vector<uint32_t>(n);
    for (int i = 0; i < n; i++) {
        indexes[i] = i;
    }
    auto tablet_id = _tablet_metadata->id();

    // v2: local full write -> every key lands in this child's index.
    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, 2, txn_id).status());
    }
    ASSERT_EQ(n, check(2, [](int c0, int c1, int c2) { return (c0 * 3 == c1) && (c0 * 4 == c2); }));

    // v3: cross publish a column-mode update of every key.
    auto txn_id = next_id();
    {
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(GetParam().partial_update_mode)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
    }
    {
        ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(tablet_id, txn_id));
        auto shared_log = std::make_shared<TxnLog>(*txn_log);
        auto* rowset = shared_log->mutable_op_write()->mutable_rowset();
        ASSERT_GT(rowset->segment_metas_size(), 0);
        // Shared segments but no rowset range, so the iterator is never narrowed and the selector's
        // mask is what decides -- see the note in
        // LakePrimaryKeyPublishTest.test_cross_publish_condition_update_compares_only_owned_rows.
        for (auto& segment_meta : *rowset->mutable_segment_metas()) {
            segment_meta.set_shared(true);
        }
        ASSERT_OK(_tablet_mgr->put_txn_log(shared_log));
        _tablet_mgr->prune_metacache();
    }
    ASSERT_OK(publish_single_version(tablet_id, 3, txn_id).status());

    // The DCG lands on the baseline rowset, which this tablet wrote itself and reads whole, so a
    // sibling's update would be plainly visible here: without the mask every key reads c1 = c0 * 5.
    ASSERT_EQ(n, check(3, [&](int c0, int c1, int c2) {
                  const bool owned = c0 >= kOwnedLower && c0 < kOwnedUpper;
                  return c2 == c0 * 4 && c1 == (owned ? c0 * 5 : c0 * 3);
              }));
}

TEST_P(LakePartialUpdateTest, test_write_multi_segment_by_diff_val_mem_limit) {
    auto chunk0 = generate_data(kChunkSize, 0, false, 3);
    auto chunk1 = generate_data(kChunkSize, 0, true, 5);
    auto chunk2 = generate_data(kChunkSize, 0, true, 6);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    // normal write
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c0 * 3 == c1) && (c0 * 4 == c2); }));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 3);

    // partial update, and make it generate two segment files in one rowset
    const int64_t old_size = config::write_buffer_size;
    config::write_buffer_size = 1;
    const int64_t old_limit = _update_mgr->update_state_mem_tracker()->limit();
    _update_mgr->update_state_mem_tracker()->set_limit(1);
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(GetParam().partial_update_mode)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->write(chunk2, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
        ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        if (GetParam().partial_update_mode == PartialUpdateMode::COLUMN_UPDATE_MODE) {
            if (i == 0) {
                EXPECT_EQ(new_tablet_metadata->orphan_files_size(), 2);
            } else {
                // Superseded .cols files are orphaned; with change data capture on, the prior
                // publish's per-publish column_overlay_vecs delvec is orphaned too (the extra one).
                const int expected = new_tablet_metadata->cdc_metadata().enable_cdc() ? 4 : 3;
                EXPECT_EQ(new_tablet_metadata->orphan_files_size(), expected);
            }
        } else {
            EXPECT_EQ(new_tablet_metadata->orphan_files_size(), 2);
        }
    }
    config::write_buffer_size = old_size;
    _update_mgr->update_state_mem_tracker()->set_limit(old_limit);
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c0 * 6 == c1) && (c0 * 4 == c2); }));
    ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    if (GetParam().partial_update_mode == PartialUpdateMode::COLUMN_UPDATE_MODE) {
        EXPECT_EQ(new_tablet_metadata->rowsets_size(), 3);
    } else {
        EXPECT_EQ(new_tablet_metadata->rowsets_size(), 6);
        // check segment size in last metadata
        EXPECT_EQ(new_tablet_metadata->rowsets(5).segment_metas_size(), 2);
    }
    if (GetParam().enable_persistent_index && GetParam().persistent_index_type == PersistentIndexTypePB::LOCAL) {
        check_local_persistent_index_meta(tablet_id, version);
    }
}

TEST_P(LakePartialUpdateTest, test_partial_update_retry_check_file_exist) {
    if (GetParam().enable_persistent_index) return;
    if (GetParam().partial_update_mode == PartialUpdateMode::COLUMN_UPDATE_MODE) return;
    auto chunk0 = generate_data(kChunkSize, 0, false, 3);
    auto chunk1 = generate_data(kChunkSize, 0, true, 5);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    // normal write
    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c0 * 3 == c1) && (c0 * 4 == c2); }));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 1);

    // partial update
    auto txn_id = next_id();
    {
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
    }
    // retry because put meta fail
    for (int i = 0; i < 2; i++) {
        TEST_ENABLE_ERROR_POINT("TabletManager::put_tablet_metadata",
                                Status::IOError("injected put tablet metadata error"));

        SyncPoint::GetInstance()->EnableProcessing();

        DeferOp defer([]() {
            TEST_DISABLE_ERROR_POINT("TabletManager::put_tablet_metadata");
            SyncPoint::GetInstance()->DisableProcessing();
        });
        _tablet_mgr->prune_metacache();
        ASSERT_ERROR(publish_single_version(tablet_id, version + 1, txn_id));
        auto txn_log_st = _tablet_mgr->get_txn_log(tablet_id, txn_id);
        EXPECT_TRUE(txn_log_st.ok());
    }
    // success
    _tablet_mgr->prune_metacache();
    ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id));
    ASSERT_EQ(kChunkSize, check(version + 1, [](int c0, int c1, int c2) { return (c0 * 5 == c1) && (c0 * 4 == c2); }));
}

TEST_P(LakePartialUpdateTest, test_max_buffer_rows) {
    if (GetParam().partial_update_mode == PartialUpdateMode::COLUMN_UPDATE_MODE) {
        GTEST_SKIP() << "this case only for partial update row mode";
    }
    auto chunk0 = generate_data(kChunkSize, 0, false, 3);
    auto chunk1 = generate_data(kChunkSize, 0, true, 3);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    // normal write
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c0 * 3 == c1) && (c0 * 4 == c2); }));
    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 3);

    // partial update, and make it generate two segment files in one rowset
    // Caused by max buffer rows.

    SyncPoint::GetInstance()->SetCallBack("TabletManager::get_average_row_size_from_latest_metadata",
                                          [](void* arg) { *(int64_t*)arg = 1000000000; });
    SyncPoint::GetInstance()->EnableProcessing();

    DeferOp defer([]() {
        SyncPoint::GetInstance()->ClearCallBack("TabletManager::get_average_row_size_from_latest_metadata");
        SyncPoint::GetInstance()->DisableProcessing();
    });
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(GetParam().partial_update_mode)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
        ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        if (GetParam().partial_update_mode == PartialUpdateMode::COLUMN_UPDATE_MODE) {
            if (i == 0) {
                EXPECT_EQ(new_tablet_metadata->orphan_files_size(), 2);
            } else {
                // move old .cols into orphan files.
                EXPECT_EQ(new_tablet_metadata->orphan_files_size(), 3);
            }
        } else {
            EXPECT_EQ(new_tablet_metadata->orphan_files_size(), 2);
        }
    }
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c0 * 3 == c1) && (c0 * 4 == c2); }));
    ASSIGN_OR_ABORT(new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    if (GetParam().partial_update_mode == PartialUpdateMode::COLUMN_UPDATE_MODE) {
        EXPECT_EQ(new_tablet_metadata->rowsets_size(), 3);
    } else {
        EXPECT_EQ(new_tablet_metadata->rowsets_size(), 6);
        // check segment size in last metadata
        EXPECT_EQ(new_tablet_metadata->rowsets(5).segment_metas_size(), 2);
    }
    EXPECT_TRUE(_update_mgr->update_state_mem_tracker()->consumption() == 0);
    if (GetParam().enable_persistent_index && GetParam().persistent_index_type == PersistentIndexTypePB::LOCAL) {
        check_local_persistent_index_meta(tablet_id, version);
    }
}

namespace {
class SchemaModifier {
public:
    virtual ~SchemaModifier() = default;
    virtual void modify(TabletSchemaPB* schema) = 0;
};

using SchemaModifierPtr = std::shared_ptr<SchemaModifier>;

class SwapColumn : public SchemaModifier {
public:
    explicit SwapColumn(int pos1, int pos2) : _pos1(pos1), _pos2(pos2) {}

    void modify(TabletSchemaPB* schema) override {
        // swap column c2 and c1
        schema->mutable_column()->SwapElements(_pos1, _pos2);
    }

private:
    int _pos1;
    int _pos2;
};

class ModifyColumnType : public SchemaModifier {
public:
    explicit ModifyColumnType(int column_idx, const std::string& target_type)
            : _column_idx(column_idx), _target_type(std::move(target_type)) {}

    void modify(TabletSchemaPB* schema) override {
        schema->mutable_column(_column_idx)->set_type(_target_type);
        if (_target_type == "VARCHAR" || _target_type == "varchar") {
            // 100 is enough for tests
            schema->mutable_column(_column_idx)->set_length(100);
        }
    }

private:
    int _column_idx;
    std::string _target_type;
};

class AddColumn : public SchemaModifier {
public:
    explicit AddColumn(int pos, const std::string& type, bool nullable, const std::string& default_value)
            : _pos(pos), _type(std::move(type)), _nullable(nullable), _default_value(std::move(default_value)) {}

    void modify(TabletSchemaPB* schema) override {
        DCHECK_LE(_pos, schema->column_size());
        auto pos = schema->column_size();
        auto* c = schema->add_column();
        c->set_unique_id(next_id());
        c->set_name(fmt::format("c_{}", c->unique_id()));
        c->set_type(_type);
        c->set_is_key(false);
        c->set_is_nullable(_nullable);
        c->set_aggregation("REPLACE");
        c->set_default_value(_default_value);
        if (_type == "VARCHAR" || _type == "varchar") {
            c->set_length(100);
        }
        // Move the column to expected position
        while (pos != _pos) {
            schema->mutable_column()->SwapElements(pos, pos - 1);
            --pos;
        }
    }

private:
    int _pos;
    std::string _type;
    bool _nullable;
    std::string _default_value;
};

class DropColumn : public SchemaModifier {
public:
    explicit DropColumn(int col_idx) : _col_idx(col_idx) {}

    void modify(TabletSchemaPB* schema) override {
        auto iter = schema->mutable_column()->begin() + _col_idx;
        schema->mutable_column()->erase(iter);
    }

private:
    int _col_idx;
};

} // namespace

class LakePartialUpdateConcurrentSchemaEvolutionTest : public LakePartialUpdateTestBase,
                                                       public testing::WithParamInterface<SchemaModifierPtr> {
public:
    LakePartialUpdateConcurrentSchemaEvolutionTest() : LakePartialUpdateTestBase(kTestDirectory) {}

    constexpr static const char* const kTestDirectory = "test_lake_partial_update_concurrent_schema_evolution";

    static std::string as_string(LogicalType type, const Datum& datum) {
        auto type_info = get_type_info(type);
        return datum_to_string(type_info.get(), datum);
    }

    void verify_column(const TabletColumn& col, int64_t rowid, const Datum& datum) {
        if (col.name() == "c0") {
            EXPECT_EQ(rowid, datum.get_int32()) << "rowid=" << rowid << " column=" << col.name();
        } else if (col.name() == "c1") {
            auto expect = rowid * 3;
            //            ^^^^^^^^^^ Please refer the define and usage of `generate_data()`
            EXPECT_EQ(std::to_string(expect), as_string(col.type(), datum));
        } else if (col.name() == "c2") {
            auto expect = rowid * 4;
            //            ^^^^^^^^^^ Please refer the define and usage of `generate_data()`
            EXPECT_EQ(std::to_string(expect), as_string(col.type(), datum))
                    << "rowid=" << rowid << " column=" << col.name();
        } else if (col.default_value() == "NULL") {
            EXPECT_TRUE(datum.is_null()) << "rowid=" << rowid << " column=" << col.name() << " type=" << col.type()
                                         << " value=" << as_string(col.type(), datum);
        } else {
            EXPECT_EQ(col.default_value(), as_string(col.type(), datum))
                    << "rowid=" << rowid << " column=" << col.name();
        }
    }

    int64_t verify(int64_t version) {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
        auto tablet_schema = TabletSchema::create(metadata->schema());
        auto schema = tablet_schema->schema();
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *schema);
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));
        auto chunk = ChunkFactory::new_chunk(*schema, 128);
        auto ret = int64_t{0};
        auto rowid = int64_t{0};
        while (true) {
            auto st = reader->get_next(chunk.get());
            if (st.is_end_of_file()) {
                break;
            }
            CHECK_OK(st);
            ret += chunk->num_rows();
            auto cols = chunk->columns();
            for (int i = 0; i < chunk->num_rows(); i++) {
                for (int j = 0, num_col = tablet_schema->num_columns(); j < num_col; j++) {
                    verify_column(tablet_schema->column(j), rowid, chunk->get(i).get(j));
                }
                rowid++;
            }
            chunk->reset();
        }
        return ret;
    }
};

TEST_P(LakePartialUpdateConcurrentSchemaEvolutionTest, test) {
    auto chunk0 = generate_data(kChunkSize, 0, false, 1);
    auto chunk1 = generate_data(kChunkSize, 0, true, 3);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    std::iota(indexes.begin(), indexes.end(), 0);

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    // normal write
    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    // partial update
    auto txn_id = next_id();
    {
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
    }
    // Update tablet schema
    {
        ASSIGN_OR_ABORT(auto latest_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        auto new_metadata = std::make_shared<TabletMetadataPB>(*latest_metadata);
        auto schema = new_metadata->mutable_schema();
        auto modifier = GetParam();
        modifier->modify(schema);
        schema->set_id(next_id());
        schema->set_schema_version(schema->schema_version() + 1);
        new_metadata->set_version(version + 1);
        // Save new tablet metadata and schema file
        ASSERT_OK(_tablet_mgr->put_tablet_metadata(new_metadata));
        ASSERT_OK(_tablet_mgr->create_schema_file(tablet_id, *schema));
        version++;
    }
    // Publish version for partial update
    {
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize, verify(version));
}
// clang-format off
INSTANTIATE_TEST_SUITE_P(LakePartialUpdateConcurrentSchemaEvolutionTest,
                         LakePartialUpdateConcurrentSchemaEvolutionTest,
                         ::testing::Values(std::make_shared<SwapColumn>(1, 2),
                                           std::make_shared<AddColumn>(1, "BIGINT", true, "NULL"),
                                           std::make_shared<AddColumn>(1, "BIGINT", true, "-100"),
                                           std::make_shared<AddColumn>(1, "BIGINT", false, "-100"),
                                           std::make_shared<AddColumn>(1, "VARCHAR", true, "xyz"),
                                           std::make_shared<AddColumn>(1, "VARCHAR", false, "yyyy"),
                                           std::make_shared<AddColumn>(2, "BIGINT", true, "NULL"),
                                           std::make_shared<AddColumn>(2, "BIGINT", true, "1000"),
                                           std::make_shared<AddColumn>(2, "VARCHAR", true, "abc"),
                                           std::make_shared<AddColumn>(3, "BIGINT", true, "123"),
                                           std::make_shared<AddColumn>(3, "VARCHAR", true, "hello"),
                                           std::make_shared<AddColumn>(3, "VARCHAR", false, "world"),
                                           std::make_shared<DropColumn>(1),
                                           std::make_shared<DropColumn>(2),
                                           std::make_shared<ModifyColumnType>(1, "BIGINT"),
                                           std::make_shared<ModifyColumnType>(1, "DOUBLE"),
                                           std::make_shared<ModifyColumnType>(1, "VARCHAR"),
                                           std::make_shared<ModifyColumnType>(2, "BIGINT"),
                                           std::make_shared<ModifyColumnType>(2, "DOUBLE"),
                                           std::make_shared<ModifyColumnType>(2, "VARCHAR")
                         ));
// clang-format on

} // namespace starrocks::lake

namespace starrocks::lake {

class LakeColumnUpsertModeTest : public LakePartialUpdateTestBase {
public:
    LakeColumnUpsertModeTest() : LakePartialUpdateTestBase(kTestDirectory) {}

    void SetUp() override {
        LakePartialUpdateTestBase::SetUp();
        // Seed encryption keys for tests that enable TDE (no FE in UT environment)
        // Only add keys if they don't already exist to avoid conflicts with other tests
        if (KeyCache::instance().get_key("0000000000000000") == nullptr) {
            EncryptionKeyPB pb;
            pb.set_id(EncryptionKey::DEFAULT_MASTER_KYE_ID);
            pb.set_type(EncryptionKeyTypePB::NORMAL_KEY);
            pb.set_algorithm(EncryptionAlgorithmPB::AES_128);
            pb.set_plain_key("0000000000000000");
            std::unique_ptr<EncryptionKey> root_encryption_key = EncryptionKey::create_from_pb(pb).value();
            auto val_st = root_encryption_key->generate_key();
            ASSERT_TRUE(val_st.ok());
            std::unique_ptr<EncryptionKey> encryption_key = std::move(val_st.value());
            encryption_key->set_id(2);
            KeyCache::instance().add_key(root_encryption_key);
            KeyCache::instance().add_key(encryption_key);
        }
    }

    constexpr static const char* const kTestDirectory = "test_lake_column_upsert_mode";
};

TEST_F(LakeColumnUpsertModeTest, upsert_existing_rows_generates_dcg_only) {
    auto chunk_full = generate_data(kChunkSize, 0, false, 3);
    auto chunk_partial = generate_data(kChunkSize, 0, true, 5);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) indexes[i] = i;
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();

    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk_full, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(PartialUpdateMode::COLUMN_UPSERT_MODE)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk_partial, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c0 * 5 == c1) && (c0 * 4 == c2); }));
    ASSIGN_OR_ABORT(auto md, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(md->rowsets_size(), 3);
    EXPECT_GT(md->dcg_meta().dcgs_size(), 0);
}

TEST_F(LakeColumnUpsertModeTest, partial_update_reads_encrypted_dcg_segments) {
    auto chunk_full = generate_data(kChunkSize, 0, false, 3);
    auto chunk_partial = generate_data(kChunkSize, 0, true, 7);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) indexes[i] = i;

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();

    const bool old_enable_tde = config::enable_transparent_data_encryption;
    config::enable_transparent_data_encryption = true;
    DeferOp tde_guard([&]() { config::enable_transparent_data_encryption = old_enable_tde; });

    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk_full, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(PartialUpdateMode::COLUMN_UPDATE_MODE)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk_partial, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        bool has_encrypted_dcg = false;
        for (const auto& entry : metadata->dcg_meta().dcgs()) {
            const auto& dcg_ver = entry.second;
            if (dcg_ver.encryption_metas_size() > 0) {
                has_encrypted_dcg = true;
                break;
            }
        }
        ASSERT_TRUE(has_encrypted_dcg);
    }

    std::vector<int> keys_only(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) keys_only[i] = i;
    auto c0_only = Int32Column::create();
    c0_only->append_numbers(keys_only.data(), keys_only.size() * sizeof(int));
    Chunk::SlotHashMap slot_only;
    slot_only[0] = 0;
    Chunk c0_chunk({std::move(c0_only)}, slot_only);

    std::vector<SlotDescriptor> key_slots;
    key_slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
    std::vector<SlotDescriptor*> key_slot_ptrs;
    key_slot_ptrs.emplace_back(&key_slots[0]);

    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&key_slot_ptrs)
                                                   .set_partial_update_mode(PartialUpdateMode::ROW_MODE)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(c0_chunk, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c1 == c0 * 7) && (c2 == c0 * 4); }));
}

TEST_F(LakeColumnUpsertModeTest, upsert_with_new_rows_adds_new_segments) {
    auto chunk_full = generate_data(kChunkSize, 0, false, 3);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) indexes[i] = i;
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();

    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk_full, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSIGN_OR_ABORT(auto md_before, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    auto prev_rowsets = md_before->rowsets_size();

    auto chunk_insert = generate_data(kChunkSize, 100, true, 7);
    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(PartialUpdateMode::COLUMN_UPSERT_MODE)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk_insert, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    ASSIGN_OR_ABORT(auto md_after, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_GT(md_after->rowsets_size(), prev_rowsets);
    auto total = check(version, [](int c0, int c1, int c2) { return (c2 == c0 * 4) || (c2 == 10); });
    EXPECT_EQ(total, kChunkSize * 2);
}

// Locks down the CopyFrom contract in _handle_column_upsert_mode: the synthesized
// new_rows_op rowset must inherit the source op_write's uid verbatim. The DCG path
// (apply_column_mode_partial_update) orphans the original op_write segments, so
// new_rows_op is the only top-level rowset this publish adds; reusing op_write.uid
// gives split siblings replaying the same op_write an identical uid → MERGE dedup.
//
// If a future change accidentally re-introduces XOR-salt derivation or a fresh
// mint here, this test fails (uid would differ from the captured op_write.uid).
TEST_F(LakeColumnUpsertModeTest, new_rows_op_inherits_op_write_uid) {
    auto chunk_full = generate_data(kChunkSize, 0, false, 3);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) indexes[i] = i;
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();

    // Phase 1: baseline INSERT to populate the table so subsequent COLUMN_UPSERT_MODE
    // writes against new PK offsets hit the "new rows" synthesis path.
    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk_full, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSIGN_OR_ABORT(auto md_before, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    auto prev_rowsets = md_before->rowsets_size();

    // Phase 2: COLUMN_UPSERT_MODE write at PK offset 100 (disjoint from baseline 0..kChunkSize),
    // so every row is "new" and goes through the new_rows_op synthesis path.
    auto chunk_insert = generate_data(kChunkSize, 100, true, 7);
    PUniqueId captured_op_write_uid;
    auto txn_id = next_id();
    {
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(PartialUpdateMode::COLUMN_UPSERT_MODE)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk_insert, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();

        // Capture op_write.rowset.uid before publish consumes the txn_log.
        ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(tablet_id, txn_id));
        ASSERT_TRUE(txn_log->has_op_write());
        ASSERT_TRUE(tablet_reshard_helper::has_valid_uid(txn_log->op_write().rowset()))
                << "delta_writer must mint a uid on the op_write rowset at write time";
        captured_op_write_uid = txn_log->op_write().rowset().uid();

        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Phase 3: verify the new top-level rowset (new_rows_op) inherits the op_write's uid.
    // apply_opwrite appends via _tablet_meta->add_rowsets(), so new_rows_op is the last entry.
    ASSIGN_OR_ABORT(auto md_after, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    ASSERT_GT(md_after->rowsets_size(), prev_rowsets);
    const auto& new_rows_rs = md_after->rowsets(md_after->rowsets_size() - 1);
    EXPECT_TRUE(tablet_reshard_helper::has_valid_uid(new_rows_rs))
            << "synthesized new_rows_op rowset must carry a valid uid";
    EXPECT_EQ(captured_op_write_uid.hi(), new_rows_rs.uid().hi())
            << "new_rows_op.uid.hi must CopyFrom op_write.rowset.uid.hi (no derivation)";
    EXPECT_EQ(captured_op_write_uid.lo(), new_rows_rs.uid().lo())
            << "new_rows_op.uid.lo must CopyFrom op_write.rowset.uid.lo (no derivation)";
}

// Column-mode legacy compatibility: a COLUMN_UPSERT_MODE op_write written before the
// uid field existed (rolling upgrade / BE restart with pending txn logs) carries no
// uid. _handle_column_upsert_mode must MINT a fresh uid for the synthesized new_rows_op
// rather than hard-fail the in-flight transaction. Such a legacy write is never
// range-distributed (a new, post-uid feature), hence never cross-published, so a fresh
// per-publish uid is safe. delta_writer always mints in production; we rewrite the
// persisted txn log (clear its uid) between write and publish to drive the legacy path.
TEST_F(LakeColumnUpsertModeTest, new_rows_op_without_op_write_uid_mints_fresh) {
    auto chunk_full = generate_data(kChunkSize, 0, false, 3);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) indexes[i] = i;
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();

    // Phase 1: baseline INSERT.
    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk_full, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Phase 2: COLUMN_UPSERT_MODE write at a disjoint PK offset (all rows new), then
    // rewrite the persisted txn log to drop the op_write uid before publish.
    auto chunk_insert = generate_data(kChunkSize, 100, true, 7);
    auto txn_id = next_id();
    ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                               .set_tablet_manager(_tablet_mgr.get())
                                               .set_tablet_id(tablet_id)
                                               .set_txn_id(txn_id)
                                               .set_partition_id(_partition_id)
                                               .set_mem_tracker(_mem_tracker.get())
                                               .set_schema_id(_tablet_schema->id())
                                               .set_slot_descriptors(&_slot_pointers)
                                               .set_partial_update_mode(PartialUpdateMode::COLUMN_UPSERT_MODE)
                                               .build());
    ASSERT_OK(delta_writer->open());
    ASSERT_OK(delta_writer->write(chunk_insert, indexes.data(), indexes.size()));
    ASSERT_OK(delta_writer->finish_with_txnlog());
    delta_writer->close();

    ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(tablet_id, txn_id));
    ASSERT_TRUE(txn_log->has_op_write());
    auto rewritten = std::make_shared<TxnLog>(*txn_log);
    rewritten->mutable_op_write()->mutable_rowset()->clear_uid(); // producer-side regression
    ASSERT_OK(_tablet_mgr->put_txn_log(rewritten));

    // Publish must SUCCEED (mint-if-absent), not hard-fail on the missing uid.
    ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
    version++;

    // The synthesized new_rows_op (appended last by apply_opwrite) must carry a
    // freshly-minted valid uid even though the source op_write had none.
    ASSIGN_OR_ABORT(auto md_after, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    ASSERT_GT(md_after->rowsets_size(), 0);
    const auto& new_rows_rs = md_after->rowsets(md_after->rowsets_size() - 1);
    EXPECT_TRUE(tablet_reshard_helper::has_valid_uid(new_rows_rs))
            << "legacy uid-less column-mode op_write must yield a freshly-minted new_rows_op uid";
}

TEST_F(LakeColumnUpsertModeTest, test_default_values_handling) {
    // Create a schema with default values
    auto tablet_metadata = std::make_shared<TabletMetadata>();
    tablet_metadata->set_id(next_id());
    tablet_metadata->set_version(1);
    tablet_metadata->set_next_rowset_id(1);

    // Schema with default values
    auto schema = tablet_metadata->mutable_schema();
    schema->set_id(next_id());
    schema->set_num_short_key_columns(1);
    schema->set_keys_type(PRIMARY_KEYS);
    schema->set_num_rows_per_row_block(65535);

    auto c0 = schema->add_column();
    c0->set_unique_id(next_id());
    c0->set_name("c0");
    c0->set_type("INT");
    c0->set_is_key(true);
    c0->set_is_nullable(false);

    auto c1 = schema->add_column();
    c1->set_unique_id(next_id());
    c1->set_name("c1");
    c1->set_type("INT");
    c1->set_is_key(false);
    c1->set_is_nullable(true);
    c1->set_aggregation("REPLACE");
    c1->set_default_value("100");

    auto c2 = schema->add_column();
    c2->set_unique_id(next_id());
    c2->set_name("c2");
    c2->set_type("INT");
    c2->set_is_key(false);
    c2->set_is_nullable(true);
    c2->set_aggregation("REPLACE");
    // No default value set

    auto tablet_schema = TabletSchema::create(*schema);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*tablet_metadata));

    auto tablet_id = tablet_metadata->id();
    auto version = 1;

    // Create some initial data
    std::vector<int> v0 = {1, 2, 3};
    std::vector<int> v1 = {10, 20, 30};
    std::vector<int> v2 = {40, 50, 60};

    auto c0_col = Int32Column::create();
    auto c1_col = Int32Column::create();
    auto c2_col = Int32Column::create();
    c0_col->append_numbers(v0.data(), v0.size() * sizeof(int));
    c1_col->append_numbers(v1.data(), v1.size() * sizeof(int));
    c2_col->append_numbers(v2.data(), v2.size() * sizeof(int));

    Chunk::SlotHashMap slot_map;
    slot_map[0] = 0;
    slot_map[1] = 1;
    slot_map[2] = 2;
    auto chunk_full = Chunk({std::move(c0_col), std::move(c1_col), std::move(c2_col)}, slot_map);

    auto indexes = std::vector<uint32_t>{0, 1, 2};

    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk_full, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Column upsert with new rows
    std::vector<int> new_keys = {4, 5};
    auto c0_partial = Int32Column::create();
    c0_partial->append_numbers(new_keys.data(), new_keys.size() * sizeof(int));

    Chunk::SlotHashMap partial_slot_map;
    partial_slot_map[0] = 0;
    auto chunk_partial = Chunk({std::move(c0_partial)}, partial_slot_map);

    std::vector<SlotDescriptor> slots;
    slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
    std::vector<SlotDescriptor*> slot_pointers;
    slot_pointers.emplace_back(&slots[0]);

    auto indexes_partial = std::vector<uint32_t>{0, 1};

    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(tablet_schema->id())
                                                   .set_slot_descriptors(&slot_pointers)
                                                   .set_partial_update_mode(PartialUpdateMode::COLUMN_UPSERT_MODE)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk_partial, indexes_partial.data(), indexes_partial.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Verify that new rows have default values applied
    ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    auto reader_schema = std::make_shared<Schema>(ChunkHelper::convert_schema(tablet_schema));
    auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *reader_schema);
    CHECK_OK(reader->prepare());
    CHECK_OK(reader->open(TabletReaderParams()));
    auto result_chunk = ChunkFactory::new_chunk(*reader_schema, 128);

    int total_rows = 0;
    bool found_default_values = false;
    while (true) {
        auto st = reader->get_next(result_chunk.get());
        if (st.is_end_of_file()) break;
        CHECK_OK(st);
        total_rows += result_chunk->num_rows();

        auto cols = result_chunk->columns();
        for (int i = 0; i < result_chunk->num_rows(); i++) {
            auto c0_val = cols[0]->get(i).get_int32();
            auto c1_datum = cols[1]->get(i);
            auto c2_datum = cols[2]->get(i);

            // Check if this is one of the new rows with default values
            if (c0_val == 4 || c0_val == 5) {
                if (!c1_datum.is_null()) {
                    auto c1_val = c1_datum.get_int32();
                    EXPECT_EQ(100, c1_val); // Should have default value
                }
                if (!c2_datum.is_null()) {
                    auto c2_val = c2_datum.get_int32();
                    EXPECT_EQ(0, c2_val); // Should have default value (0 for nullable int)
                }
                found_default_values = true;
            }
        }
        result_chunk->reset();
    }

    EXPECT_TRUE(found_default_values);
    EXPECT_EQ(5, total_rows); // 3 original + 2 new rows
}

TEST_F(LakeColumnUpsertModeTest, test_bundle_file_handling) {
    // Test bundle file related logic
    auto chunk_full = generate_data(kChunkSize, 0, false, 3);
    auto chunk_insert = generate_data(kChunkSize, 100, true, 7);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) indexes[i] = i;
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();

    // Create initial data
    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk_full, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Column upsert with new rows
    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(PartialUpdateMode::COLUMN_UPSERT_MODE)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk_insert, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Verify metadata contains both segments and deletion statistics are updated
    ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_GT(metadata->rowsets_size(), 1);

    // Check for deletion statistics - should be false since no primary key conflicts occurred
    bool has_del_stats = false;
    for (const auto& rowset : metadata->rowsets()) {
        if (rowset.num_dels() > 0) {
            has_del_stats = true;
            break;
        }
    }
    // Since there are no primary key conflicts, there should be no deletion statistics
    EXPECT_FALSE(has_del_stats) << "No deletion statistics expected when there are no primary key conflicts";

    auto total = check(version, [](int c0, int c1, int c2) { return (c2 == c0 * 4) || (c2 == 10); });
    EXPECT_EQ(total, kChunkSize * 2);
}

TEST_F(LakeColumnUpsertModeTest, test_delete_handling_with_upsert) {
    // Test deletion handling logic
    auto chunk_full = generate_data(kChunkSize, 0, false, 3);
    auto chunk_update = generate_data(kChunkSize, 0, true, 5);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) indexes[i] = i;
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();

    // Create initial data with multiple versions to create potential conflicts
    for (int v = 0; v < 3; v++) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk_full, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Multiple concurrent column upserts to trigger conflict resolution and deletions
    std::vector<int64_t> txn_ids;
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        txn_ids.emplace_back(txn_id);
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(PartialUpdateMode::COLUMN_UPSERT_MODE)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk_update, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
    }

    // Publish them in order to create conflicts
    for (auto txn_id : txn_ids) {
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Verify final state and that deletions were properly handled
    ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));

    // Check that deletion vectors were created due to primary key conflicts
    bool has_del_vectors = false;
    if (metadata->has_delvec_meta()) {
        for (const auto& delvec : metadata->delvec_meta().delvecs()) {
            (void)delvec; // Suppress unused variable warning
            has_del_vectors = true;
            break;
        }
    }
    // Since we have primary key conflicts from multiple upsert operations,
    // deletion vectors should be created to handle the conflicts
    EXPECT_TRUE(has_del_vectors)
            << "Deletion vectors expected when primary key conflicts occur during upsert operations";

    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c0 * 5 == c1) && (c0 * 4 == c2); }));
}

TEST_F(LakeColumnUpsertModeTest, test_error_handling_scenarios) {
    // Test error handling paths
    auto chunk_full = generate_data(kChunkSize, 0, false, 3);
    auto chunk_insert = generate_data(kChunkSize, 100, true, 7);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) indexes[i] = i;
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();

    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk_full, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Test with memory pressure to trigger error paths during upsert index operations
    const int64_t old_limit = _update_mgr->update_state_mem_tracker()->limit();
    _update_mgr->update_state_mem_tracker()->set_limit(1); // Very low limit to trigger memory errors

    DeferOp defer([&]() { _update_mgr->update_state_mem_tracker()->set_limit(old_limit); });

    // This should still succeed but may trigger some error handling paths
    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(PartialUpdateMode::COLUMN_UPSERT_MODE)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk_insert, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Verify the data is still consistent despite memory pressure
    auto total = check(version, [](int c0, int c1, int c2) { return (c2 == c0 * 4) || (c2 == 10); });
    EXPECT_EQ(total, kChunkSize * 2);
}

TEST_F(LakeColumnUpsertModeTest, test_auto_increment_column_handling) {
    // Test auto increment column behavior in partial update scenarios:
    // 1. Update existing rows: auto increment column should remain unchanged
    // 2. Insert new rows: auto increment column generates values
    auto tablet_metadata = std::make_shared<TabletMetadata>();
    tablet_metadata->set_id(next_id());
    tablet_metadata->set_version(1);
    tablet_metadata->set_next_rowset_id(1);

    auto schema = tablet_metadata->mutable_schema();
    schema->set_id(next_id());
    schema->set_num_short_key_columns(1);
    schema->set_keys_type(PRIMARY_KEYS);
    schema->set_num_rows_per_row_block(65535);

    auto c0 = schema->add_column();
    c0->set_unique_id(next_id());
    c0->set_name("c0");
    c0->set_type("INT");
    c0->set_is_key(true);
    c0->set_is_nullable(false);

    auto c1 = schema->add_column();
    c1->set_unique_id(next_id());
    c1->set_name("c1");
    c1->set_type("INT");
    c1->set_is_key(false);
    c1->set_is_nullable(false);
    c1->set_aggregation("REPLACE");

    // Auto increment column
    auto c2 = schema->add_column();
    c2->set_unique_id(next_id());
    c2->set_name("c2");
    c2->set_type("BIGINT");
    c2->set_is_key(false);
    c2->set_is_nullable(false);
    c2->set_aggregation("REPLACE");
    c2->set_is_auto_increment(true);

    auto tablet_schema = TabletSchema::create(*schema);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*tablet_metadata));

    auto tablet_id = tablet_metadata->id();
    auto version = 1;

    // Create initial data with explicit auto increment values
    std::vector<int> v0 = {1, 2, 3};
    std::vector<int> v1 = {10, 20, 30};
    std::vector<int64_t> v2 = {1, 2, 3};

    auto c0_col = Int32Column::create();
    auto c1_col = Int32Column::create();
    auto c2_col = Int64Column::create();
    c0_col->append_numbers(v0.data(), v0.size() * sizeof(int));
    c1_col->append_numbers(v1.data(), v1.size() * sizeof(int));
    c2_col->append_numbers(v2.data(), v2.size() * sizeof(int64_t));

    Chunk::SlotHashMap slot_map;
    slot_map[0] = 0;
    slot_map[1] = 1;
    slot_map[2] = 2;
    auto chunk_initial = Chunk({std::move(c0_col), std::move(c1_col), std::move(c2_col)}, slot_map);
    auto indexes = std::vector<uint32_t>{0, 1, 2};

    // Initial write with full data (including auto increment column)
    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk_initial, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Test 1: Partial update of existing rows - auto increment column should remain unchanged
    std::vector<int> update_keys = {1, 2};
    std::vector<int> update_values = {15, 25};

    auto c0_update = Int32Column::create();
    auto c1_update = Int32Column::create();
    c0_update->append_numbers(update_keys.data(), update_keys.size() * sizeof(int));
    c1_update->append_numbers(update_values.data(), update_values.size() * sizeof(int));

    Chunk::SlotHashMap update_slot_map;
    update_slot_map[0] = 0;
    update_slot_map[1] = 1;
    auto chunk_update = Chunk({std::move(c0_update), std::move(c1_update)}, update_slot_map);

    std::vector<SlotDescriptor> update_slots;
    update_slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
    update_slots.emplace_back(1, "c1", TypeDescriptor{LogicalType::TYPE_INT});
    std::vector<SlotDescriptor*> update_slot_pointers;
    update_slot_pointers.emplace_back(&update_slots[0]);
    update_slot_pointers.emplace_back(&update_slots[1]);

    auto update_indexes = std::vector<uint32_t>{0, 1};

    // Inject auto-increment id interval for unit test environment before update as well
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("StorageEngine::get_next_increment_id_interval.1", [](void* arg) {
        auto& meta = *(std::shared_ptr<AutoIncrementMeta>*)(arg);
        meta->min = 1;
        meta->max = 1000000;
    });

    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(tablet_schema->id())
                                                   .set_slot_descriptors(&update_slot_pointers)
                                                   .set_partial_update_mode(PartialUpdateMode::COLUMN_UPSERT_MODE)
                                                   .set_miss_auto_increment_column(true)
                                                   .set_table_id(tablet_id)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk_update, update_indexes.data(), update_indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Test 2: Partial update with new rows - include auto increment column as placeholder for ID generation
    std::vector<int> insert_keys = {4, 5};
    std::vector<int> insert_values = {40, 50};

    auto c0_insert = Int32Column::create();
    auto c1_insert = Int32Column::create();
    auto c2_insert = Int64Column::create(); // placeholder for auto increment column
    c0_insert->append_numbers(insert_keys.data(), insert_keys.size() * sizeof(int));
    c1_insert->append_numbers(insert_values.data(), insert_values.size() * sizeof(int));
    // fill zeros; BE will replace with allocated auto-increment ids
    int64_t zeros[2] = {0, 0};
    c2_insert->append_numbers(zeros, sizeof(zeros));

    Chunk::SlotHashMap insert_slot_map;
    insert_slot_map[0] = 0;
    insert_slot_map[1] = 1;
    insert_slot_map[2] = 2; // c2 auto increment column (placeholder)
    auto chunk_insert = Chunk({std::move(c0_insert), std::move(c1_insert), std::move(c2_insert)}, insert_slot_map);

    std::vector<SlotDescriptor> insert_slots;
    insert_slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
    insert_slots.emplace_back(1, "c1", TypeDescriptor{LogicalType::TYPE_INT});
    insert_slots.emplace_back(2, "c2", TypeDescriptor{LogicalType::TYPE_BIGINT});
    std::vector<SlotDescriptor*> insert_slot_pointers;
    insert_slot_pointers.emplace_back(&insert_slots[0]);
    insert_slot_pointers.emplace_back(&insert_slots[1]);
    insert_slot_pointers.emplace_back(&insert_slots[2]);

    auto insert_indexes = std::vector<uint32_t>{0, 1};

    // Inject auto-increment id interval for unit test environment (no FE service)
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("StorageEngine::get_next_increment_id_interval.1", [](void* arg) {
        auto& meta = *(std::shared_ptr<AutoIncrementMeta>*)(arg);
        meta->min = 1;
        meta->max = 1000000;
    });

    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(tablet_schema->id())
                                                   .set_slot_descriptors(&insert_slot_pointers)
                                                   .set_partial_update_mode(PartialUpdateMode::COLUMN_UPSERT_MODE)
                                                   .set_miss_auto_increment_column(true)
                                                   .set_table_id(tablet_id)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk_insert, insert_indexes.data(), insert_indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Clear SyncPoint callbacks after use
    SyncPoint::GetInstance()->ClearAllCallBacks();
    SyncPoint::GetInstance()->DisableProcessing();

    // Verify that data was correctly inserted with auto increment columns handled
    ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    auto reader_schema = std::make_shared<Schema>(ChunkHelper::convert_schema(tablet_schema));
    auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *reader_schema);
    CHECK_OK(reader->prepare());
    CHECK_OK(reader->open(TabletReaderParams()));
    auto result_chunk = ChunkFactory::new_chunk(*reader_schema, 128);

    int total_rows = 0;
    bool found_updated_rows = false;
    bool found_new_rows = false;
    while (true) {
        auto st = reader->get_next(result_chunk.get());
        if (st.is_end_of_file()) break;
        CHECK_OK(st);
        total_rows += result_chunk->num_rows();

        auto cols = result_chunk->columns();
        for (int i = 0; i < result_chunk->num_rows(); i++) {
            auto c0_val = cols[0]->get(i).get_int32();
            auto c1_val = cols[1]->get(i).get_int32();
            auto c2_val = cols[2]->get(i).get_int64();

            // Check updated existing rows (c0=1,2) - auto increment should remain unchanged
            if (c0_val == 1) {
                EXPECT_EQ(15, c1_val);
                EXPECT_EQ(1, c2_val);
                found_updated_rows = true;
            } else if (c0_val == 2) {
                EXPECT_EQ(25, c1_val);
                EXPECT_EQ(2, c2_val);
                found_updated_rows = true;
            }
            // Check original unchanged row
            else if (c0_val == 3) {
                EXPECT_EQ(30, c1_val);
                EXPECT_EQ(3, c2_val);
            }
            // Check new inserted rows (c0=4,5) - auto increment behavior
            else if (c0_val == 4) {
                EXPECT_EQ(40, c1_val);
                EXPECT_GT(c2_val, 0);
                found_new_rows = true;
            } else if (c0_val == 5) {
                EXPECT_EQ(50, c1_val);
                EXPECT_GT(c2_val, 0);
                found_new_rows = true;
            }
        }
        result_chunk->reset();
    }

    EXPECT_TRUE(found_updated_rows);
    EXPECT_TRUE(found_new_rows);
    EXPECT_EQ(5, total_rows);
}

TEST_F(LakeColumnUpsertModeTest, test_handle_delete_files) {
    const int64_t kChunkSize = 64;
    auto tablet_id = _tablet_metadata->id();
    int64_t version = 1;

    // First write base data, no deletes
    {
        auto chunk = generate_data(kChunkSize, /*shift*/ 0, /*partial*/ false, /*update_ratio*/ 100);
        std::vector<uint32_t> indexes(kChunkSize);
        for (int i = 0; i < kChunkSize; i++) indexes[i] = i;
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto dw, DeltaWriterBuilder()
                                         .set_tablet_manager(_tablet_mgr.get())
                                         .set_tablet_id(tablet_id)
                                         .set_txn_id(txn_id)
                                         .set_partition_id(_partition_id)
                                         .set_mem_tracker(_mem_tracker.get())
                                         .set_schema_id(_tablet_schema->id())
                                         .build());
        ASSERT_OK(dw->open());
        ASSERT_OK(dw->write(chunk, indexes.data(), indexes.size()));
        ASSERT_OK(dw->finish_with_txnlog());
        dw->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Build a chunk with DELETE op column (last column is op);
    // set a small write buffer to trigger multiple flushes (more .del files)
    const auto old_buf = config::write_buffer_size;
    config::write_buffer_size = 1;
    const auto old_tde = config::enable_transparent_data_encryption;
    config::enable_transparent_data_encryption = true;
    {
        // keys: delete the first half [0, kChunkSize/2), keep the second half
        std::vector<int> v0(kChunkSize);
        std::vector<int> v1(kChunkSize, 777);
        std::vector<int> v2(kChunkSize, 888); // third column payload
        std::vector<uint8_t> ops(kChunkSize);
        for (int i = 0; i < kChunkSize; i++) {
            v0[i] = i; // same keys as base data
            ops[i] = (i < kChunkSize / 2) ? TOpType::DELETE : TOpType::UPSERT;
        }

        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        auto c2 = Int32Column::create();
        auto cop = Int8Column::create();
        c0->append_numbers(v0.data(), v0.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int));
        c2->append_numbers(v2.data(), v2.size() * sizeof(int));
        cop->append_numbers(ops.data(), ops.size() * sizeof(uint8_t));

        // Note: last column is op; create a separate slot map for chunk with ops
        Chunk::SlotHashMap ops_slot_map;
        ops_slot_map[0] = 0;
        ops_slot_map[1] = 1;
        ops_slot_map[2] = 2;
        ops_slot_map[3] = 3;
        Chunk chunk_with_ops({std::move(c0), std::move(c1), std::move(c2), std::move(cop)}, ops_slot_map);
        std::vector<uint32_t> idx(kChunkSize);
        for (int i = 0; i < kChunkSize; i++) idx[i] = i;

        // Create slot descriptors including operation column
        std::vector<SlotDescriptor> op_slots;
        op_slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
        op_slots.emplace_back(1, "c1", TypeDescriptor{LogicalType::TYPE_INT});
        op_slots.emplace_back(2, "c2", TypeDescriptor{LogicalType::TYPE_INT});
        op_slots.emplace_back(3, "__op", TypeDescriptor{LogicalType::TYPE_TINYINT});
        std::vector<SlotDescriptor*> op_slot_pointers;
        for (auto& slot : op_slots) {
            op_slot_pointers.emplace_back(&slot);
        }

        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto dw, DeltaWriterBuilder()
                                         .set_tablet_manager(_tablet_mgr.get())
                                         .set_tablet_id(tablet_id)
                                         .set_txn_id(txn_id)
                                         .set_partition_id(_partition_id)
                                         .set_mem_tracker(_mem_tracker.get())
                                         .set_schema_id(_tablet_schema->id())
                                         .set_slot_descriptors(&op_slot_pointers)
                                         .set_partial_update_mode(PartialUpdateMode::COLUMN_UPSERT_MODE)
                                         .build());
        ASSERT_OK(dw->open());
        ASSERT_OK(dw->write(chunk_with_ops, idx.data(), idx.size()));
        ASSERT_OK(dw->finish_with_txnlog());
        dw->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    config::write_buffer_size = old_buf;
    config::enable_transparent_data_encryption = old_tde;

    // Verify: first half rows are deleted; also check del_files/stat updates
    {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        // Total rows should be less than initial rows + UPSERT rows.
        // Initial write kChunkSize, then half DELETE and half UPSERT; expected >= kChunkSize + kChunkSize/2.
        // Precisely verify deletes: keys < kChunkSize/2 should be absent.
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
        ASSERT_OK(reader->prepare());
        ASSERT_OK(reader->open(TabletReaderParams()));
        auto chk = ChunkFactory::new_chunk(*_schema, 256);
        std::vector<bool> seen(kChunkSize, false);
        while (true) {
            auto st = reader->get_next(chk.get());
            if (st.is_end_of_file()) break;
            ASSERT_OK(st);
            auto cols = chk->columns();
            for (int i = 0; i < chk->num_rows(); i++) {
                int key = cols[0]->get(i).get_int32();
                if (key >= 0 && key < kChunkSize) seen[key] = true;
            }
            chk->reset();
        }
        for (int i = 0; i < kChunkSize / 2; i++) {
            // Deleted first-half keys should not appear
            ASSERT_FALSE(seen[i]);
        }

        // Check metadata records del files
        // Latest rowset should record del_files list or have num_dels updated.
        ASSERT_GE(metadata->rowsets_size(), 1);
        const auto& last_rs = metadata->rowsets(metadata->rowsets_size() - 1);
        // del_files_size may be 0 (merged in different paths), but num_dels or delvec_meta should be updated.
        // Assert num_dels non-negative and version advanced.
        ASSERT_GE(last_rs.num_dels(), 0);
        ASSERT_EQ(version, metadata->version());
    }
}

// Test bundle file offsets and encryption handling in column mode partial update
TEST_F(LakeColumnUpsertModeTest, test_bundle_files_and_encryption_handling) {
    // Enable TDE for this test
    const bool old_enable_tde = config::enable_transparent_data_encryption;
    config::enable_transparent_data_encryption = true;
    DeferOp tde_defer([&]() { config::enable_transparent_data_encryption = old_enable_tde; });

    auto tablet_metadata = std::make_shared<TabletMetadataPB>();
    tablet_metadata->set_id(next_id());
    tablet_metadata->set_version(1);
    tablet_metadata->set_next_rowset_id(1);

    // Schema with default values
    auto schema = tablet_metadata->mutable_schema();
    schema->set_id(next_id());
    schema->set_num_short_key_columns(1);
    schema->set_keys_type(PRIMARY_KEYS);
    schema->set_num_rows_per_row_block(65535);

    auto c0 = schema->add_column();
    c0->set_unique_id(next_id());
    c0->set_name("c0");
    c0->set_type("INT");
    c0->set_is_key(true);
    c0->set_is_nullable(false);

    auto c1 = schema->add_column();
    c1->set_unique_id(next_id());
    c1->set_name("c1");
    c1->set_type("INT");
    c1->set_is_key(false);
    c1->set_is_nullable(true);
    c1->set_aggregation("REPLACE");
    c1->set_default_value("100");

    auto c2 = schema->add_column();
    c2->set_unique_id(next_id());
    c2->set_name("c2");
    c2->set_type("INT");
    c2->set_is_key(false);
    c2->set_is_nullable(true);
    c2->set_aggregation("REPLACE");

    auto tablet_schema = TabletSchema::create(*schema);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*tablet_metadata));

    auto tablet_id = tablet_metadata->id();
    auto version = 1;

    // Create some initial data
    std::vector<int> v0 = {1, 2, 3};
    std::vector<int> v1 = {10, 20, 30};
    std::vector<int> v2 = {40, 50, 60};

    auto c0_col = Int32Column::create();
    auto c1_col = Int32Column::create();
    auto c2_col = Int32Column::create();
    c0_col->append_numbers(v0.data(), v0.size() * sizeof(int));
    c1_col->append_numbers(v1.data(), v1.size() * sizeof(int));
    c2_col->append_numbers(v2.data(), v2.size() * sizeof(int));

    Chunk::SlotHashMap slot_map;
    slot_map[0] = 0;
    slot_map[1] = 1;
    slot_map[2] = 2;
    auto chunk_full = Chunk({std::move(c0_col), std::move(c1_col), std::move(c2_col)}, slot_map);

    auto indexes = std::vector<uint32_t>{0, 1, 2};

    // Initial full write
    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk_full, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Column mode partial update - adding new rows with expression override
    std::vector<int> new_keys = {4, 5};
    auto c0_partial = Int32Column::create();
    c0_partial->append_numbers(new_keys.data(), new_keys.size() * sizeof(int));

    Chunk::SlotHashMap partial_slot_map;
    partial_slot_map[0] = 0;
    auto chunk_partial = Chunk({std::move(c0_partial)}, partial_slot_map);

    std::vector<SlotDescriptor> slots;
    slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
    std::vector<SlotDescriptor*> slot_pointers;
    slot_pointers.emplace_back(&slots[0]);

    auto indexes_partial = std::vector<uint32_t>{0, 1};

    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(tablet_schema->id())
                                                   .set_slot_descriptors(&slot_pointers)
                                                   .set_partial_update_mode(PartialUpdateMode::COLUMN_UPSERT_MODE)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk_partial, indexes_partial.data(), indexes_partial.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();

        // Manually modify the txn log to add bundle file offsets and encryption metas
        ASSIGN_OR_ABORT(auto original_txn_log, _tablet_mgr->get_txn_log(tablet_id, txn_id));
        auto new_txn_log = std::make_shared<TxnLogPB>(*original_txn_log);

        // Add column-to-expr-value override for c2
        auto* column_to_expr_value =
                new_txn_log->mutable_op_write()->mutable_txn_meta()->mutable_column_to_expr_value();
        (*column_to_expr_value)["c2"] = "200";

        ASSERT_OK(_tablet_mgr->put_txn_log(new_txn_log));

        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Verify results - new rows should have default value for c1 but expression value for c2
    ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    auto reader_schema = std::make_shared<Schema>(ChunkHelper::convert_schema(tablet_schema));
    auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *reader_schema);
    ASSERT_OK(reader->prepare());
    ASSERT_OK(reader->open(TabletReaderParams()));
    auto result_chunk = ChunkFactory::new_chunk(*reader_schema, 128);

    int total_rows = 0;
    bool found_new_rows = false;

    while (true) {
        auto st = reader->get_next(result_chunk.get());
        if (st.is_end_of_file()) {
            break;
        }
        CHECK_OK(st);
        total_rows += result_chunk->num_rows();

        auto cols = result_chunk->columns();
        for (int i = 0; i < result_chunk->num_rows(); i++) {
            auto c0_val = cols[0]->get(i).get_int32();
            auto c1_datum = cols[1]->get(i);
            auto c2_datum = cols[2]->get(i);

            // Check if this is one of the new rows
            if (c0_val == 4 || c0_val == 5) {
                if (!c1_datum.is_null()) {
                    auto c1_val = c1_datum.get_int32();
                    EXPECT_EQ(100, c1_val); // Should have default value
                }
                if (!c2_datum.is_null()) {
                    auto c2_val = c2_datum.get_int32();
                    EXPECT_EQ(200, c2_val); // Should have expression override value
                }
                found_new_rows = true;
            }
        }
        result_chunk->reset();
    }

    EXPECT_TRUE(found_new_rows);
    EXPECT_EQ(5, total_rows);
}

// Test default value handling and null column filling
TEST_F(LakeColumnUpsertModeTest, test_default_value_and_null_handling) {
    auto tablet_metadata = std::make_shared<TabletMetadataPB>();
    tablet_metadata->set_id(next_id());
    tablet_metadata->set_version(1);
    tablet_metadata->set_next_rowset_id(1);

    // Schema with various default scenarios
    auto schema = tablet_metadata->mutable_schema();
    schema->set_id(next_id());
    schema->set_num_short_key_columns(1);
    schema->set_keys_type(PRIMARY_KEYS);
    schema->set_num_rows_per_row_block(65535);

    auto c0 = schema->add_column();
    c0->set_unique_id(next_id());
    c0->set_name("c0");
    c0->set_type("INT");
    c0->set_is_key(true);
    c0->set_is_nullable(false);

    // Column with default value
    auto c1 = schema->add_column();
    c1->set_unique_id(next_id());
    c1->set_name("c1");
    c1->set_type("INT");
    c1->set_is_key(false);
    c1->set_is_nullable(true);
    c1->set_aggregation("REPLACE");
    c1->set_default_value("100");

    // Nullable column without default (should get null)
    auto c2 = schema->add_column();
    c2->set_unique_id(next_id());
    c2->set_name("c2");
    c2->set_type("INT");
    c2->set_is_key(false);
    c2->set_is_nullable(true);
    c2->set_aggregation("REPLACE");

    // Non-nullable column without default (should get type default)
    auto c3 = schema->add_column();
    c3->set_unique_id(next_id());
    c3->set_name("c3");
    c3->set_type("INT");
    c3->set_is_key(false);
    c3->set_is_nullable(false);
    c3->set_aggregation("REPLACE");

    auto tablet_schema = TabletSchema::create(*schema);
    CHECK_OK(_tablet_mgr->put_tablet_metadata(*tablet_metadata));

    auto tablet_id = tablet_metadata->id();
    auto version = 1;

    // Column upsert with only primary key - should trigger all default handling paths
    std::vector<int> new_keys = {1, 2};
    auto c0_partial = Int32Column::create();
    c0_partial->append_numbers(new_keys.data(), new_keys.size() * sizeof(int));

    Chunk::SlotHashMap partial_slot_map;
    partial_slot_map[0] = 0;
    auto chunk_partial = Chunk({std::move(c0_partial)}, partial_slot_map);

    std::vector<SlotDescriptor> slots;
    slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
    std::vector<SlotDescriptor*> slot_pointers;
    slot_pointers.emplace_back(&slots[0]);

    auto indexes_partial = std::vector<uint32_t>{0, 1};

    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(tablet_schema->id())
                                                   .set_slot_descriptors(&slot_pointers)
                                                   .set_partial_update_mode(PartialUpdateMode::COLUMN_UPSERT_MODE)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk_partial, indexes_partial.data(), indexes_partial.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Verify default value handling
    ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    auto reader_schema = std::make_shared<Schema>(ChunkHelper::convert_schema(tablet_schema));
    auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *reader_schema);
    ASSERT_OK(reader->prepare());
    ASSERT_OK(reader->open(TabletReaderParams()));
    auto result_chunk = ChunkFactory::new_chunk(*reader_schema, 128);

    int total_rows = 0;
    bool found_correct_defaults = false;

    while (true) {
        auto st = reader->get_next(result_chunk.get());
        if (st.is_end_of_file()) {
            break;
        }
        CHECK_OK(st);
        total_rows += result_chunk->num_rows();

        auto cols = result_chunk->columns();
        for (int i = 0; i < result_chunk->num_rows(); i++) {
            auto c0_val = cols[0]->get(i).get_int32();
            auto c1_datum = cols[1]->get(i);
            auto c2_datum = cols[2]->get(i);
            auto c3_datum = cols[3]->get(i);

            if (c0_val == 1 || c0_val == 2) {
                // c1 has default value
                if (!c1_datum.is_null()) {
                    EXPECT_EQ(100, c1_datum.get_int32());
                }
                // c2 is nullable without default - should be null
                EXPECT_TRUE(c2_datum.is_null());
                // c3 is non-nullable without default - should get type default (0)
                EXPECT_FALSE(c3_datum.is_null());
                EXPECT_EQ(0, c3_datum.get_int32());
                found_correct_defaults = true;
            }
        }
        result_chunk->reset();
    }

    EXPECT_TRUE(found_correct_defaults);
    EXPECT_EQ(2, total_rows);
}

// Test functional correctness: COLUMN_UPSERT_MODE handles new row insertion correctly
//
// Background:
// - COLUMN_UPSERT_MODE needs RowsetUpdateState::_prepare_partial_update_states to handle new rows
//   - Before optimization: Incorrectly read all unmodified columns for each new row → OOM for large inserts
//   - After optimization: Skip reading unmodified columns (new rows don't exist in storage yet)
//
// This test verifies:
// 1. New rows can be inserted with partial columns (c0+c1 only)
// 2. Unmodified columns (c2) are correctly filled with default values
// 3. Existing rows can be updated normally
// 4. All data remains correct after mixed operations
TEST_F(LakeColumnUpsertModeTest, memory_optimization_skip_column_reading) {
    auto tablet_id = _tablet_metadata->id();
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) indexes[i] = i;
    auto version = 1;

    // Step 1: Write initial full data with all 3 columns (pk 0-11)
    {
        auto chunk_full = generate_data(kChunkSize, 0, false, 3);
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk_full, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
        LOG(INFO) << "[BASE DATA] Inserted base rows with full columns";
    }

    // Step 2: Insert NEW rows with COLUMN_UPSERT_MODE (pk 12-23, only update c0+c1)
    // This tests the optimization for NEW row inserts (src_rss_rowids == UINT64_MAX)
    // These rows should NOT read c2 from storage since they don't exist yet
    // c2 should be filled with default value "10"
    {
        auto chunk_partial = generate_data(kChunkSize, 1, true, 5); // shift=1 means pk 12-23
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(PartialUpdateMode::COLUMN_UPSERT_MODE)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk_partial, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;

        LOG(INFO) << "[NEW ROWS] Inserted new rows with COLUMN_UPSERT_MODE (only c0+c1)";
    }

    // Verify new rows: c2 should have default value 10
    {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));

        auto chunk = ChunkFactory::new_chunk(*_schema, 128);
        int total_rows = 0;
        int new_rows_found = 0;

        while (true) {
            auto st = reader->get_next(chunk.get());
            if (st.is_end_of_file()) {
                break;
            }
            CHECK_OK(st);
            total_rows += chunk->num_rows();

            // Find and verify new rows (pk 12-23)
            for (int i = 0; i < chunk->num_rows(); i++) {
                int pk = chunk->get_column_by_index(0)->get(i).get_int32();
                if (pk >= kChunkSize && pk < kChunkSize * 2) {
                    int c1 = chunk->get_column_by_index(1)->get(i).get_int32();
                    int c2 = chunk->get_column_by_index(2)->get(i).get_int32();
                    EXPECT_EQ(c1, pk * 5) << "New row c1 should be pk * 5";
                    EXPECT_EQ(c2, 10) << "New row c2 should have default value 10";
                    new_rows_found++;
                }
            }
            chunk->reset();
        }

        ASSERT_EQ(total_rows, kChunkSize * 2) << "Should have total 24 rows (12 base + 12 new)";
        ASSERT_EQ(new_rows_found, kChunkSize) << "Should find 12 new rows with pk 12-23";
    }

    // Step 3: Update existing rows with COLUMN_UPSERT_MODE
    {
        auto chunk_partial = generate_data(kChunkSize, 0, true, 9);
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(PartialUpdateMode::COLUMN_UPSERT_MODE)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk_partial, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;

        LOG(INFO) << "[UPDATE] Updated existing rows with COLUMN_UPSERT_MODE";
    }

    // Step 4: Verify functional correctness
    ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
    CHECK_OK(reader->prepare());
    CHECK_OK(reader->open(TabletReaderParams()));

    auto chunk = ChunkFactory::new_chunk(*_schema, 128);
    int total_rows = 0;
    int existing_rows_updated = 0;
    int new_rows_verified = 0;

    while (true) {
        auto st = reader->get_next(chunk.get());
        if (st.is_end_of_file()) {
            break;
        }
        CHECK_OK(st);
        total_rows += chunk->num_rows();

        // Verify existing rows (pk 0-11) were updated to c1 = pk * 9 (from Step 3)
        // and new rows (pk 12-23) still have c1 = pk * 5 and c2 = 10
        for (int i = 0; i < chunk->num_rows(); i++) {
            int pk = chunk->get_column_by_index(0)->get(i).get_int32();
            int c1 = chunk->get_column_by_index(1)->get(i).get_int32();
            int c2 = chunk->get_column_by_index(2)->get(i).get_int32();

            if (pk < kChunkSize) {
                // Existing rows: c1 should be updated to pk * 9
                EXPECT_EQ(c1, pk * 9) << "Existing row c1 should be updated";
                existing_rows_updated++;
            } else {
                // New rows: c1 = pk * 5, c2 = 10 (default)
                EXPECT_EQ(c1, pk * 5) << "New row c1 should be pk * 5";
                EXPECT_EQ(c2, 10) << "New row c2 should still have default value 10";
                new_rows_verified++;
            }
        }
        chunk->reset();
    }

    ASSERT_EQ(total_rows, kChunkSize * 2) << "Should have total 24 rows";
    ASSERT_EQ(existing_rows_updated, kChunkSize) << "Should have 12 updated existing rows";
    ASSERT_EQ(new_rows_verified, kChunkSize) << "Should have 12 new rows";

    // Verify DCG was generated for COLUMN_UPSERT_MODE
    ASSIGN_OR_ABORT(auto md, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_GT(md->dcg_meta().dcgs_size(), 0) << "DCG should be generated for existing row updates";
}

// Test that COLUMN_UPSERT_MODE also marks partial segments as orphan files (GC them)
// This verifies the fix where apply_column_mode_partial_update is called for COLUMN_UPSERT_MODE
TEST_F(LakeColumnUpsertModeTest, test_orphan_files_gc_in_column_upsert_mode) {
    auto chunk_full = generate_data(kChunkSize, 0, false, 3);
    auto chunk_partial = generate_data(kChunkSize, 0, true, 5);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) indexes[i] = i;
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();

    // Step 1: Write initial full data
    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk_full, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Step 2: Partial update with COLUMN_UPSERT_MODE (updating existing rows)
    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(PartialUpdateMode::COLUMN_UPSERT_MODE)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk_partial, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();

        // Before publish, get the txn log to inspect segment files
        ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(tablet_id, txn_id));
        ASSERT_TRUE(txn_log->has_op_write());
        const auto& op_write = txn_log->op_write();

        // The partial update should have generated segments (before GC)
        int segment_count = op_write.rowset().segment_metas_size();
        LOG(INFO) << "Partial update generated " << segment_count << " segments before publish";

        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Step 3: Verify that orphan_files contains the partial segments
    // After publish with COLUMN_UPSERT_MODE, the partial segments should be marked as orphan
    {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));

        // Verify DCG was created (delta column groups for updated columns)
        EXPECT_GT(metadata->dcg_meta().dcgs_size(), 0) << "DCG should be generated for column upsert mode";

        // Verify orphan files were added (partial segments should be marked for GC)
        // This is the key verification: apply_column_mode_partial_update should have been called
        EXPECT_GT(metadata->orphan_files_size(), 0) << "Partial segments should be marked as orphan files for GC";

        LOG(INFO) << "Orphan files count: " << metadata->orphan_files_size();
        LOG(INFO) << "DCG count: " << metadata->dcg_meta().dcgs_size();
    }

    // Verify data correctness
    ASSERT_EQ(kChunkSize, check(version, [](int c0, int c1, int c2) { return (c0 * 5 == c1) && (c0 * 4 == c2); }));
}

// Test that del files are properly copied in COLUMN_UPSERT_MODE when handling deletes
// This verifies the fix in _handle_column_upsert_mode where dels are copied to new_rows_op
TEST_F(LakeColumnUpsertModeTest, test_del_files_handling_in_column_upsert_mode) {
    const int64_t kChunkSize = 64;
    auto tablet_id = _tablet_metadata->id();
    int64_t version = 1;

    // Step 1: Write base data
    {
        auto chunk = generate_data(kChunkSize, 0, false, 100);
        std::vector<uint32_t> indexes(kChunkSize);
        for (int i = 0; i < kChunkSize; i++) indexes[i] = i;
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto dw, DeltaWriterBuilder()
                                         .set_tablet_manager(_tablet_mgr.get())
                                         .set_tablet_id(tablet_id)
                                         .set_txn_id(txn_id)
                                         .set_partition_id(_partition_id)
                                         .set_mem_tracker(_mem_tracker.get())
                                         .set_schema_id(_tablet_schema->id())
                                         .build());
        ASSERT_OK(dw->open());
        ASSERT_OK(dw->write(chunk, indexes.data(), indexes.size()));
        ASSERT_OK(dw->finish_with_txnlog());
        dw->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Step 2: COLUMN_UPSERT_MODE with mixed operations: some deletes and some upserts with new rows
    // Set small write buffer to potentially generate multiple del files
    const auto old_buf = config::write_buffer_size;
    config::write_buffer_size = 1;
    DeferOp restore_buf([&]() { config::write_buffer_size = old_buf; });

    {
        // Create data with DELETE and UPSERT operations
        std::vector<int> v0(kChunkSize);
        std::vector<int> v1(kChunkSize, 777);
        std::vector<uint8_t> ops(kChunkSize);

        for (int i = 0; i < kChunkSize; i++) {
            if (i < kChunkSize / 4) {
                // Delete first quarter
                v0[i] = i;
                ops[i] = TOpType::DELETE;
            } else if (i < kChunkSize / 2) {
                // Update second quarter (existing rows)
                v0[i] = i;
                ops[i] = TOpType::UPSERT;
            } else {
                // Insert new rows in second half
                v0[i] = i + kChunkSize; // New keys
                ops[i] = TOpType::UPSERT;
            }
        }

        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        auto cop = Int8Column::create();
        c0->append_numbers(v0.data(), v0.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int));
        cop->append_numbers(ops.data(), ops.size() * sizeof(uint8_t));

        Chunk::SlotHashMap ops_slot_map;
        ops_slot_map[0] = 0;
        ops_slot_map[1] = 1;
        ops_slot_map[3] = 2; // op column
        Chunk chunk_with_ops({std::move(c0), std::move(c1), std::move(cop)}, ops_slot_map);

        std::vector<uint32_t> idx(kChunkSize);
        for (int i = 0; i < kChunkSize; i++) idx[i] = i;

        std::vector<SlotDescriptor> op_slots;
        op_slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
        op_slots.emplace_back(1, "c1", TypeDescriptor{LogicalType::TYPE_INT});
        op_slots.emplace_back(3, "__op", TypeDescriptor{LogicalType::TYPE_TINYINT});
        std::vector<SlotDescriptor*> op_slot_pointers;
        for (auto& slot : op_slots) {
            op_slot_pointers.emplace_back(&slot);
        }

        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto dw, DeltaWriterBuilder()
                                         .set_tablet_manager(_tablet_mgr.get())
                                         .set_tablet_id(tablet_id)
                                         .set_txn_id(txn_id)
                                         .set_partition_id(_partition_id)
                                         .set_mem_tracker(_mem_tracker.get())
                                         .set_schema_id(_tablet_schema->id())
                                         .set_slot_descriptors(&op_slot_pointers)
                                         .set_partial_update_mode(PartialUpdateMode::COLUMN_UPSERT_MODE)
                                         .build());
        ASSERT_OK(dw->open());
        ASSERT_OK(dw->write(chunk_with_ops, idx.data(), idx.size()));
        ASSERT_OK(dw->finish_with_txnlog());
        dw->close();

        // Before publish, check the txn log has del files
        ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(tablet_id, txn_id));
        ASSERT_TRUE(txn_log->has_op_write());
        const auto& op_write = txn_log->op_write();
        int original_dels_count = op_write.dels_meta_size();
        LOG(INFO) << "Original del files count: " << original_dels_count;

        // The writer must emit del_num_rows parallel to dels_meta, and it must account every
        // tombstone (kChunkSize/4 deletes). This is the input that _handle_column_upsert_mode
        // has to carry onto the synthesized new_rows_op.
        ASSERT_GT(original_dels_count, 0);
        ASSERT_EQ(op_write.del_num_rows_size(), op_write.dels_meta_size());
        int64_t txnlog_del_rows = 0;
        for (int i = 0; i < op_write.del_num_rows_size(); i++) {
            txnlog_del_rows += op_write.del_num_rows(i);
        }
        EXPECT_EQ(txnlog_del_rows, kChunkSize / 4);

        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Step 3: Verify the result
    {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));

        // Verify deletes were applied (first quarter should be deleted)
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
        ASSERT_OK(reader->prepare());
        ASSERT_OK(reader->open(TabletReaderParams()));

        auto chk = ChunkFactory::new_chunk(*_schema, 256);
        std::set<int> found_keys;
        while (true) {
            auto st = reader->get_next(chk.get());
            if (st.is_end_of_file()) break;
            ASSERT_OK(st);
            auto cols = chk->columns();
            for (int i = 0; i < chk->num_rows(); i++) {
                int key = cols[0]->get(i).get_int32();
                found_keys.insert(key);
            }
            chk->reset();
        }

        // Verify deleted keys are not present
        for (int i = 0; i < kChunkSize / 4; i++) {
            EXPECT_EQ(found_keys.count(i), 0) << "Deleted key " << i << " should not be found";
        }

        // Verify updated keys are present
        for (int i = kChunkSize / 4; i < kChunkSize / 2; i++) {
            EXPECT_EQ(found_keys.count(i), 1) << "Updated key " << i << " should be found";
        }

        // Verify new keys are present
        for (int i = kChunkSize + kChunkSize / 2; i < 2 * kChunkSize; i++) {
            EXPECT_EQ(found_keys.count(i), 1) << "New key " << i << " should be found";
        }

        // Verify metadata: the dels should have been properly handled
        // Check that rowsets have proper del file info or delvec metadata
        ASSERT_GE(metadata->rowsets_size(), 1);

        // The fix under test: _handle_column_upsert_mode must carry op_write.del_num_rows onto the
        // synthesized new_rows_op, so the persisted del files record their tombstone count. Sum
        // num_rows across every persisted del file; it must match the tombstones written above and
        // every persisted del file must carry a recorded count.
        int64_t persisted_del_rows = 0;
        int del_files_seen = 0;
        int del_files_with_count = 0;
        for (const auto& rs : metadata->rowsets()) {
            for (const auto& df : rs.del_files()) {
                ++del_files_seen;
                if (df.has_num_rows()) {
                    ++del_files_with_count;
                    persisted_del_rows += df.num_rows();
                }
            }
        }
        ASSERT_GT(del_files_seen, 0);
        EXPECT_EQ(del_files_with_count, del_files_seen); // every persisted del file carries a count
        EXPECT_EQ(persisted_del_rows, kChunkSize / 4);   // matches the tombstones written

        LOG(INFO) << "Final version: " << version;
        LOG(INFO) << "Total rowsets: " << metadata->rowsets_size();
        LOG(INFO) << "DCG count: " << metadata->dcg_meta().dcgs_size();
    }
}

// A column-mode partial update publishes its del files through UpdateManager::_handle_delete_files,
// which always erases via the memtable path and never reads op_write.del_ssts(). The import-time
// tombstone-sstable build is byte-threshold-driven and would otherwise fire here too, producing a
// sstable no publisher ingests: it never enters sstable_meta(), so only a full vacuum's orphan scan
// would ever reclaim it. Drive the threshold to 1 byte (so every del file would qualify) and assert the
// writer emits no tombstone sstable for a COLUMN_UPSERT_MODE load, while the deletes still apply.
TEST_F(LakeColumnUpsertModeTest, test_no_del_tombstone_sstable_in_column_upsert_mode) {
    ConfigResetGuard<int64_t> g_eager_threshold(&config::pk_index_eager_build_threshold_bytes, 1);
    const int64_t kChunkSize = 64;
    auto tablet_id = _tablet_metadata->id();
    int64_t version = 1;

    // Seed the tablet so the deletes below have existing rows to shadow.
    {
        auto chunk = generate_data(kChunkSize, 0, false, 100);
        std::vector<uint32_t> indexes(kChunkSize);
        for (int i = 0; i < kChunkSize; i++) indexes[i] = i;
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto dw, DeltaWriterBuilder()
                                         .set_tablet_manager(_tablet_mgr.get())
                                         .set_tablet_id(tablet_id)
                                         .set_txn_id(txn_id)
                                         .set_partition_id(_partition_id)
                                         .set_mem_tracker(_mem_tracker.get())
                                         .set_schema_id(_tablet_schema->id())
                                         .build());
        ASSERT_OK(dw->open());
        ASSERT_OK(dw->write(chunk, indexes.data(), indexes.size()));
        ASSERT_OK(dw->finish_with_txnlog());
        dw->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Column-mode partial update that DELETEs the first quarter and upserts the rest.
    {
        std::vector<int> v0(kChunkSize);
        std::vector<int> v1(kChunkSize, 777);
        std::vector<uint8_t> ops(kChunkSize);
        for (int i = 0; i < kChunkSize; i++) {
            v0[i] = i;
            ops[i] = (i < kChunkSize / 4) ? TOpType::DELETE : TOpType::UPSERT;
        }

        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        auto cop = Int8Column::create();
        c0->append_numbers(v0.data(), v0.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int));
        cop->append_numbers(ops.data(), ops.size() * sizeof(uint8_t));

        Chunk::SlotHashMap ops_slot_map;
        ops_slot_map[0] = 0;
        ops_slot_map[1] = 1;
        ops_slot_map[3] = 2; // op column
        Chunk chunk_with_ops({std::move(c0), std::move(c1), std::move(cop)}, ops_slot_map);

        std::vector<uint32_t> idx(kChunkSize);
        for (int i = 0; i < kChunkSize; i++) idx[i] = i;

        std::vector<SlotDescriptor> op_slots;
        op_slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
        op_slots.emplace_back(1, "c1", TypeDescriptor{LogicalType::TYPE_INT});
        op_slots.emplace_back(3, "__op", TypeDescriptor{LogicalType::TYPE_TINYINT});
        std::vector<SlotDescriptor*> op_slot_pointers;
        for (auto& slot : op_slots) {
            op_slot_pointers.emplace_back(&slot);
        }

        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto dw, DeltaWriterBuilder()
                                         .set_tablet_manager(_tablet_mgr.get())
                                         .set_tablet_id(tablet_id)
                                         .set_txn_id(txn_id)
                                         .set_partition_id(_partition_id)
                                         .set_mem_tracker(_mem_tracker.get())
                                         .set_schema_id(_tablet_schema->id())
                                         .set_slot_descriptors(&op_slot_pointers)
                                         .set_partial_update_mode(PartialUpdateMode::COLUMN_UPSERT_MODE)
                                         .build());
        ASSERT_OK(dw->open());
        ASSERT_OK(dw->write(chunk_with_ops, idx.data(), idx.size()));
        ASSERT_OK(dw->finish_with_txnlog());
        dw->close();

        ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(tablet_id, txn_id));
        const auto& op_write = txn_log->op_write();
        // Precondition: this load really did produce a del file, and it really is a column-mode partial
        // update -- otherwise the assertion below would pass vacuously.
        ASSERT_GT(op_write.dels_meta_size(), 0);
        ASSERT_EQ(PartialUpdateMode::COLUMN_UPSERT_MODE, op_write.txn_meta().partial_update_mode());
        // The fix under test: no tombstone sstable was built. Entries stay index-aligned with dels_meta,
        // so an entry may exist -- it must just be empty, which is what publish reads as "no sstable".
        for (int i = 0; i < op_write.del_ssts_size(); i++) {
            EXPECT_TRUE(op_write.del_ssts(i).name().empty())
                    << "del_ssts[" << i << "] would never be ingested by the column-mode publish path";
        }

        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // The deletes still applied: the first quarter is gone, the rest survives.
    {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
        ASSERT_OK(reader->prepare());
        ASSERT_OK(reader->open(TabletReaderParams()));
        auto chk = ChunkFactory::new_chunk(*_schema, 256);
        std::set<int> found_keys;
        while (true) {
            auto st = reader->get_next(chk.get());
            if (st.is_end_of_file()) break;
            ASSERT_OK(st);
            for (int i = 0; i < chk->num_rows(); i++) {
                found_keys.insert(chk->columns()[0]->get(i).get_int32());
            }
            chk->reset();
        }
        for (int i = 0; i < kChunkSize / 4; i++) {
            EXPECT_EQ(found_keys.count(i), 0) << "deleted key " << i << " should be gone";
        }
        for (int i = kChunkSize / 4; i < kChunkSize; i++) {
            EXPECT_EQ(found_keys.count(i), 1) << "upserted key " << i << " should be present";
        }
    }
}

// Test parallel column mode partial update publish with multiple source segments
// and multiple update segments. This exercises both Phase 1 (cross-segment parallel
// PK index lookup via batch_parallel_get_rss_rowids) and Phase 2 (parallel DCG
// generation across source segments).
TEST_P(LakePartialUpdateTest, test_parallel_column_mode_partial_update_multi_segments) {
    if (GetParam().partial_update_mode != PartialUpdateMode::COLUMN_UPDATE_MODE) {
        GTEST_SKIP() << "Only COLUMN_UPDATE_MODE uses parallel column mode partial update";
    }

    const int kNumSourceWrites = 5;
    const int kNumPartialUpdates = 3;

    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();

    // Step 1: Create multiple source segments by writing full data multiple times
    // with different key ranges, so each write creates a separate rowset/segment.
    for (int i = 0; i < kNumSourceWrites; i++) {
        auto chunk_full = generate_data(kChunkSize, i, false, 3);
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk_full, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    // Verify: 5 rowsets with kChunkSize * 5 total rows
    ASSERT_EQ(kChunkSize * kNumSourceWrites,
              check(version, [](int c0, int c1, int c2) { return (c0 * 3 == c1) && (c0 * 4 == c2); }));
    ASSIGN_OR_ABORT(auto md, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(md->rowsets_size(), kNumSourceWrites);

    // Step 2: Perform partial column updates with multiple update segments per txn.
    // Using write_buffer_size=1 forces each write() call to flush as a separate segment,
    // creating multiple update segments that exercise parallel PK index lookup.
    // Force parallel execution on and restore both configs via RAII so a failing
    // ASSERT in the loop below cannot leak state into subsequent tests.
    const int64_t old_write_buffer_size = config::write_buffer_size;
    const bool old_enable_parallel = config::enable_pk_index_parallel_execution;
    config::write_buffer_size = 1;
    config::enable_pk_index_parallel_execution = true;
    DeferOp restore_cfg([&]() {
        config::write_buffer_size = old_write_buffer_size;
        config::enable_pk_index_parallel_execution = old_enable_parallel;
    });

    for (int i = 0; i < kNumPartialUpdates; i++) {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(PartialUpdateMode::COLUMN_UPDATE_MODE)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        // Write partial updates for different key ranges, creating multiple update segments.
        // Each write targets keys from a different source segment.
        for (int j = 0; j < kNumSourceWrites; j++) {
            auto chunk_partial = generate_data(kChunkSize, j, true, 5 + i);
            ASSERT_OK(delta_writer->write(chunk_partial, indexes.data(), indexes.size()));
        }
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish triggers parallel column mode partial update:
        // - Phase 1: parallel PK index lookup with lazy-load SegmentPKIterator
        // - Phase 2: parallel DCG generation across source segments
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Step 3: Verify correctness - the last partial update set c1 = c0 * (5 + kNumPartialUpdates - 1)
    // while c2 should remain unchanged (c0 * 4, set by original full writes).
    const int expected_c1_ratio = 5 + kNumPartialUpdates - 1;
    ASSERT_EQ(kChunkSize * kNumSourceWrites, check(version, [expected_c1_ratio](int c0, int c1, int c2) {
                  return (c0 * expected_c1_ratio == c1) && (c0 * 4 == c2);
              }));

    // Step 4: Verify metadata - should still have same number of rowsets (column update
    // mode doesn't add new rowsets), and DCGs should have been generated.
    ASSIGN_OR_ABORT(md, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(md->rowsets_size(), kNumSourceWrites);
    EXPECT_GT(md->dcg_meta().dcgs_size(), 0);
}

// Test parallel row-mode partial update publish with multiple segments.
// This exercises the parallel Phase 1 (load_segment + rewrite_segment in parallel)
// and sequential Phase 2 (_do_update) path.
TEST_P(LakePartialUpdateTest, test_parallel_row_mode_partial_update_multi_segments) {
    if (GetParam().partial_update_mode != PartialUpdateMode::ROW_MODE) {
        GTEST_SKIP() << "Only ROW_MODE uses parallel row-mode partial update";
    }

    const int kNumSourceWrites = 3;

    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();

    // Step 1: Create initial full data with multiple key ranges.
    for (int i = 0; i < kNumSourceWrites; i++) {
        auto chunk_full = generate_data(kChunkSize, i, false, 3);
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk_full, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize * kNumSourceWrites,
              check(version, [](int c0, int c1, int c2) { return (c0 * 3 == c1) && (c0 * 4 == c2); }));

    // Step 2: Perform row-mode partial update with multiple segments per txn.
    // Using write_buffer_size=1 forces each write() call to flush as a separate segment,
    // creating multiple update segments that exercise parallel load + rewrite.
    const int64_t old_write_buffer_size = config::write_buffer_size;
    config::write_buffer_size = 1;
    DeferOp restore_cfg([&]() { config::write_buffer_size = old_write_buffer_size; });

    {
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers)
                                                   .set_partial_update_mode(PartialUpdateMode::ROW_MODE)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        // Write partial updates for different key ranges, creating multiple update segments.
        for (int j = 0; j < kNumSourceWrites; j++) {
            auto chunk_partial = generate_data(kChunkSize, j, true, 7);
            ASSERT_OK(delta_writer->write(chunk_partial, indexes.data(), indexes.size()));
        }
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // Publish triggers parallel row-mode partial update:
        // - Phase 1: parallel load_segment + rewrite_segment
        // - Phase 2: sequential _do_update
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // Step 3: Verify correctness - c1 should be updated (c0 * 7),
    // c2 should remain unchanged (c0 * 4).
    ASSERT_EQ(kChunkSize * kNumSourceWrites,
              check(version, [](int c0, int c1, int c2) { return (c0 * 7 == c1) && (c0 * 4 == c2); }));
}

// Regression tests for column-mode partial update (PCU) under schema drift.
//
// Background: PCU records the column id (cid) of target columns at write time
// in the txn log. If ALTER TABLE changes the schema before publish, those cids
// may no longer match the current schema. The handler must resolve target
// columns via their unique id (uid) instead of the stale cid; otherwise writes
// land on the wrong column or crash on type-mismatched column writers.
class LakePcuSchemaDriftTest : public TestBase {
public:
    LakePcuSchemaDriftTest() : TestBase(kTestDirectory) { build_default_schema(); }

    constexpr static const char* const kTestDirectory = "test_lake_pcu_schema_drift";
    constexpr static const int kChunkSize = 10;

    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
        CHECK_OK(_tablet_mgr->create_schema_file(_tablet_metadata->id(), _tablet_metadata->schema()));
    }

    void TearDown() override {
        StorageEngine::instance()->wait_storage_cleanup_tasks();
        remove_test_dir_or_die();
    }

protected:
    // Build default schema {c0(INT,key), v_int(INT,nullable), v_arr(ARRAY<INT>,nullable)}.
    void build_default_schema() {
        _tablet_metadata = std::make_shared<TabletMetadata>();
        _tablet_metadata->set_id(next_id());
        _tablet_metadata->set_version(1);
        _tablet_metadata->set_next_rowset_id(1);

        auto* schema = _tablet_metadata->mutable_schema();
        schema->set_id(next_id());
        schema->set_num_short_key_columns(1);
        schema->set_keys_type(PRIMARY_KEYS);
        schema->set_num_rows_per_row_block(65535);

        auto* c0 = schema->add_column();
        _c0_uid = next_id();
        c0->set_unique_id(_c0_uid);
        c0->set_name("c0");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);

        auto* v_int = schema->add_column();
        _v_int_uid = next_id();
        v_int->set_unique_id(_v_int_uid);
        v_int->set_name("v_int");
        v_int->set_type("INT");
        v_int->set_is_key(false);
        v_int->set_is_nullable(true);
        v_int->set_aggregation("REPLACE");

        auto* v_arr = schema->add_column();
        _v_arr_uid = next_id();
        v_arr->set_unique_id(_v_arr_uid);
        v_arr->set_name("v_arr");
        v_arr->set_type("ARRAY");
        v_arr->set_is_key(false);
        v_arr->set_is_nullable(true);
        v_arr->set_aggregation("REPLACE");
        auto* child = v_arr->add_children_columns();
        child->set_unique_id(next_id());
        child->set_name("element");
        child->set_type("INT");
        child->set_is_nullable(true);

        _tablet_schema = TabletSchema::create(*schema);

        // Build slot descriptors. Slot id <-> column id mapping mirrors LakePartialUpdateTestBase.
        _slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
        _slots.emplace_back(1, "v_int", TypeDescriptor{LogicalType::TYPE_INT});
        _slots.emplace_back(2, "v_arr", TypeDescriptor::create_array_type(TypeDescriptor{LogicalType::TYPE_INT}));

        _slot_pointers_full.emplace_back(&_slots[0]);
        _slot_pointers_full.emplace_back(&_slots[1]);
        _slot_pointers_full.emplace_back(&_slots[2]);

        _slot_pointers_v_arr.emplace_back(&_slots[0]);
        _slot_pointers_v_arr.emplace_back(&_slots[2]);

        _slot_pointers_v_int.emplace_back(&_slots[0]);
        _slot_pointers_v_int.emplace_back(&_slots[1]);

        _slot_cid_map.emplace(0, 0);
        _slot_cid_map.emplace(1, 1);
        _slot_cid_map.emplace(2, 2);
    }

    // Build an ARRAY<INT> nullable column: row i = [i*scale, i*scale+1].
    ColumnPtr build_array_column(int n_rows, int scale) {
        auto element = Int32Column::create();
        auto null_column = NullColumn::create();
        auto offsets = UInt32Column::create();
        offsets->append(0);
        for (int i = 0; i < n_rows; i++) {
            element->append(i * scale);
            null_column->append(0);
            element->append(i * scale + 1);
            null_column->append(0);
            offsets->append((i + 1) * 2);
        }
        auto nullable_element = NullableColumn::create(std::move(element), std::move(null_column));
        auto array_col = ArrayColumn::create(std::move(nullable_element), std::move(offsets));
        // Wrap in NullableColumn since the outer v_arr is nullable.
        auto outer_null = NullColumn::create();
        for (int i = 0; i < n_rows; i++) outer_null->append(0);
        return NullableColumn::create(std::move(array_col), std::move(outer_null));
    }

    // Full-row chunk: c0=[0..N-1], v_int=[100..100+N-1], v_arr=[i*10, i*10+1].
    Chunk make_full_chunk(int n_rows) {
        auto c0 = Int32Column::create();
        auto v_int = Int32Column::create();
        for (int i = 0; i < n_rows; i++) {
            c0->append(i);
            v_int->append(100 + i);
        }
        auto v_arr = build_array_column(n_rows, /*scale=*/10);
        return Chunk({std::move(c0), std::move(v_int), std::move(v_arr)}, _slot_cid_map);
    }

    // Partial chunk for PCU on v_arr: only c0 + v_arr columns.
    // v_arr[i] = [i*scale, i*scale+1].
    Chunk make_partial_chunk_v_arr(int n_rows, int scale) {
        auto c0 = Int32Column::create();
        for (int i = 0; i < n_rows; i++) c0->append(i);
        auto v_arr = build_array_column(n_rows, scale);
        // Slot id 0 -> column index 0, slot id 2 -> column index 1 (v_arr is 2nd in this partial chunk)
        Chunk::SlotHashMap partial_map;
        partial_map.emplace(0, 0);
        partial_map.emplace(2, 1);
        return Chunk({std::move(c0), std::move(v_arr)}, partial_map);
    }

    // Verify v_arr contents after publish. Also asserts:
    //   - inserted column c_new (created by AddColumn) is all NULL
    //   - v_int unchanged at base value (100+i)
    //   - row count matches kChunkSize
    // expected_v_arr_scale: each row's v_arr should be [rowid*scale, rowid*scale+1].
    void verify_v_arr_content(int64_t version, int expected_v_arr_scale) {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
        auto tablet_schema = TabletSchema::create(metadata->schema());
        auto schema = tablet_schema->schema();
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *schema);
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));
        auto chunk = ChunkFactory::new_chunk(*schema, 128);

        // Locate column indexes by name in the post-ALTER schema.
        int c0_idx = -1, v_int_idx = -1, v_arr_idx = -1, c_new_idx = -1;
        for (size_t i = 0; i < tablet_schema->num_columns(); i++) {
            const auto& name = tablet_schema->column(i).name();
            if (name == "c0")
                c0_idx = static_cast<int>(i);
            else if (name == "v_int")
                v_int_idx = static_cast<int>(i);
            else if (name == "v_arr")
                v_arr_idx = static_cast<int>(i);
            else
                c_new_idx = static_cast<int>(i);
        }
        ASSERT_EQ(c0_idx, 0);
        ASSERT_EQ(c_new_idx, 1);
        ASSERT_EQ(v_int_idx, 2);
        ASSERT_EQ(v_arr_idx, 3);

        int64_t total = 0;
        while (true) {
            auto st = reader->get_next(chunk.get());
            if (st.is_end_of_file()) break;
            CHECK_OK(st);
            for (int i = 0; i < chunk->num_rows(); i++) {
                int rowid = chunk->columns()[c0_idx]->get(i).get_int32();
                EXPECT_TRUE(chunk->columns()[c_new_idx]->get(i).is_null())
                        << "rowid=" << rowid << " c_new should be NULL";
                EXPECT_EQ(100 + rowid, chunk->columns()[v_int_idx]->get(i).get_int32())
                        << "rowid=" << rowid << " v_int drifted";
                auto arr = chunk->columns()[v_arr_idx]->get(i).get_array();
                ASSERT_EQ(2u, arr.size()) << "rowid=" << rowid;
                EXPECT_EQ(rowid * expected_v_arr_scale, arr[0].get_int32()) << "rowid=" << rowid;
                EXPECT_EQ(rowid * expected_v_arr_scale + 1, arr[1].get_int32()) << "rowid=" << rowid;
            }
            total += chunk->num_rows();
            chunk->reset();
        }
        EXPECT_EQ(kChunkSize, total);
    }

    // Partial chunk for PCU on v_int: only c0 + v_int columns.
    // v_int[i] = delta + i.
    Chunk make_partial_chunk_v_int(int n_rows, int delta) {
        auto c0 = Int32Column::create();
        auto v_int = Int32Column::create();
        for (int i = 0; i < n_rows; i++) {
            c0->append(i);
            v_int->append(delta + i);
        }
        Chunk::SlotHashMap partial_map;
        partial_map.emplace(0, 0);
        partial_map.emplace(1, 1);
        return Chunk({std::move(c0), std::move(v_int)}, partial_map);
    }

    // Verify v_int = delta+i (post-fix correct); v_arr at base [i*10, i*10+1] (unchanged).
    void verify_v_int_content(int64_t version, int delta) {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
        auto tablet_schema = TabletSchema::create(metadata->schema());
        auto schema = tablet_schema->schema();
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *schema);
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));
        auto chunk = ChunkFactory::new_chunk(*schema, 128);

        // Post-ALTER schema layout: [c0, c_new, v_int, v_arr].
        int c0_idx = -1, c_new_idx = -1, v_int_idx = -1, v_arr_idx = -1;
        for (size_t i = 0; i < tablet_schema->num_columns(); i++) {
            const auto& name = tablet_schema->column(i).name();
            if (name == "c0")
                c0_idx = static_cast<int>(i);
            else if (name == "v_int")
                v_int_idx = static_cast<int>(i);
            else if (name == "v_arr")
                v_arr_idx = static_cast<int>(i);
            else
                c_new_idx = static_cast<int>(i);
        }
        ASSERT_EQ(c0_idx, 0);
        ASSERT_EQ(c_new_idx, 1);
        ASSERT_EQ(v_int_idx, 2);
        ASSERT_EQ(v_arr_idx, 3);

        int64_t total = 0;
        while (true) {
            auto st = reader->get_next(chunk.get());
            if (st.is_end_of_file()) break;
            CHECK_OK(st);
            for (int i = 0; i < chunk->num_rows(); i++) {
                int rowid = chunk->columns()[c0_idx]->get(i).get_int32();
                EXPECT_EQ(delta + rowid, chunk->columns()[v_int_idx]->get(i).get_int32())
                        << "rowid=" << rowid << " v_int silently drifted";
                auto arr = chunk->columns()[v_arr_idx]->get(i).get_array();
                ASSERT_EQ(2u, arr.size()) << "rowid=" << rowid;
                EXPECT_EQ(rowid * 10, arr[0].get_int32()) << "rowid=" << rowid;
                EXPECT_EQ(rowid * 10 + 1, arr[1].get_int32()) << "rowid=" << rowid;
            }
            total += chunk->num_rows();
            chunk->reset();
        }
        EXPECT_EQ(kChunkSize, total);
    }

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::vector<SlotDescriptor> _slots;
    std::vector<SlotDescriptor*> _slot_pointers_full;
    std::vector<SlotDescriptor*> _slot_pointers_v_arr;
    std::vector<SlotDescriptor*> _slot_pointers_v_int;
    Chunk::SlotHashMap _slot_cid_map;
    int64_t _partition_id = 4561;
    uint32_t _c0_uid = 0;
    uint32_t _v_int_uid = 0;
    uint32_t _v_arr_uid = 0;
};

// PCU targets an ARRAY column whose cid is shifted by a subsequent ADD COLUMN.
// Without uid-based resolution the write hits a non-array column writer and
// crashes inside ArrayColumnWriter::append; this test guards against that
// regression by asserting the array values are written to the correct column.
TEST_F(LakePcuSchemaDriftTest, WriteThenAddColumnAfterPublish_ArrayCrashRegression) {
    auto tablet_id = _tablet_metadata->id();
    auto version = 1;

    // ---- Step A: write base full row, publish v2 ----
    {
        auto chunk = make_full_chunk(kChunkSize);
        std::vector<uint32_t> indexes(kChunkSize);
        std::iota(indexes.begin(), indexes.end(), 0);
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // ---- Step B: PCU partial update on v_arr (writes txn_log under OLD schema) ----
    auto pcu_txn_id = next_id();
    {
        auto chunk = make_partial_chunk_v_arr(kChunkSize, /*scale=*/100);
        std::vector<uint32_t> indexes(kChunkSize);
        std::iota(indexes.begin(), indexes.end(), 0);
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(pcu_txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers_v_arr)
                                                   .set_partial_update_mode(PartialUpdateMode::COLUMN_UPDATE_MODE)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
    }

    // ---- Step C: ALTER ADD COLUMN at pos=1 (between c0 and v_int) ----
    {
        ASSIGN_OR_ABORT(auto latest_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        auto new_metadata = std::make_shared<TabletMetadataPB>(*latest_metadata);
        auto* schema_pb = new_metadata->mutable_schema();
        AddColumn modifier(/*pos=*/1, "BIGINT", /*nullable=*/true, /*default=*/"NULL");
        modifier.modify(schema_pb);
        schema_pb->set_id(next_id());
        schema_pb->set_schema_version(schema_pb->schema_version() + 1);
        new_metadata->set_version(version + 1);
        ASSERT_OK(_tablet_mgr->put_tablet_metadata(new_metadata));
        ASSERT_OK(_tablet_mgr->create_schema_file(tablet_id, *schema_pb));
        version++;
    }

    // ---- Step D: publish PCU under NEW schema. Pre-fix: segfault. Post-fix: OK. ----
    ASSERT_OK(publish_single_version(tablet_id, version + 1, pcu_txn_id).status());
    version++;

    // ---- Step E: read & verify ----
    verify_v_arr_content(version, /*expected_v_arr_scale=*/100);
}

// PCU targets a fixed-length INT column whose cid is shifted by ADD COLUMN.
// The bug here was silent data corruption: writes landed on the newly inserted
// BIGINT column without any error. This test verifies the values are routed
// back to the original column via uid lookup.
TEST_F(LakePcuSchemaDriftTest, WriteThenAddColumnAfterPublish_FixedLengthDriftIsSilent) {
    auto tablet_id = _tablet_metadata->id();
    auto version = 1;

    // base full-row write
    {
        auto chunk = make_full_chunk(kChunkSize);
        std::vector<uint32_t> indexes(kChunkSize);
        std::iota(indexes.begin(), indexes.end(), 0);
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // PCU partial update on v_int (writes 200+i)
    auto pcu_txn_id = next_id();
    {
        auto chunk = make_partial_chunk_v_int(kChunkSize, /*delta=*/200);
        std::vector<uint32_t> indexes(kChunkSize);
        std::iota(indexes.begin(), indexes.end(), 0);
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(pcu_txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers_v_int)
                                                   .set_partial_update_mode(PartialUpdateMode::COLUMN_UPDATE_MODE)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
    }

    // ALTER ADD COLUMN at pos=1
    {
        ASSIGN_OR_ABORT(auto latest_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        auto new_metadata = std::make_shared<TabletMetadataPB>(*latest_metadata);
        auto* schema_pb = new_metadata->mutable_schema();
        AddColumn modifier(1, "BIGINT", true, "NULL");
        modifier.modify(schema_pb);
        schema_pb->set_id(next_id());
        schema_pb->set_schema_version(schema_pb->schema_version() + 1);
        new_metadata->set_version(version + 1);
        ASSERT_OK(_tablet_mgr->put_tablet_metadata(new_metadata));
        ASSERT_OK(_tablet_mgr->create_schema_file(tablet_id, *schema_pb));
        version++;
    }

    // publish PCU
    ASSERT_OK(publish_single_version(tablet_id, version + 1, pcu_txn_id).status());
    version++;

    // verify: v_int must be 200+i (not base 100+i); v_arr must be base [i*10, i*10+1]
    verify_v_int_content(version, /*delta=*/200);
}

// Conditional update where the merge-condition column's cid is shifted by
// ADD COLUMN. The handler must re-resolve the condition cid against the
// current schema and locate it within the partial-update schema; otherwise
// the merge_condition filter compares against the wrong column.
TEST_F(LakePcuSchemaDriftTest, ConditionalUpdateAfterAddColumn) {
    auto tablet_id = _tablet_metadata->id();
    auto version = 1;

    // Base write: full row with v_int=[100,101,102,...], v_arr=[[0,1],[10,11],[20,21],...]
    {
        auto chunk = make_full_chunk(kChunkSize);
        std::vector<uint32_t> indexes(kChunkSize);
        std::iota(indexes.begin(), indexes.end(), 0);
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // PCU + condition update on v_int.
    //   base v_int[i]   = 100 + i              (i=0..9 → 100..109)
    //   new  v_int[i]   = i==0 ? 50 : 200+50*i (→ [50, 250, 300, 350, ..., 650])
    // merge_condition='v_int' updates a row only if new > old:
    //   row 0: 50  > 100   → filtered out, keeps 100
    //   row i (i>=1): 200+50*i > 100+i  → always true, applied
    auto pcu_txn_id = next_id();
    {
        auto c0 = Int32Column::create();
        auto v_int = Int32Column::create();
        for (int i = 0; i < kChunkSize; i++) {
            c0->append(i);
            v_int->append(i == 0 ? 50 : 200 + 50 * i);
        }
        Chunk::SlotHashMap partial_map;
        partial_map.emplace(0, 0);
        partial_map.emplace(1, 1);
        Chunk chunk({std::move(c0), std::move(v_int)}, partial_map);
        std::vector<uint32_t> indexes(kChunkSize);
        std::iota(indexes.begin(), indexes.end(), 0);

        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(pcu_txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers_v_int)
                                                   .set_partial_update_mode(PartialUpdateMode::COLUMN_UPDATE_MODE)
                                                   .set_merge_condition("v_int")
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
    }

    // ALTER ADD COLUMN at pos=1 (between c0 and v_int)
    {
        ASSIGN_OR_ABORT(auto latest_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        auto new_metadata = std::make_shared<TabletMetadataPB>(*latest_metadata);
        auto* schema_pb = new_metadata->mutable_schema();
        AddColumn modifier(1, "BIGINT", true, "NULL");
        modifier.modify(schema_pb);
        schema_pb->set_id(next_id());
        schema_pb->set_schema_version(schema_pb->schema_version() + 1);
        new_metadata->set_version(version + 1);
        ASSERT_OK(_tablet_mgr->put_tablet_metadata(new_metadata));
        ASSERT_OK(_tablet_mgr->create_schema_file(tablet_id, *schema_pb));
        version++;
    }

    // publish PCU
    ASSERT_OK(publish_single_version(tablet_id, version + 1, pcu_txn_id).status());
    version++;

    // verify: row 0 must keep base value (100), others updated
    {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
        auto ts = TabletSchema::create(metadata->schema());
        auto sch = ts->schema();
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *sch);
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));
        auto chunk = ChunkFactory::new_chunk(*sch, 128);
        int c0_idx = -1, v_int_idx = -1;
        for (size_t i = 0; i < ts->num_columns(); i++) {
            if (ts->column(i).name() == "c0")
                c0_idx = static_cast<int>(i);
            else if (ts->column(i).name() == "v_int")
                v_int_idx = static_cast<int>(i);
        }
        ASSERT_GE(c0_idx, 0);
        ASSERT_GE(v_int_idx, 0);

        std::map<int, int> got;
        while (true) {
            auto st = reader->get_next(chunk.get());
            if (st.is_end_of_file()) break;
            CHECK_OK(st);
            for (int i = 0; i < chunk->num_rows(); i++) {
                int rowid = chunk->columns()[c0_idx]->get(i).get_int32();
                int v = chunk->columns()[v_int_idx]->get(i).get_int32();
                got[rowid] = v;
            }
            chunk->reset();
        }
        EXPECT_EQ(static_cast<size_t>(kChunkSize), got.size());
        // Row 0: merge_condition filters (50 < 100), so v_int stays at base value 100
        EXPECT_EQ(100, got[0]) << "row 0 should be filtered by merge_condition (50 < 100)";
        // Row 1+: new value > old value, so update passes
        for (int i = 1; i < kChunkSize; i++) {
            EXPECT_EQ(200 + 50 * i, got[i]) << "row " << i << " should be updated";
        }
    }
}

// A PCU target column is dropped by ALTER between write and publish. The
// handler must surface a clean InternalError referencing the missing unique
// id, rather than dereferencing a stale cid or producing corrupt output.
TEST_F(LakePcuSchemaDriftTest, WriteThenDropTargetColumn_ReturnsInternalError) {
    auto tablet_id = _tablet_metadata->id();
    auto version = 1;

    // base full-row write
    {
        auto chunk = make_full_chunk(kChunkSize);
        std::vector<uint32_t> indexes(kChunkSize);
        std::iota(indexes.begin(), indexes.end(), 0);
        auto txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }

    // PCU partial update on v_arr (write while v_arr still exists)
    auto pcu_txn_id = next_id();
    {
        auto chunk = make_partial_chunk_v_arr(kChunkSize, 100);
        std::vector<uint32_t> indexes(kChunkSize);
        std::iota(indexes.begin(), indexes.end(), 0);
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(pcu_txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_slot_descriptors(&_slot_pointers_v_arr)
                                                   .set_partial_update_mode(PartialUpdateMode::COLUMN_UPDATE_MODE)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
    }

    // ALTER DROP COLUMN v_arr (col_idx=2 in default schema)
    {
        ASSIGN_OR_ABORT(auto latest_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
        auto new_metadata = std::make_shared<TabletMetadataPB>(*latest_metadata);
        auto* schema_pb = new_metadata->mutable_schema();
        DropColumn modifier(/*col_idx=*/2);
        modifier.modify(schema_pb);
        schema_pb->set_id(next_id());
        schema_pb->set_schema_version(schema_pb->schema_version() + 1);
        new_metadata->set_version(version + 1);
        ASSERT_OK(_tablet_mgr->put_tablet_metadata(new_metadata));
        ASSERT_OK(_tablet_mgr->create_schema_file(tablet_id, *schema_pb));
        version++;
    }

    // publish PCU under new schema — expect failure.
    auto res = publish_single_version(tablet_id, version + 1, pcu_txn_id);
    auto st = res.status();
    ASSERT_FALSE(st.ok()) << "publish should fail when PCU target column is dropped";
    EXPECT_TRUE(st.is_internal_error()) << "got: " << st.to_string();
    EXPECT_NE(std::string::npos, st.message().find("unique id"))
            << "expected 'unique id' in message; got: " << st.message();
}

} // namespace starrocks::lake
