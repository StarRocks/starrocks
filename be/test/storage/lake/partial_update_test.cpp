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

#include <random>

#include "column/chunk.h"
#include "column/datum_convert.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"
#include "storage/chunk_helper.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/meta_file.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/test_util.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/tablet_schema.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"
#include "testutil/sync_point.h"

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
    }

    void TearDown() override {
        // check primary index cache's ref
        EXPECT_TRUE(_update_mgr->TEST_check_primary_index_cache_ref(_tablet_metadata->id(), 1));
        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
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

    int64_t check(int64_t version, std::function<bool(int c0, int c1, int c2)> check_fn) {
        ASSIGN_OR_ABORT(auto metadata, _tablet_mgr->get_tablet_metadata(_tablet_metadata->id(), version));
        auto reader = std::make_shared<TabletReader>(_tablet_mgr.get(), metadata, *_schema);
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));
        auto chunk = ChunkHelper::new_chunk(*_schema, 128);
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
                // move old .cols files into orphan files.
                EXPECT_EQ(new_tablet_metadata->orphan_files_size(), 2);
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
    if (GetParam().partial_update_mode == PartialUpdateMode::COLUMN_UPDATE_MODE) {
        return;
    }

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
                // move old .cols into orphan files.
                EXPECT_EQ(new_tablet_metadata->orphan_files_size(), 3);
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
        EXPECT_EQ(new_tablet_metadata->rowsets(5).segments_size(), 2);
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
                // move old .cols into orphan files.
                EXPECT_EQ(new_tablet_metadata->orphan_files_size(), 3);
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
        EXPECT_EQ(new_tablet_metadata->rowsets(5).segments_size(), 2);
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
        txn_ids.push_back(txn_id);
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
        txn_ids.push_back(txn_id);
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
        EXPECT_EQ(new_tablet_metadata->rowsets(5).segments_size(), 2);
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
        txn_ids.push_back(txn_id);
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
                // move old .cols into orphan files.
                EXPECT_EQ(new_tablet_metadata->orphan_files_size(), 2);
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
                // move old .cols into orphan files.
                EXPECT_EQ(new_tablet_metadata->orphan_files_size(), 2);
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

        SyncPoint::GetInstance()->SetCallBack("ProtobufFile::save:serialize", [](void* arg) { *(bool*)arg = false; });
        SyncPoint::GetInstance()->EnableProcessing();
        ASSERT_ERROR(publish_single_version(tablet_id, version + 1, txn_id).status());
        SyncPoint::GetInstance()->ClearCallBack("ProtobufFile::save:serialize");
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

INSTANTIATE_TEST_SUITE_P(
        LakePartialUpdateTest, LakePartialUpdateTest,
        ::testing::Values(PrimaryKeyParam{true}, PrimaryKeyParam{false},
                          PrimaryKeyParam{true, PersistentIndexTypePB::CLOUD_NATIVE},
                          PrimaryKeyParam{.enable_persistent_index = true,
                                          .partial_update_mode = PartialUpdateMode::COLUMN_UPDATE_MODE},
                          PrimaryKeyParam{.enable_persistent_index = false,
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
        ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
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
                EXPECT_EQ(new_tablet_metadata->orphan_files_size(), 3);
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
        EXPECT_EQ(new_tablet_metadata->rowsets(5).segments_size(), 2);
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
        EXPECT_EQ(new_tablet_metadata->rowsets(5).segments_size(), 2);
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
    explicit ModifyColumnType(int column_idx, std::string target_type)
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
    explicit AddColumn(int pos, std::string type, bool nullable, std::string default_value)
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
        auto chunk = ChunkHelper::new_chunk(*schema, 128);
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
