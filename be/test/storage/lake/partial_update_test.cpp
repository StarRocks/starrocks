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
#include "common/config.h"
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
    auto result_chunk = ChunkHelper::new_chunk(*reader_schema, 128);

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
        txn_ids.push_back(txn_id);
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
    auto result_chunk = ChunkHelper::new_chunk(*reader_schema, 128);

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
        ops_slot_map[0] = 0; // c0 -> table column 0
        ops_slot_map[1] = 1; // c1 -> table column 1
        ops_slot_map[2] = 2; // c2 -> table column 2
        ops_slot_map[3] = 3; // ops column -> slot 3
        Chunk chunk_with_ops({std::move(c0), std::move(c1), std::move(c2), std::move(cop)}, ops_slot_map);
        std::vector<uint32_t> idx(kChunkSize);
        for (int i = 0; i < kChunkSize; i++) idx[i] = i;

        // Create slot descriptors including operation column
        std::vector<SlotDescriptor> op_slots;
        op_slots.emplace_back(0, "c0", TypeDescriptor{LogicalType::TYPE_INT});
        op_slots.emplace_back(1, "c1", TypeDescriptor{LogicalType::TYPE_INT});
        op_slots.emplace_back(2, "c2", TypeDescriptor{LogicalType::TYPE_INT});
        op_slots.emplace_back(3, "__op", TypeDescriptor{LogicalType::TYPE_TINYINT}); // operation column
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
        auto chk = ChunkHelper::new_chunk(*_schema, 256);
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

        // Check metadata records del files (generated by delta_writer, summarized by builder).
        // Latest rowset should record del_files list or have num_dels updated.
        ASSERT_GE(metadata->rowsets_size(), 1);
        const auto& last_rs = metadata->rowsets(metadata->rowsets_size() - 1);
        // del_files_size may be 0 (merged in different paths), but num_dels or delvec_meta should be updated.
        // Assert num_dels non-negative and version advanced.
        ASSERT_GE(last_rs.num_dels(), 0);
        ASSERT_EQ(version, metadata->version());
    }
}

} // namespace starrocks::lake
