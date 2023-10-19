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
#include <memory>
#include <random>

#include "column/chunk.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"
#include "fs/fs_util.h"
#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/lake/compaction_policy.h"
#include "storage/lake/compaction_test_utils.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/horizontal_compaction_task.h"
#include "storage/lake/join_path.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/test_util.h"
#include "storage/lake/update_compaction_state.h"
#include "storage/lake/vertical_compaction_task.h"
#include "storage/tablet_schema.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"

namespace starrocks::lake {

class LakePrimaryKeyCompactionTest : public TestBase, public testing::WithParamInterface<CompactionParam> {
public:
    LakePrimaryKeyCompactionTest() : TestBase(kTestDirectory), _partition_id(next_id()) {
        _tablet_metadata = std::make_shared<TabletMetadata>();
        _tablet_metadata->set_id(next_id());
        _tablet_metadata->set_version(1);
        _tablet_metadata->set_cumulative_point(0);
        _tablet_metadata->set_next_rowset_id(1);
        _tablet_metadata->set_enable_persistent_index(GetParam().enable_persistent_index);
        //
        //  | column | type | KEY | NULL |
        //  +--------+------+-----+------+
        //  |   c0   |  INT | YES |  NO  |
        //  |   c1   |  INT | NO  |  NO  |
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

        _tablet_schema = TabletSchema::create(*schema);
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(*_tablet_schema));
    }

protected:
    constexpr static const char* const kTestDirectory = "test_lake_pk_compaction_task";
    constexpr static const int kChunkSize = 12;

    void SetUp() override {
        config::vertical_compaction_max_columns_per_group = GetParam().vertical_compaction_max_columns_per_group;
        (void)fs::remove_all(kTestDirectory);
        CHECK_OK(fs::create_directories(lake::join_path(kTestDirectory, lake::kSegmentDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestDirectory, lake::kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestDirectory, lake::kTxnLogDirectoryName)));
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override {
        // check primary index cache's ref
        EXPECT_TRUE(_update_mgr->TEST_check_primary_index_cache_ref(_tablet_metadata->id(), 1));
        EXPECT_EQ(_update_mgr->compaction_state_mem_tracker()->consumption(), 0);
        (void)fs::remove_all(kTestDirectory);
    }

    Chunk generate_data(int64_t chunk_size, int shift) {
        std::vector<int> v0(chunk_size);
        std::vector<int> v1(chunk_size);
        for (int i = 0; i < chunk_size; i++) {
            v0[i] = i + shift * chunk_size;
        }
        auto rng = std::default_random_engine{};
        std::shuffle(v0.begin(), v0.end(), rng);
        for (int i = 0; i < chunk_size; i++) {
            v1[i] = v0[i] * 3;
        }

        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        c0->append_numbers(v0.data(), v0.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int));
        return Chunk({c0, c1}, _schema);
    }

    Chunk generate_data2(int64_t chunk_size, int interval, int shift) {
        std::vector<int> v0(chunk_size);
        std::vector<int> v1(chunk_size);
        for (int i = 0; i < chunk_size; i++) {
            v0[i] = i * interval + shift;
        }
        auto rng = std::default_random_engine{};
        std::shuffle(v0.begin(), v0.end(), rng);
        for (int i = 0; i < chunk_size; i++) {
            v1[i] = v0[i] * 3;
        }

        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        c0->append_numbers(v0.data(), v0.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int));
        return Chunk({c0, c1}, _schema);
    }

    int64_t read(int64_t version) {
        ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_metadata->id()));
        ASSIGN_OR_ABORT(auto reader, tablet.new_reader(version, *_schema));
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
            chunk->reset();
        }
        return ret;
    }

    void get_key_list(int64_t version, std::vector<int>& key_list) {
        ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(_tablet_metadata->id()));
        ASSIGN_OR_ABORT(auto reader, tablet.new_reader(version, *_schema));
        CHECK_OK(reader->prepare());
        CHECK_OK(reader->open(TabletReaderParams()));
        auto chunk = ChunkHelper::new_chunk(*_schema, 128);

        while (true) {
            auto st = reader->get_next(chunk.get());
            if (st.is_end_of_file()) {
                break;
            }
            CHECK_OK(st);
            for (int i = 0; i < chunk->num_rows(); i++) {
                key_list.push_back(chunk->columns()[0]->get(i).get_int32());
            }
            chunk->reset();
        }
    }

    void check_task(CompactionTaskPtr& task) {
        if (GetParam().algorithm == HORIZONTAL_COMPACTION) {
            ASSERT_TRUE(dynamic_cast<HorizontalCompactionTask*>(task.get()) != nullptr);
        } else {
            ASSERT_EQ(GetParam().algorithm, VERTICAL_COMPACTION);
            ASSERT_TRUE(dynamic_cast<VerticalCompactionTask*>(task.get()) != nullptr);
        }
    }

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    int64_t _partition_id;
};

// each time overwrite last rows
TEST_P(LakePrimaryKeyCompactionTest, test1) {
    // Prepare data for writing
    auto chunk0 = generate_data(kChunkSize, 0);
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        auto delta_writer =
                DeltaWriter::create(_tablet_mgr.get(), tablet_id, txn_id, _partition_id, nullptr, _mem_tracker.get());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize, read(version));
    ASSIGN_OR_ABORT(auto new_tablet_metadata1, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata1->rowsets_size(), 3);

    ExecEnv::GetInstance()->delete_file_thread_pool()->wait();
    // make sure delvecs have been generated
    for (int i = 0; i < 2; i++) {
        auto itr = new_tablet_metadata1->delvec_meta().version_to_file().find(version - i);
        EXPECT_TRUE(itr != new_tablet_metadata1->delvec_meta().version_to_file().end());
        auto delvec_file = itr->second;
        EXPECT_TRUE(fs::path_exist(_lp->delvec_location(tablet_id, delvec_file.name())));
    }

    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id));

    auto txn_id = next_id();
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(_tablet_metadata->id(), version, txn_id));
    check_task(task);
    CompactionTask::Progress progress;
    ASSERT_OK(task->execute(&progress, CompactionTask::kNoCancelFn));
    EXPECT_EQ(100, progress.value());
    ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
    version++;
    ASSERT_EQ(kChunkSize, read(version));
    if (GetParam().enable_persistent_index) {
        check_local_persistent_index_meta(tablet_id, version);
    }

    ASSIGN_OR_ABORT(auto new_tablet_metadata2, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata2->rowsets_size(), 1);
    EXPECT_EQ(3, new_tablet_metadata2->compaction_inputs_size());
    EXPECT_FALSE(new_tablet_metadata2->has_prev_garbage_version());
}

// test write 3 diff chunk
TEST_P(LakePrimaryKeyCompactionTest, test2) {
    // Prepare data for writing
    std::vector<Chunk> chunks;
    for (int i = 0; i < 3; i++) {
        chunks.push_back(generate_data(kChunkSize, i));
    }
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        auto delta_writer =
                DeltaWriter::create(_tablet_mgr.get(), tablet_id, txn_id, _partition_id, nullptr, _mem_tracker.get());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunks[i], indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize * 3, read(version));

    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id));

    auto txn_id = next_id();
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(_tablet_metadata->id(), version, txn_id));
    check_task(task);
    CompactionTask::Progress progress;
    ASSERT_OK(task->execute(&progress, CompactionTask::kNoCancelFn));
    EXPECT_EQ(100, progress.value());
    ASSERT_OK(publish_single_version(_tablet_metadata->id(), version + 1, txn_id).status());
    version++;
    ASSERT_EQ(kChunkSize * 3, read(version));
    if (GetParam().enable_persistent_index) {
        check_local_persistent_index_meta(tablet_id, version);
    }

    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 1);
    EXPECT_EQ(3, new_tablet_metadata->compaction_inputs_size());
    EXPECT_FALSE(new_tablet_metadata->has_prev_garbage_version());
}

// test write empty chunk
TEST_P(LakePrimaryKeyCompactionTest, test3) {
    // Prepare data for writing
    std::vector<Chunk> chunks;
    for (int i = 0; i < 3; i++) {
        if (i == 1) {
            chunks.push_back(generate_data(0, 0));
        } else {
            chunks.push_back(generate_data(kChunkSize, i));
        }
    }
    auto indexes = std::vector<uint32_t>(kChunkSize);
    auto indexes_empty = std::vector<uint32_t>();
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        auto delta_writer =
                DeltaWriter::create(_tablet_mgr.get(), tablet_id, txn_id, _partition_id, nullptr, _mem_tracker.get());
        ASSERT_OK(delta_writer->open());
        if (i == 1) {
            ASSERT_OK(delta_writer->write(chunks[i], indexes_empty.data(), indexes_empty.size()));
        } else {
            ASSERT_OK(delta_writer->write(chunks[i], indexes.data(), indexes.size()));
        }
        ASSERT_OK(delta_writer->finish());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize * 2, read(version));

    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id));

    auto txn_id = next_id();
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(_tablet_metadata->id(), version, txn_id));
    check_task(task);
    CompactionTask::Progress progress;
    ASSERT_OK(task->execute(&progress, CompactionTask::kNoCancelFn));
    EXPECT_EQ(100, progress.value());
    ASSERT_OK(publish_single_version(_tablet_metadata->id(), version + 1, txn_id).status());
    version++;
    if (GetParam().enable_persistent_index) {
        check_local_persistent_index_meta(tablet_id, version);
    }
    ASSERT_EQ(kChunkSize * 2, read(version));

    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 1);
    EXPECT_EQ(2, new_tablet_metadata->compaction_inputs_size());
    EXPECT_FALSE(new_tablet_metadata->has_prev_garbage_version());
}

TEST_P(LakePrimaryKeyCompactionTest, test_compaction_policy) {
    // Prepare data for writing
    std::vector<Chunk> chunks;
    for (int i = 0; i < 3; i++) {
        chunks.push_back(generate_data(kChunkSize, i));
    }
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        auto delta_writer =
                DeltaWriter::create(_tablet_mgr.get(), tablet_id, txn_id, _partition_id, nullptr, _mem_tracker.get());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunks[i], indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize * 3, read(version));
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id));

    ASSIGN_OR_ABORT(auto compaction_policy,
                    CompactionPolicy::create_compaction_policy(std::make_shared<Tablet>(tablet)));
    config::max_update_compaction_num_singleton_deltas = 1000;
    ASSIGN_OR_ABORT(auto input_rowsets, compaction_policy->pick_rowsets(version));
    EXPECT_EQ(3, input_rowsets.size());

    config::max_update_compaction_num_singleton_deltas = 2;
    ASSIGN_OR_ABORT(auto input_rowsets2, compaction_policy->pick_rowsets(version));
    EXPECT_EQ(2, input_rowsets2.size());

    config::max_update_compaction_num_singleton_deltas = 1;
    ASSIGN_OR_ABORT(auto input_rowsets3, compaction_policy->pick_rowsets(version));
    EXPECT_EQ(1, input_rowsets3.size());
}

TEST_P(LakePrimaryKeyCompactionTest, test_compaction_policy2) {
    // Prepare data for writing
    std::vector<Chunk> chunks;
    std::vector<std::vector<uint32_t>> indexes_list;
    for (int i = 0; i < 3; i++) {
        chunks.push_back(generate_data(kChunkSize * (i + 1), i));
        auto indexes = std::vector<uint32_t>(kChunkSize * (i + 1));
        for (int j = 0; j < kChunkSize * (i + 1); j++) {
            indexes[j] = j;
        }
        indexes_list.push_back(indexes);
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        auto delta_writer =
                DeltaWriter::create(_tablet_mgr.get(), tablet_id, txn_id, _partition_id, nullptr, _mem_tracker.get());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunks[i], indexes_list[i].data(), indexes_list[i].size()));
        ASSERT_OK(delta_writer->finish());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    {
        auto txn_id = next_id();
        auto delta_writer =
                DeltaWriter::create(_tablet_mgr.get(), tablet_id, txn_id, _partition_id, nullptr, _mem_tracker.get());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunks[0], indexes_list[0].data(), indexes_list[0].size()));
        ASSERT_OK(delta_writer->finish());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize * 6, read(version));
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id));

    config::max_update_compaction_num_singleton_deltas = 4;
    ASSIGN_OR_ABORT(auto compaction_policy,
                    CompactionPolicy::create_compaction_policy(std::make_shared<Tablet>(tablet)));
    ASSIGN_OR_ABORT(auto input_rowsets, compaction_policy->pick_rowsets(version));
    EXPECT_EQ(4, input_rowsets.size());

    // check the rowset order, pick rowset#1 first, because it have deleted rows.
    // Next order is rowset#4#2#3, by their byte size.
    EXPECT_EQ(input_rowsets[0]->id(), 1);
    EXPECT_EQ(input_rowsets[1]->id(), 4);
    EXPECT_EQ(input_rowsets[2]->id(), 2);
    EXPECT_EQ(input_rowsets[3]->id(), 3);
}

TEST_P(LakePrimaryKeyCompactionTest, test_compaction_policy3) {
    // Prepare data for writing
    std::vector<Chunk> chunks;
    std::vector<std::vector<uint32_t>> indexes_list;
    for (int i = 0; i < 6; i++) {
        chunks.push_back(generate_data(kChunkSize, i));
        auto indexes = std::vector<uint32_t>(kChunkSize);
        for (int j = 0; j < kChunkSize; j++) {
            indexes[j] = j;
        }
        indexes_list.push_back(indexes);
    }

    const int64_t old_size = config::write_buffer_size;
    config::write_buffer_size = 1;
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    size_t chunk_index = 0;
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        auto delta_writer =
                DeltaWriter::create(_tablet_mgr.get(), tablet_id, txn_id, _partition_id, nullptr, _mem_tracker.get());
        ASSERT_OK(delta_writer->open());
        for (int seg_cnt = 0; seg_cnt <= i; seg_cnt++) {
            ASSERT_OK(delta_writer->write(chunks[chunk_index], indexes_list[chunk_index].data(),
                                          indexes_list[chunk_index].size()));
            chunk_index++;
        }
        ASSERT_OK(delta_writer->finish());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    {
        auto txn_id = next_id();
        auto delta_writer =
                DeltaWriter::create(_tablet_mgr.get(), tablet_id, txn_id, _partition_id, nullptr, _mem_tracker.get());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunks[0], indexes_list[0].data(), indexes_list[0].size()));
        ASSERT_OK(delta_writer->finish());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    config::write_buffer_size = old_size;
    ASSERT_EQ(kChunkSize * 6, read(version));
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id));

    config::max_update_compaction_num_singleton_deltas = 4;
    ASSIGN_OR_ABORT(auto compaction_policy,
                    CompactionPolicy::create_compaction_policy(std::make_shared<Tablet>(tablet)));
    ASSIGN_OR_ABORT(auto input_rowsets, compaction_policy->pick_rowsets(version));
    EXPECT_EQ(4, input_rowsets.size());
    EXPECT_EQ(1, input_rowsets[0]->num_segments());
    EXPECT_EQ(3, input_rowsets[1]->num_segments());
    EXPECT_EQ(2, input_rowsets[2]->num_segments());
    EXPECT_EQ(1, input_rowsets[3]->num_segments());

    // check the rowset order, pick rowset#1 first, because it have deleted rows.
    // Next order is rowset#4#2#3, by their segment count.
    EXPECT_EQ(input_rowsets[0]->id(), 1);
    EXPECT_EQ(input_rowsets[1]->id(), 4);
    EXPECT_EQ(input_rowsets[2]->id(), 2);
    EXPECT_EQ(input_rowsets[3]->id(), 7);
}

TEST_P(LakePrimaryKeyCompactionTest, test_compaction_score_by_policy) {
    // Prepare data for writing
    std::vector<Chunk> chunks;
    for (int i = 0; i < 3; i++) {
        chunks.push_back(generate_data(kChunkSize, i));
    }
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        auto delta_writer =
                DeltaWriter::create(_tablet_mgr.get(), tablet_id, txn_id, _partition_id, nullptr, _mem_tracker.get());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunks[i], indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize * 3, read(version));
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id));
    ASSIGN_OR_ABORT(auto tablet_meta, tablet.get_metadata(version));

    ASSIGN_OR_ABORT(auto compaction_policy,
                    CompactionPolicy::create_compaction_policy(std::make_shared<Tablet>(tablet)));
    config::max_update_compaction_num_singleton_deltas = 1000;
    ASSIGN_OR_ABORT(auto input_rowsets, compaction_policy->pick_rowsets(version));
    EXPECT_EQ(3, input_rowsets.size());
    EXPECT_EQ(3, compaction_score(*tablet_meta));

    config::max_update_compaction_num_singleton_deltas = 2;
    ASSIGN_OR_ABORT(auto input_rowsets2, compaction_policy->pick_rowsets(version));
    EXPECT_EQ(2, input_rowsets2.size());
    EXPECT_EQ(2, compaction_score(*tablet_meta));

    config::max_update_compaction_num_singleton_deltas = 1;
    ASSIGN_OR_ABORT(auto input_rowsets3, compaction_policy->pick_rowsets(version));
    EXPECT_EQ(1, input_rowsets3.size());
    EXPECT_EQ(1, compaction_score(*tablet_meta));
    config::max_update_compaction_num_singleton_deltas = 1000;
}

TEST_P(LakePrimaryKeyCompactionTest, test_compaction_sorted) {
    // Prepare data for writing
    std::vector<Chunk> chunks;
    for (int i = 0; i < 3; i++) {
        chunks.push_back(generate_data2(kChunkSize, 3, i));
    }
    auto indexes = std::vector<uint32_t>(kChunkSize);
    for (int i = 0; i < kChunkSize; i++) {
        indexes[i] = i;
    }
    // write chunk without order
    std::vector<int> chunk_write_without_order = {1, 0, 2};

    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    for (int i = 0; i < 3; i++) {
        auto txn_id = next_id();
        auto delta_writer =
                DeltaWriter::create(_tablet_mgr.get(), tablet_id, txn_id, _partition_id, nullptr, _mem_tracker.get());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunks[chunk_write_without_order[i]], indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish());
        delta_writer->close();
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
    }
    ASSERT_EQ(kChunkSize * 3, read(version));

    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id));

    auto txn_id = next_id();
    ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(_tablet_metadata->id(), version, txn_id));
    check_task(task);
    CompactionTask::Progress progress;
    ASSERT_OK(task->execute(&progress, CompactionTask::kNoCancelFn));
    EXPECT_EQ(100, progress.value());
    // check compaction state
    ASSIGN_OR_ABORT(auto txn_log, tablet.get_txn_log(txn_id));
    RowsetPtr output_rowset = std::make_shared<Rowset>(
            &tablet, std::make_shared<RowsetMetadata>(txn_log->op_compaction().output_rowset()));
    auto compaction_state = std::make_unique<CompactionState>(output_rowset.get(), _update_mgr.get());
    for (size_t i = 0; i < compaction_state->pk_cols.size(); i++) {
        ASSERT_OK(compaction_state->load_segments(output_rowset.get(), *_tablet_schema, i));
        auto& pk_col = compaction_state->pk_cols[i];
        EXPECT_EQ(_update_mgr->compaction_state_mem_tracker()->consumption(), pk_col->memory_usage());
        compaction_state->release_segments(i);
        EXPECT_EQ(_update_mgr->compaction_state_mem_tracker()->consumption(), 0);
    }
    // publish version
    ASSERT_OK(publish_single_version(_tablet_metadata->id(), version + 1, txn_id).status());
    version++;
    ASSERT_EQ(kChunkSize * 3, read(version));

    ASSIGN_OR_ABORT(auto new_tablet_metadata, _tablet_mgr->get_tablet_metadata(tablet_id, version));
    EXPECT_EQ(new_tablet_metadata->rowsets_size(), 1);
    EXPECT_EQ(3, new_tablet_metadata->compaction_inputs_size());
    EXPECT_FALSE(new_tablet_metadata->has_prev_garbage_version());

    // check compact to one rowset with in order
    std::vector<int> key_list;
    get_key_list(version, key_list);
    for (int i = 0; i < key_list.size() - 1; i++) {
        EXPECT_TRUE(key_list[i] < key_list[i + 1]);
    }
}

INSTANTIATE_TEST_SUITE_P(LakePrimaryKeyCompactionTest, LakePrimaryKeyCompactionTest,
                         ::testing::Values(CompactionParam{HORIZONTAL_COMPACTION, 5, false},
                                           CompactionParam{VERTICAL_COMPACTION, 1, false},
                                           CompactionParam{HORIZONTAL_COMPACTION, 5, true},
                                           CompactionParam{VERTICAL_COMPACTION, 1, true}),
                         to_string_param_name);

} // namespace starrocks::lake
