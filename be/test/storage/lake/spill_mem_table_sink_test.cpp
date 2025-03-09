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

#include "storage/lake/spill_mem_table_sink.h"

#include <gtest/gtest.h>

#include "column/chunk.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"
#include "exec/spill/options.h"
#include "exec/spill/serde.h"
#include "exec/spill/spiller.h"
#include "fs/fs.h"
#include "storage/lake/general_tablet_writer.h"
#include "storage/lake/load_spill_block_manager.h"
#include "storage/lake/pk_tablet_writer.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/test_util.h"
#include "storage/tablet_schema.h"
#include "testutil/assert.h"
#include "util/raw_container.h"
#include "util/runtime_profile.h"

namespace starrocks::lake {

class SpillMemTableSinkTest : public TestBase {
public:
    SpillMemTableSinkTest() : TestBase(kTestDir) {
        _tablet_metadata = generate_simple_tablet_metadata(PRIMARY_KEYS);

        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

    void SetUp() override {
        (void)FileSystem::Default()->create_dir_recursive(kTestDir);
        CHECK_OK(fs::create_directories(lake::join_path(kTestDir, lake::kSegmentDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestDir, lake::kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestDir, lake::kTxnLogDirectoryName)));
    }

    void TearDown() override { (void)FileSystem::Default()->delete_dir_recursive(kTestDir); }

    ChunkPtr gen_data(int64_t chunk_size, int shift) {
        std::vector<int> v0(chunk_size);
        std::vector<int> v1(chunk_size);
        for (int i = 0; i < chunk_size; i++) {
            v0[i] = i + shift * chunk_size;
        }
        for (int i = 0; i < chunk_size; i++) {
            v1[i] = v0[i] * 3;
        }

        auto c0 = Int32Column::create();
        auto c1 = Int32Column::create();
        c0->append_numbers(v0.data(), v0.size() * sizeof(int));
        c1->append_numbers(v1.data(), v1.size() * sizeof(int));
        return std::make_shared<Chunk>(Columns{std::move(c0), std::move(c1)}, _schema);
    }

protected:
    constexpr static const char* const kTestDir = "./spill_mem_table_sink_test";

    constexpr static const int kChunkSize = 12;

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    RuntimeProfile _dummy_runtime_profile{"dummy"};
};

TEST_F(SpillMemTableSinkTest, test_flush_chunk) {
    int64_t tablet_id = 1;
    int64_t txn_id = 1;
    std::unique_ptr<LoadSpillBlockManager> block_manager =
            std::make_unique<LoadSpillBlockManager>(TUniqueId(), tablet_id, txn_id, kTestDir);
    ASSERT_OK(block_manager->init());
    std::unique_ptr<TabletWriter> tablet_writer = std::make_unique<HorizontalGeneralTabletWriter>(
            _tablet_mgr.get(), tablet_id, _tablet_schema, txn_id, false);
    SpillMemTableSink sink(block_manager.get(), tablet_writer.get(), &_dummy_runtime_profile);
    for (int i = 0; i < 3; i++) {
        // 3 times
        auto chunk = gen_data(kChunkSize, i);
        starrocks::SegmentPB segment;
        // write chunk to spill
        ASSERT_OK(sink.flush_chunk(*chunk, &segment, false));
        // read block
        auto block = block_manager->block_container()->get_block(i, 0);
        ASSERT_TRUE(block != nullptr);
        ASSERT_FALSE(block_manager->block_container()->empty());
        spill::SerdeContext ctx;
        spill::BlockReaderOptions options2;
        RuntimeProfile::Counter* read_io_timer = ADD_TIMER(&_dummy_runtime_profile, "ReadIOTime");
        RuntimeProfile::Counter* read_io_count = ADD_COUNTER(&_dummy_runtime_profile, "ReadIOCount", TUnit::UNIT);
        RuntimeProfile::Counter* read_io_bytes = ADD_COUNTER(&_dummy_runtime_profile, "ReadIOBytes", TUnit::BYTES);
        options2.read_io_timer = read_io_timer;
        options2.read_io_count = read_io_count;
        options2.read_io_bytes = read_io_bytes;
        // 3. read block data
        auto reader = block->get_reader(options2);
        ASSIGN_OR_ABORT(auto result_chunk, sink.get_spiller()->serde()->deserialize(ctx, reader.get()));
        // 4. check result
        for (int j = 0; j < kChunkSize; j++) {
            EXPECT_EQ(j + i * kChunkSize, result_chunk->get(j)[0].get_int32());
            EXPECT_EQ((j + i * kChunkSize) * 3, result_chunk->get(j)[1].get_int32());
        }
    }
    ASSERT_OK(sink.merge_blocks_to_segments());
    ASSERT_EQ(1, tablet_writer->files().size());
}

TEST_F(SpillMemTableSinkTest, test_flush_chunk_with_deletes) {
    int64_t tablet_id = 1;
    int64_t txn_id = 1;
    std::unique_ptr<LoadSpillBlockManager> block_manager =
            std::make_unique<LoadSpillBlockManager>(TUniqueId(), tablet_id, txn_id, kTestDir);
    ASSERT_OK(block_manager->init());
    std::unique_ptr<TabletWriter> tablet_writer = std::make_unique<HorizontalPkTabletWriter>(
            _tablet_mgr.get(), tablet_id, _tablet_schema, txn_id, nullptr, false);
    SpillMemTableSink sink(block_manager.get(), tablet_writer.get(), &_dummy_runtime_profile);
    for (int i = 0; i < 3; i++) {
        // 3 times
        auto chunk = gen_data(kChunkSize, i);
        starrocks::SegmentPB segment;
        // write chunk to spill
        ASSERT_OK(sink.flush_chunk_with_deletes(*chunk, *(chunk->columns()[0]), &segment, false));
        // read block
        auto block = block_manager->block_container()->get_block(i, 0);
        ASSERT_TRUE(block != nullptr);
        spill::SerdeContext ctx;
        spill::BlockReaderOptions options2;
        RuntimeProfile::Counter* read_io_timer = ADD_TIMER(&_dummy_runtime_profile, "ReadIOTime");
        RuntimeProfile::Counter* read_io_count = ADD_COUNTER(&_dummy_runtime_profile, "ReadIOCount", TUnit::UNIT);
        RuntimeProfile::Counter* read_io_bytes = ADD_COUNTER(&_dummy_runtime_profile, "ReadIOBytes", TUnit::BYTES);
        options2.read_io_timer = read_io_timer;
        options2.read_io_count = read_io_count;
        options2.read_io_bytes = read_io_bytes;
        // 3. read block data
        auto reader = block->get_reader(options2);
        ASSIGN_OR_ABORT(auto result_chunk, sink.get_spiller()->serde()->deserialize(ctx, reader.get()));
        // 4. check result
        for (int j = 0; j < kChunkSize; j++) {
            EXPECT_EQ(j + i * kChunkSize, result_chunk->get(j)[0].get_int32());
            EXPECT_EQ((j + i * kChunkSize) * 3, result_chunk->get(j)[1].get_int32());
        }
    }
    ASSERT_EQ(3, tablet_writer->files().size());
}

TEST_F(SpillMemTableSinkTest, test_flush_chunk2) {
    int64_t tablet_id = 1;
    int64_t txn_id = 1;
    std::unique_ptr<LoadSpillBlockManager> block_manager =
            std::make_unique<LoadSpillBlockManager>(TUniqueId(), tablet_id, txn_id, kTestDir);
    ASSERT_OK(block_manager->init());
    std::unique_ptr<TabletWriter> tablet_writer = std::make_unique<HorizontalGeneralTabletWriter>(
            _tablet_mgr.get(), tablet_id, _tablet_schema, txn_id, false);
    SpillMemTableSink sink(block_manager.get(), tablet_writer.get(), &_dummy_runtime_profile);
    auto chunk = gen_data(kChunkSize, 0);
    starrocks::SegmentPB segment;
    ASSERT_OK(sink.flush_chunk(*chunk, &segment, true));
    ASSERT_EQ(1, tablet_writer->files().size());
}

TEST_F(SpillMemTableSinkTest, test_flush_chunk_with_delete2) {
    int64_t tablet_id = 1;
    int64_t txn_id = 1;
    std::unique_ptr<LoadSpillBlockManager> block_manager =
            std::make_unique<LoadSpillBlockManager>(TUniqueId(), tablet_id, txn_id, kTestDir);
    ASSERT_OK(block_manager->init());
    std::unique_ptr<TabletWriter> tablet_writer = std::make_unique<HorizontalPkTabletWriter>(
            _tablet_mgr.get(), tablet_id, _tablet_schema, txn_id, nullptr, false);
    SpillMemTableSink sink(block_manager.get(), tablet_writer.get(), &_dummy_runtime_profile);
    auto chunk = gen_data(kChunkSize, 0);
    starrocks::SegmentPB segment;
    ASSERT_OK(sink.flush_chunk_with_deletes(*chunk, *(chunk->columns()[0]), &segment, true));
    ASSERT_EQ(2, tablet_writer->files().size());
}

TEST_F(SpillMemTableSinkTest, test_flush_chunk_with_limit) {
    int64_t tablet_id = 1;
    int64_t txn_id = 1;
    std::unique_ptr<LoadSpillBlockManager> block_manager =
            std::make_unique<LoadSpillBlockManager>(TUniqueId(), tablet_id, txn_id, kTestDir);
    ASSERT_OK(block_manager->init());
    std::unique_ptr<TabletWriter> tablet_writer = std::make_unique<HorizontalGeneralTabletWriter>(
            _tablet_mgr.get(), tablet_id, _tablet_schema, txn_id, false);
    SpillMemTableSink sink(block_manager.get(), tablet_writer.get(), &_dummy_runtime_profile);
    for (int i = 0; i < 3; i++) {
        int64_t old_val = config::load_spill_max_chunk_bytes;
        config::load_spill_max_chunk_bytes = 1;
        // 3 times
        auto chunk = gen_data(kChunkSize, i);
        starrocks::SegmentPB segment;
        // write chunk to spill
        ASSERT_OK(sink.flush_chunk(*chunk, &segment, false));
        // read block
        auto block = block_manager->block_container()->get_block(i, 0);
        ASSERT_TRUE(block != nullptr);
        ASSERT_FALSE(block_manager->block_container()->empty());
        spill::SerdeContext ctx;
        spill::BlockReaderOptions options2;
        RuntimeProfile::Counter* read_io_timer = ADD_TIMER(&_dummy_runtime_profile, "ReadIOTime");
        RuntimeProfile::Counter* read_io_count = ADD_COUNTER(&_dummy_runtime_profile, "ReadIOCount", TUnit::UNIT);
        RuntimeProfile::Counter* read_io_bytes = ADD_COUNTER(&_dummy_runtime_profile, "ReadIOBytes", TUnit::BYTES);
        options2.read_io_timer = read_io_timer;
        options2.read_io_count = read_io_count;
        options2.read_io_bytes = read_io_bytes;
        // 3. read block data
        auto reader = block->get_reader(options2);
        for (int j = 0; j < kChunkSize; j++) {
            ASSIGN_OR_ABORT(auto result_chunk, sink.get_spiller()->serde()->deserialize(ctx, reader.get()));
            // 4. check result
            EXPECT_EQ(j + i * kChunkSize, result_chunk->get(0)[0].get_int32());
            EXPECT_EQ((j + i * kChunkSize) * 3, result_chunk->get(0)[1].get_int32());
        }
        config::load_spill_max_chunk_bytes = old_val;
    }
    ASSERT_OK(sink.merge_blocks_to_segments());
    ASSERT_EQ(1, tablet_writer->files().size());
}

TEST_F(SpillMemTableSinkTest, test_merge) {
    int64_t tablet_id = 1;
    int64_t txn_id = 1;
    std::unique_ptr<LoadSpillBlockManager> block_manager =
            std::make_unique<LoadSpillBlockManager>(TUniqueId(), tablet_id, txn_id, kTestDir);
    ASSERT_OK(block_manager->init());
    std::unique_ptr<TabletWriter> tablet_writer = std::make_unique<HorizontalGeneralTabletWriter>(
            _tablet_mgr.get(), tablet_id, _tablet_schema, txn_id, false);
    SpillMemTableSink sink(block_manager.get(), tablet_writer.get(), &_dummy_runtime_profile);
    auto chunk = gen_data(kChunkSize, 0);
    starrocks::SegmentPB segment0;
    ASSERT_OK(sink.flush_chunk(*chunk, &segment0, false));
    starrocks::SegmentPB segment1;
    ASSERT_OK(sink.flush_chunk(*chunk, &segment1, true));
    ASSERT_OK(sink.merge_blocks_to_segments());
    ASSERT_EQ(1, tablet_writer->files().size());
}

} // namespace starrocks::lake