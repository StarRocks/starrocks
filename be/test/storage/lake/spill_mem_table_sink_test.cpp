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

#include "base/testutil/assert.h"
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
#include "storage/lake/pk_tablet_writer.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/test_util.h"
#include "storage/load_spill_block_manager.h"
#include "storage/load_spill_pipeline_merge_context.h"
#include "storage/tablet_schema.h"
#include "util/raw_container.h"
#include "util/runtime_profile.h"

namespace starrocks::lake {

struct SpillMemTableSinkTestParams {
    bool enable_load_spill_parallel_merge = false;
    int64_t load_spill_max_merge_bytes = 1073741824;
};

class SpillMemTableSinkTest : public TestBase, testing::WithParamInterface<SpillMemTableSinkTestParams> {
public:
    SpillMemTableSinkTest() : TestBase(kTestDir) {
        _tablet_metadata = generate_simple_tablet_metadata(PRIMARY_KEYS);

        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
    }

    void SetUp() override {
        _old_enable_load_spill_parallel_merge = config::enable_load_spill_parallel_merge;
        _old_load_spill_max_merge_bytes = config::load_spill_max_merge_bytes;
        config::enable_load_spill_parallel_merge = GetParam().enable_load_spill_parallel_merge;
        config::load_spill_max_merge_bytes = GetParam().load_spill_max_merge_bytes;
        (void)FileSystem::Default()->create_dir_recursive(kTestDir);
        CHECK_OK(fs::create_directories(lake::join_path(kTestDir, lake::kSegmentDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestDir, lake::kMetadataDirectoryName)));
        CHECK_OK(fs::create_directories(lake::join_path(kTestDir, lake::kTxnLogDirectoryName)));
    }

    void TearDown() override {
        (void)FileSystem::Default()->delete_dir_recursive(kTestDir);
        config::enable_load_spill_parallel_merge = _old_enable_load_spill_parallel_merge;
        config::load_spill_max_merge_bytes = _old_load_spill_max_merge_bytes;
    }

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
    bool _old_enable_load_spill_parallel_merge = false;
    int64_t _old_load_spill_max_merge_bytes = 1073741824;
};

TEST_P(SpillMemTableSinkTest, test_flush_chunk) {
    int64_t tablet_id = 1;
    int64_t txn_id = 1;
    std::unique_ptr<LoadSpillBlockManager> block_manager = std::make_unique<LoadSpillBlockManager>(
            TUniqueId(), UniqueId(tablet_id, txn_id).to_thrift(), kTestDir, nullptr);
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
    ASSERT_EQ(config::enable_load_spill_parallel_merge ? 3 : 1, tablet_writer->segments().size());
}

TEST_P(SpillMemTableSinkTest, test_flush_chunk_with_deletes) {
    int64_t tablet_id = 1;
    int64_t txn_id = 1;
    std::unique_ptr<LoadSpillBlockManager> block_manager = std::make_unique<LoadSpillBlockManager>(
            TUniqueId(), UniqueId(tablet_id, txn_id).to_thrift(), kTestDir, nullptr);
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
    ASSERT_EQ(3, tablet_writer->dels().size());
}

TEST_P(SpillMemTableSinkTest, test_flush_chunk2) {
    int64_t tablet_id = 1;
    int64_t txn_id = 1;
    std::unique_ptr<LoadSpillBlockManager> block_manager = std::make_unique<LoadSpillBlockManager>(
            TUniqueId(), UniqueId(tablet_id, txn_id).to_thrift(), kTestDir, nullptr);
    ASSERT_OK(block_manager->init());
    std::unique_ptr<TabletWriter> tablet_writer = std::make_unique<HorizontalGeneralTabletWriter>(
            _tablet_mgr.get(), tablet_id, _tablet_schema, txn_id, false);
    SpillMemTableSink sink(block_manager.get(), tablet_writer.get(), &_dummy_runtime_profile);
    auto chunk = gen_data(kChunkSize, 0);
    starrocks::SegmentPB segment;
    ASSERT_OK(sink.flush_chunk(*chunk, &segment, true, nullptr, 0));
    ASSERT_EQ(1, tablet_writer->segments().size());
}

TEST_P(SpillMemTableSinkTest, test_flush_chunk_with_delete2) {
    int64_t tablet_id = 1;
    int64_t txn_id = 1;
    std::unique_ptr<LoadSpillBlockManager> block_manager = std::make_unique<LoadSpillBlockManager>(
            TUniqueId(), UniqueId(tablet_id, txn_id).to_thrift(), kTestDir, nullptr);
    ASSERT_OK(block_manager->init());
    std::unique_ptr<TabletWriter> tablet_writer = std::make_unique<HorizontalPkTabletWriter>(
            _tablet_mgr.get(), tablet_id, _tablet_schema, txn_id, nullptr, false);
    SpillMemTableSink sink(block_manager.get(), tablet_writer.get(), &_dummy_runtime_profile);
    auto chunk = gen_data(kChunkSize, 0);
    starrocks::SegmentPB segment;
    ASSERT_OK(sink.flush_chunk_with_deletes(*chunk, *(chunk->columns()[0]), &segment, true, nullptr, 0));
    ASSERT_EQ(1, tablet_writer->segments().size());
    ASSERT_EQ(1, tablet_writer->dels().size());
}

TEST_P(SpillMemTableSinkTest, test_flush_chunk_with_limit) {
    int64_t tablet_id = 1;
    int64_t txn_id = 1;
    std::unique_ptr<LoadSpillBlockManager> block_manager = std::make_unique<LoadSpillBlockManager>(
            TUniqueId(), UniqueId(tablet_id, txn_id).to_thrift(), kTestDir, nullptr);
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
    ASSERT_EQ(config::enable_load_spill_parallel_merge ? 3 : 1, tablet_writer->segments().size());
}

TEST_P(SpillMemTableSinkTest, test_merge) {
    int64_t tablet_id = 1;
    int64_t txn_id = 1;
    std::unique_ptr<LoadSpillBlockManager> block_manager = std::make_unique<LoadSpillBlockManager>(
            TUniqueId(), UniqueId(tablet_id, txn_id).to_thrift(), kTestDir, nullptr);
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
    ASSERT_EQ(config::enable_load_spill_parallel_merge ? 2 : 1, tablet_writer->segments().size());
}

TEST_P(SpillMemTableSinkTest, test_out_of_disk_space) {
    TEST_ENABLE_ERROR_POINT("PosixFileSystem::pre_allocate",
                            Status::CapacityLimitExceed("injected pre_allocate error"));
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        TEST_DISABLE_ERROR_POINT("PosixFileSystem::pre_allocate");
        SyncPoint::GetInstance()->DisableProcessing();
    });
    int64_t tablet_id = 1;
    int64_t txn_id = 1;
    std::unique_ptr<LoadSpillBlockManager> block_manager = std::make_unique<LoadSpillBlockManager>(
            TUniqueId(), UniqueId(tablet_id, txn_id).to_thrift(), kTestDir, nullptr);
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
    ASSERT_EQ(config::enable_load_spill_parallel_merge ? 2 : 1, tablet_writer->segments().size());
}

TEST_P(SpillMemTableSinkTest, test_flush_chunk_with_slot_idx) {
    int64_t tablet_id = 1;
    int64_t txn_id = 1;
    std::unique_ptr<LoadSpillBlockManager> block_manager = std::make_unique<LoadSpillBlockManager>(
            TUniqueId(), UniqueId(tablet_id, txn_id).to_thrift(), kTestDir, nullptr);
    ASSERT_OK(block_manager->init());
    std::unique_ptr<TabletWriter> tablet_writer = std::make_unique<HorizontalGeneralTabletWriter>(
            _tablet_mgr.get(), tablet_id, _tablet_schema, txn_id, false);
    SpillMemTableSink sink(block_manager.get(), tablet_writer.get(), &_dummy_runtime_profile);

    // Flush chunks with different slot_idx
    for (int i = 0; i < 3; i++) {
        auto chunk = gen_data(kChunkSize, i);
        starrocks::SegmentPB segment;
        // Pass slot_idx to flush_chunk
        ASSERT_OK(sink.flush_chunk(*chunk, &segment, false, nullptr, i));
    }

    // Verify block groups were created with correct slot_idx
    auto& groups = block_manager->block_container()->block_groups();
    ASSERT_EQ(3, groups.size());
    ASSERT_EQ(0, groups[0].slot_idx);
    ASSERT_EQ(1, groups[1].slot_idx);
    ASSERT_EQ(2, groups[2].slot_idx);

    ASSERT_OK(sink.merge_blocks_to_segments());
    ASSERT_EQ(config::enable_load_spill_parallel_merge ? 3 : 1, tablet_writer->segments().size());
}

TEST_P(SpillMemTableSinkTest, test_flush_chunk_with_deletes_and_slot_idx) {
    int64_t tablet_id = 1;
    int64_t txn_id = 1;
    std::unique_ptr<LoadSpillBlockManager> block_manager = std::make_unique<LoadSpillBlockManager>(
            TUniqueId(), UniqueId(tablet_id, txn_id).to_thrift(), kTestDir, nullptr);
    ASSERT_OK(block_manager->init());
    std::unique_ptr<TabletWriter> tablet_writer = std::make_unique<HorizontalPkTabletWriter>(
            _tablet_mgr.get(), tablet_id, _tablet_schema, txn_id, nullptr, false);
    SpillMemTableSink sink(block_manager.get(), tablet_writer.get(), &_dummy_runtime_profile);

    // Flush chunks with deletes and different slot_idx
    for (int i = 0; i < 3; i++) {
        auto chunk = gen_data(kChunkSize, i);
        starrocks::SegmentPB segment;
        // Pass slot_idx to flush_chunk_with_deletes
        ASSERT_OK(sink.flush_chunk_with_deletes(*chunk, *(chunk->columns()[0]), &segment, false, nullptr, i));
    }

    // Verify block groups were created with correct slot_idx
    auto& groups = block_manager->block_container()->block_groups();
    ASSERT_EQ(3, groups.size());
    ASSERT_EQ(0, groups[0].slot_idx);
    ASSERT_EQ(1, groups[1].slot_idx);
    ASSERT_EQ(2, groups[2].slot_idx);
}

TEST_P(SpillMemTableSinkTest, test_slot_idx_ordering_after_merge) {
    int64_t tablet_id = 1;
    int64_t txn_id = 1;
    std::unique_ptr<LoadSpillBlockManager> block_manager = std::make_unique<LoadSpillBlockManager>(
            TUniqueId(), UniqueId(tablet_id, txn_id).to_thrift(), kTestDir, nullptr);
    ASSERT_OK(block_manager->init());
    std::unique_ptr<TabletWriter> tablet_writer = std::make_unique<HorizontalGeneralTabletWriter>(
            _tablet_mgr.get(), tablet_id, _tablet_schema, txn_id, false);
    SpillMemTableSink sink(block_manager.get(), tablet_writer.get(), &_dummy_runtime_profile);

    // Flush chunks in non-sequential slot_idx order
    auto chunk0 = gen_data(kChunkSize, 0);
    auto chunk1 = gen_data(kChunkSize, 1);
    auto chunk2 = gen_data(kChunkSize, 2);
    auto chunk3 = gen_data(kChunkSize, 3);

    starrocks::SegmentPB segment;
    ASSERT_OK(sink.flush_chunk(*chunk0, &segment, false, nullptr, 2));
    ASSERT_OK(sink.flush_chunk(*chunk1, &segment, false, nullptr, 1));
    ASSERT_OK(sink.flush_chunk(*chunk2, &segment, false, nullptr, 3));
    ASSERT_OK(sink.flush_chunk(*chunk3, &segment, false, nullptr, 0));

    // Before merge, groups are in insertion order
    auto& groups = block_manager->block_container()->block_groups();
    ASSERT_EQ(4, groups.size());
    ASSERT_EQ(2, groups[0].slot_idx);
    ASSERT_EQ(1, groups[1].slot_idx);
    ASSERT_EQ(3, groups[2].slot_idx);
    ASSERT_EQ(0, groups[3].slot_idx);

    // Merge blocks to segments - this should sort by slot_idx
    ASSERT_OK(sink.merge_blocks_to_segments());

    ASSERT_EQ(0, groups.size()); // Original groups cleared after merge

    ASSERT_TRUE(tablet_writer->segments().size() > 0);
}

TEST_P(SpillMemTableSinkTest, test_flush_data_size_with_slot_idx) {
    int64_t tablet_id = 1;
    int64_t txn_id = 1;
    std::unique_ptr<LoadSpillBlockManager> block_manager = std::make_unique<LoadSpillBlockManager>(
            TUniqueId(), UniqueId(tablet_id, txn_id).to_thrift(), kTestDir, nullptr);
    ASSERT_OK(block_manager->init());
    std::unique_ptr<TabletWriter> tablet_writer = std::make_unique<HorizontalGeneralTabletWriter>(
            _tablet_mgr.get(), tablet_id, _tablet_schema, txn_id, false);
    SpillMemTableSink sink(block_manager.get(), tablet_writer.get(), &_dummy_runtime_profile);

    auto chunk = gen_data(kChunkSize, 0);
    starrocks::SegmentPB segment;
    int64_t flush_data_size = 0;

    // Test with slot_idx and flush_data_size parameter
    ASSERT_OK(sink.flush_chunk(*chunk, &segment, false, &flush_data_size, 10));

    // Verify flush_data_size was set
    ASSERT_GT(flush_data_size, 0);

    // Verify block group was created with correct slot_idx
    auto& groups = block_manager->block_container()->block_groups();
    ASSERT_EQ(1, groups.size());
    ASSERT_EQ(10, groups[0].slot_idx);
}

INSTANTIATE_TEST_SUITE_P(SpillMemTableSinkTest, SpillMemTableSinkTest,
                         ::testing::Values(SpillMemTableSinkTestParams{false, 1073741824},
                                           SpillMemTableSinkTestParams{true, 1024}));

} // namespace starrocks::lake
