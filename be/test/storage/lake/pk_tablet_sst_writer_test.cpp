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

#include "storage/lake/pk_tablet_sst_writer.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <random>

#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "common/logging.h"
#include "fs/fs_util.h"
#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/lake/compaction_task.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/join_path.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/txn_log.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/tablet_schema.h"
#include "test_util.h"
#include "testutil/assert.h"
#include "testutil/id_generator.h"
#include "testutil/sync_point.h"

namespace starrocks::lake {

using namespace starrocks;

inline std::shared_ptr<TabletMetadataPB> generate_tablet_metadata(KeysType keys_type) {
    auto metadata = std::make_shared<TabletMetadata>();
    metadata->set_id(next_id());
    metadata->set_version(1);
    metadata->set_cumulative_point(0);
    metadata->set_next_rowset_id(1);
    //
    //  | column | type | KEY | NULL |
    //  +--------+------+-----+------+
    //  |   c0   |  VARCHAR | YES |  NO  |
    //  |   c1   |  INT | NO  |  NO  |
    auto schema = metadata->mutable_schema();
    schema->set_keys_type(keys_type);
    schema->set_id(next_id());
    schema->set_num_short_key_columns(1);
    schema->set_num_rows_per_row_block(65535);
    auto c0 = schema->add_column();
    {
        c0->set_unique_id(next_id());
        c0->set_name("c0");
        c0->set_type("VARCHAR");
        c0->set_is_key(true);
        c0->set_is_nullable(false);
        c0->set_length(20);
    }
    auto c1 = schema->add_column();
    {
        c1->set_unique_id(next_id());
        c1->set_name("c1");
        c1->set_type("INT");
        c1->set_is_key(false);
        c1->set_is_nullable(false);
        c1->set_aggregation(keys_type == DUP_KEYS ? "NONE" : "REPLACE");
    }
    return metadata;
}

class PkTabletSSTWriterTest : public TestBase {
public:
    PkTabletSSTWriterTest() : TestBase(kTestDirectory) {
        _tablet_metadata = generate_tablet_metadata(PRIMARY_KEYS);
        _tablet_schema = TabletSchema::create(_tablet_metadata->schema());
        _schema = std::make_shared<Schema>(ChunkHelper::convert_schema(_tablet_schema));
        ASSIGN_OR_ABORT(_fs, FileSystem::CreateSharedFromString(kTestDirectory));
    }

protected:
    void SetUp() override {
        clear_and_init_test_dir();
        CHECK_OK(_tablet_mgr->put_tablet_metadata(*_tablet_metadata));
    }

    void TearDown() override { remove_test_dir_ignore_error(); }

    Chunk generate_data(int64_t chunk_size, int64_t start_index = 0) {
        std::vector<std::pair<std::string, int>> data(chunk_size);
        for (int i = 0; i < chunk_size; i++) {
            data[i] = {fmt::format("key_{:10d}", start_index + i), (start_index + i) * 3};
        }

        auto c0 = BinaryColumn::create();
        auto c1 = Int32Column::create();
        for (const auto& [key, value] : data) {
            c0->append(key);
            c1->append(value);
        }
        return Chunk({std::move(c0), std::move(c1)}, _schema);
    }

    constexpr static const char* const kTestDirectory = "test_pk_tablet_sst_writer";

    std::shared_ptr<TabletMetadata> _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
    std::shared_ptr<Schema> _schema;
    int64_t _partition_id = 456;
    RuntimeProfile _dummy_runtime_profile{"dummy"};
    std::shared_ptr<FileSystem> _fs;
};

TEST_F(PkTabletSSTWriterTest, test_pk_tablet_sst_writer_basic_operations) {
    const int64_t tablet_id = 12345;

    // Create PkTabletSSTWriter
    auto pk_sst_writer = std::make_unique<PkTabletSSTWriter>(_tablet_schema, _tablet_mgr.get(), tablet_id);

    // Test reset_sst_writer
    auto location_provider = std::make_shared<FixedLocationProvider>(kTestDirectory);
    ASSERT_OK(pk_sst_writer->reset_sst_writer(location_provider, _fs));

    // Generate test data
    const int64_t chunk_size = 100;
    auto chunk = generate_data(chunk_size);

    // Test append_sst_record
    ASSERT_OK(pk_sst_writer->append_sst_record(chunk));

    // Add more data
    auto chunk2 = generate_data(50, chunk_size);
    ASSERT_OK(pk_sst_writer->append_sst_record(chunk2));

    // Test flush_sst_writer
    ASSIGN_OR_ABORT(auto file_info, pk_sst_writer->flush_sst_writer());

    // Verify file info
    ASSERT_FALSE(file_info.path.empty());
    ASSERT_GT(file_info.size, 0);
    ASSERT_TRUE(file_info.path.ends_with(".sst"));
}

TEST_F(PkTabletSSTWriterTest, test_pk_tablet_sst_writer_multiple_chunks) {
    const int64_t tablet_id = 67890;

    auto pk_sst_writer = std::make_unique<PkTabletSSTWriter>(_tablet_schema, _tablet_mgr.get(), tablet_id);

    // Reset writer
    auto location_provider = std::make_shared<FixedLocationProvider>(kTestDirectory);
    ASSERT_OK(pk_sst_writer->reset_sst_writer(location_provider, _fs));

    // Add multiple chunks with different sizes
    std::vector<int64_t> chunk_sizes = {10, 50, 100, 25};
    int64_t start_index = 0;
    for (auto size : chunk_sizes) {
        auto chunk = generate_data(size, start_index);
        ASSERT_OK(pk_sst_writer->append_sst_record(chunk));
        start_index += size;
    }

    // Flush and get result
    ASSIGN_OR_ABORT(auto file_info, pk_sst_writer->flush_sst_writer());

    // Verify results
    ASSERT_FALSE(file_info.path.empty());
    ASSERT_GT(file_info.size, 0);
}

TEST_F(PkTabletSSTWriterTest, test_pk_tablet_sst_writer_error_handling) {
    const int64_t tablet_id = 11111;

    auto pk_sst_writer = std::make_unique<PkTabletSSTWriter>(_tablet_schema, _tablet_mgr.get(), tablet_id);

    // Test append_sst_record before reset - should fail
    auto chunk = generate_data(10);
    ASSERT_ERROR(pk_sst_writer->append_sst_record(chunk));

    // Test flush_sst_writer before reset - should fail
    ASSERT_ERROR(pk_sst_writer->flush_sst_writer());

    // Reset writer
    auto location_provider = std::make_shared<FixedLocationProvider>(kTestDirectory);
    ASSERT_OK(pk_sst_writer->reset_sst_writer(location_provider, _fs));

    // Now operations should work
    ASSERT_OK(pk_sst_writer->append_sst_record(chunk));
    ASSIGN_OR_ABORT(auto file_info, pk_sst_writer->flush_sst_writer());
    ASSERT_FALSE(file_info.path.empty());
}

TEST_F(PkTabletSSTWriterTest, test_pk_tablet_sst_writer_empty_chunk) {
    const int64_t tablet_id = 22222;

    auto pk_sst_writer = std::make_unique<PkTabletSSTWriter>(_tablet_schema, _tablet_mgr.get(), tablet_id);

    // Reset writer
    auto location_provider = std::make_shared<FixedLocationProvider>(kTestDirectory);
    ASSERT_OK(pk_sst_writer->reset_sst_writer(location_provider, _fs));

    // Create empty chunk
    auto empty_chunk = generate_data(0);
    ASSERT_OK(pk_sst_writer->append_sst_record(empty_chunk));

    // Add some real data
    auto chunk = generate_data(50);
    ASSERT_OK(pk_sst_writer->append_sst_record(chunk));

    // Flush
    ASSIGN_OR_ABORT(auto file_info, pk_sst_writer->flush_sst_writer());
    ASSERT_FALSE(file_info.path.empty());
    ASSERT_GT(file_info.size, 0);
}

TEST_F(PkTabletSSTWriterTest, test_pk_tablet_sst_writer_reuse) {
    const int64_t tablet_id = 33333;

    auto pk_sst_writer = std::make_unique<PkTabletSSTWriter>(_tablet_schema, _tablet_mgr.get(), tablet_id);

    // First use
    auto location_provider = std::make_shared<FixedLocationProvider>(kTestDirectory);
    ASSERT_OK(pk_sst_writer->reset_sst_writer(location_provider, _fs));

    auto chunk1 = generate_data(30);
    ASSERT_OK(pk_sst_writer->append_sst_record(chunk1));

    ASSIGN_OR_ABORT(auto file_info1, pk_sst_writer->flush_sst_writer());
    ASSERT_FALSE(file_info1.path.empty());

    // Reuse the writer for a second file
    ASSERT_OK(pk_sst_writer->reset_sst_writer(location_provider, _fs));

    auto chunk2 = generate_data(40, 30);
    ASSERT_OK(pk_sst_writer->append_sst_record(chunk2));

    ASSIGN_OR_ABORT(auto file_info2, pk_sst_writer->flush_sst_writer());
    ASSERT_FALSE(file_info2.path.empty());

    // The two files should be different
    ASSERT_NE(file_info1.path, file_info2.path);
}

TEST_F(PkTabletSSTWriterTest, test_publish_multi_segments_with_sst) {
    auto chunk0 = generate_data(12);
    auto chunk1 = generate_data(12, 12);
    auto chunk2 = generate_data(12, 24);
    auto indexes = std::vector<uint32_t>(chunk0.num_rows());
    for (uint32_t i = 0, n = chunk0.num_rows(); i < n; i++) {
        indexes[i] = i;
    }
    auto version = 1;
    auto tablet_id = _tablet_metadata->id();
    const int64_t old_size = config::write_buffer_size;
    config::write_buffer_size = 1;
    const bool old_enable_pk_parallel_execution = config::enable_pk_parallel_execution;
    config::enable_pk_parallel_execution = true;
    for (int i = 0; i < 5; i++) {
        int64_t txn_id = next_id();
        ASSIGN_OR_ABORT(auto delta_writer, DeltaWriterBuilder()
                                                   .set_tablet_manager(_tablet_mgr.get())
                                                   .set_tablet_id(tablet_id)
                                                   .set_txn_id(txn_id)
                                                   .set_partition_id(_partition_id)
                                                   .set_mem_tracker(_mem_tracker.get())
                                                   .set_schema_id(_tablet_schema->id())
                                                   .set_profile(&_dummy_runtime_profile)
                                                   .build());
        ASSERT_OK(delta_writer->open());
        ASSERT_OK(delta_writer->write(chunk0, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->write(chunk1, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->write(chunk2, indexes.data(), indexes.size()));
        ASSERT_OK(delta_writer->finish_with_txnlog());
        delta_writer->close();
        // read txnlog
        ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(tablet_id, txn_id));
        EXPECT_EQ(txn_log->op_write().ssts_size(), 1);
        // Publish version
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        EXPECT_TRUE(_update_mgr->TEST_check_update_state_cache_absent(tablet_id, txn_id));
        version++;
    }
    // Compaction
    {
        auto old_val = config::lake_pk_compaction_min_input_segments;
        config::lake_pk_compaction_min_input_segments = 1;
        int64_t txn_id = next_id();
        auto task_context = std::make_unique<CompactionTaskContext>(txn_id, tablet_id, version, false, false, nullptr);
        ASSIGN_OR_ABORT(auto task, _tablet_mgr->compact(task_context.get()));
        ASSERT_OK(task->execute(CompactionTask::kNoCancelFn));
        EXPECT_EQ(100, task_context->progress.value());
        ASSIGN_OR_ABORT(auto txn_log, _tablet_mgr->get_txn_log(tablet_id, txn_id));
        EXPECT_EQ(txn_log->op_compaction().input_rowsets_size(), 5);
        EXPECT_EQ(txn_log->op_compaction().ssts_size(), 1);
        ASSERT_OK(publish_single_version(tablet_id, version + 1, txn_id).status());
        version++;
        config::lake_pk_compaction_min_input_segments = old_val;
    }
    // update memory usage, should large than zero
    config::write_buffer_size = old_size;
    config::enable_pk_parallel_execution = old_enable_pk_parallel_execution;
}

} // namespace starrocks::lake