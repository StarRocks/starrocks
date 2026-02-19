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

#include "storage/lake_meta_reader.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "base/testutil/id_generator.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/schema.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"
#include "runtime/descriptor_helper.h"
#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/lake/delta_writer.h"
#include "storage/lake/metacache.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/rowset/rowset_options.h"
#include "storage/tablet_schema.h"
#include "test_util.h"

namespace starrocks::lake {

using namespace starrocks;

class LakeMetaReaderTest : public TestBase {
public:
    LakeMetaReaderTest() : TestBase(kTestDirectory) {}

    void SetUp() override { clear_and_init_test_dir(); }

    void TearDown() override { remove_test_dir_ignore_error(); }

protected:
    constexpr static const char* const kTestDirectory = "test_lake_meta_reader";

    // Create a simple tablet with given keys type and write some data
    StatusOr<int64_t> create_tablet_with_data(KeysType keys_type, int num_rowsets = 1, int segments_per_rowset = 1) {
        auto tablet_metadata = generate_simple_tablet_metadata(keys_type);
        int64_t tablet_id = tablet_metadata->id();
        auto tablet_schema = TabletSchema::create(tablet_metadata->schema());
        auto schema = std::make_shared<Schema>(ChunkHelper::convert_schema(tablet_schema));

        RETURN_IF_ERROR(_tablet_mgr->put_tablet_metadata(*tablet_metadata));

        ASSIGN_OR_RETURN(auto tablet, _tablet_mgr->get_tablet(tablet_id));

        // Write data
        for (int r = 0; r < num_rowsets; r++) {
            int64_t txn_id = next_id();
            ASSIGN_OR_RETURN(auto writer, tablet.new_writer(kHorizontal, txn_id));
            RETURN_IF_ERROR(writer->open());

            for (int s = 0; s < segments_per_rowset; s++) {
                // Create simple chunk
                std::vector<int> k0{1 + s * 10, 2 + s * 10, 3 + s * 10, 4 + s * 10, 5 + s * 10};
                std::vector<int> v0{2 + s * 10, 4 + s * 10, 6 + s * 10, 8 + s * 10, 10 + s * 10};

                auto c0 = Int32Column::create();
                auto c1 = Int32Column::create();
                c0->append_numbers(k0.data(), k0.size() * sizeof(int));
                c1->append_numbers(v0.data(), v0.size() * sizeof(int));

                Chunk chunk({std::move(c0), std::move(c1)}, schema);
                RETURN_IF_ERROR(writer->write(chunk));
                RETURN_IF_ERROR(writer->finish());
            }

            auto files = writer->segments();
            writer->close();

            // Publish version
            auto txn_log = std::make_shared<TxnLog>();
            txn_log->set_tablet_id(tablet_id);
            txn_log->set_txn_id(txn_id);
            auto op_write = txn_log->mutable_op_write();
            for (auto& file : files) {
                op_write->mutable_rowset()->add_segments(file.path);
            }
            op_write->mutable_rowset()->set_num_rows(5 * segments_per_rowset);
            op_write->mutable_rowset()->set_data_size(100);
            op_write->mutable_rowset()->set_overlapped(true);

            RETURN_IF_ERROR(_tablet_mgr->put_txn_log(txn_log));

            int64_t version = r + 2;
            ASSIGN_OR_RETURN(auto new_metadata,
                             TEST_publish_single_version(_tablet_mgr.get(), tablet_id, version, txn_id));
        }

        return tablet_id;
    }
};

// Test _get_segments with primary key table
TEST_F(LakeMetaReaderTest, test_get_segments_with_primary_key_table) {
    // Create a primary key tablet with 1 rowset containing 2 segments
    ASSIGN_OR_ABORT(auto tablet_id, create_tablet_with_data(PRIMARY_KEYS, 1, 2));

    // Get the tablet
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id, 2));

    // Call TEST_get_segments
    LakeMetaReader reader;
    std::vector<SegmentSharedPtr> segments;
    std::vector<SegmentMetaCollectOptions> options_list;

    ASSERT_OK(reader.TEST_get_segments(tablet, &segments, &options_list));

    // Verify we got 2 segments
    ASSERT_EQ(2, segments.size());
    ASSERT_EQ(2, options_list.size());

    // Verify options for primary key table
    for (int i = 0; i < options_list.size(); i++) {
        auto& options = options_list[i];
        EXPECT_TRUE(options.is_primary_keys) << "Expected is_primary_keys to be true for PK table";
        EXPECT_EQ(tablet_id, options.tablet_id) << "Expected tablet_id to match";
        EXPECT_EQ(2, options.version) << "Expected version to be 2";
        EXPECT_EQ(i, options.segment_id) << "Expected segment_id to be " << i;
        EXPECT_NE(nullptr, options.dcg_loader) << "Expected dcg_loader to be set for PK table";
    }
}

// Test _get_segments with non-primary key table (DUP_KEYS)
TEST_F(LakeMetaReaderTest, test_get_segments_with_dup_keys_table) {
    // Create a duplicate keys tablet with 1 rowset containing 2 segments
    ASSIGN_OR_ABORT(auto tablet_id, create_tablet_with_data(DUP_KEYS, 1, 2));

    // Get the tablet
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id, 2));

    // Call TEST_get_segments
    LakeMetaReader reader;
    std::vector<SegmentSharedPtr> segments;
    std::vector<SegmentMetaCollectOptions> options_list;

    ASSERT_OK(reader.TEST_get_segments(tablet, &segments, &options_list));

    // Verify we got 2 segments
    ASSERT_EQ(2, segments.size());
    // Verify options for non-primary key table
    ASSERT_EQ(2, options_list.size()) << "Expected 2 options for non-PK table";
}

// Test _get_segments with multiple rowsets
TEST_F(LakeMetaReaderTest, test_get_segments_with_multiple_rowsets) {
    // Create a primary key tablet with 3 rowsets, each containing 2 segments
    ASSIGN_OR_ABORT(auto tablet_id, create_tablet_with_data(PRIMARY_KEYS, 3, 2));

    // Get the tablet
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id, 4));

    // Call TEST_get_segments
    LakeMetaReader reader;
    std::vector<SegmentSharedPtr> segments;
    std::vector<SegmentMetaCollectOptions> options_list;

    ASSERT_OK(reader.TEST_get_segments(tablet, &segments, &options_list));

    // Verify we got 6 segments (3 rowsets * 2 segments each)
    ASSERT_EQ(6, segments.size());
    ASSERT_EQ(6, options_list.size());

    // Verify all options have correct primary key settings
    for (const auto& options : options_list) {
        EXPECT_TRUE(options.is_primary_keys);
        EXPECT_EQ(tablet_id, options.tablet_id);
        EXPECT_EQ(4, options.version);
        EXPECT_NE(nullptr, options.dcg_loader);
    }

    // Verify segment_ids are correctly set (should be 0, 1 for each rowset)
    int expected_seg_id = 0;
    int rowset_count = 0;
    for (const auto& options : options_list) {
        EXPECT_EQ(expected_seg_id, options.segment_id)
                << "Expected segment_id " << expected_seg_id << " at position " << rowset_count;
        expected_seg_id++;
        if (expected_seg_id >= 2) {
            expected_seg_id = 0;
            rowset_count++;
        }
    }
}

// Test _get_segments returns correct rowset ids for primary key table
TEST_F(LakeMetaReaderTest, test_get_segments_rowset_ids) {
    // Create a primary key tablet with 2 rowsets
    ASSIGN_OR_ABORT(auto tablet_id, create_tablet_with_data(PRIMARY_KEYS, 2, 1));

    // Get the tablet
    ASSIGN_OR_ABORT(auto tablet, _tablet_mgr->get_tablet(tablet_id, 3));

    // Call TEST_get_segments
    LakeMetaReader reader;
    std::vector<SegmentSharedPtr> segments;
    std::vector<SegmentMetaCollectOptions> options_list;

    ASSERT_OK(reader.TEST_get_segments(tablet, &segments, &options_list));

    // Verify we got 2 segments (2 rowsets * 1 segment each)
    ASSERT_EQ(2, segments.size());
    ASSERT_EQ(2, options_list.size());

    // Verify rowset ids are different for different rowsets
    EXPECT_NE(options_list[0].pk_rowsetid, options_list[1].pk_rowsetid)
            << "Expected different rowset ids for different rowsets";
}

} // namespace starrocks::lake
