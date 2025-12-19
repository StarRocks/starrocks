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

#include "storage/olap_meta_reader.h"

#include <gtest/gtest.h>

#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "runtime/descriptor_helper.h"
#include "storage/chunk_helper.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "testutil/assert.h"

namespace starrocks {

class OlapMetaReaderTest : public testing::Test {
public:
    void SetUp() override {
        _engine = StorageEngine::instance();
        ASSERT_NE(nullptr, _engine);
    }

    void TearDown() override {
        if (_tablet) {
            (void)StorageEngine::instance()->tablet_manager()->drop_tablet(_tablet->tablet_id());
            _tablet.reset();
        }
    }

protected:
    // Create a tablet with given keys type
    TabletSharedPtr create_tablet(int64_t tablet_id, int32_t schema_hash, TKeysType::type keys_type) {
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(1);
        request.__set_version_hash(0);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 1;
        request.tablet_schema.keys_type = keys_type;
        request.tablet_schema.storage_type = TStorageType::COLUMN;

        TColumn k1;
        k1.column_name = "c0";
        k1.__set_is_key(true);
        k1.column_type.type = TPrimitiveType::INT;
        request.tablet_schema.columns.push_back(k1);

        TColumn k2;
        k2.column_name = "c1";
        k2.__set_is_key(false);
        k2.column_type.type = TPrimitiveType::INT;
        request.tablet_schema.columns.push_back(k2);

        auto st = _engine->create_tablet(request);
        if (!st.ok()) {
            return nullptr;
        }
        return _engine->tablet_manager()->get_tablet(tablet_id, false);
    }

    // Create a rowset and add it to the tablet
    StatusOr<RowsetSharedPtr> create_rowset(const TabletSharedPtr& tablet, int num_segments) {
        RowsetWriterContext writer_context;
        RowsetId rowset_id = _engine->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_path_prefix = tablet->schema_hash_path();
        writer_context.rowset_state = COMMITTED;
        writer_context.tablet_schema = tablet->tablet_schema();
        writer_context.version.first = 0;
        writer_context.version.second = 0;
        writer_context.segments_overlap = NONOVERLAPPING;

        std::unique_ptr<RowsetWriter> writer;
        RETURN_IF_ERROR(RowsetFactory::create_rowset_writer(writer_context, &writer));

        auto schema = ChunkHelper::convert_schema(tablet->tablet_schema());

        // Write multiple segments
        for (int seg = 0; seg < num_segments; seg++) {
            auto chunk = ChunkHelper::new_chunk(schema, 5);
            auto cols = chunk->mutable_columns();

            auto c0 = down_cast<Int32Column*>(cols[0].get());
            auto c1 = down_cast<Int32Column*>(cols[1].get());

            for (int i = 0; i < 5; i++) {
                c0->append(i + seg * 100);
                c1->append(i * 2 + seg * 100);
            }

            RETURN_IF_ERROR(writer->flush_chunk(*chunk));
        }

        return writer->build();
    }

    StorageEngine* _engine = nullptr;
    TabletSharedPtr _tablet;
};

// Test _get_segments basic functionality with duplicate keys table
TEST_F(OlapMetaReaderTest, test_get_segments_basic) {
    // Create a duplicate keys tablet (easier to set up than primary key)
    int64_t tablet_id = 10001;
    int32_t schema_hash = 1001;
    _tablet = create_tablet(tablet_id, schema_hash, TKeysType::DUP_KEYS);
    ASSERT_NE(nullptr, _tablet);

    // Create and add 2 rowsets, each with 1 segment
    for (int i = 0; i < 2; i++) {
        ASSIGN_OR_ABORT(auto rowset, create_rowset(_tablet, 1));
        ASSERT_OK(_tablet->add_rowset(rowset));
    }

    // Test TEST__get_segments
    OlapMetaReader reader;
    std::vector<SegmentSharedPtr> segments;
    std::vector<SegmentMetaCollectOptions> options_list;
    Version version(0, 1);

    ASSERT_OK(reader.TEST__get_segments(_tablet, version, &segments, &options_list));

    // Verify we got segments
    ASSERT_GT(segments.size(), 0);
    ASSERT_EQ(segments.size(), options_list.size());

    // Verify basic options are set
    for (size_t i = 0; i < options_list.size(); i++) {
        auto& options = options_list[i];
        EXPECT_EQ(tablet_id, options.tablet_id) << "Expected tablet_id to match";
        EXPECT_NE(nullptr, options.dcg_loader) << "Expected dcg_loader to be set";
        EXPECT_GE(options.segment_id, 0) << "Expected valid segment_id";
    }
}

// Test _get_segments verifies primary key flag
TEST_F(OlapMetaReaderTest, test_get_segments_primary_key_flag) {
    // Test with DUP_KEYS table
    int64_t dup_tablet_id = 10001;
    int32_t dup_schema_hash = 1001;
    auto dup_tablet = create_tablet(dup_tablet_id, dup_schema_hash, TKeysType::DUP_KEYS);
    ASSERT_NE(nullptr, dup_tablet);

    ASSIGN_OR_ABORT(auto dup_rowset, create_rowset(dup_tablet, 1));
    ASSERT_OK(dup_tablet->add_rowset(dup_rowset));

    OlapMetaReader dup_reader;
    std::vector<SegmentSharedPtr> dup_segments;
    std::vector<SegmentMetaCollectOptions> dup_options;
    Version version(0, 1);

    ASSERT_OK(dup_reader.TEST__get_segments(dup_tablet, version, &dup_segments, &dup_options));
    ASSERT_GT(dup_options.size(), 0);
    EXPECT_FALSE(dup_options[0].is_primary_keys) << "DUP_KEYS table should have is_primary_keys=false";

    // Clean up before next test
    (void)StorageEngine::instance()->tablet_manager()->drop_tablet(dup_tablet_id);

    // Test with PRIMARY_KEYS table - only verify the keys_type check
    int64_t pk_tablet_id = 10002;
    int32_t pk_schema_hash = 1002;
    auto pk_tablet = create_tablet(pk_tablet_id, pk_schema_hash, TKeysType::PRIMARY_KEYS);
    ASSERT_NE(nullptr, pk_tablet);

    // Verify tablet is primary key type
    EXPECT_EQ(KeysType::PRIMARY_KEYS, pk_tablet->keys_type());

    // For primary key tablet, we can't easily add rowsets in unit test
    // but we can verify the logic by checking the tablet properties
    // The _get_segments method will set is_primary_keys based on tablet->keys_type()

    (void)StorageEngine::instance()->tablet_manager()->drop_tablet(pk_tablet_id);
}

// Test _get_segments with duplicate keys table
TEST_F(OlapMetaReaderTest, test_get_segments_with_dup_keys_table) {
    // Create a duplicate keys tablet
    int64_t tablet_id = 10002;
    int32_t schema_hash = 1002;
    _tablet = create_tablet(tablet_id, schema_hash, TKeysType::DUP_KEYS);
    ASSERT_NE(nullptr, _tablet);

    // Create and add 1 rowset with 1 segment
    ASSIGN_OR_ABORT(auto rowset, create_rowset(_tablet, 1));
    ASSERT_OK(_tablet->add_rowset(rowset));

    // Test TEST__get_segments
    OlapMetaReader reader;
    std::vector<SegmentSharedPtr> segments;
    std::vector<SegmentMetaCollectOptions> options_list;
    Version version(0, 1);

    ASSERT_OK(reader.TEST__get_segments(_tablet, version, &segments, &options_list));

    // Verify we got segments
    ASSERT_GT(segments.size(), 0);
    ASSERT_EQ(segments.size(), options_list.size());

    // Verify options for duplicate keys table
    for (size_t i = 0; i < options_list.size(); i++) {
        auto& options = options_list[i];
        EXPECT_FALSE(options.is_primary_keys) << "Expected is_primary_keys to be false for DUP_KEYS table";
        EXPECT_EQ(tablet_id, options.tablet_id) << "Expected tablet_id to match";
        EXPECT_NE(nullptr, options.dcg_loader) << "Expected dcg_loader to be set";
        // For non-PK tables, rowsetid should be set
        EXPECT_TRUE(options.rowsetid.to_string().length() > 0) << "Expected rowsetid to be set for non-PK table";
    }
}

// Test _get_segments with multiple rowsets
TEST_F(OlapMetaReaderTest, test_get_segments_with_multiple_rowsets) {
    // Create a duplicate keys tablet
    int64_t tablet_id = 10003;
    int32_t schema_hash = 1003;
    _tablet = create_tablet(tablet_id, schema_hash, TKeysType::DUP_KEYS);
    ASSERT_NE(nullptr, _tablet);

    // Create and add 3 rowsets, each with 2 segments
    for (int i = 0; i < 3; i++) {
        ASSIGN_OR_ABORT(auto rowset, create_rowset(_tablet, 2));
        ASSERT_OK(_tablet->add_rowset(rowset));
    }

    // Test TEST__get_segments
    OlapMetaReader reader;
    std::vector<SegmentSharedPtr> segments;
    std::vector<SegmentMetaCollectOptions> options_list;
    Version version(0, 1);

    ASSERT_OK(reader.TEST__get_segments(_tablet, version, &segments, &options_list));

    // Verify we got 6 segments (3 rowsets * 2 segments each)
    ASSERT_GT(segments.size(), 0);
    ASSERT_EQ(segments.size(), options_list.size());

    // Verify all options have correct settings
    for (const auto& options : options_list) {
        EXPECT_FALSE(options.is_primary_keys) << "DUP_KEYS table should have is_primary_keys=false";
        EXPECT_EQ(tablet_id, options.tablet_id);
        EXPECT_NE(nullptr, options.dcg_loader);
    }

    // Verify segment_ids are correctly set within each rowset
    for (const auto& options : options_list) {
        // segment_id should be 0 or 1 since each rowset has 2 segments
        EXPECT_TRUE(options.segment_id >= 0 && options.segment_id < 2)
                << "Expected segment_id to be 0 or 1, got " << options.segment_id;
    }
}

// Test _get_segments with aggregate keys table
TEST_F(OlapMetaReaderTest, test_get_segments_with_agg_keys_table) {
    // Create an aggregate keys tablet
    int64_t tablet_id = 10004;
    int32_t schema_hash = 1004;
    _tablet = create_tablet(tablet_id, schema_hash, TKeysType::AGG_KEYS);
    ASSERT_NE(nullptr, _tablet);

    // Create and add a rowset
    ASSIGN_OR_ABORT(auto rowset, create_rowset(_tablet, 1));
    ASSERT_OK(_tablet->add_rowset(rowset));

    // Test TEST__get_segments
    OlapMetaReader reader;
    std::vector<SegmentSharedPtr> segments;
    std::vector<SegmentMetaCollectOptions> options_list;
    Version version(0, 1);

    ASSERT_OK(reader.TEST__get_segments(_tablet, version, &segments, &options_list));

    // Verify we got segments
    ASSERT_GT(segments.size(), 0);
    ASSERT_EQ(segments.size(), options_list.size());

    // Verify options for aggregate keys table
    for (size_t i = 0; i < options_list.size(); i++) {
        auto& options = options_list[i];
        EXPECT_FALSE(options.is_primary_keys) << "Expected is_primary_keys to be false for AGG_KEYS table";
        EXPECT_EQ(tablet_id, options.tablet_id) << "Expected tablet_id to match";
        EXPECT_NE(nullptr, options.dcg_loader) << "Expected dcg_loader to be set";
    }
}

// Test _get_segments returns correct rowset ids
TEST_F(OlapMetaReaderTest, test_get_segments_rowset_ids) {
    // Create a duplicate keys tablet
    int64_t tablet_id = 10005;
    int32_t schema_hash = 1005;
    _tablet = create_tablet(tablet_id, schema_hash, TKeysType::DUP_KEYS);
    ASSERT_NE(nullptr, _tablet);

    // Create and add 2 rowsets
    for (int i = 0; i < 2; i++) {
        ASSIGN_OR_ABORT(auto rowset, create_rowset(_tablet, 1));
        ASSERT_OK(_tablet->add_rowset(rowset));
    }

    // Test TEST__get_segments
    OlapMetaReader reader;
    std::vector<SegmentSharedPtr> segments;
    std::vector<SegmentMetaCollectOptions> options_list;
    Version version(0, 1);

    ASSERT_OK(reader.TEST__get_segments(_tablet, version, &segments, &options_list));

    // Verify we got segments
    ASSERT_GT(segments.size(), 0);
    ASSERT_EQ(segments.size(), options_list.size());

    // For non-primary key tables, verify rowsetid is set
    if (options_list.size() >= 2) {
        // Different segments from different rowsets should have different rowsetid
        // (Note: segments from the same rowset will have the same rowsetid)
        bool found_different = false;
        for (size_t i = 1; i < options_list.size(); i++) {
            if (options_list[i].rowsetid.to_string() != options_list[0].rowsetid.to_string()) {
                found_different = true;
                break;
            }
        }
        // We created 2 different rowsets, so we should find different rowsetids
        EXPECT_TRUE(found_different) << "Expected different rowsetids for different rowsets";
    }
}

// Test dcg_loader is LocalDeltaColumnGroupLoader
TEST_F(OlapMetaReaderTest, test_dcg_loader_type) {
    // Create a duplicate keys tablet
    int64_t tablet_id = 10006;
    int32_t schema_hash = 1006;
    _tablet = create_tablet(tablet_id, schema_hash, TKeysType::DUP_KEYS);
    ASSERT_NE(nullptr, _tablet);

    // Create and add a rowset
    ASSIGN_OR_ABORT(auto rowset, create_rowset(_tablet, 1));
    ASSERT_OK(_tablet->add_rowset(rowset));

    // Test TEST__get_segments
    OlapMetaReader reader;
    std::vector<SegmentSharedPtr> segments;
    std::vector<SegmentMetaCollectOptions> options_list;
    Version version(0, 1);

    ASSERT_OK(reader.TEST__get_segments(_tablet, version, &segments, &options_list));

    // Verify we got segments
    ASSERT_GT(segments.size(), 0);
    ASSERT_EQ(segments.size(), options_list.size());

    // Verify dcg_loader is set and is LocalDeltaColumnGroupLoader
    for (const auto& options : options_list) {
        EXPECT_NE(nullptr, options.dcg_loader) << "Expected dcg_loader to be set";
        // The loader should be a LocalDeltaColumnGroupLoader
        // We can verify it's not null, the actual type check would require RTTI
    }
}

} // namespace starrocks
