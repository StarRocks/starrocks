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

#include "base/testutil/assert.h"
#include "column/chunk.h"
#include "column/datum.h"
#include "column/fixed_length_column.h"
#include "runtime/descriptor_helper.h"
#include "storage/chunk_helper.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_updates.h"

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
    // Create a primary key tablet (following TabletUpdatesTest pattern)
    TabletSharedPtr create_tablet(int64_t tablet_id, int32_t schema_hash) {
        TCreateTabletReq request;
        request.tablet_id = tablet_id;
        request.__set_version(1);
        request.__set_version_hash(0);
        request.tablet_schema.schema_hash = schema_hash;
        request.tablet_schema.short_key_column_count = 1;
        request.tablet_schema.keys_type = TKeysType::PRIMARY_KEYS;
        request.tablet_schema.storage_type = TStorageType::COLUMN;

        TColumn k1;
        k1.column_name = "pk";
        k1.__set_is_key(true);
        k1.column_type.type = TPrimitiveType::BIGINT;
        request.tablet_schema.columns.push_back(k1);

        TColumn k2;
        k2.column_name = "v1";
        k2.__set_is_key(false);
        k2.column_type.type = TPrimitiveType::SMALLINT;
        request.tablet_schema.columns.push_back(k2);

        TColumn k3;
        k3.column_name = "v2";
        k3.__set_is_key(false);
        k3.column_type.type = TPrimitiveType::INT;
        request.tablet_schema.columns.push_back(k3);

        auto st = _engine->create_tablet(request);
        if (!st.ok()) {
            return nullptr;
        }
        return _engine->tablet_manager()->get_tablet(tablet_id, false);
    }

    // Create a rowset (following TabletUpdatesTest pattern)
    RowsetSharedPtr create_rowset(const TabletSharedPtr& tablet, const vector<int64_t>& keys) {
        RowsetWriterContext writer_context;
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_path_prefix = tablet->schema_hash_path();
        writer_context.rowset_state = COMMITTED;
        writer_context.tablet_schema = tablet->thread_safe_get_tablet_schema();
        writer_context.version.first = 0;
        writer_context.version.second = 0;
        writer_context.segments_overlap = NONOVERLAPPING;

        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());

        auto schema = ChunkHelper::convert_schema(tablet->thread_safe_get_tablet_schema());
        auto chunk = ChunkHelper::new_chunk(schema, keys.size());
        auto cols = chunk->mutable_columns();

        for (int64_t key : keys) {
            cols[0]->append_datum(Datum(key));
            cols[1]->append_datum(Datum((int16_t)(key % 100 + 1)));
            cols[2]->append_datum(Datum((int32_t)(key % 1000 + 2)));
        }

        if (!keys.empty()) {
            CHECK_OK(writer->flush_chunk(*chunk));
        }
        return *writer->build();
    }

    // Create a rowset with multiple segments (following TabletUpdatesTest pattern)
    RowsetSharedPtr create_rowset_with_multiple_segments(const TabletSharedPtr& tablet,
                                                         const vector<vector<int64_t>>& keys_by_segment) {
        RowsetWriterContext writer_context;
        RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.rowset_id = rowset_id;
        writer_context.tablet_id = tablet->tablet_id();
        writer_context.tablet_schema_hash = tablet->schema_hash();
        writer_context.partition_id = 0;
        writer_context.rowset_path_prefix = tablet->schema_hash_path();
        writer_context.rowset_state = COMMITTED;
        writer_context.tablet_schema = tablet->thread_safe_get_tablet_schema();
        writer_context.version.first = 0;
        writer_context.version.second = 0;
        writer_context.segments_overlap = OVERLAP_UNKNOWN;

        std::unique_ptr<RowsetWriter> writer;
        EXPECT_TRUE(RowsetFactory::create_rowset_writer(writer_context, &writer).ok());

        auto schema = ChunkHelper::convert_schema(tablet->thread_safe_get_tablet_schema());
        for (const auto& keys : keys_by_segment) {
            auto chunk = ChunkHelper::new_chunk(schema, keys.size());
            auto cols = chunk->mutable_columns();

            for (int64_t key : keys) {
                cols[0]->append_datum(Datum(key));
                cols[1]->append_datum(Datum((int16_t)(key % 100 + 1)));
                cols[2]->append_datum(Datum((int32_t)(key % 1000 + 2)));
            }

            if (!keys.empty()) {
                CHECK_OK(writer->flush_chunk(*chunk));
            }
        }
        return *writer->build();
    }

    StorageEngine* _engine = nullptr;
    TabletSharedPtr _tablet;
};

// Test _get_segments basic functionality with primary key table
TEST_F(OlapMetaReaderTest, test_get_segments_basic) {
    // Create a primary key tablet
    int64_t tablet_id = 10001;
    int32_t schema_hash = 1001;
    _tablet = create_tablet(tablet_id, schema_hash);
    ASSERT_NE(nullptr, _tablet);
    ASSERT_EQ(1, _tablet->updates()->version_history_count());

    // Create and commit 2 rowsets
    std::vector<int64_t> keys1 = {1, 2, 3, 4, 5};
    auto rs1 = create_rowset(_tablet, keys1);
    ASSERT_OK(_tablet->rowset_commit(2, rs1));
    ASSERT_EQ(2, _tablet->updates()->max_version());

    std::vector<int64_t> keys2 = {6, 7, 8, 9, 10};
    auto rs2 = create_rowset(_tablet, keys2);
    ASSERT_OK(_tablet->rowset_commit(3, rs2));
    ASSERT_EQ(3, _tablet->updates()->max_version());

    // Test TEST_get_segments
    OlapMetaReader reader;
    std::vector<SegmentSharedPtr> segments;
    std::vector<SegmentMetaCollectOptions> options_list;
    Version version(0, 3);

    ASSERT_OK(reader.TEST_get_segments(_tablet, version, &segments, &options_list));

    // Verify we got segments
    ASSERT_GT(segments.size(), 0);
    ASSERT_EQ(segments.size(), options_list.size());

    // Verify options for primary key table
    for (size_t i = 0; i < options_list.size(); i++) {
        auto& options = options_list[i];
        EXPECT_TRUE(options.is_primary_keys) << "Expected is_primary_keys to be true for PK table";
        EXPECT_EQ(tablet_id, options.tablet_id) << "Expected tablet_id to match";
        EXPECT_NE(nullptr, options.dcg_loader) << "Expected dcg_loader to be set";
        EXPECT_GE(options.segment_id, 0) << "Expected valid segment_id";
    }
}

// Test _get_segments with multiple rowsets
TEST_F(OlapMetaReaderTest, test_get_segments_with_multiple_rowsets) {
    // Create a primary key tablet
    int64_t tablet_id = 10002;
    int32_t schema_hash = 1002;
    _tablet = create_tablet(tablet_id, schema_hash);
    ASSERT_NE(nullptr, _tablet);

    // Create and commit 3 rowsets
    for (int i = 0; i < 3; i++) {
        std::vector<int64_t> keys;
        for (int j = 0; j < 5; j++) {
            keys.push_back(i * 10 + j);
        }
        auto rs = create_rowset(_tablet, keys);
        ASSERT_OK(_tablet->rowset_commit(i + 2, rs));
    }
    ASSERT_EQ(4, _tablet->updates()->max_version());

    // Test TEST_get_segments
    OlapMetaReader reader;
    std::vector<SegmentSharedPtr> segments;
    std::vector<SegmentMetaCollectOptions> options_list;
    Version version(0, 4);

    ASSERT_OK(reader.TEST_get_segments(_tablet, version, &segments, &options_list));

    // Verify we got 3 segments (3 rowsets * 1 segment each)
    ASSERT_EQ(3, segments.size());
    ASSERT_EQ(3, options_list.size());

    // Verify all options have correct settings
    for (const auto& options : options_list) {
        EXPECT_TRUE(options.is_primary_keys) << "Expected is_primary_keys to be true for PK table";
        EXPECT_EQ(tablet_id, options.tablet_id);
        EXPECT_NE(nullptr, options.dcg_loader);
    }
}

// Test _get_segments with multiple segments per rowset
TEST_F(OlapMetaReaderTest, test_get_segments_with_multiple_segments) {
    // Create a primary key tablet
    int64_t tablet_id = 10003;
    int32_t schema_hash = 1003;
    _tablet = create_tablet(tablet_id, schema_hash);
    ASSERT_NE(nullptr, _tablet);

    // Create and commit a rowset with 3 segments
    std::vector<vector<int64_t>> keys_by_segment = {{1, 2, 3}, {4, 5, 6}, {7, 8, 9}};
    auto rs = create_rowset_with_multiple_segments(_tablet, keys_by_segment);
    ASSERT_OK(_tablet->rowset_commit(2, rs));
    ASSERT_EQ(2, _tablet->updates()->max_version());

    // Test TEST_get_segments
    OlapMetaReader reader;
    std::vector<SegmentSharedPtr> segments;
    std::vector<SegmentMetaCollectOptions> options_list;
    Version version(0, 2);

    ASSERT_OK(reader.TEST_get_segments(_tablet, version, &segments, &options_list));

    // Verify we got 3 segments
    ASSERT_EQ(3, segments.size());
    ASSERT_EQ(3, options_list.size());

    // Verify segment_ids are correctly set (0, 1, 2)
    for (int i = 0; i < 3; i++) {
        EXPECT_EQ(i, options_list[i].segment_id) << "Expected segment_id to be " << i;
    }
}

// Test _get_segments returns correct rowset ids
TEST_F(OlapMetaReaderTest, test_get_segments_rowset_ids) {
    // Create a primary key tablet
    int64_t tablet_id = 10004;
    int32_t schema_hash = 1004;
    _tablet = create_tablet(tablet_id, schema_hash);
    ASSERT_NE(nullptr, _tablet);

    // Create and commit 2 rowsets
    std::vector<int64_t> keys1 = {1, 2, 3};
    auto rs1 = create_rowset(_tablet, keys1);
    ASSERT_OK(_tablet->rowset_commit(2, rs1));

    std::vector<int64_t> keys2 = {4, 5, 6};
    auto rs2 = create_rowset(_tablet, keys2);
    ASSERT_OK(_tablet->rowset_commit(3, rs2));

    // Test TEST_get_segments
    OlapMetaReader reader;
    std::vector<SegmentSharedPtr> segments;
    std::vector<SegmentMetaCollectOptions> options_list;
    Version version(0, 3);

    ASSERT_OK(reader.TEST_get_segments(_tablet, version, &segments, &options_list));

    // Verify we got 2 segments
    ASSERT_EQ(2, segments.size());
    ASSERT_EQ(2, options_list.size());

    // Verify rowset ids are different for different rowsets
    EXPECT_NE(options_list[0].pk_rowsetid, options_list[1].pk_rowsetid)
            << "Expected different pk_rowsetids for different rowsets";
}

// Test dcg_loader is set for primary key tables
TEST_F(OlapMetaReaderTest, test_dcg_loader_set) {
    // Create a primary key tablet
    int64_t tablet_id = 10005;
    int32_t schema_hash = 1005;
    _tablet = create_tablet(tablet_id, schema_hash);
    ASSERT_NE(nullptr, _tablet);

    // Create and commit a rowset
    std::vector<int64_t> keys = {1, 2, 3, 4, 5};
    auto rs = create_rowset(_tablet, keys);
    ASSERT_OK(_tablet->rowset_commit(2, rs));

    // Test TEST_get_segments
    OlapMetaReader reader;
    std::vector<SegmentSharedPtr> segments;
    std::vector<SegmentMetaCollectOptions> options_list;
    Version version(0, 2);

    ASSERT_OK(reader.TEST_get_segments(_tablet, version, &segments, &options_list));

    // Verify we got segments
    ASSERT_GT(segments.size(), 0);
    ASSERT_EQ(segments.size(), options_list.size());

    // Verify dcg_loader is set and is LocalDeltaColumnGroupLoader
    for (const auto& options : options_list) {
        EXPECT_NE(nullptr, options.dcg_loader) << "Expected dcg_loader to be set";
        // The loader should be a LocalDeltaColumnGroupLoader
    }
}

} // namespace starrocks
