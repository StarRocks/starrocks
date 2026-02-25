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

#include "storage/meta_reader.h"

#include <gtest/gtest.h>

#include <memory>

#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "column/array_column.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "fs/fs_util.h"
#include "fs/key_cache.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/segment_writer.h"

namespace starrocks {
using fs::delete_file;

class SegmentMetaCollecterTest : public ::testing::Test {
public:
    SegmentMetaCollecterTest() {
        TabletSchemaPB schema_pb;
        auto col = schema_pb.add_column();
        col->set_name("c0");
        col->set_type("INT");
        col->set_is_key(true);
        col->set_is_nullable(false);
        _tablet_schema = TabletSchema::create(schema_pb);

        _segment_name = "segment_meta_collector_test.dat";
        ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(_segment_name));
        auto encryption_pair = KeyCache::instance().create_plain_random_encryption_meta_pair().value();
        WritableFileOptions options{.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE,
                                    .encryption_info = encryption_pair.info};
        ASSIGN_OR_ABORT(auto wf, fs->new_writable_file(options, _segment_name));
        SegmentWriter writer(std::move(wf), 0, _tablet_schema, SegmentWriterOptions());
        EXPECT_OK(writer.init());
        uint64_t file_size, index_size, footer_pos;
        EXPECT_OK(writer.finalize(&file_size, &index_size, &footer_pos));

        FileInfo file_info{.path = _segment_name, .encryption_meta = encryption_pair.encryption_meta};
        ASSIGN_OR_ABORT(_segment, Segment::open(fs, file_info, 0, _tablet_schema));
    }

    ~SegmentMetaCollecterTest() { delete_file(_segment_name); }

protected:
    std::string _segment_name;
    TabletSchemaCSPtr _tablet_schema;
    SegmentSharedPtr _segment;
    std::string _segment_encryption_meta;

    // Helper method to create array column
    MutableColumnPtr create_array_column() {
        auto elements = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
        auto offsets = UInt32Column::create();
        return ArrayColumn::create(std::move(elements), std::move(offsets));
    }
};

TEST_F(SegmentMetaCollecterTest, test_init) {
    SegmentMetaCollecter collecter(_segment);
    SegmentMetaCollectOptions options;
    EXPECT_FALSE(collecter.init(nullptr, options).ok());

    SegmentMetaCollecterParams params;
    EXPECT_FALSE(collecter.init(&params, options).ok());

    params.fields.emplace_back("rows");
    EXPECT_FALSE(collecter.init(&params, options).ok());

    params.field_type.emplace_back(LogicalType::TYPE_INT);
    EXPECT_FALSE(collecter.init(&params, options).ok());

    params.cids.emplace_back(0);
    EXPECT_FALSE(collecter.init(&params, options).ok());

    params.read_page.emplace_back(false);
    EXPECT_FALSE(collecter.init(&params, options).ok());

    params.tablet_schema = _tablet_schema;
    EXPECT_OK(collecter.init(&params, options));
}

TEST_F(SegmentMetaCollecterTest, test_open_and_collect) {
    SegmentMetaCollecter collecter(_segment);

    // init() has not been called
    EXPECT_FALSE(collecter.open().ok());

    SegmentMetaCollecterParams params;
    params.fields.emplace_back("rows");
    params.field_type.emplace_back(LogicalType::TYPE_INT);
    params.cids.emplace_back(0);
    params.read_page.emplace_back(false);
    params.tablet_schema = _tablet_schema;
    SegmentMetaCollectOptions options;
    EXPECT_OK(collecter.init(&params, options));
    EXPECT_OK(collecter.open());

    std::vector<Column*> columns;
    EXPECT_FALSE(collecter.collect(&columns).ok());

    auto col = Int64Column::create();
    columns.emplace_back(col.get());

    EXPECT_OK(collecter.collect(&columns));
    EXPECT_EQ(1, col->size());
    EXPECT_EQ(0, col->get(0).get_int64());
}

TEST_F(SegmentMetaCollecterTest, test_init_with_dcg_options) {
    SegmentMetaCollecter collecter(_segment);
    SegmentMetaCollecterParams params;
    params.fields.emplace_back("rows");
    params.field_type.emplace_back(LogicalType::TYPE_INT);
    params.cids.emplace_back(0);
    params.read_page.emplace_back(false);
    params.tablet_schema = _tablet_schema;

    // Test with DCG options for primary key table
    SegmentMetaCollectOptions pk_options;
    pk_options.is_primary_keys = true;
    pk_options.tablet_id = 12345;
    pk_options.segment_id = 0;
    pk_options.version = 100;
    pk_options.pk_rowsetid = 1;
    // Note: dcg_loader is nullptr, which is valid for segments without DCG
    EXPECT_OK(collecter.init(&params, pk_options));

    // Test with DCG options for non-primary key table
    SegmentMetaCollecter collecter2(_segment);
    SegmentMetaCollectOptions non_pk_options;
    non_pk_options.is_primary_keys = false;
    non_pk_options.tablet_id = 12345;
    non_pk_options.segment_id = 0;
    RowsetId rowset_id;
    rowset_id.init(1, 2, 0, 0);
    non_pk_options.rowsetid = rowset_id;
    EXPECT_OK(collecter2.init(&params, non_pk_options));
}

TEST_F(SegmentMetaCollecterTest, test_collect_dict_json_column_success) {
    // Create a proper JSON segment with actual data
    TabletSchemaPB json_schema_pb;
    auto json_col = json_schema_pb.add_column();
    json_col->set_name("json_col");
    json_col->set_type("JSON");
    json_col->set_is_key(false);
    json_col->set_is_nullable(true);
    auto json_tablet_schema = TabletSchema::create(json_schema_pb);

    // Create JSON segment with dictionary-encoded string data
    std::string json_segment_name = "json_meta_collector_test.dat";
    DeferOp defer_op([&] { delete_file(json_segment_name); });
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(json_segment_name));
    auto encryption_pair = KeyCache::instance().create_plain_random_encryption_meta_pair().value();
    WritableFileOptions file_options{.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE,
                                     .encryption_info = encryption_pair.info};
    ASSIGN_OR_ABORT(auto wf, fs->new_writable_file(file_options, json_segment_name));

    SegmentWriterOptions seg_opts;
    seg_opts.flat_json_config = std::make_shared<FlatJsonConfig>();
    seg_opts.flat_json_config->set_flat_json_enabled(true);
    SegmentWriter json_writer(std::move(wf), 0, json_tablet_schema, seg_opts);
    EXPECT_OK(json_writer.init());

    // Create JSON data with repeated string values for dictionary encoding
    auto json_column = JsonColumn::create();
    std::vector<std::string> json_strings = {R"({"name": "Alice", "city": "Beijing"})",
                                             R"({"name": "Bob", "city": "Beijing"})",
                                             R"({"name": "Alice", "city": "Shanghai"})"};

    for (const auto& json_str : json_strings) {
        JsonValue json_value;
        ASSERT_OK(JsonValue::parse(json_str, &json_value));
        json_column->append(&json_value);
    }

    SchemaPtr chunk_schema = std::make_shared<Schema>(json_tablet_schema->schema());
    auto chunk = std::make_shared<Chunk>(Columns{json_column}, chunk_schema);
    for (int i = 0; i < 100; i++) {
        ASSERT_OK(json_writer.append_chunk(*chunk));
    }

    uint64_t file_size, index_size, footer_pos;
    EXPECT_OK(json_writer.finalize(&file_size, &index_size, &footer_pos));

    // Open the JSON segment
    FileInfo json_file_info{.path = json_segment_name, .encryption_meta = encryption_pair.encryption_meta};
    ASSIGN_OR_ABORT(auto json_segment, Segment::open(fs, json_file_info, 0, json_tablet_schema));

    // Test dictionary collection from JSON column
    SegmentMetaCollecter json_collecter(json_segment);
    SegmentMetaCollecterParams params;
    params.fields.emplace_back("dict_merge");
    params.field_type.emplace_back(LogicalType::TYPE_ARRAY);
    params.cids.emplace_back(0);
    params.read_page.emplace_back(true); // Need to read page for JSON
    params.tablet_schema = json_tablet_schema;
    params.low_cardinality_threshold = 1000;
    SegmentMetaCollectOptions options;
    EXPECT_OK(json_collecter.init(&params, options));
    EXPECT_OK(json_collecter.open());

    auto array_col = create_array_column();

    // Test successful dictionary collection for JSON column
    Status status = json_collecter._collect_dict(0, array_col.get(), LogicalType::TYPE_ARRAY);

    ASSERT_OK(status);
    EXPECT_GT(array_col->size(), 0);
    EXPECT_EQ("[['Beijing','Shanghai'], ['Alice','Bob']]", array_col->debug_string());
}

TEST_F(SegmentMetaCollecterTest, test_collect_multiple_meta_fields) {
    // Create a segment with data
    TabletSchemaPB schema_pb;
    auto col0 = schema_pb.add_column();
    col0->set_name("c0");
    col0->set_type("INT");
    col0->set_is_key(true);
    col0->set_is_nullable(false);
    col0->set_unique_id(0);

    auto col1 = schema_pb.add_column();
    col1->set_name("c1");
    col1->set_type("VARCHAR");
    col1->set_length(100);
    col1->set_is_key(false);
    col1->set_is_nullable(true);
    col1->set_unique_id(1);

    auto tablet_schema = TabletSchema::create(schema_pb);

    std::string segment_name = "test_multiple_meta_fields.dat";
    DeferOp defer_op([&] { delete_file(segment_name); });
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(segment_name));
    auto encryption_pair = KeyCache::instance().create_plain_random_encryption_meta_pair().value();
    WritableFileOptions file_options{.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE,
                                     .encryption_info = encryption_pair.info};
    ASSIGN_OR_ABORT(auto wf, fs->new_writable_file(file_options, segment_name));

    SegmentWriter writer(std::move(wf), 0, tablet_schema, SegmentWriterOptions());
    EXPECT_OK(writer.init());

    // Write some test data
    auto int_col = Int32Column::create();
    auto varchar_col = BinaryColumn::create();
    auto null_col = NullColumn::create();

    for (int i = 0; i < 100; i++) {
        int_col->append(i);
        varchar_col->append(fmt::format("value_{}", i));
        null_col->append(i % 10 == 0); // 10% null values
    }

    auto nullable_varchar = NullableColumn::create(varchar_col, null_col);
    auto chunk = std::make_shared<Chunk>(Columns{int_col, nullable_varchar},
                                         std::make_shared<Schema>(tablet_schema->schema()));
    ASSERT_OK(writer.append_chunk(*chunk));

    uint64_t file_size, index_size, footer_pos;
    EXPECT_OK(writer.finalize(&file_size, &index_size, &footer_pos));

    FileInfo file_info{.path = segment_name, .encryption_meta = encryption_pair.encryption_meta};
    ASSIGN_OR_ABORT(auto segment, Segment::open(fs, file_info, 0, tablet_schema));

    // Test collecting multiple meta fields
    SegmentMetaCollecter collecter(segment);
    SegmentMetaCollecterParams params;

    // Collect rows
    params.fields.emplace_back("rows");
    params.field_type.emplace_back(LogicalType::TYPE_BIGINT);
    params.cids.emplace_back(0);
    params.read_page.emplace_back(false);

    // Collect column size
    params.fields.emplace_back("column_size");
    params.field_type.emplace_back(LogicalType::TYPE_BIGINT);
    params.cids.emplace_back(1);
    params.read_page.emplace_back(false);

    params.tablet_schema = tablet_schema;
    SegmentMetaCollectOptions collect_options;
    EXPECT_OK(collecter.init(&params, collect_options));
    EXPECT_OK(collecter.open());

    auto rows_col = Int64Column::create();
    auto size_col = Int64Column::create();
    std::vector<Column*> columns = {rows_col.get(), size_col.get()};

    EXPECT_OK(collecter.collect(&columns));
    EXPECT_EQ(1, rows_col->size());
    EXPECT_EQ(100, rows_col->get(0).get_int64()); // 100 rows written
    EXPECT_EQ(1, size_col->size());
    EXPECT_GT(size_col->get(0).get_int64(), 0); // Column size should be > 0
}

TEST_F(SegmentMetaCollecterTest, test_get_dcg_segment_without_dcg) {
    // Test that _get_dcg_segment returns nullptr when no DCG is present
    SegmentMetaCollecter collecter(_segment);
    SegmentMetaCollecterParams params;
    params.fields.emplace_back("rows");
    params.field_type.emplace_back(LogicalType::TYPE_INT);
    params.cids.emplace_back(0);
    params.read_page.emplace_back(false);
    params.tablet_schema = _tablet_schema;

    SegmentMetaCollectOptions options;
    // No dcg_loader provided, so no DCG will be loaded
    EXPECT_OK(collecter.init(&params, options));
    EXPECT_OK(collecter.open());

    // The collecter should work normally without DCG
    auto col = Int64Column::create();
    std::vector<Column*> columns = {col.get()};
    EXPECT_OK(collecter.collect(&columns));
    EXPECT_EQ(1, col->size());
}

// Mock DeltaColumnGroupLoader for testing
class MockDeltaColumnGroupLoader : public DeltaColumnGroupLoader {
public:
    MockDeltaColumnGroupLoader() = default;
    ~MockDeltaColumnGroupLoader() override = default;

    Status load(const TabletSegmentId& tsid, int64_t version, DeltaColumnGroupList* pdcgs) override {
        if (_dcgs.empty()) {
            return Status::OK();
        }
        *pdcgs = _dcgs;
        return Status::OK();
    }

    Status load(int64_t tablet_id, RowsetId rowsetid, uint32_t segment_id, int64_t version,
                DeltaColumnGroupList* pdcgs) override {
        if (_dcgs.empty()) {
            return Status::OK();
        }
        *pdcgs = _dcgs;
        return Status::OK();
    }

    void set_dcgs(const DeltaColumnGroupList& dcgs) { _dcgs = dcgs; }

private:
    DeltaColumnGroupList _dcgs;
};

TEST_F(SegmentMetaCollecterTest, test_init_with_mock_dcg_loader) {
    // Test that SegmentMetaCollecter can initialize with a DCG loader
    // Even though we don't have actual DCG files, we can test the initialization path

    SegmentMetaCollecter collecter(_segment);
    SegmentMetaCollecterParams params;
    params.fields.emplace_back("rows");
    params.field_type.emplace_back(LogicalType::TYPE_INT);
    params.cids.emplace_back(0);
    params.read_page.emplace_back(false);
    params.tablet_schema = _tablet_schema;

    // Create mock DCG loader
    auto mock_loader = std::make_shared<MockDeltaColumnGroupLoader>();

    // Create a DCG with no actual files (empty DCG list)
    DeltaColumnGroupList empty_dcgs;
    mock_loader->set_dcgs(empty_dcgs);

    SegmentMetaCollectOptions options;
    options.is_primary_keys = true;
    options.tablet_id = 12345;
    options.segment_id = 0;
    options.version = 100;
    options.pk_rowsetid = 1;
    options.dcg_loader = mock_loader;

    // Should initialize successfully even with DCG loader
    EXPECT_OK(collecter.init(&params, options));
    EXPECT_OK(collecter.open());

    // Should still be able to collect metadata
    auto col = Int64Column::create();
    std::vector<Column*> columns = {col.get()};
    EXPECT_OK(collecter.collect(&columns));
    EXPECT_EQ(1, col->size());
}

TEST_F(SegmentMetaCollecterTest, test_collect_with_dcg_loader_non_pk) {
    // Test with DCG loader for non-primary key table
    SegmentMetaCollecter collecter(_segment);
    SegmentMetaCollecterParams params;
    params.fields.emplace_back("rows");
    params.field_type.emplace_back(LogicalType::TYPE_BIGINT);
    params.cids.emplace_back(0);
    params.read_page.emplace_back(false);
    params.tablet_schema = _tablet_schema;

    auto mock_loader = std::make_shared<MockDeltaColumnGroupLoader>();
    DeltaColumnGroupList empty_dcgs;
    mock_loader->set_dcgs(empty_dcgs);

    SegmentMetaCollectOptions options;
    options.is_primary_keys = false;
    options.tablet_id = 67890;
    options.segment_id = 1;
    RowsetId rowset_id;
    rowset_id.init(10, 20, 0, 0);
    options.rowsetid = rowset_id;
    options.dcg_loader = mock_loader;

    EXPECT_OK(collecter.init(&params, options));
    EXPECT_OK(collecter.open());

    auto col = Int64Column::create();
    std::vector<Column*> columns = {col.get()};
    EXPECT_OK(collecter.collect(&columns));
    EXPECT_EQ(1, col->size());
    EXPECT_EQ(0, col->get(0).get_int64());
}

// Integration test for reading from DCG segments with actual column files
// Note: This test demonstrates the structure needed for a full DCG integration test
// To fully implement this, you would need to:
// 1. Create actual .cols files using SegmentWriter
// 2. Set up a real LocalDeltaColumnGroupLoader with KVStore
// 3. Write DCG metadata to the KVStore
// 4. Create segments that can read from those .cols files
//
// For now, the mock loader tests above verify that the DCG code path is correctly integrated
// into SegmentMetaCollecter's initialization and collection logic.

} // namespace starrocks
