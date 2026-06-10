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

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/schema.h"
#include "fs/fs.h"
#include "gen_cpp/segment.pb.h"
#include "storage/chunk_helper.h"
#include "storage/index/compound_index_common.h"
#include "storage/index/compound_index_file_reader.h"
#include "storage/index/index_descriptor.h"
#include "storage/rowset/segment_writer.h"
#include "storage/tablet_schema.h"
#include "testutil/assert.h"
#include "util/json_util.h"

namespace starrocks {

namespace {

std::string make_tempdir(const std::string& prefix) {
    const char* base = std::getenv("TEST_TMPDIR");
    if (base == nullptr || *base == '\0') base = "/tmp";
    std::string tmpl = std::string(base) + "/" + prefix + "_XXXXXX";
    std::vector<char> buf(tmpl.begin(), tmpl.end());
    buf.push_back('\0');
    char* dir = ::mkdtemp(buf.data());
    return dir ? std::string(dir) : std::string{};
}

struct PathCleanup {
    std::string p;
    ~PathCleanup() {
        std::error_code ec;
        std::filesystem::remove_all(p, ec);
    }
};

// Build a TabletSchema with one INT key column and one or more VARCHAR value
// columns, each with a GIN tantivy index.
TabletSchemaCSPtr make_tantivy_schema(int num_text_cols) {
    TabletSchemaPB pb;
    pb.set_keys_type(KeysType::DUP_KEYS);
    pb.set_num_short_key_columns(1);

    // Key column: k0 INT
    {
        auto* col = pb.add_column();
        col->set_unique_id(0);
        col->set_name("k0");
        col->set_type("INT");
        col->set_is_key(true);
        col->set_is_nullable(false);
        col->set_aggregation("NONE");
    }

    for (int i = 0; i < num_text_cols; ++i) {
        int uid = i + 1;
        auto* col = pb.add_column();
        col->set_unique_id(uid);
        col->set_name("v" + std::to_string(i));
        col->set_type("VARCHAR");
        col->set_length(65535);
        col->set_is_key(false);
        col->set_is_nullable(true);
        col->set_aggregation("NONE");

        // GIN index with tantivy implementation.
        auto* idx = pb.add_table_indices();
        idx->set_index_id(100 + i);
        idx->set_index_name("gin_v" + std::to_string(i));
        idx->set_index_type(GIN);
        idx->add_col_unique_id(uid);

        std::map<std::string, std::map<std::string, std::string>> props;
        props["common_properties"]["imp_lib"] = "tantivy";
        props["index_properties"] = {};
        props["search_properties"] = {};
        props["extra_properties"] = {};
        idx->set_index_properties(to_json(props));
    }

    return std::make_shared<TabletSchema>(pb);
}

} // namespace

// Write a segment with tantivy GIN indexes and verify a .idx compound file is produced.
TEST(SegmentWriterTantivyTest, SingleTantivyColumn) {
    std::string temp_dir = make_tempdir("seg_tantivy_test");
    ASSERT_FALSE(temp_dir.empty());
    PathCleanup cleanup{temp_dir};

    auto tablet_schema = make_tantivy_schema(1);

    // Prepare SegmentWriterOptions.
    SegmentWriterOptions opts;
    opts.num_rows_per_block = 10;
    opts.segment_file_mark.rowset_path_prefix = temp_dir;
    opts.segment_file_mark.rowset_id = "test_rowset";

    std::string seg_path = temp_dir + "/test_rowset_0.dat";
    auto fs = FileSystem::Default();
    ASSIGN_OR_ABORT(auto wfile, fs->new_writable_file(seg_path));

    // Verify schema has GIN index on v0 (unique_id=1).
    ASSERT_TRUE(tablet_schema->has_index(1, GIN)) << "TabletSchema missing GIN index for col uid=1";

    SegmentWriter writer(std::move(wfile), /*segment_id=*/0, tablet_schema, opts);
    ASSERT_OK(writer.init());

    // Build a chunk: k0=INT, v0=VARCHAR(nullable).
    auto schema = ChunkHelper::convert_schema(tablet_schema);
    auto chunk = ChunkHelper::new_chunk(schema, 5);

    // k0: [0, 1, 2, 3, 4]
    auto& k0 = chunk->get_column_by_index(0);
    for (int i = 0; i < 5; ++i) {
        k0->append_datum(Datum(static_cast<int32_t>(i)));
    }

    // v0: ["hello world", "starrocks search", NULL, "tantivy index", "quick brown fox"]
    auto& v0 = chunk->get_column_by_index(1);
    v0->append_datum(Datum(Slice("hello world")));
    v0->append_datum(Datum(Slice("starrocks search")));
    v0->append_datum(Datum());
    v0->append_datum(Datum(Slice("tantivy index")));
    v0->append_datum(Datum(Slice("quick brown fox")));

    ASSERT_OK(writer.append_chunk(*chunk));

    uint64_t file_size = 0, index_size = 0, footer_position = 0;
    ASSERT_OK(writer.finalize(&file_size, &index_size, &footer_position));

    // Verify .idx compound file was created.
    std::string bin_path = IndexDescriptor::compound_index_file_path(temp_dir, "test_rowset", 0);
    ASSERT_TRUE(std::filesystem::exists(bin_path)) << "expected compound .idx at: " << bin_path;

    // Verify the .idx can be parsed by CompoundIndexFileReader.
    ASSIGN_OR_ABORT(auto reader, CompoundIndexFileReader::open(bin_path));

    auto layout_or = reader->find_index(CompoundIndexKind::INVERTED_TANTIVY, 100);
    ASSERT_TRUE(layout_or.ok()) << layout_or.status().message();
    auto& layout = layout_or.value();
    EXPECT_EQ(CompoundIndexKind::INVERTED_TANTIVY, layout.kind);
    EXPECT_EQ(100, layout.index_id);
    EXPECT_FALSE(layout.files.empty());

    // Verify tantivy temp dir was cleaned up (the .ivt directory should be gone).
    std::string ivt_path = IndexDescriptor::inverted_index_file_path(temp_dir, "test_rowset", 0, 100);
    EXPECT_FALSE(std::filesystem::exists(ivt_path)) << "tantivy temp dir should have been cleaned up: " << ivt_path;
}

// Write a segment with 2 tantivy GIN columns and verify both entries appear in the .idx.
TEST(SegmentWriterTantivyTest, TwoTantivyColumns) {
    std::string temp_dir = make_tempdir("seg_tantivy_2col");
    ASSERT_FALSE(temp_dir.empty());
    PathCleanup cleanup{temp_dir};

    auto tablet_schema = make_tantivy_schema(2);

    SegmentWriterOptions opts;
    opts.num_rows_per_block = 10;
    opts.segment_file_mark.rowset_path_prefix = temp_dir;
    opts.segment_file_mark.rowset_id = "rs2col";

    std::string seg_path = temp_dir + "/rs2col_0.dat";
    auto fs = FileSystem::Default();
    ASSIGN_OR_ABORT(auto wfile, fs->new_writable_file(seg_path));

    SegmentWriter writer(std::move(wfile), /*segment_id=*/0, tablet_schema, opts);
    ASSERT_OK(writer.init());

    auto schema = ChunkHelper::convert_schema(tablet_schema);
    auto chunk = ChunkHelper::new_chunk(schema, 3);

    auto& k0 = chunk->get_column_by_index(0);
    for (int i = 0; i < 3; ++i) {
        k0->append_datum(Datum(static_cast<int32_t>(i)));
    }

    auto& v0 = chunk->get_column_by_index(1);
    v0->append_datum(Datum(Slice("alpha beta")));
    v0->append_datum(Datum(Slice("gamma delta")));
    v0->append_datum(Datum(Slice("epsilon")));

    auto& v1 = chunk->get_column_by_index(2);
    v1->append_datum(Datum(Slice("one two three")));
    v1->append_datum(Datum());
    v1->append_datum(Datum(Slice("four five")));

    ASSERT_OK(writer.append_chunk(*chunk));

    uint64_t file_size = 0, index_size = 0, footer_position = 0;
    ASSERT_OK(writer.finalize(&file_size, &index_size, &footer_position));

    std::string bin_path = IndexDescriptor::compound_index_file_path(temp_dir, "rs2col", 0);
    ASSERT_TRUE(std::filesystem::exists(bin_path));

    ASSIGN_OR_ABORT(auto reader, CompoundIndexFileReader::open(bin_path));

    // index_id=100 for v0, index_id=101 for v1.
    auto l0 = reader->find_index(CompoundIndexKind::INVERTED_TANTIVY, 100);
    ASSERT_TRUE(l0.ok()) << l0.status().message();
    EXPECT_FALSE(l0.value().files.empty());

    auto l1 = reader->find_index(CompoundIndexKind::INVERTED_TANTIVY, 101);
    ASSERT_TRUE(l1.ok()) << l1.status().message();
    EXPECT_FALSE(l1.value().files.empty());
}

// Verify that the segment .dat file is still valid (footer can be parsed).
TEST(SegmentWriterTantivyTest, SegmentFooterValid) {
    std::string temp_dir = make_tempdir("seg_tantivy_footer");
    ASSERT_FALSE(temp_dir.empty());
    PathCleanup cleanup{temp_dir};

    auto tablet_schema = make_tantivy_schema(1);

    SegmentWriterOptions opts;
    opts.num_rows_per_block = 10;
    opts.segment_file_mark.rowset_path_prefix = temp_dir;
    opts.segment_file_mark.rowset_id = "rsfooter";

    std::string seg_path = temp_dir + "/rsfooter_0.dat";
    auto fs = FileSystem::Default();
    ASSIGN_OR_ABORT(auto wfile, fs->new_writable_file(seg_path));

    SegmentWriter writer(std::move(wfile), /*segment_id=*/0, tablet_schema, opts);
    ASSERT_OK(writer.init());

    auto schema = ChunkHelper::convert_schema(tablet_schema);
    auto chunk = ChunkHelper::new_chunk(schema, 2);

    auto& k0 = chunk->get_column_by_index(0);
    k0->append_datum(Datum(static_cast<int32_t>(0)));
    k0->append_datum(Datum(static_cast<int32_t>(1)));

    auto& v0 = chunk->get_column_by_index(1);
    v0->append_datum(Datum(Slice("segment footer test")));
    v0->append_datum(Datum(Slice("verification")));

    ASSERT_OK(writer.append_chunk(*chunk));

    uint64_t file_size = 0, index_size = 0, footer_position = 0;
    ASSERT_OK(writer.finalize(&file_size, &index_size, &footer_position));

    EXPECT_GT(file_size, 0u);
    EXPECT_GT(footer_position, 0u);
    EXPECT_EQ(writer.num_rows(), 2u);
}

} // namespace starrocks
