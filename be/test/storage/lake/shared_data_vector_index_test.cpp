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

#include "base/testutil/assert.h"
#include "column/array_column.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "common/config_vector_index_fwd.h"
#include "fs/fs_factory.h"
#include "fs/fs_util.h"
#include "storage/chunk_helper.h"
#include "storage/index/index_descriptor.h"
#include "storage/index/vector/vector_index_reader.h"
#include "storage/index/vector/vector_index_reader_factory.h"
#include "storage/index/vector/vector_index_writer.h"
#include "storage/lake/filenames.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/general_tablet_writer.h"
#include "storage/lake/join_path.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/test_util.h"
#include "storage/rowset/segment_file_info.h"
#include "storage/rowset/segment_writer.h"
#include "storage/tablet_schema.h"

#ifdef WITH_TENANN
#include "storage/index/vector/tenann/tenann_index_utils.h"
#endif

namespace starrocks::lake {

// Unit tests for vector index support on shared-data write path.
// Shared-data mode uses LocationProvider + FileSystem to write segments and vector index files
// to remote storage (S3/HDFS). These tests verify VectorIndexWriter and path generation work
// correctly with the path layout used by LocationProvider::segment_location().
class SharedDataVectorIndexTest : public ::testing::Test {
public:
    SharedDataVectorIndexTest() = default;

protected:
    void SetUp() override {
        _test_dir = "shared_data_vector_index_test_" + std::to_string(GetCurrentTimeMicros());
        CHECK_OK(fs::remove_all(_test_dir));
        CHECK_OK(fs::create_directories(lake::join_path(_test_dir, lake::kSegmentDirectoryName)));
        ASSIGN_OR_ABORT(_fs, FileSystemFactory::CreateSharedFromString(_test_dir));
        _location_provider = std::make_shared<FixedLocationProvider>(_test_dir);
    }

    void TearDown() override { (void)fs::remove_all(_test_dir); }

    std::shared_ptr<TabletIndex> create_tablet_index(int64_t index_id = 0, int32_t col_unique_id = 1) {
        std::shared_ptr<TabletIndex> tablet_index = std::make_shared<TabletIndex>();
        TabletIndexPB index_pb;
        index_pb.set_index_id(index_id);
        index_pb.set_index_name("vector_index");
        index_pb.set_index_type(IndexType::VECTOR);
        index_pb.add_col_unique_id(col_unique_id);
        tablet_index->init_from_pb(index_pb);
        tablet_index->add_common_properties("index_type", "hnsw");
        tablet_index->add_common_properties("dim", "3");
        tablet_index->add_common_properties("is_vector_normed", "false");
        tablet_index->add_common_properties("metric_type", "l2_distance");
        tablet_index->add_index_properties("efconstruction", "40");
        tablet_index->add_index_properties("m", "16");
        return tablet_index;
    }

    ColumnPtr create_array_column_with_vectors(size_t num_rows) {
        auto element = FixedLengthColumn<float>::create();
        auto null_column = NullColumn::create();
        auto offsets = UInt32Column::create();
        offsets->append(0);

        for (size_t i = 0; i < num_rows; ++i) {
            element->append(static_cast<float>(i) + 1.1f);
            element->append(static_cast<float>(i) + 2.2f);
            element->append(static_cast<float>(i) + 3.3f);
            null_column->append(0);
            null_column->append(0);
            null_column->append(0);
            offsets->append((i + 1) * 3);
        }

        auto nullable_column = NullableColumn::create(std::move(element), std::move(null_column));
        return ArrayColumn::create(std::move(nullable_column), std::move(offsets));
    }

    std::string _test_dir;
    std::shared_ptr<FileSystem> _fs;
    std::shared_ptr<LocationProvider> _location_provider;
};

// Test VectorIndexWriter writing to a path that follows shared-data layout.
// The path is generated using LocationProvider::segment_location(tablet_id, vi_filename),
// which is the same path format used by fill_vector_index_file_paths in general_tablet_writer.
TEST_F(SharedDataVectorIndexTest, test_vector_index_write_shared_data_path) {
    // Set threshold low enough so that 5 rows triggers index build
    ConfigResetGuard<int32_t> threshold_guard(&config::config_vector_index_default_build_threshold, 1);

    const int64_t tablet_id = 12345;
    const int64_t index_id = 0;
    const std::string segment_name = "0000000000000001_6bc1edf0-fba6-4aa1-b0d4-ee5b88ef156b.dat";

    std::string vi_filename = gen_vector_index_filename(segment_name, index_id);
    std::string vector_index_path = _location_provider->segment_location(tablet_id, vi_filename);

    auto tablet_index = create_tablet_index(index_id);
    std::unique_ptr<VectorIndexWriter> vector_index_writer;
    VectorIndexWriter::create(tablet_index, vector_index_path, true, &vector_index_writer);
    ASSERT_OK(vector_index_writer->init());

    auto array_column = create_array_column_with_vectors(5);
    ASSERT_OK(vector_index_writer->append(*array_column));
    ASSERT_EQ(vector_index_writer->size(), 5);

    uint64_t index_size = 0;
    ASSERT_OK(vector_index_writer->finish(&index_size));
    ASSERT_GT(index_size, 0);

    // Verify the vector index file exists at the shared-data path
    ASSERT_OK(_fs->path_exists(vector_index_path));

#ifndef WITH_TENANN
    // Without tenann, the EmptyVectorIndexBuilder stub flushes a mark_word file.
    // (With tenann, a real ANN index is produced; we only assert path existence
    // and non-zero size above.)
    ASSIGN_OR_ABORT(auto index_file, _fs->new_random_access_file(vector_index_path));
    ASSIGN_OR_ABORT(auto data, index_file->read_all());
    ASSERT_EQ(data, IndexDescriptor::mark_word);
#endif
}

// Test that gen_vector_index_filename produces correct filename for shared-data segment names.
TEST_F(SharedDataVectorIndexTest, test_vector_index_filename_for_shared_data_segment) {
    std::string segment_name = "0000000000000001_6bc1edf0-fba6-4aa1-b0d4-ee5b88ef156b.dat";
    std::string vi_name = gen_vector_index_filename(segment_name, 0);
    ASSERT_EQ(vi_name, "0000000000000001_6bc1edf0-fba6-4aa1-b0d4-ee5b88ef156b_0.vi");

    vi_name = gen_vector_index_filename(segment_name, 123);
    ASSERT_EQ(vi_name, "0000000000000001_6bc1edf0-fba6-4aa1-b0d4-ee5b88ef156b_123.vi");

    // Verify full path from LocationProvider matches expected layout
    const int64_t tablet_id = 1;
    std::string full_path = _location_provider->segment_location(tablet_id, vi_name);
    std::string expected = lake::join_path(lake::join_path(_test_dir, lake::kSegmentDirectoryName), vi_name);
    ASSERT_EQ(full_path, expected);
}

// VectorIndexWriter::finish() short-circuits when the IVFPQ build threshold is not
// met: no .vi file is produced and index_size stays 0. Reader-side, the missing
// file surfaces as NotFound; vacuum sees no vector_index_id recorded in segment_meta
// (because has_vector_index_written() stays false), so there is nothing to clean up.
TEST_F(SharedDataVectorIndexTest, test_vector_index_empty_mark_shared_data_path) {
    ConfigResetGuard<int32_t> threshold_guard(&config::config_vector_index_default_build_threshold, 100);

    const int64_t tablet_id = 999;
    const int64_t index_id = 0;
    std::string segment_name = "0000000000000002_abc12345-6789-0def-1234-567890abcdef.dat";
    std::string vi_filename = gen_vector_index_filename(segment_name, index_id);
    std::string vector_index_path = _location_provider->segment_location(tablet_id, vi_filename);

    // Build an ivfpq tablet_index from scratch — using create_tablet_index() and then
    // setting "index_type" = "ivfpq" via add_common_properties does not work because
    // TabletIndex::add_common_properties uses std::map::insert (no overwrite), so the
    // "hnsw" inserted by create_tablet_index() would remain. ivfpq + threshold > rows
    // is what drives the writer down the threshold-not-met short-circuit we want to
    // exercise here.
    auto tablet_index = std::make_shared<TabletIndex>();
    TabletIndexPB index_pb;
    index_pb.set_index_id(index_id);
    index_pb.set_index_name("vector_index");
    index_pb.set_index_type(IndexType::VECTOR);
    index_pb.add_col_unique_id(1);
    tablet_index->init_from_pb(index_pb);
    tablet_index->add_common_properties("index_type", "ivfpq");
    tablet_index->add_common_properties("dim", "3");
    tablet_index->add_common_properties("is_vector_normed", "false");
    tablet_index->add_common_properties("metric_type", "l2_distance");
    // nlist/nbits/m_ivfpq are required by get_vector_meta's ivfpq validation
    // (the read PR tightened the schema). They're not exercised here because
    // finish() short-circuits before any meta consumer runs.
    tablet_index->add_index_properties("nlist", "1");
    tablet_index->add_index_properties("nbits", "8");
    tablet_index->add_index_properties("m_ivfpq", "3");

    std::unique_ptr<VectorIndexWriter> vector_index_writer;
    VectorIndexWriter::create(tablet_index, vector_index_path, true, &vector_index_writer);
    ASSERT_OK(vector_index_writer->init());

    auto array_column = create_array_column_with_vectors(3);
    ASSERT_OK(vector_index_writer->append(*array_column));

    uint64_t index_size = 0;
    ASSERT_OK(vector_index_writer->finish(&index_size));

    // Threshold not met -> finish() returns OK without writing any file.
    ASSERT_EQ(index_size, 0);
    ASSERT_TRUE(_fs->path_exists(vector_index_path).is_not_found());
}

// =============== Tablet-writer-level tests ================
//
// Drive HorizontalGeneralTabletWriter and VerticalGeneralTabletWriter end-to-end with a
// vector-indexed schema to exercise the shared-data write path:
//   * general_tablet_writer.cpp::fill_vector_index_file_paths (both location-provider
//     and tablet-manager fallbacks)
//   * SegmentFileInfo::vector_index_ids propagation for both writer flavors
//   * segment_writer.cpp VECTOR init branch that picks up pre-populated paths, and the
//     per-segment STANDALONE footer flag
//
// Uses TestBase for _tablet_mgr/_lp plumbing so segments land under a real test dir.
class SharedDataTabletWriterVITest : public TestBase {
public:
    SharedDataTabletWriterVITest()
            : TestBase("shared_data_tablet_writer_vi_test_" + std::to_string(GetCurrentTimeMicros())) {
        clear_and_init_test_dir();
    }

protected:
    static constexpr int64_t kTabletId = 20001;
    static constexpr int64_t kIndexId = 77;
    static constexpr int32_t kKeyColUniqueId = 1;
    static constexpr int32_t kVectorColUniqueId = 2;

    TabletSchemaPB create_vi_schema_pb() {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(DUP_KEYS);
        schema_pb.set_num_short_key_columns(1);

        auto* c0 = schema_pb.add_column();
        c0->set_unique_id(kKeyColUniqueId);
        c0->set_name("pk");
        c0->set_type("INT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);

        // Vector index requires a non-nullable outer array column
        // (DCHECK in ArrayColumnWriter::append).
        auto* c1 = schema_pb.add_column();
        c1->set_unique_id(kVectorColUniqueId);
        c1->set_name("vec");
        c1->set_type("ARRAY");
        c1->set_is_key(false);
        c1->set_is_nullable(false);
        auto* child = c1->add_children_columns();
        child->set_unique_id(3);
        child->set_name("element");
        child->set_type("FLOAT");
        child->set_is_nullable(true);

        auto* idx = schema_pb.add_table_indices();
        idx->set_index_id(kIndexId);
        idx->set_index_name("vec_idx");
        idx->set_index_type(IndexType::VECTOR);
        idx->add_col_unique_id(kVectorColUniqueId);
        std::string props_json = R"({
            "common_properties": {
                "index_type": "hnsw",
                "dim": "3",
                "is_vector_normed": "false",
                "metric_type": "l2_distance"
            },
            "index_properties": {
                "efconstruction": "40",
                "m": "16"
            }
        })";
        idx->set_index_properties(props_json);
        return schema_pb;
    }

    ChunkUniquePtr build_chunk(const TabletSchemaCSPtr& tablet_schema, int num_rows) {
        auto schema = ChunkHelper::convert_schema(tablet_schema);
        auto chunk = ChunkHelper::new_chunk(schema, num_rows);
        for (int i = 0; i < num_rows; ++i) {
            chunk->get_column_raw_ptr_by_index(0)->append_datum(Datum(static_cast<int32_t>(i)));
            DatumArray arr;
            arr.emplace_back(static_cast<float>(i) + 0.1f);
            arr.emplace_back(static_cast<float>(i) + 0.2f);
            arr.emplace_back(static_cast<float>(i) + 0.3f);
            chunk->get_column_raw_ptr_by_index(1)->append_datum(Datum(arr));
        }
        return chunk;
    }
};

// HorizontalGeneralTabletWriter without set_location_provider/set_fs: fill_vector_index_file_paths
// must fall back to tablet_mgr->segment_location for the .vi path. Verifies that the written
// SegmentFileInfo records vector_index_ids and that a .vi artifact lands next to the segment.
TEST_F(SharedDataTabletWriterVITest, test_horizontal_writer_vi_via_tablet_mgr) {
    ConfigResetGuard<int32_t> threshold_guard(&config::config_vector_index_default_build_threshold, 1);

    auto schema_pb = create_vi_schema_pb();
    auto tablet_schema = TabletSchema::create(schema_pb);

    int64_t txn_id = 2001;
    auto writer = std::make_unique<HorizontalGeneralTabletWriter>(_tablet_mgr.get(), kTabletId, tablet_schema, txn_id,
                                                                  /*is_compaction=*/false);
    ASSERT_OK(writer->open());

    auto chunk = build_chunk(tablet_schema, 5);
    ASSERT_OK(writer->write(*chunk));
    ASSERT_OK(writer->finish());

    ASSERT_EQ(writer->segments().size(), 1);
    const auto& seg = writer->segments().front();
    EXPECT_EQ(seg.num_rows, 5);
    ASSERT_EQ(seg.vector_index_ids.size(), 1);
    EXPECT_EQ(seg.vector_index_ids[0], kIndexId);

    std::string seg_path = _tablet_mgr->segment_location(kTabletId, seg.path);
    std::string vi_path = _tablet_mgr->segment_location(kTabletId, gen_vector_index_filename(seg.path, kIndexId));
    EXPECT_TRUE(fs::path_exist(seg_path));
    EXPECT_TRUE(fs::path_exist(vi_path));

    writer->close();
}

// HorizontalGeneralTabletWriter with set_location_provider/set_fs: fill_vector_index_file_paths
// must resolve .vi paths through LocationProvider (the shared-data S3/HDFS branch). Uses the same
// underlying local path so we can still assert file existence.
TEST_F(SharedDataTabletWriterVITest, test_horizontal_writer_vi_via_location_provider) {
    ConfigResetGuard<int32_t> threshold_guard(&config::config_vector_index_default_build_threshold, 1);

    auto schema_pb = create_vi_schema_pb();
    auto tablet_schema = TabletSchema::create(schema_pb);

    int64_t txn_id = 2002;
    auto writer = std::make_unique<HorizontalGeneralTabletWriter>(_tablet_mgr.get(), kTabletId, tablet_schema, txn_id,
                                                                  /*is_compaction=*/false);
    writer->set_location_provider(_lp);
    ASSIGN_OR_ABORT(auto fs, FileSystemFactory::CreateSharedFromString(_test_dir));
    writer->set_fs(fs);
    ASSERT_OK(writer->open());

    auto chunk = build_chunk(tablet_schema, 3);
    ASSERT_OK(writer->write(*chunk));
    ASSERT_OK(writer->finish());

    ASSERT_EQ(writer->segments().size(), 1);
    const auto& seg = writer->segments().front();
    EXPECT_EQ(seg.num_rows, 3);
    ASSERT_EQ(seg.vector_index_ids.size(), 1);
    EXPECT_EQ(seg.vector_index_ids[0], kIndexId);

    std::string vi_path = _lp->segment_location(kTabletId, gen_vector_index_filename(seg.path, kIndexId));
    EXPECT_TRUE(fs::path_exist(vi_path));

    writer->close();
}

// VerticalGeneralTabletWriter writes key columns first, then value columns. Covers the
// matching vector_index_ids propagation + fill_vector_index_file_paths in VerticalGeneralTabletWriter
// and the segment_writer VECTOR init branch under vertical mode.
TEST_F(SharedDataTabletWriterVITest, test_vertical_writer_vi) {
    ConfigResetGuard<int32_t> threshold_guard(&config::config_vector_index_default_build_threshold, 1);

    auto schema_pb = create_vi_schema_pb();
    auto tablet_schema = TabletSchema::create(schema_pb);

    int64_t txn_id = 2003;
    auto writer = std::make_unique<VerticalGeneralTabletWriter>(_tablet_mgr.get(), kTabletId, tablet_schema, txn_id,
                                                                /*max_rows_per_segment=*/INT32_MAX,
                                                                /*is_compaction=*/false);
    ASSERT_OK(writer->open());

    // VerticalGeneralTabletWriter::write_columns expects a chunk whose columns match
    // the provided column_indexes. Build per-column chunks rather than passing the
    // full multi-column chunk.
    constexpr int kRows = 4;
    auto key_schema = std::make_shared<Schema>(ChunkHelper::convert_schema(tablet_schema, {0}));
    auto vec_schema = std::make_shared<Schema>(ChunkHelper::convert_schema(tablet_schema, {1}));

    auto key_col = Int32Column::create();
    for (int i = 0; i < kRows; ++i) {
        key_col->append(i);
    }
    Chunk key_chunk({std::move(key_col)}, key_schema);

    auto element_col = FixedLengthColumn<float>::create();
    auto element_null = NullColumn::create();
    auto offsets = UInt32Column::create();
    offsets->append(0);
    for (int i = 0; i < kRows; ++i) {
        element_col->append(static_cast<float>(i) + 0.1f);
        element_col->append(static_cast<float>(i) + 0.2f);
        element_col->append(static_cast<float>(i) + 0.3f);
        element_null->append(0);
        element_null->append(0);
        element_null->append(0);
        offsets->append((i + 1) * 3);
    }
    auto nullable_elements = NullableColumn::create(std::move(element_col), std::move(element_null));
    auto vec_col = ArrayColumn::create(std::move(nullable_elements), std::move(offsets));
    Chunk vec_chunk({std::move(vec_col)}, vec_schema);

    ASSERT_OK(writer->write_columns(key_chunk, /*column_indexes=*/{0}, /*is_key=*/true));
    ASSERT_OK(writer->flush_columns());
    ASSERT_OK(writer->write_columns(vec_chunk, /*column_indexes=*/{1}, /*is_key=*/false));
    ASSERT_OK(writer->flush_columns());
    ASSERT_OK(writer->finish());

    ASSERT_EQ(writer->segments().size(), 1);
    const auto& seg = writer->segments().front();
    EXPECT_EQ(seg.num_rows, kRows);
    ASSERT_EQ(seg.vector_index_ids.size(), 1);
    EXPECT_EQ(seg.vector_index_ids[0], kIndexId);

    std::string vi_path = _tablet_mgr->segment_location(kTabletId, gen_vector_index_filename(seg.path, kIndexId));
    EXPECT_TRUE(fs::path_exist(vi_path));

    writer->close();
}

// Exercise the segment_writer.cpp fallback branch that resolves the vector index path
// via IndexDescriptor when vector_index_file_paths is empty. This is the path a caller
// that hasn't been migrated to pre-populate the map would take (shared-nothing style).
// Without this coverage, the else-branch sits as dead code even though it's still
// load-bearing for callers that don't go through general_tablet_writer/rowset_writer.
TEST_F(SharedDataTabletWriterVITest, test_segment_writer_vi_fallback_to_index_descriptor) {
    ConfigResetGuard<int32_t> threshold_guard(&config::config_vector_index_default_build_threshold, 1);

    auto schema_pb = create_vi_schema_pb();
    auto tablet_schema = TabletSchema::create(schema_pb);

    std::string rowset_dir = _test_dir + "/rowset_fallback";
    ASSERT_TRUE(fs::create_directories(rowset_dir).ok());
    std::string seg_path = rowset_dir + "/segment_0.dat";

    SegmentWriterOptions opts;
    // Intentionally leave opts.vector_index_file_paths empty to hit the else-branch.
    opts.segment_file_mark.rowset_path_prefix = rowset_dir;
    opts.segment_file_mark.rowset_id = "1";

    ASSIGN_OR_ABORT(auto wfile, fs::new_writable_file(seg_path));
    auto writer = std::make_unique<SegmentWriter>(std::move(wfile), /*segment_id=*/0, tablet_schema, opts);
    ASSERT_OK(writer->init());

    constexpr int kRows = 3;
    auto schema = ChunkHelper::convert_schema(tablet_schema);
    auto chunk = ChunkHelper::new_chunk(schema, kRows);
    for (int i = 0; i < kRows; ++i) {
        chunk->get_column_raw_ptr_by_index(0)->append_datum(Datum(static_cast<int32_t>(i)));
        DatumArray arr;
        arr.emplace_back(static_cast<float>(i) + 0.1f);
        arr.emplace_back(static_cast<float>(i) + 0.2f);
        arr.emplace_back(static_cast<float>(i) + 0.3f);
        chunk->get_column_raw_ptr_by_index(1)->append_datum(Datum(arr));
    }
    ASSERT_OK(writer->append_chunk(*chunk));

    uint64_t seg_size = 0, idx_size = 0, footer_pos = 0;
    ASSERT_OK(writer->finalize(&seg_size, &idx_size, &footer_pos));

    std::string expected_vi = fmt::format("{}/{}_{}_{}.vi", rowset_dir, opts.segment_file_mark.rowset_id, 0, kIndexId);
    EXPECT_TRUE(fs::path_exist(expected_vi));
}

// ==================== Read Path Tests ====================

#ifdef WITH_TENANN

// Test reading a vector index file from shared-data path using VectorIndexReaderFactory with FileSystem.
// This verifies the FS-aware create_from_file overload works correctly.
TEST_F(SharedDataVectorIndexTest, test_vector_index_read_shared_data_path) {
    // Set threshold low enough so that 5 rows triggers index build
    ConfigResetGuard<int32_t> threshold_guard(&config::config_vector_index_default_build_threshold, 1);

    const int64_t tablet_id = 12345;
    const int64_t index_id = 0;
    const std::string segment_name = "0000000000000001_6bc1edf0-fba6-4aa1-b0d4-ee5b88ef156b.dat";

    std::string vi_filename = gen_vector_index_filename(segment_name, index_id);
    std::string vector_index_path = _location_provider->segment_location(tablet_id, vi_filename);

    // Write vector index first
    auto tablet_index = create_tablet_index(index_id);
    tablet_index->add_search_properties("efsearch", "40");

    std::unique_ptr<VectorIndexWriter> vector_index_writer;
    VectorIndexWriter::create(tablet_index, vector_index_path, true, &vector_index_writer);
    ASSERT_OK(vector_index_writer->init());

    auto array_column = create_array_column_with_vectors(5);
    ASSERT_OK(vector_index_writer->append(*array_column));
    uint64_t index_size = 0;
    ASSERT_OK(vector_index_writer->finish(&index_size));
    ASSERT_GT(index_size, 0);

    // Read vector index using FS-aware factory
    const auto& empty_meta = std::map<std::string, std::string>{};
    ASSIGN_OR_ABORT(auto meta, get_vector_meta(tablet_index, empty_meta));
    auto index_meta = std::make_shared<tenann::IndexMeta>(std::move(meta));

    std::shared_ptr<VectorIndexReader> reader;
    ASSERT_OK(VectorIndexReaderFactory::create_from_file(vector_index_path, index_meta, &reader, _fs.get()));
    ASSERT_NE(reader, nullptr);

    // init_searcher with FileSystem should succeed
    ASSERT_OK(reader->init_searcher(*index_meta, vector_index_path, _fs.get()));
}

// Test that reading an empty mark .vi file via FS-aware path returns EmptyIndexReader.
TEST_F(SharedDataVectorIndexTest, test_vector_index_read_empty_mark_shared_data_path) {
    ConfigResetGuard<int32_t> threshold_guard(&config::config_vector_index_default_build_threshold, 100);

    const int64_t tablet_id = 999;
    const int64_t index_id = 0;
    std::string segment_name = "0000000000000002_abc12345-6789-0def-1234-567890abcdef.dat";
    std::string vi_filename = gen_vector_index_filename(segment_name, index_id);
    std::string vector_index_path = _location_provider->segment_location(tablet_id, vi_filename);

    // Write empty mark file directly (the current writer skips file generation
    // when threshold is not met, so we create the empty mark manually to test the reader)
    auto tablet_index = create_tablet_index(index_id);
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(vector_index_path));
    ASSERT_OK(wfile->append(IndexDescriptor::mark_word));
    ASSERT_OK(wfile->close());

    // Read via FS-aware factory — should detect empty mark
    const auto& empty_meta = std::map<std::string, std::string>{};
    ASSIGN_OR_ABORT(auto meta, get_vector_meta(tablet_index, empty_meta));
    auto index_meta = std::make_shared<tenann::IndexMeta>(std::move(meta));

    std::shared_ptr<VectorIndexReader> reader;
    ASSERT_OK(VectorIndexReaderFactory::create_from_file(vector_index_path, index_meta, &reader, _fs.get()));
    ASSERT_NE(reader, nullptr);

    // EmptyIndexReader.init_searcher returns NotSupported
    auto status = reader->init_searcher(*index_meta, vector_index_path, _fs.get());
    ASSERT_TRUE(status.is_not_supported());
}

// Test that FS-aware create_from_file returns NotFound for non-existent path.
TEST_F(SharedDataVectorIndexTest, test_vector_index_read_not_found) {
    std::string non_existent_path = _test_dir + "/data/non_existent.vi";
    auto index_meta = std::make_shared<tenann::IndexMeta>();

    std::shared_ptr<VectorIndexReader> reader;
    auto status = VectorIndexReaderFactory::create_from_file(non_existent_path, index_meta, &reader, _fs.get());
    ASSERT_TRUE(status.is_not_found());
}

#endif // WITH_TENANN

// SegmentWriterOptions::skip_vector_index gates the VECTOR branch of
// SegmentWriter::init that wires up standalone_index_file_paths for the
// vector column. With skip_vector_index=true, no .vi path is configured,
// no .vi file is produced, and _has_vector_index_written stays false.
//
// HorizontalGeneralTabletWriter sets this flag for bundle-file segments
// (segments that share a single underlying data file across the rowset)
// because the segment filename in metadata is the bundle filename, not
// the per-segment name used to derive .vi paths. Producing .vi files for
// bundle segments would generate paths that don't match what readers
// look up, so the writer suppresses them.
//
// This test pins down the SegmentWriter-level contract directly, without
// the BundleWritableFileContext scaffolding, so a regression that drops
// the `&& !_opts.skip_vector_index` guard would fail here.
TEST_F(SharedDataTabletWriterVITest, test_segment_writer_skip_vector_index_no_vi_artifact) {
    ConfigResetGuard<int32_t> threshold_guard(&config::config_vector_index_default_build_threshold, 1);

    auto schema_pb = create_vi_schema_pb();
    auto tablet_schema = TabletSchema::create(schema_pb);

    std::string rowset_dir = _test_dir + "/rowset_skip_vi";
    ASSERT_TRUE(fs::create_directories(rowset_dir).ok());
    std::string seg_path = rowset_dir + "/segment_0.dat";

    SegmentWriterOptions opts;
    opts.segment_file_mark.rowset_path_prefix = rowset_dir;
    opts.segment_file_mark.rowset_id = "1";
    opts.skip_vector_index = true; // simulate the bundle-file path

    ASSIGN_OR_ABORT(auto wfile, fs::new_writable_file(seg_path));
    auto writer = std::make_unique<SegmentWriter>(std::move(wfile), /*segment_id=*/0, tablet_schema, opts);
    ASSERT_OK(writer->init());

    constexpr int kRows = 4;
    auto schema = ChunkHelper::convert_schema(tablet_schema);
    auto chunk = ChunkHelper::new_chunk(schema, kRows);
    for (int i = 0; i < kRows; ++i) {
        chunk->get_column_raw_ptr_by_index(0)->append_datum(Datum(static_cast<int32_t>(i)));
        DatumArray arr;
        arr.emplace_back(static_cast<float>(i) + 0.1f);
        arr.emplace_back(static_cast<float>(i) + 0.2f);
        arr.emplace_back(static_cast<float>(i) + 0.3f);
        chunk->get_column_raw_ptr_by_index(1)->append_datum(Datum(arr));
    }
    ASSERT_OK(writer->append_chunk(*chunk));

    uint64_t seg_size = 0, idx_size = 0, footer_pos = 0;
    ASSERT_OK(writer->finalize(&seg_size, &idx_size, &footer_pos));

    // Per-column write_vector_index() must report 0 standalone size, leaving
    // _has_vector_index_written = false so the tablet writer does NOT advertise
    // vector_index_ids on this segment's SegmentFileInfo.
    EXPECT_FALSE(writer->has_vector_index_written());

    // No .vi artifact must land on disk for this segment under any of the
    // path-resolution branches.
    std::string expected_vi_index_descriptor =
            fmt::format("{}/{}_{}_{}.vi", rowset_dir, opts.segment_file_mark.rowset_id, 0, kIndexId);
    EXPECT_FALSE(fs::path_exist(expected_vi_index_descriptor));
}

// SegmentWriter::has_vector_index_written should also be false when the
// tablet schema has no VECTOR index at all — the per-column write_vector_index()
// loop never sets standalone_index_size > 0. This pins down the false-by-default
// branch that the existing tests cover only implicitly.
TEST_F(SharedDataTabletWriterVITest, test_segment_writer_no_vi_column_has_vector_index_written_false) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_short_key_columns(1);
    auto* c0 = schema_pb.add_column();
    c0->set_unique_id(1);
    c0->set_name("pk");
    c0->set_type("INT");
    c0->set_is_key(true);
    c0->set_is_nullable(false);
    auto* c1 = schema_pb.add_column();
    c1->set_unique_id(2);
    c1->set_name("v");
    c1->set_type("INT");
    c1->set_is_key(false);
    c1->set_is_nullable(false);
    auto tablet_schema = TabletSchema::create(schema_pb);

    std::string rowset_dir = _test_dir + "/rowset_no_vi";
    ASSERT_TRUE(fs::create_directories(rowset_dir).ok());
    std::string seg_path = rowset_dir + "/segment_0.dat";

    SegmentWriterOptions opts;
    opts.segment_file_mark.rowset_path_prefix = rowset_dir;
    opts.segment_file_mark.rowset_id = "1";

    ASSIGN_OR_ABORT(auto wfile, fs::new_writable_file(seg_path));
    auto writer = std::make_unique<SegmentWriter>(std::move(wfile), /*segment_id=*/0, tablet_schema, opts);
    ASSERT_OK(writer->init());

    auto schema = ChunkHelper::convert_schema(tablet_schema);
    auto chunk = ChunkHelper::new_chunk(schema, 3);
    for (int i = 0; i < 3; ++i) {
        chunk->get_column_raw_ptr_by_index(0)->append_datum(Datum(static_cast<int32_t>(i)));
        chunk->get_column_raw_ptr_by_index(1)->append_datum(Datum(static_cast<int32_t>(i * 10)));
    }
    ASSERT_OK(writer->append_chunk(*chunk));

    uint64_t seg_size = 0, idx_size = 0, footer_pos = 0;
    ASSERT_OK(writer->finalize(&seg_size, &idx_size, &footer_pos));

    EXPECT_FALSE(writer->has_vector_index_written());
}

} // namespace starrocks::lake
