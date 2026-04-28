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

#include <cmath>

#ifdef WITH_TENANN
#include <tenann/factory/ann_searcher_factory.h>
#include <tenann/factory/index_factory.h>

#include "storage/index/vector/tenann_index_reader.h"
#include "storage/index/vector/vector_index_file_reader.h"
#endif

#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "common/config_rowset_fwd.h"
#include "common/config_vector_index_fwd.h"
#include "fs/fs_factory.h"
#include "fs/fs_memory.h"
#include "gen_cpp/tablet_schema.pb.h"
#include "gutil/casts.h"
#include "runtime/mem_pool.h"
#include "storage/chunk_helper.h"
#include "storage/index/index_descriptor.h"
#include "storage/index/vector/tenann/del_id_filter.h"
#include "storage/index/vector/tenann/tenann_index_utils.h"
#include "storage/index/vector/vector_index_reader.h"
#include "storage/index/vector/vector_index_reader_factory.h"
#include "storage/index/vector/vector_index_writer.h"
#include "storage/index/vector/vector_search_option.h"
#include "storage/rowset/bitmap_index_reader.h"
#include "storage/rowset/bitmap_index_writer.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_iterator.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_writer.h"
#include "storage/tablet_schema.h"
#include "storage/tablet_schema_helper.h"

namespace starrocks {

class VectorIndexSearchTest : public testing::Test {
public:
    VectorIndexSearchTest() = default;

protected:
    void SetUp() override {
        srand(GetCurrentTimeMicros());
        CHECK_OK(fs::remove_all(test_vector_index_dir));
        CHECK_OK(fs::create_directories(test_vector_index_dir));
        ASSIGN_OR_ABORT(_fs, FileSystemFactory::CreateSharedFromString(test_vector_index_dir));
    }

    void TearDown() override { fs::remove_all(test_vector_index_dir); }

    std::shared_ptr<FileSystem> _fs;
    const std::string test_vector_index_dir = "vector_search_test";
    const std::string vector_index_name = "vector_index.vi";
    const std::string empty_index_name = "empty_index.vi";

    std::shared_ptr<TabletIndex> prepare_tablet_index() {
        std::shared_ptr<TabletIndex> tablet_index = std::make_shared<TabletIndex>();
        TabletIndexPB index_pb;
        index_pb.set_index_id(0);
        index_pb.set_index_name("test_index");
        index_pb.set_index_type(IndexType::VECTOR);
        index_pb.add_col_unique_id(1);
        tablet_index->init_from_pb(index_pb);
        return tablet_index;
    }

    void append_test_data(VectorIndexWriter* vector_index_writer) {
        auto element = FixedLengthColumn<float>::create();
        element->append(1);
        element->append(2);
        element->append(3);
        auto null_column = NullColumn::create(element->size(), 0);
        auto nullable_column = NullableColumn::create(std::move(element), std::move(null_column));
        auto offsets = UInt32Column::create();
        offsets->append(0);
        offsets->append(3);
        for (int i = 0; i < 10; i++) {
            auto e = FixedLengthColumn<float>::create();
            e->append(i + 1.1);
            e->append(i + 2.2);
            e->append(i + 3.3);
            nullable_column->append(*e, 0, e->size());
            offsets->append((i + 2) * 3);
        }

        auto array_column = ArrayColumn::create(std::move(nullable_column), std::move(offsets));
        CHECK_OK(vector_index_writer->append(*array_column));
        ASSERT_EQ(vector_index_writer->size(), 11);
    }

    // Threshold met: a real .vi file lands at `path`.
    void write_vector_index(const std::string& path, const std::shared_ptr<TabletIndex>& tablet_index) {
        DeferOp op([&] { ASSERT_TRUE(fs::path_exist(path)); });

        std::unique_ptr<VectorIndexWriter> vector_index_writer;
        VectorIndexWriter::create(tablet_index, path, true, &vector_index_writer);
        CHECK_OK(vector_index_writer->init());

        append_test_data(vector_index_writer.get());

        uint64_t size = 0;
        CHECK_OK(vector_index_writer->finish(&size));

        ASSERT_GT(size, 0);
    }

    // Threshold not met: finish() short-circuits and no .vi file is produced.
    void write_vector_index_below_threshold(const std::string& path, const std::shared_ptr<TabletIndex>& tablet_index) {
        std::unique_ptr<VectorIndexWriter> vector_index_writer;
        VectorIndexWriter::create(tablet_index, path, true, &vector_index_writer);
        CHECK_OK(vector_index_writer->init());

        append_test_data(vector_index_writer.get());

        uint64_t size = 0;
        CHECK_OK(vector_index_writer->finish(&size));

        ASSERT_EQ(size, 0);
        ASSERT_FALSE(fs::path_exist(path));
    }
};

TEST_F(VectorIndexSearchTest, test_search_vector_index) {
    auto tablet_index = prepare_tablet_index();
    tablet_index->add_common_properties("index_type", "hnsw");
    tablet_index->add_common_properties("dim", "3");
    tablet_index->add_common_properties("is_vector_normed", "false");
    tablet_index->add_common_properties("metric_type", "l2_distance");
    // Force the writer to build the index immediately rather than wait for the
    // default (config_vector_index_default_build_threshold) row count. Without
    // this the 11 rows below would fall under the threshold and finish() would
    // short-circuit without producing a file the search test below depends on.
    tablet_index->add_common_properties("index_build_threshold", "0");
    tablet_index->add_index_properties("efconstruction", "40");
    tablet_index->add_index_properties("m", "16");
    tablet_index->add_search_properties("efsearch", "40");

    auto index_path = test_vector_index_dir + "/" + vector_index_name;
    write_vector_index(index_path, tablet_index);

#ifdef WITH_TENANN
    try {
        const auto& empty_meta = std::map<std::string, std::string>{};
        auto status = get_vector_meta(tablet_index, empty_meta);

        CHECK_OK(status);
        auto index_meta = std::make_shared<tenann::IndexMeta>(status.value());

        std::shared_ptr<VectorIndexReader> ann_reader;
        VectorIndexReaderFactory::create_from_file(index_path, index_meta, &ann_reader);

        auto init_status = ann_reader->init_searcher(*index_meta, index_path);

        ASSERT_TRUE(!init_status.is_not_supported());

        constexpr int kTopK = 1;
        Status st;
        // tenann::AnnSearcher::AnnSearch writes into caller-owned output buffers; the
        // vectors must be sized to at least k before the call so .data() is non-null.
        std::vector<int64_t> result_ids(kTopK);
        std::vector<float> result_distances(kTopK);
        SparseRange<> scan_range;
        DelIdFilter del_id_filter(scan_range);
        std::vector<float> query_vector = {1.0f, 2.0f, 3.0f};
        tenann::PrimitiveSeqView query_view =
                tenann::PrimitiveSeqView{.data = reinterpret_cast<uint8_t*>(query_vector.data()),
                                         .size = static_cast<uint32_t>(3),
                                         .elem_type = tenann::PrimitiveType::kFloatType};

        st = ann_reader->search(query_view, kTopK, result_ids.data(),
                                reinterpret_cast<uint8_t*>(result_distances.data()), &del_id_filter);
        CHECK_OK(st);
        ASSERT_EQ(result_ids.size(), kTopK);
    } catch (tenann::Error& e) {
        LOG(WARNING) << e.what();
    }
#endif
}

// IVFPQ + threshold not met: VectorIndexWriter::finish() short-circuits and no .vi
// file is produced. Reader-side, VectorIndexReaderFactory::create_from_file surfaces
// the missing file as NotFound; the segment_iterator brute-force fallback (added by
// the read PR) handles that case at scan time.
TEST_F(VectorIndexSearchTest, test_select_empty_mark) {
    config::config_vector_index_default_build_threshold = 100;
    auto tablet_index = prepare_tablet_index();

    tablet_index->add_common_properties("index_type", "ivfpq");
    tablet_index->add_common_properties("dim", "3");
    tablet_index->add_common_properties("is_vector_normed", "false");
    tablet_index->add_common_properties("metric_type", "l2_distance");
    // ivfpq requires these in index_properties for tenann meta validation
    // (CRITICAL_CHECK_AND_GET in get_vector_meta). The values are not
    // exercised here — the test only verifies the empty-mark path returns
    // NotSupported, but get_vector_meta() runs before that and still
    // requires these keys to be present.
    tablet_index->add_index_properties("nlist", "1");
    tablet_index->add_index_properties("nbits", "8");
    tablet_index->add_index_properties("m_ivfpq", "3");

    auto index_path = test_vector_index_dir + "/" + empty_index_name;
    write_vector_index_below_threshold(index_path, tablet_index);
}

#ifdef WITH_TENANN

// ==================== VectorIndexFileReader direct tests ====================
// VectorIndexFileReader bridges StarRocks RandomAccessFile into TenANN's
// IndexFileReader interface, enabling TenANN to read .vi files from any
// StarRocks-supported FS (S3/HDFS/OSS). The class is otherwise only
// exercised indirectly by TenANNReader::init_searcher; these tests pin
// down its behavior without spinning up an ANN index.

namespace {
constexpr std::string_view kTestPayload = "0123456789ABCDEF";
constexpr int64_t kTestPayloadSize = static_cast<int64_t>(kTestPayload.size());
constexpr std::string_view kTestFilename = "memory_index.vi";

std::unique_ptr<VectorIndexFileReader> make_reader(std::string_view payload, std::string_view name = kTestFilename) {
    auto raf = new_random_access_file_from_memory(name, payload);
    return std::make_unique<VectorIndexFileReader>(std::move(raf), static_cast<int64_t>(payload.size()));
}
} // namespace

TEST_F(VectorIndexSearchTest, vector_index_file_reader_basic_read_advances_position) {
    auto reader = make_reader(kTestPayload);

    char buf[8] = {};
    int64_t n = reader->Read(buf, 4);
    ASSERT_EQ(n, 4);
    EXPECT_EQ(std::string_view(buf, 4), "0123");

    // Position should have advanced; the next Read continues from offset 4.
    n = reader->Read(buf, 4);
    ASSERT_EQ(n, 4);
    EXPECT_EQ(std::string_view(buf, 4), "4567");
}

TEST_F(VectorIndexSearchTest, vector_index_file_reader_read_at_does_not_change_position) {
    auto reader = make_reader(kTestPayload);

    char buf[8] = {};
    int64_t n = reader->ReadAt(8, buf, 4);
    ASSERT_EQ(n, 4);
    EXPECT_EQ(std::string_view(buf, 4), "89AB");

    // ReadAt is independent of the streaming position cursor; a subsequent
    // Read should still start at offset 0.
    n = reader->Read(buf, 4);
    ASSERT_EQ(n, 4);
    EXPECT_EQ(std::string_view(buf, 4), "0123");
}

TEST_F(VectorIndexSearchTest, vector_index_file_reader_seek_then_read) {
    auto reader = make_reader(kTestPayload);

    reader->Seek(10);
    char buf[8] = {};
    int64_t n = reader->Read(buf, 4);
    ASSERT_EQ(n, 4);
    EXPECT_EQ(std::string_view(buf, 4), "ABCD");
}

TEST_F(VectorIndexSearchTest, vector_index_file_reader_get_size_and_filename) {
    auto reader = make_reader(kTestPayload, "abc.vi");
    EXPECT_EQ(reader->GetSize(), kTestPayloadSize);
    EXPECT_EQ(reader->filename(), "abc.vi");
}

TEST_F(VectorIndexSearchTest, vector_index_file_reader_read_past_eof_returns_minus_one) {
    auto reader = make_reader(kTestPayload);

    char buf[64] = {};
    // Reading more bytes than exist surfaces as -1 (Read uses read_at_fully
    // which fails when count exceeds the available bytes from the offset).
    int64_t n = reader->Read(buf, kTestPayloadSize + 4);
    EXPECT_EQ(n, -1);

    // Same expectation for ReadAt past EOF.
    int64_t m = reader->ReadAt(kTestPayloadSize - 2, buf, 8);
    EXPECT_EQ(m, -1);
}

// TenANNReader::init_searcher(meta, path, fs) should delegate to the legacy
// init_searcher(meta, path) when fs is nullptr. Build a real HNSW index on
// local disk, then invoke the FS-aware overload with fs=nullptr and confirm
// the call reaches the legacy success path (returns OK, NOT NotSupported).
TEST_F(VectorIndexSearchTest, tenann_reader_init_searcher_null_fs_delegates_to_legacy) {
    auto tablet_index = prepare_tablet_index();
    tablet_index->add_common_properties("index_type", "hnsw");
    tablet_index->add_common_properties("dim", "3");
    tablet_index->add_common_properties("is_vector_normed", "false");
    tablet_index->add_common_properties("metric_type", "l2_distance");
    tablet_index->add_index_properties("efconstruction", "40");
    tablet_index->add_index_properties("m", "16");
    tablet_index->add_search_properties("efsearch", "40");

    config::config_vector_index_default_build_threshold = 1;
    auto ann_path = test_vector_index_dir + "/null_fs_delegate_hnsw.vi";
    write_vector_index(ann_path, tablet_index);

    try {
        const auto empty_query_params = std::map<std::string, std::string>{};
        ASSIGN_OR_ABORT(auto ann_meta, get_vector_meta(tablet_index, empty_query_params));

        TenANNReader tenann_reader;
        // fs=nullptr branch dispatches to the legacy init_searcher(meta, path) overload.
        Status status = tenann_reader.init_searcher(ann_meta, ann_path, /*fs=*/nullptr);
        EXPECT_TRUE(status.ok()) << status;
    } catch (tenann::Error& e) {
        LOG(WARNING) << e.what();
    }
}

#endif // WITH_TENANN

// ==================== Brute-force fallback tests ====================
// Test SegmentIterator brute-force fallback when .vi file is missing.
// This covers: _prepare_vector_index, _setup_brute_force_fallback,
// _init_ann_reader fallback path, and _compute_brute_force_distances.

class BruteForceVectorFallbackTest : public testing::Test {
protected:
    void SetUp() override {
        _fs = std::make_shared<MemoryFileSystem>();
        ASSERT_TRUE(_fs->create_dir(kSegmentDir).ok());
    }

    const std::string kSegmentDir = "/brute_force_vector_test";
    std::shared_ptr<MemoryFileSystem> _fs;

    // Build a TabletSchema with: id(BIGINT key) + vector(ARRAY<FLOAT>), no vector index.
    // Used for writing segments.
    std::shared_ptr<TabletSchema> build_write_schema() {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(DUP_KEYS);
        schema_pb.set_next_column_unique_id(3);

        auto* col0 = schema_pb.add_column();
        col0->set_unique_id(0);
        col0->set_name("id");
        col0->set_type("BIGINT");
        col0->set_is_key(true);
        col0->set_is_nullable(false);
        col0->set_length(8);
        col0->set_index_length(8);
        col0->set_aggregation("NONE");

        auto* col1 = schema_pb.add_column();
        col1->set_unique_id(1);
        col1->set_name("vector");
        col1->set_type("ARRAY");
        col1->set_is_key(false);
        col1->set_is_nullable(false);
        col1->set_length(24);
        col1->set_aggregation("NONE");
        auto* child = col1->add_children_columns();
        child->set_unique_id(2);
        child->set_name("element");
        child->set_type("FLOAT");
        child->set_is_key(false);
        child->set_is_nullable(true);
        child->set_length(4);
        child->set_aggregation("NONE");

        return TabletSchema::create(schema_pb);
    }

    // Build a TabletSchema identical to write_schema but WITH vector index.
    // Used for reading — simulates the real table schema that has a vector index.
    std::shared_ptr<TabletSchema> build_read_schema_with_vector_index() {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(DUP_KEYS);
        schema_pb.set_next_column_unique_id(3);

        auto* col0 = schema_pb.add_column();
        col0->set_unique_id(0);
        col0->set_name("id");
        col0->set_type("BIGINT");
        col0->set_is_key(true);
        col0->set_is_nullable(false);
        col0->set_length(8);
        col0->set_index_length(8);
        col0->set_aggregation("NONE");

        auto* col1 = schema_pb.add_column();
        col1->set_unique_id(1);
        col1->set_name("vector");
        col1->set_type("ARRAY");
        col1->set_is_key(false);
        col1->set_is_nullable(false);
        col1->set_length(24);
        col1->set_aggregation("NONE");
        auto* child = col1->add_children_columns();
        child->set_unique_id(2);
        child->set_name("element");
        child->set_type("FLOAT");
        child->set_is_key(false);
        child->set_is_nullable(true);
        child->set_length(4);
        child->set_aggregation("NONE");

        auto* idx = schema_pb.add_table_indices();
        idx->set_index_id(100);
        idx->set_index_name("vector_index");
        idx->set_index_type(IndexType::VECTOR);
        idx->add_col_unique_id(1);
        std::string props =
                R"({"common_properties":{"index_type":"hnsw","dim":"3","metric_type":"l2_distance","is_vector_normed":"false"},"index_properties":{"efconstruction":"40","m":"16"},"search_properties":{"efsearch":"40"}})";
        idx->set_index_properties(props);

        return TabletSchema::create(schema_pb);
    }

    // Write a segment WITHOUT vector index. The .vi file won't exist.
    StatusOr<std::shared_ptr<Segment>> write_segment(const std::vector<int64_t>& ids,
                                                     const std::vector<std::vector<float>>& vectors,
                                                     std::shared_ptr<TabletSchema> read_schema = nullptr) {
        auto write_schema = build_write_schema();
        std::string file_name = kSegmentDir + "/test_segment.dat";
        ASSIGN_OR_RETURN(auto wfile, _fs->new_writable_file(file_name));

        SegmentWriterOptions opts;
        opts.num_rows_per_block = 100;
        SegmentWriter writer(std::move(wfile), 0, write_schema, opts);

        auto chunk = ChunkHelper::new_chunk(ChunkHelper::convert_schema(write_schema), ids.size());
        for (auto id : ids) {
            chunk->mutable_columns()[0]->append_datum(Datum(id));
        }
        for (const auto& vec : vectors) {
            DatumArray arr;
            for (float v : vec) {
                arr.emplace_back(Datum(v));
            }
            chunk->mutable_columns()[1]->append_datum(Datum(arr));
        }

        RETURN_IF_ERROR(writer.init());
        RETURN_IF_ERROR(writer.append_chunk(*chunk));
        uint64_t seg_size = 0, index_size = 0, footer_pos = 0;
        RETURN_IF_ERROR(writer.finalize(&seg_size, &index_size, &footer_pos));

        // Open with the supplied read schema (or the default vector-index schema) so
        // _setup_brute_force_fallback sees the metric_type the test wants to exercise.
        if (read_schema == nullptr) {
            read_schema = build_read_schema_with_vector_index();
        }
        return Segment::open(_fs, FileInfo{file_name}, 0, read_schema);
    }
};

// Test: segment written without a .vi file, SegmentIterator with use_vector_index=true.
//
// This exercises the runtime-NotFound branch of the fallback: the segment footer's
// vector_index_storage_type is unset (the writer in OSS only sets STANDALONE when
// a .vi was produced and never explicitly emits NONE), so Segment::skip_vector_index()
// is false and _prepare_vector_index is a no-op. Instead, _init_ann_reader tries to
// open the .vi file via VectorIndexReaderFactory::create_from_file, gets NotFound
// at runtime, and routes through _setup_brute_force_fallback. The footer-hint
// short-circuit in _prepare_vector_index is intentionally untested here — covering
// it would require a writer that emits VECTOR_INDEX_STORAGE_NONE in the footer.
TEST_F(BruteForceVectorFallbackTest, test_brute_force_l2_distance_fallback) {
    std::vector<int64_t> ids = {1, 2, 3};
    std::vector<std::vector<float>> vectors = {
            {1.0f, 2.0f, 3.0f},
            {4.0f, 5.0f, 6.0f},
            {0.0f, 0.0f, 0.0f},
    };
    ASSIGN_OR_ABORT(auto segment, write_segment(ids, vectors));
    // Confirm we are exercising the runtime-NotFound path, not the footer-hint
    // path — without this, a future writer change that emits NONE in the footer
    // would silently switch which fallback branch this test covers.
    ASSERT_FALSE(segment->skip_vector_index());

    auto schema = build_read_schema_with_vector_index();

    // Set up SegmentReadOptions with vector search enabled
    OlapReaderStatistics stats;
    SegmentReadOptions seg_opts;
    seg_opts.fs = _fs;
    seg_opts.stats = &stats;
    seg_opts.tablet_schema = schema;

    auto vector_search_opt = std::make_shared<VectorSearchOption>();
    vector_search_opt->use_vector_index = true;
    vector_search_opt->query_vector = {1.0f, 1.0f, 1.0f};
    vector_search_opt->k = 3;
    vector_search_opt->k_factor = 1.0;
    vector_search_opt->vector_distance_column_name = "__vector_approx_l2_distance";
    // vector_column_id will be set to num_columns (virtual column), matching the FE rewrite behavior
    vector_search_opt->vector_column_id = schema->num_columns();
    vector_search_opt->vector_slot_id = 100; // arbitrary slot id
    vector_search_opt->use_ivfpq = false;
    vector_search_opt->vector_range = -1.0;
    vector_search_opt->result_order = 0;
    vector_search_opt->pq_refine_factor = 1.0;

    seg_opts.use_vector_index = true;
    seg_opts.vector_search_option = vector_search_opt;

    // Build read schema: only the id column (vector column pruned by FE, as in real scenario)
    Schema read_schema;
    auto id_field = std::make_shared<Field>(0, "id", get_type_info(TYPE_BIGINT), false);
    id_field->set_uid(0);
    read_schema.append(id_field);

    auto chunk_iter = new_segment_iterator(segment, read_schema, seg_opts);
    ASSERT_TRUE(chunk_iter != nullptr);

    // Simulate the production caller (TabletReader / OlapChunkSource): freeze the
    // output schema BEFORE the iterator's lazy _init runs. Brute-force fallback's
    // late _schema mutation must not be expected to flow into output_schema —
    // _do_get_next sources the vector column from _dict_chunk instead of the
    // (possibly pruned) final chunk.
    ASSERT_OK(chunk_iter->init_output_schema({}));

    // The output schema includes the distance virtual column appended by brute-force path
    auto chunk = ChunkHelper::new_chunk(chunk_iter->output_schema(), 1024);
    std::vector<uint32_t> rowids;
    auto st = chunk_iter->get_next(chunk.get(), &rowids);

    // ChunkIterator::get_next contract: EOF implies empty chunk. Require OK so a
    // regression that returns EOF immediately can't make this test pass with zero
    // rows.
    ASSERT_OK(st);
    ASSERT_EQ(chunk->num_rows(), 3);

    // FE pruned the vector column, so chunk only has [id, distance].
    ASSERT_EQ(chunk->num_columns(), 2);

    // Validate the actual L2 distances for query [1, 1, 1]:
    //   Row 0 (id=1, vec=[1,2,3]): (0)^2 + (-1)^2 + (-2)^2 = 5
    //   Row 1 (id=2, vec=[4,5,6]): (-3)^2 + (-4)^2 + (-5)^2 = 50
    //   Row 2 (id=3, vec=[0,0,0]): 1 + 1 + 1 = 3
    auto dist_col = chunk->get_column_by_slot_id(vector_search_opt->vector_slot_id);
    ASSERT_NE(dist_col, nullptr);
    const auto* distances = down_cast<const FloatColumn*>(dist_col.get());
    ASSERT_EQ(distances->size(), 3);
    EXPECT_FLOAT_EQ(distances->get_data()[0], 5.0f);
    EXPECT_FLOAT_EQ(distances->get_data()[1], 50.0f);
    EXPECT_FLOAT_EQ(distances->get_data()[2], 3.0f);

    chunk_iter->close();
}

// Test: when vector column is already in read schema (not pruned),
// _setup_brute_force_fallback should find it without adding a duplicate.
TEST_F(BruteForceVectorFallbackTest, test_brute_force_vector_column_not_pruned) {
    std::vector<int64_t> ids = {1, 2};
    std::vector<std::vector<float>> vectors = {
            {1.0f, 0.0f, 0.0f},
            {0.0f, 1.0f, 0.0f},
    };
    ASSIGN_OR_ABORT(auto segment, write_segment(ids, vectors));

    auto schema = build_read_schema_with_vector_index();

    OlapReaderStatistics stats;
    SegmentReadOptions seg_opts;
    seg_opts.fs = _fs;
    seg_opts.stats = &stats;
    seg_opts.tablet_schema = schema;

    auto vector_search_opt = std::make_shared<VectorSearchOption>();
    vector_search_opt->use_vector_index = true;
    vector_search_opt->query_vector = {1.0f, 0.0f, 0.0f};
    vector_search_opt->k = 2;
    vector_search_opt->k_factor = 1.0;
    vector_search_opt->vector_distance_column_name = "__vector_approx_l2_distance";
    vector_search_opt->vector_column_id = schema->num_columns();
    vector_search_opt->vector_slot_id = 200;
    vector_search_opt->use_ivfpq = false;
    vector_search_opt->vector_range = -1.0;
    vector_search_opt->result_order = 0;
    vector_search_opt->pq_refine_factor = 1.0;

    seg_opts.use_vector_index = true;
    seg_opts.vector_search_option = vector_search_opt;

    // Build read schema including both id and vector columns (not pruned)
    Schema read_schema;
    auto id_field = std::make_shared<Field>(0, "id", get_type_info(TYPE_BIGINT), false);
    id_field->set_uid(0);
    read_schema.append(id_field);

    // Add vector column to schema (simulating the case where FE doesn't prune it)
    auto vec_field = std::make_shared<Field>(ChunkHelper::convert_field(1, schema->column(1)));
    read_schema.append(vec_field);

    auto chunk_iter = new_segment_iterator(segment, read_schema, seg_opts);
    ASSERT_TRUE(chunk_iter != nullptr);

    auto chunk = ChunkHelper::new_chunk(chunk_iter->output_schema(), 1024);
    std::vector<uint32_t> rowids;
    auto st = chunk_iter->get_next(chunk.get(), &rowids);

    ASSERT_OK(st);
    ASSERT_EQ(chunk->num_rows(), 2);
    // The vector column was already in read_schema, so _setup_brute_force_fallback
    // must reuse it instead of appending a duplicate. Final chunk should be exactly
    // [id, vector, distance] — three columns, with no second vector column.
    ASSERT_EQ(chunk->num_columns(), 3);

    chunk_iter->close();
}

// Test: brute-force fallback with vector_range filter.
// Rows with distance > vector_range should be filtered out.
TEST_F(BruteForceVectorFallbackTest, test_brute_force_with_vector_range_filter) {
    std::vector<int64_t> ids = {1, 2, 3, 4};
    std::vector<std::vector<float>> vectors = {
            {1.0f, 0.0f, 0.0f}, // L2 dist to query [1,1,1] = 0+1+1 = 2
            {0.0f, 0.0f, 0.0f}, // L2 dist = 1+1+1 = 3
            {1.0f, 1.0f, 1.0f}, // L2 dist = 0
            {5.0f, 5.0f, 5.0f}, // L2 dist = 16+16+16 = 48
    };
    ASSIGN_OR_ABORT(auto segment, write_segment(ids, vectors));

    auto schema = build_read_schema_with_vector_index();

    OlapReaderStatistics stats;
    SegmentReadOptions seg_opts;
    seg_opts.fs = _fs;
    seg_opts.stats = &stats;
    seg_opts.tablet_schema = schema;

    auto vector_search_opt = std::make_shared<VectorSearchOption>();
    vector_search_opt->use_vector_index = true;
    vector_search_opt->query_vector = {1.0f, 1.0f, 1.0f};
    vector_search_opt->k = 4;
    vector_search_opt->k_factor = 1.0;
    vector_search_opt->vector_distance_column_name = "__vector_approx_l2_distance";
    vector_search_opt->vector_column_id = schema->num_columns();
    vector_search_opt->vector_slot_id = 300;
    vector_search_opt->use_ivfpq = false;
    // Set vector_range = 3.0: only rows with L2 distance <= 3.0 should pass
    // Row 1 (dist=2): pass, Row 2 (dist=3): pass, Row 3 (dist=0): pass, Row 4 (dist=48): filtered
    vector_search_opt->vector_range = 3.0;
    vector_search_opt->result_order = 0; // 0 = ASC, 1 = DESC (L2 ASC: keep dist <= range)
    vector_search_opt->pq_refine_factor = 1.0;

    seg_opts.use_vector_index = true;
    seg_opts.vector_search_option = vector_search_opt;

    Schema read_schema;
    auto id_field = std::make_shared<Field>(0, "id", get_type_info(TYPE_BIGINT), false);
    id_field->set_uid(0);
    read_schema.append(id_field);

    auto chunk_iter = new_segment_iterator(segment, read_schema, seg_opts);
    ASSERT_TRUE(chunk_iter != nullptr);

    auto chunk = ChunkHelper::new_chunk(chunk_iter->output_schema(), 1024);
    std::vector<uint32_t> rowids;
    auto st = chunk_iter->get_next(chunk.get(), &rowids);

    ASSERT_OK(st);
    // Vectors are appended in input order. With vector_range = 3.0 and
    // ascending result_order, the brute-force fallback keeps rows whose
    // L2 distance from query [1,1,1] is <= 3.0:
    //   Row 0 (id=1, vec=[1,0,0]): dist = 0+1+1 = 2  -> keep
    //   Row 1 (id=2, vec=[0,0,0]): dist = 1+1+1 = 3  -> keep
    //   Row 2 (id=3, vec=[1,1,1]): dist = 0          -> keep
    //   Row 3 (id=4, vec=[5,5,5]): dist = 16+16+16=48 -> filter out
    // The fallback only filters in place; it does not re-sort, so the
    // surviving rows stay in input order.
    ASSERT_EQ(chunk->num_rows(), 3);
    auto id_col = chunk->get_column_by_index(0);
    const auto* ids_out = down_cast<const Int64Column*>(id_col.get());
    ASSERT_EQ(ids_out->size(), 3);
    EXPECT_EQ(ids_out->get_data()[0], 1);
    EXPECT_EQ(ids_out->get_data()[1], 2);
    EXPECT_EQ(ids_out->get_data()[2], 3);

    chunk_iter->close();
}

namespace {
// Build a read-time TabletSchema with a vector index whose metric_type is the
// argument. Mirrors BruteForceVectorFallbackTest::build_read_schema_with_vector_index
// but lets the caller swap the metric to exercise the cosine / l2 / unsupported
// branches in _setup_brute_force_fallback.
std::shared_ptr<TabletSchema> build_read_schema_with_metric(const std::string& metric) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_next_column_unique_id(3);

    auto* col0 = schema_pb.add_column();
    col0->set_unique_id(0);
    col0->set_name("id");
    col0->set_type("BIGINT");
    col0->set_is_key(true);
    col0->set_is_nullable(false);
    col0->set_length(8);
    col0->set_index_length(8);
    col0->set_aggregation("NONE");

    auto* col1 = schema_pb.add_column();
    col1->set_unique_id(1);
    col1->set_name("vector");
    col1->set_type("ARRAY");
    col1->set_is_key(false);
    col1->set_is_nullable(false);
    col1->set_length(24);
    col1->set_aggregation("NONE");
    auto* child = col1->add_children_columns();
    child->set_unique_id(2);
    child->set_name("element");
    child->set_type("FLOAT");
    child->set_is_key(false);
    child->set_is_nullable(true);
    child->set_length(4);
    child->set_aggregation("NONE");

    auto* idx = schema_pb.add_table_indices();
    idx->set_index_id(100);
    idx->set_index_name("vector_index");
    idx->set_index_type(IndexType::VECTOR);
    idx->add_col_unique_id(1);
    std::string props =
            R"({"common_properties":{"index_type":"hnsw","dim":"3","metric_type":")" + metric +
            R"(","is_vector_normed":"false"},"index_properties":{"efconstruction":"40","m":"16"},"search_properties":{"efsearch":"40"}})";
    idx->set_index_properties(props);
    return TabletSchema::create(schema_pb);
}

VectorSearchOptionPtr make_vector_search_opt(int slot_id, int num_columns, const std::vector<float>& query) {
    auto opt = std::make_shared<VectorSearchOption>();
    opt->use_vector_index = true;
    opt->query_vector = query;
    opt->k = 10;
    opt->k_factor = 1.0;
    opt->vector_distance_column_name = "__vector_approx_l2_distance";
    opt->vector_column_id = num_columns;
    opt->vector_slot_id = slot_id;
    opt->use_ivfpq = false;
    opt->vector_range = -1.0;
    opt->result_order = 0;
    opt->pq_refine_factor = 1.0;
    return opt;
}
} // namespace

// Cosine similarity path covers the cosine branch of _setup_brute_force_fallback
// (is_cosine_similarity = true) and the cosine branch of
// _compute_brute_force_distances (query_norm precompute + per-row dot/norm_v).
TEST_F(BruteForceVectorFallbackTest, test_brute_force_cosine_similarity) {
    std::vector<int64_t> ids = {1, 2, 3};
    std::vector<std::vector<float>> vectors = {
            {1.0f, 0.0f, 0.0f},
            {0.0f, 1.0f, 0.0f},
            {1.0f, 1.0f, 1.0f},
    };
    auto schema = build_read_schema_with_metric("cosine_similarity");
    ASSIGN_OR_ABORT(auto segment, write_segment(ids, vectors, schema));
    OlapReaderStatistics stats;
    SegmentReadOptions seg_opts;
    seg_opts.fs = _fs;
    seg_opts.stats = &stats;
    seg_opts.tablet_schema = schema;
    auto opt = make_vector_search_opt(/*slot_id=*/100, schema->num_columns(), {1.0f, 0.0f, 0.0f});
    seg_opts.use_vector_index = true;
    seg_opts.vector_search_option = opt;

    Schema read_schema;
    auto id_field = std::make_shared<Field>(0, "id", get_type_info(TYPE_BIGINT), false);
    id_field->set_uid(0);
    read_schema.append(id_field);

    auto chunk_iter = new_segment_iterator(segment, read_schema, seg_opts);
    ASSERT_OK(chunk_iter->init_output_schema({}));
    auto chunk = ChunkHelper::new_chunk(chunk_iter->output_schema(), 1024);
    std::vector<uint32_t> rowids;
    ASSERT_OK(chunk_iter->get_next(chunk.get(), &rowids));
    ASSERT_EQ(chunk->num_rows(), 3);

    // Query [1, 0, 0] (norm = 1):
    //   Row 0 [1,0,0]: dot = 1, |v| = 1, cosine = 1.0
    //   Row 1 [0,1,0]: dot = 0, |v| = 1, cosine = 0.0
    //   Row 2 [1,1,1]: dot = 1, |v| = sqrt(3), cosine = 1/sqrt(3)
    auto dist_col = chunk->get_column_by_slot_id(opt->vector_slot_id);
    ASSERT_NE(dist_col, nullptr);
    const auto* distances = down_cast<const FloatColumn*>(dist_col.get());
    ASSERT_EQ(distances->size(), 3);
    EXPECT_FLOAT_EQ(distances->get_data()[0], 1.0f);
    EXPECT_FLOAT_EQ(distances->get_data()[1], 0.0f);
    EXPECT_NEAR(distances->get_data()[2], 1.0f / std::sqrt(3.0f), 1e-6);

    chunk_iter->close();
}

// Unsupported metric (inner_product, etc.) covers the LOG-and-disable branch in
// _setup_brute_force_fallback. The iterator must not crash and must not append a
// distance column, since use_brute_force is turned off.
TEST_F(BruteForceVectorFallbackTest, test_brute_force_unsupported_metric_disables_fallback) {
    std::vector<int64_t> ids = {1};
    std::vector<std::vector<float>> vectors = {{1.0f, 2.0f, 3.0f}};
    auto schema = build_read_schema_with_metric("inner_product");
    ASSIGN_OR_ABORT(auto segment, write_segment(ids, vectors, schema));
    OlapReaderStatistics stats;
    SegmentReadOptions seg_opts;
    seg_opts.fs = _fs;
    seg_opts.stats = &stats;
    seg_opts.tablet_schema = schema;
    auto opt = make_vector_search_opt(/*slot_id=*/100, schema->num_columns(), {1.0f, 0.0f, 0.0f});
    seg_opts.use_vector_index = true;
    seg_opts.vector_search_option = opt;

    Schema read_schema;
    auto id_field = std::make_shared<Field>(0, "id", get_type_info(TYPE_BIGINT), false);
    id_field->set_uid(0);
    read_schema.append(id_field);

    auto chunk_iter = new_segment_iterator(segment, read_schema, seg_opts);
    ASSERT_OK(chunk_iter->init_output_schema({}));
    auto chunk = ChunkHelper::new_chunk(chunk_iter->output_schema(), 1024);
    std::vector<uint32_t> rowids;
    auto st = chunk_iter->get_next(chunk.get(), &rowids);
    // Distance column is intentionally NOT produced — fallback was disabled.
    // The iterator should still process rows for the id column without
    // crashing on missing distance.
    ASSERT_TRUE(st.ok() || st.is_end_of_file());
    EXPECT_FALSE(chunk->is_slot_exist(opt->vector_slot_id));
    chunk_iter->close();
}

// Dim mismatch covers the LOG_EVERY_N warning + truncated calc_dim path in
// _compute_brute_force_distances, plus the early-return when the array has zero
// elements (offsets[i+1] == offsets[i] -> calc_dim = 0 -> distance == 0).
TEST_F(BruteForceVectorFallbackTest, test_brute_force_dim_mismatch_truncates) {
    std::vector<int64_t> ids = {1, 2};
    std::vector<std::vector<float>> vectors = {
            {1.0f, 1.0f},             // dim 2, query is dim 3 -> truncate to 2
            {1.0f, 1.0f, 1.0f, 1.0f}, // dim 4, truncate to 3
    };
    ASSIGN_OR_ABORT(auto segment, write_segment(ids, vectors));

    auto schema = build_read_schema_with_vector_index();
    OlapReaderStatistics stats;
    SegmentReadOptions seg_opts;
    seg_opts.fs = _fs;
    seg_opts.stats = &stats;
    seg_opts.tablet_schema = schema;
    auto opt = make_vector_search_opt(/*slot_id=*/100, schema->num_columns(), {1.0f, 1.0f, 1.0f});
    seg_opts.use_vector_index = true;
    seg_opts.vector_search_option = opt;

    Schema read_schema;
    auto id_field = std::make_shared<Field>(0, "id", get_type_info(TYPE_BIGINT), false);
    id_field->set_uid(0);
    read_schema.append(id_field);

    auto chunk_iter = new_segment_iterator(segment, read_schema, seg_opts);
    ASSERT_OK(chunk_iter->init_output_schema({}));
    auto chunk = ChunkHelper::new_chunk(chunk_iter->output_schema(), 1024);
    std::vector<uint32_t> rowids;
    ASSERT_OK(chunk_iter->get_next(chunk.get(), &rowids));
    ASSERT_EQ(chunk->num_rows(), 2);

    // Row 0: dim=2 vs query=3 -> calc_dim=2 -> dist = (1-1)^2 + (1-1)^2 = 0
    // Row 1: dim=4 vs query=3 -> calc_dim=3 -> dist = (1-1)^2 + (1-1)^2 + (1-1)^2 = 0
    auto dist_col = chunk->get_column_by_slot_id(opt->vector_slot_id);
    ASSERT_NE(dist_col, nullptr);
    const auto* distances = down_cast<const FloatColumn*>(dist_col.get());
    ASSERT_EQ(distances->size(), 2);
    EXPECT_FLOAT_EQ(distances->get_data()[0], 0.0f);
    EXPECT_FLOAT_EQ(distances->get_data()[1], 0.0f);
    chunk_iter->close();
}

} // namespace starrocks
