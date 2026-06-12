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
#include "column/chunk_factory.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "common/config_rowset_fwd.h"
#include "common/config_vector_index_fwd.h"
#include "fs/fs_factory.h"
#include "fs/fs_memory.h"
#include "gen_cpp/tablet_schema.pb.h"
#include "gutil/casts.h"
#include "gutil/walltime.h"
#include "runtime/mem_pool.h"
#include "storage/chunk_helper.h"
#include "storage/column_predicate.h"
#include "storage/index/index_descriptor.h"
#include "storage/index/vector/tenann/del_id_filter.h"
#include "storage/index/vector/tenann/tenann_index_utils.h"
#include "storage/index/vector/vector_filter_strategy.h"
#include "storage/index/vector/vector_index_reader.h"
#include "storage/index/vector/vector_index_reader_factory.h"
#include "storage/index/vector/vector_index_writer.h"
#include "storage/predicate_tree/predicate_tree.h"
#include "storage/primitive/vector_search_option.h"
#include "storage/rowset/bitmap_index_reader.h"
#include "storage/rowset/bitmap_index_writer.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_iterator.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_writer.h"
#include "storage/tablet_schema.h"
#include "storage/tablet_schema_helper.h"

namespace starrocks {

// Resolver truth table (design doc §4). Pure function; no tenann dependency (runs without WITH_TENANN).
TEST(AnnFilterResolverTest, truth_table) {
    using S = AnnFilterStrategy;
    auto mk = [](S user_choice, bool cfg, bool resid, bool above, bool exact, bool supp) {
        AnnFilterResolveInputs in;
        in.user_choice = user_choice;
        in.prefilter_enabled = cfg;
        in.has_residual = resid;
        in.has_above_predicate = above;
        in.exact_possible = exact;
        in.supports_filtered = supp;
        return resolve_ann_filter_strategy(in);
    };

    // ---- AUTO ----
    // no residual -> plain ANN top-k (exact + complete by construction)
    EXPECT_EQ(S::PRE, mk(S::AUTO, true, false, false, true, true));
    // residual + a predicate evaluated above the iterator -> BRUTE (completeness)
    EXPECT_EQ(S::BRUTE, mk(S::AUTO, true, true, true, true, true));
    // residual, no above-predicate, but bitmap cannot be made exact -> BRUTE (exactness)
    EXPECT_EQ(S::BRUTE, mk(S::AUTO, true, true, false, false, true));
    // residual, reader cannot do filtered search -> BRUTE
    EXPECT_EQ(S::BRUTE, mk(S::AUTO, true, true, false, true, false));
    // residual, config (kill-switch) off -> BRUTE (Doris-equivalent)
    EXPECT_EQ(S::BRUTE, mk(S::AUTO, false, true, false, true, true));
    // residual, exact + complete + reader-capable -> PRE (selectivity decided later, in execution)
    EXPECT_EQ(S::PRE, mk(S::AUTO, true, true, false, true, true));

    // ---- explicit PRE: may NOT bypass the completeness/exactness invariant ----
    EXPECT_EQ(S::PRE, mk(S::PRE, true, true, false, true, true));
    EXPECT_EQ(S::BRUTE, mk(S::PRE, true, true, true, true, true));   // above-predicate -> BRUTE
    EXPECT_EQ(S::BRUTE, mk(S::PRE, true, true, false, false, true)); // not exact -> BRUTE

    // ---- explicit POST / BRUTE (user-forced) ----
    EXPECT_EQ(S::POST, mk(S::POST, true, true, false, true, true));
    EXPECT_EQ(S::BRUTE, mk(S::BRUTE, true, true, false, true, true));
}

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

// HNSW + sq8 quantizer: build a real quantized .vi and search it. The query path is the same as
// the non-quantized index (the quantizer is a build/meta-time property); this pins that a quantized
// HNSW index builds and is searchable. The lossy index distance it returns is what the FE refine
// path (enable_vector_index_refine) re-ranks exactly on the full-precision vectors above the scan.
TEST_F(VectorIndexSearchTest, test_search_hnsw_quantizer_sq8) {
    auto tablet_index = prepare_tablet_index();
    tablet_index->add_common_properties("index_type", "hnsw");
    tablet_index->add_common_properties("dim", "3");
    tablet_index->add_common_properties("is_vector_normed", "false");
    tablet_index->add_common_properties("metric_type", "l2_distance");
    tablet_index->add_common_properties("index_build_threshold", "0");
    tablet_index->add_index_properties("efconstruction", "40");
    tablet_index->add_index_properties("m", "16");
    tablet_index->add_index_properties("quantizer", "sq8"); // non-flat -> quantized index
    tablet_index->add_search_properties("efsearch", "40");

    auto index_path = test_vector_index_dir + "/hnsw_sq8_index.vi";
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
        std::vector<int64_t> result_ids(kTopK);
        std::vector<float> result_distances(kTopK);
        SparseRange<> scan_range;
        DelIdFilter del_id_filter(scan_range);
        std::vector<float> query_vector = {1.0f, 2.0f, 3.0f};
        tenann::PrimitiveSeqView query_view =
                tenann::PrimitiveSeqView{.data = reinterpret_cast<uint8_t*>(query_vector.data()),
                                         .size = static_cast<uint32_t>(3),
                                         .elem_type = tenann::PrimitiveType::kFloatType};

        auto st = ann_reader->search(query_view, kTopK, result_ids.data(),
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

        auto chunk = ChunkFactory::new_chunk(ChunkHelper::convert_schema(write_schema), ids.size());
        for (auto id : ids) {
            chunk->columns()[0]->as_mutable_ptr()->append_datum(Datum(id));
        }
        for (const auto& vec : vectors) {
            DatumArray arr;
            for (float v : vec) {
                arr.emplace_back(Datum(v));
            }
            chunk->columns()[1]->as_mutable_ptr()->append_datum(Datum(arr));
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
    vector_search_opt->refine_distance = false;
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
    auto chunk = ChunkFactory::new_chunk(chunk_iter->output_schema(), 1024);
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

// Test: same missing-.vi segment as above, but refine_distance = true. The refine path recomputes
// the exact distance from the full-precision vectors ABOVE the scan, so the segment iterator must
// NOT set up the brute-force fallback. Contrast with test_brute_force_l2_distance_fallback (same
// setup, refine_distance = false), which appends a [id, distance] output: here no distance column
// is synthesized -- the iterator returns the raw read-schema rows for the upper layer to refine.
TEST_F(BruteForceVectorFallbackTest, test_refine_distance_missing_vi_skips_brute_fallback) {
    std::vector<int64_t> ids = {1, 2, 3};
    std::vector<std::vector<float>> vectors = {
            {1.0f, 2.0f, 3.0f},
            {4.0f, 5.0f, 6.0f},
            {0.0f, 0.0f, 0.0f},
    };
    ASSIGN_OR_ABORT(auto segment, write_segment(ids, vectors));
    ASSERT_FALSE(segment->skip_vector_index());

    auto schema = build_read_schema_with_vector_index();

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
    vector_search_opt->vector_column_id = schema->num_columns();
    vector_search_opt->vector_slot_id = 100;
    vector_search_opt->refine_distance = true; // refine path: no BE-produced distance column
    vector_search_opt->vector_range = -1.0;
    vector_search_opt->result_order = 0;
    vector_search_opt->pq_refine_factor = 1.0;

    seg_opts.use_vector_index = true;
    seg_opts.vector_search_option = vector_search_opt;

    // Read only the id column. On the trust path the brute fallback would still append a distance
    // column; on the refine path it must not, so the output stays exactly the read schema.
    Schema read_schema;
    auto id_field = std::make_shared<Field>(0, "id", get_type_info(TYPE_BIGINT), false);
    id_field->set_uid(0);
    read_schema.append(id_field);

    auto chunk_iter = new_segment_iterator(segment, read_schema, seg_opts);
    ASSERT_TRUE(chunk_iter != nullptr);
    ASSERT_OK(chunk_iter->init_output_schema({}));

    auto chunk = ChunkFactory::new_chunk(chunk_iter->output_schema(), 1024);
    std::vector<uint32_t> rowids;
    auto st = chunk_iter->get_next(chunk.get(), &rowids);
    ASSERT_OK(st);
    ASSERT_EQ(chunk->num_rows(), 3);

    // No brute-force fallback was set up, so no distance column was appended: output is id only,
    // and the distance slot is absent (the refine path produces no BE-side distance column).
    ASSERT_EQ(chunk->num_columns(), 1);
    ASSERT_FALSE(chunk->is_slot_exist(vector_search_opt->vector_slot_id));

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
    vector_search_opt->refine_distance = false;
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

    auto chunk = ChunkFactory::new_chunk(chunk_iter->output_schema(), 1024);
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
    vector_search_opt->refine_distance = false;
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

    auto chunk = ChunkFactory::new_chunk(chunk_iter->output_schema(), 1024);
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
    opt->refine_distance = false;
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
    auto chunk = ChunkFactory::new_chunk(chunk_iter->output_schema(), 1024);
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
    auto chunk = ChunkFactory::new_chunk(chunk_iter->output_schema(), 1024);
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
    auto chunk = ChunkFactory::new_chunk(chunk_iter->output_schema(), 1024);
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

// Production-shape test for the defensive ladder in _compute_brute_force_distances:
// FE plan pruned v from the read schema (lazy-mat HNSW + SELECT id), and .vi is
// missing, so _setup_brute_force_fallback re-adds v to BE's _schema; v lives in
// _dict_chunk only, not in the output chunk. The lookup must take the _dict_chunk
// branch and never return InternalError.
TEST_F(BruteForceVectorFallbackTest, test_brute_force_with_lazy_mat_pruned_embedding) {
    std::vector<int64_t> ids = {1, 2, 3};
    std::vector<std::vector<float>> vectors = {
            {0.0f, 0.0f, 0.0f}, // dist to [1,1,1] = 3
            {1.0f, 1.0f, 1.0f}, // dist to [1,1,1] = 0
            {2.0f, 2.0f, 2.0f}, // dist to [1,1,1] = 3
    };
    ASSIGN_OR_ABORT(auto segment, write_segment(ids, vectors));

    // Confirm the segment has a real .vi expectation (footer does NOT skip_vector_index);
    // the brute-force path here is the runtime-NotFound branch, not the footer-hint
    // branch. This pins down which fallback we're testing.
    ASSERT_FALSE(segment->skip_vector_index());

    auto schema = build_read_schema_with_vector_index();
    OlapReaderStatistics stats;
    SegmentReadOptions seg_opts;
    seg_opts.fs = _fs;
    seg_opts.stats = &stats;
    seg_opts.tablet_schema = schema;
    auto opt = make_vector_search_opt(/*slot_id=*/100, schema->num_columns(), {1.0f, 1.0f, 1.0f});
    seg_opts.use_vector_index = true;
    seg_opts.vector_search_option = opt;

    // Read schema with ONLY id — this models the FE plan after lazy-mat has pruned
    // the embedding column from the scan output (HNSW + SELECT id pattern). The
    // vector column does NOT appear in the read schema we hand to the iterator.
    Schema lazy_mat_read_schema;
    auto id_field = std::make_shared<Field>(0, "id", get_type_info(TYPE_BIGINT), false);
    id_field->set_uid(0);
    lazy_mat_read_schema.append(id_field);

    auto chunk_iter = new_segment_iterator(segment, lazy_mat_read_schema, seg_opts);
    // Freeze the output schema BEFORE the iterator's lazy init runs. This is the
    // production shape: TabletReader / OlapChunkSource pins output_schema early,
    // so _setup_brute_force_fallback's late _schema mutation cannot flow back into
    // the output chunk — exactly the condition under which the defensive ladder
    // matters.
    ASSERT_OK(chunk_iter->init_output_schema({}));

    auto chunk = ChunkFactory::new_chunk(chunk_iter->output_schema(), 1024);
    std::vector<uint32_t> rowids;
    auto st = chunk_iter->get_next(chunk.get(), &rowids);

    // Must NOT be the defensive InternalError branch — the brute-force path's
    // _dict_chunk must have the embedding column for the distance computation
    // to succeed. A regression that leaves _dict_chunk without v would surface
    // as a non-OK status here.
    ASSERT_OK(st);

    ASSERT_EQ(chunk->num_rows(), 3);

    // Critical: the output chunk has ONLY id + distance, NOT the embedding.
    // The embedding was read into _dict_chunk internally, consumed by the distance
    // kernel, and dropped before emit. A regression that propagates v to the
    // output (e.g. removes the FE-pruned schema's restriction) would fail here.
    ASSERT_EQ(chunk->num_columns(), 2);

    // L2 distances against [1,1,1]:
    //   id=1 vec=[0,0,0] -> 1+1+1 = 3
    //   id=2 vec=[1,1,1] -> 0
    //   id=3 vec=[2,2,2] -> 1+1+1 = 3
    auto dist_col = chunk->get_column_by_slot_id(opt->vector_slot_id);
    ASSERT_NE(dist_col, nullptr);
    const auto* distances = down_cast<const FloatColumn*>(dist_col.get());
    ASSERT_EQ(distances->size(), 3);
    EXPECT_FLOAT_EQ(distances->get_data()[0], 3.0f);
    EXPECT_FLOAT_EQ(distances->get_data()[1], 0.0f);
    EXPECT_FLOAT_EQ(distances->get_data()[2], 3.0f);

    chunk_iter->close();
}

// Direct unit tests for resolve_brute_force_vector_column. The "missing in both" branch is
// unreachable from production iteration (use_brute_force=true together with FE-pruned output
// implies _setup_brute_force_fallback added v to _schema, so _dict_chunk holds it), so the
// resolver's defensive InternalError is covered here instead — both to lift coverage and to
// pin the corruption guard against future regressions in the iterator.
namespace {

ChunkPtr make_chunk_with_cid(ColumnId cid) {
    ChunkPtr chunk = std::make_shared<Chunk>();
    ColumnPtr col = Int32Column::create();
    chunk->append_column(col, cid, /*is_column_id=*/true);
    return chunk;
}

} // namespace

TEST(ResolveBruteForceVectorColumnTest, prefers_output_chunk_when_present) {
    // Both chunks carry cid=7. Production semantics: when FE keeps v eager, _build_final_chunk
    // swaps v into the output chunk and the resolver must take that copy (not _dict_chunk's).
    auto chunk = make_chunk_with_cid(7);
    auto dict_chunk = make_chunk_with_cid(7);
    ASSIGN_OR_ABORT(auto col, resolve_brute_force_vector_column(chunk.get(), dict_chunk.get(), 7));
    EXPECT_EQ(col.get(), chunk->get_column_by_id(7).get());
    EXPECT_NE(col.get(), dict_chunk->get_column_by_id(7).get());
}

TEST(ResolveBruteForceVectorColumnTest, falls_back_to_dict_chunk_when_pruned_from_output) {
    // Output chunk does not carry v (FE pruned it after lazy-mat), _dict_chunk does (BE
    // re-added v in _setup_brute_force_fallback). Resolver must take the _dict_chunk copy.
    ChunkPtr chunk = std::make_shared<Chunk>();
    auto dict_chunk = make_chunk_with_cid(7);
    ASSIGN_OR_ABORT(auto col, resolve_brute_force_vector_column(chunk.get(), dict_chunk.get(), 7));
    EXPECT_EQ(col.get(), dict_chunk->get_column_by_id(7).get());
}

TEST(ResolveBruteForceVectorColumnTest, internal_error_when_missing_in_both) {
    // Both chunks lack the cid. Defensive branch must return InternalError rather than
    // letting Chunk::get_column_by_id default-insert and return _columns[0].
    ChunkPtr chunk = std::make_shared<Chunk>();
    ChunkPtr dict_chunk = std::make_shared<Chunk>();
    auto st = resolve_brute_force_vector_column(chunk.get(), dict_chunk.get(), 42);
    ASSERT_FALSE(st.ok());
    EXPECT_TRUE(st.status().is_internal_error());
    EXPECT_NE(st.status().to_string().find("vector column 42 missing"), std::string::npos);
    EXPECT_NE(st.status().to_string().find("late-materialization"), std::string::npos);
}

TEST(ResolveBruteForceVectorColumnTest, internal_error_when_dict_chunk_is_null) {
    // _context->_dict_chunk is a ChunkPtr; .get() returns nullptr if it was never set up.
    // Resolver must not dereference and must take the InternalError path.
    ChunkPtr chunk = std::make_shared<Chunk>();
    auto st = resolve_brute_force_vector_column(chunk.get(), /*dict_chunk=*/nullptr, 42);
    ASSERT_FALSE(st.ok());
    EXPECT_TRUE(st.status().is_internal_error());
}

#ifdef WITH_TENANN
// Validates the residual-predicate PRE-filter in SegmentIterator::_get_row_ranges_by_vector_index:
// with a real HNSW .vi present (use_vector_index=true), a scalar predicate on a column WITHOUT an exact
// index must be early-evaluated into the ANN candidate, so the returned top-k contains ONLY matching rows.
// Data: 8 rows, vec_i=[i,0,0,0], filter_col=i, query=[0,0,0,0] (L2 dist = i^2), predicate filter_col>=4.
//   - WITH pre-filter (this change): k=3 nearest among {4,5,6,7} = rows 4,5,6.
//   - WITHOUT it (post-filter): ANN picks rows 0,1,2 (filter<4) -> filtered out -> 0 rows (the bug).
class VectorResidualPrefilterTest : public testing::Test {
protected:
    void SetUp() override {
        CHECK_OK(fs::remove_all(kDir));
        CHECK_OK(fs::create_directories(kDir));
        ASSIGN_OR_ABORT(_fs, FileSystemFactory::CreateSharedFromString(kDir));
    }
    void TearDown() override { (void)fs::remove_all(kDir); }

    const std::string kDir = "vector_residual_prefilter_test";
    static constexpr int64_t kIndexId = 100;
    std::shared_ptr<FileSystem> _fs;

    TabletSchemaPB base_schema_pb() {
        TabletSchemaPB pb;
        pb.set_keys_type(DUP_KEYS);
        pb.set_next_column_unique_id(4);
        auto* c0 = pb.add_column();
        c0->set_unique_id(0);
        c0->set_name("id");
        c0->set_type("BIGINT");
        c0->set_is_key(true);
        c0->set_is_nullable(false);
        c0->set_length(8);
        c0->set_index_length(8);
        c0->set_aggregation("NONE");
        auto* c1 = pb.add_column();
        c1->set_unique_id(1);
        c1->set_name("vector");
        c1->set_type("ARRAY");
        c1->set_is_key(false);
        c1->set_is_nullable(false);
        c1->set_length(24);
        c1->set_aggregation("NONE");
        auto* child = c1->add_children_columns();
        child->set_unique_id(2);
        child->set_name("element");
        child->set_type("FLOAT");
        child->set_is_key(false);
        child->set_is_nullable(true);
        child->set_length(4);
        child->set_aggregation("NONE");
        auto* c3 = pb.add_column();
        c3->set_unique_id(3);
        c3->set_name("filter_col");
        c3->set_type("INT");
        c3->set_is_key(false);
        c3->set_is_nullable(false);
        c3->set_length(4);
        c3->set_index_length(4);
        c3->set_aggregation("NONE");
        return pb;
    }
    std::shared_ptr<TabletSchema> write_schema() { return TabletSchema::create(base_schema_pb()); }
    std::shared_ptr<TabletSchema> read_schema_with_index(const std::string& metric = "l2_distance") {
        auto pb = base_schema_pb();
        auto* idx = pb.add_table_indices();
        idx->set_index_id(kIndexId);
        idx->set_index_name("vector_index");
        idx->set_index_type(IndexType::VECTOR);
        idx->add_col_unique_id(1);
        idx->set_index_properties(R"({"common_properties":{"index_type":"hnsw","dim":"4","metric_type":")" + metric +
                                  R"(","is_vector_normed":"false"},)"
                                  R"("index_properties":{"efconstruction":"40","m":"16"},"search_properties":{"efsearch":"40"}})");
        return TabletSchema::create(pb);
    }
    ColumnPtr make_array_column(const std::vector<std::vector<float>>& vecs) {
        auto elem = FixedLengthColumn<float>::create();
        auto nulls = NullColumn::create();
        auto offsets = UInt32Column::create();
        offsets->append(0);
        uint32_t off = 0;
        for (const auto& v : vecs) {
            for (float f : v) {
                elem->append(f);
                nulls->append(0);
            }
            off += v.size();
            offsets->append(off);
        }
        auto nullable = NullableColumn::create(std::move(elem), std::move(nulls));
        return ArrayColumn::create(std::move(nullable), std::move(offsets));
    }

    // Optional knobs for run_residual_case; the defaults reproduce the original l2 fixture
    // (vecs[i] = {i,0,0,0}, all-zero query).
    struct ResidualCaseConfig {
        std::string metric = "l2_distance";
        std::vector<std::vector<float>> vecs; // empty -> default {i, 0, 0, 0}
        std::vector<float> query = {0.f, 0.f, 0.f, 0.f};
    };

    // Runs the residual-predicate ANN query and asserts the top-k contains only matching rows.
    // Both the PRE (early-eval) and POST (oversample + read-time filter) paths must return the same
    // correct result for this data.
    void run_residual_case(PredicateTree pred_tree, bool above_predicate, std::vector<int64_t>* out_ids,
                           AnnFilterStrategy user_choice = AnnFilterStrategy::AUTO, bool pred_col_late_mat = false,
                           int64_t* out_search_ns = nullptr, const ResidualCaseConfig* cfg_in = nullptr,
                           std::vector<float>* out_distances = nullptr) {
        const ResidualCaseConfig default_cfg;
        const ResidualCaseConfig& cfg = cfg_in != nullptr ? *cfg_in : default_cfg;
        const int N = 8;
        std::vector<int64_t> ids(N);
        std::vector<std::vector<float>> vecs(N);
        std::vector<int32_t> filt(N);
        for (int i = 0; i < N; i++) {
            ids[i] = i;
            vecs[i] = cfg.vecs.empty() ? std::vector<float>{static_cast<float>(i), 0.f, 0.f, 0.f} : cfg.vecs[i];
            filt[i] = i;
        }

        // write segment (id, vector, filter_col)
        auto wschema = write_schema();
        std::string seg_file = kDir + "/seg.dat";
        ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(seg_file));
        SegmentWriterOptions wopts;
        SegmentWriter writer(std::move(wfile), 0, wschema, wopts);
        auto chunk = ChunkFactory::new_chunk(ChunkHelper::convert_schema(wschema), N);
        for (int i = 0; i < N; i++) chunk->columns()[0]->as_mutable_ptr()->append_datum(Datum(ids[i]));
        for (int i = 0; i < N; i++) {
            DatumArray a;
            for (float f : vecs[i]) a.emplace_back(Datum(f));
            chunk->columns()[1]->as_mutable_ptr()->append_datum(Datum(a));
        }
        for (int i = 0; i < N; i++) chunk->columns()[2]->as_mutable_ptr()->append_datum(Datum(filt[i]));
        ASSERT_OK(writer.init());
        ASSERT_OK(writer.append_chunk(*chunk));
        uint64_t seg_sz = 0, idx_sz = 0, footer = 0;
        ASSERT_OK(writer.finalize(&seg_sz, &idx_sz, &footer));
        auto rschema = read_schema_with_index(cfg.metric);
        ASSIGN_OR_ABORT(auto segment, Segment::open(_fs, FileInfo{seg_file}, 0, rschema));

        // build the real HNSW .vi at exactly the path _init_ann_reader computes
        RowsetId rid;
        rid.init(2);
        std::string vi_path = IndexDescriptor::vector_index_file_path(kDir, rid.to_string(), 0, kIndexId);
        {
            auto tablet_index = std::make_shared<TabletIndex>();
            TabletIndexPB ipb;
            ipb.set_index_id(kIndexId);
            ipb.set_index_name("vector_index");
            ipb.set_index_type(IndexType::VECTOR);
            ipb.add_col_unique_id(1);
            tablet_index->init_from_pb(ipb);
            tablet_index->add_common_properties("index_type", "hnsw");
            tablet_index->add_common_properties("dim", "4");
            tablet_index->add_common_properties("is_vector_normed", "false");
            tablet_index->add_common_properties("metric_type", cfg.metric);
            tablet_index->add_common_properties("index_build_threshold", "0");
            tablet_index->add_index_properties("efconstruction", "40");
            tablet_index->add_index_properties("m", "16");
            tablet_index->add_search_properties("efsearch", "40");
            std::unique_ptr<VectorIndexWriter> viw;
            VectorIndexWriter::create(tablet_index, vi_path, true, &viw);
            ASSERT_OK(viw->init());
            ASSERT_OK(viw->append(*make_array_column(vecs)));
            uint64_t sz = 0;
            ASSERT_OK(viw->finish(&sz));
            ASSERT_GT(sz, 0);
        }
        ASSERT_TRUE(fs::path_exist(vi_path));

        // run SegmentIterator: ANN query [0,0,0,0], k=3, residual predicate filter_col >= 4
        OlapReaderStatistics stats;
        SegmentReadOptions seg_opts;
        seg_opts.fs = _fs;
        seg_opts.stats = &stats;
        seg_opts.tablet_schema = rschema;
        seg_opts.rowset_path = kDir;
        seg_opts.rowsetid = rid;

        auto vs = std::make_shared<VectorSearchOption>();
        vs->use_vector_index = true;
        vs->query_vector = cfg.query;
        vs->k = 3;
        vs->k_factor = 1.0;
        vs->vector_distance_column_name = "__vector_approx_l2_distance";
        vs->vector_column_id = rschema->num_columns();
        vs->vector_slot_id = 100;
        vs->refine_distance = false;
        vs->vector_range = -1.0;
        vs->result_order = 0;
        vs->pq_refine_factor = 1.0;
        vs->filter_strategy = static_cast<int>(user_choice);
        seg_opts.use_vector_index = true;
        seg_opts.vector_search_option = vs;
        seg_opts.has_predicate_above_iterator = above_predicate;
        seg_opts.enable_predicate_col_late_materialize = pred_col_late_mat;
        seg_opts.pred_tree = std::move(pred_tree);

        // output id + filter_col so we can assert the predicate held on every returned row
        Schema read_schema;
        auto idf = std::make_shared<Field>(0, "id", get_type_info(TYPE_BIGINT), false);
        idf->set_uid(0);
        read_schema.append(idf);
        auto ff = std::make_shared<Field>(2, "filter_col", get_type_info(TYPE_INT), false);
        ff->set_uid(3);
        read_schema.append(ff);

        auto it = new_segment_iterator(segment, read_schema, seg_opts);
        ASSERT_TRUE(it != nullptr);
        ASSERT_OK(it->init_output_schema({}));
        auto out = ChunkFactory::new_chunk(it->output_schema(), 16);
        auto st = it->get_next(out.get());
        ASSERT_TRUE(st.ok() || st.is_end_of_file()) << "get_next status: " << st.to_string();

        // Output-chunk integrity: every column must be exactly num_rows long. A short column is the
        // chunk-level signature of the crash an upstream ProjectOperator would hit (ColumnRef on a
        // shorter-than-num_rows column) -- e.g. the post-read ANN distance column appended out of sync
        // with a residual-filtered, multi-output-column read.
        for (size_t c = 0; c < out->num_columns(); c++) {
            ASSERT_EQ(out->get_column_by_index(c)->size(), out->num_rows())
                    << "output chunk column " << c << " size != num_rows (" << out->num_rows() << ")";
        }

        // Correctness (all paths): every returned row satisfies the residual predicate (filter_col>=4).
        // Returns the returned ids (ascending -- rows are emitted in rowid order) so each test asserts the
        // exact set. PRE returns the k=3 nearest matching {4,5,6}; brute (no upstream TopN in this UT)
        // returns ALL matching {4,5,6,7}. Without the filter the ANN would pick rows 0,1,2 (filter_col<4).
        const auto* idcol = down_cast<const Int64Column*>(out->get_column_by_index(0).get());
        const auto* fcol = down_cast<const Int32Column*>(out->get_column_by_index(1).get());
        std::vector<int64_t> got;
        for (size_t i = 0; i < out->num_rows(); i++) {
            EXPECT_GE(fcol->get_data()[i], 4) << "returned row violates residual predicate filter_col>=4";
            got.push_back(idcol->get_data()[i]);
        }
        // The ANN distance column is appended to the output chunk after the declared output columns
        // (id, filter_col). Expose its values so metric regressions (cosine scored as L2) are visible:
        // ids alone cannot catch them when every candidate is returned anyway.
        if (out_distances != nullptr) {
            ASSERT_EQ(out->num_columns(), 3u) << "expected the appended ANN distance column";
            const auto* dcol = down_cast<const FloatColumn*>(out->get_column_by_index(2).get());
            out_distances->assign(dcol->get_data().begin(), dcol->get_data().end());
        }
        it->close();
        if (out_ids != nullptr) *out_ids = got;
        // vector_search_timer accumulates ONLY inside the ANN search block; it stays exactly 0 when the
        // cardinality short-circuit skipped the search (a deterministic marker, not a timing assertion).
        if (out_search_ns != nullptr) *out_search_ns = stats.vector_search_timer;
    }
};

// Builds an AND(pred_tree) holding a single column predicate `filter_col >= 4`. The caller owns
// `pred` (the tree stores a raw pointer), so it must outlive the run_residual_case() call.
static PredicateTree make_ge4_tree(std::unique_ptr<ColumnPredicate>& pred) {
    pred.reset(new_column_ge_predicate(get_type_info(TYPE_INT), 2, "4"));
    PredicateAndNode root;
    root.add_child(PredicateColumnNode(pred.get()));
    return PredicateTree::create(std::move(root));
}

TEST_F(VectorResidualPrefilterTest, residual_predicate_prefilters_ann) {
    // filter_col >= 4 (single column, immediate). default config, no above-iterator predicate -> PRE
    // (filtered ANN). Returns exactly the k=3 nearest matching rows; id 7 (matching but farther) is
    // dropped by the segment-level k-limit.
    std::unique_ptr<ColumnPredicate> pred;
    std::vector<int64_t> ids;
    run_residual_case(make_ge4_tree(pred), /*above_predicate=*/false, &ids);
    EXPECT_EQ(ids, (std::vector<int64_t>{4, 5, 6}));
}

TEST_F(VectorResidualPrefilterTest, residual_config_off_routes_to_brute) {
    // config (kill-switch) off -> AUTO routes the residual to exact brute-force (Doris-equivalent),
    // NOT the approximate POST path. Brute has no segment-level k-limit -> all matching {4,5,6,7}.
    bool saved = config::enable_vector_index_residual_prefilter;
    config::enable_vector_index_residual_prefilter = false;
    DeferOp restore([&] { config::enable_vector_index_residual_prefilter = saved; });
    std::unique_ptr<ColumnPredicate> pred;
    std::vector<int64_t> ids;
    run_residual_case(make_ge4_tree(pred), /*above_predicate=*/false, &ids);
    EXPECT_EQ(ids, (std::vector<int64_t>{4, 5, 6, 7}));
}

TEST_F(VectorResidualPrefilterTest, residual_above_predicate_routes_to_brute) {
    // A predicate is evaluated above the iterator -> the resolver must route to exact brute-force
    // (completeness, design doc §2/§4): a segment-level ANN k-limit would under-return. Brute returns
    // ALL matching rows {4,5,6,7}; PRE would return only {4,5,6}.
    std::unique_ptr<ColumnPredicate> pred;
    std::vector<int64_t> ids;
    run_residual_case(make_ge4_tree(pred), /*above_predicate=*/true, &ids);
    EXPECT_EQ(ids, (std::vector<int64_t>{4, 5, 6, 7}));
}

TEST_F(VectorResidualPrefilterTest, residual_or_predicate_prefilters_ann) {
    // filter_col = 4 OR filter_col = 6 -- an OR compound, NOT present in the immediate-column map.
    // The whole-tree bitmap must evaluate it; the old immediate-map path ignores the OR, so the ANN
    // top-k (rows 0,1,2) post-filters to nothing (the Gap2 under-return). Matching rows are {4,6};
    // PRE returns both (k=3, only 2 match).
    std::unique_ptr<ColumnPredicate> eq4(new_column_eq_predicate(get_type_info(TYPE_INT), 2, "4"));
    std::unique_ptr<ColumnPredicate> eq6(new_column_eq_predicate(get_type_info(TYPE_INT), 2, "6"));
    PredicateOrNode or_node;
    or_node.add_child(PredicateColumnNode(eq4.get()));
    or_node.add_child(PredicateColumnNode(eq6.get()));
    PredicateAndNode root;
    root.add_child(std::move(or_node));
    std::vector<int64_t> ids;
    run_residual_case(PredicateTree::create(std::move(root)), /*above_predicate=*/false, &ids);
    EXPECT_EQ(ids, (std::vector<int64_t>{4, 6}));
}

TEST_F(VectorResidualPrefilterTest, explicit_brute_force_returns_all_matching) {
    // ann_filter_strategy = brute_force: force exact brute-force even though PRE would be safe here.
    // Brute has no segment-level k-limit -> all matching rows {4,5,6,7} (PRE/AUTO return only {4,5,6}).
    std::unique_ptr<ColumnPredicate> pred;
    std::vector<int64_t> ids;
    run_residual_case(make_ge4_tree(pred), /*above_predicate=*/false, &ids, AnnFilterStrategy::BRUTE);
    EXPECT_EQ(ids, (std::vector<int64_t>{4, 5, 6, 7}));
}

TEST_F(VectorResidualPrefilterTest, explicit_post_filter_oversamples) {
    // ann_filter_strategy = post_filter: the approximate opt-in. POST has no pre-filter bitmap; it
    // over-fetches search_k = k * k_factor * vector_index_residual_post_filter_oversample (3 * 3 = 9 >= N)
    // candidates by distance, then applies the read-time predicate filter_col>=4. The oversample is what
    // keeps POST from a total recall miss: without it, search_k=3 would return only the 3 nearest rows
    // (0,1,2), all of which fail the filter -> {}. With it, the over-fetched window reaches {4,5,6,7}.
    // POST stays approximate (a filter selective enough to survive past the oversampled window can still
    // under-return); AUTO/PRE/BRUTE remain the exact paths.
    std::unique_ptr<ColumnPredicate> pred;
    std::vector<int64_t> ids;
    run_residual_case(make_ge4_tree(pred), /*above_predicate=*/false, &ids, AnnFilterStrategy::POST);
    EXPECT_EQ(ids, (std::vector<int64_t>{4, 5, 6, 7}));
}

TEST_F(VectorResidualPrefilterTest, residual_with_predicate_col_late_materialize) {
    // Reproduces the cluster SIGSEGV: a residual predicate + predicate-column late materialization
    // (enable_predicate_col_late_materialize -- set from query_options on a real cluster, default-off in
    // UTs, which is why earlier UTs missed this) + a multi-output-column read (id + filter_col). The ANN
    // distance column is appended after the read; on this path the output chunk ends up mis-sized and an
    // upstream ColumnRef dereferences a short column. Expect no crash, every column == num_rows, and the
    // k=3 nearest matching rows {4,5,6}.
    std::unique_ptr<ColumnPredicate> pred;
    std::vector<int64_t> ids;
    run_residual_case(make_ge4_tree(pred), /*above_predicate=*/false, &ids, AnnFilterStrategy::AUTO,
                      /*pred_col_late_mat=*/true);
    EXPECT_EQ(ids, (std::vector<int64_t>{4, 5, 6}));
}

// `filter_col >= <value>` variant of make_ge4_tree, for the short-circuit cardinality cases.
static PredicateTree make_ge_tree(std::unique_ptr<ColumnPredicate>& pred, const char* value) {
    pred.reset(new_column_ge_predicate(get_type_info(TYPE_INT), 2, value));
    PredicateAndNode root;
    root.add_child(PredicateColumnNode(pred.get()));
    return PredicateTree::create(std::move(root));
}

TEST_F(VectorResidualPrefilterTest, cardinality_below_k_short_circuits_search) {
    // filter_col >= 6 -> candidate bitmap {6,7}, cardinality 2 < k=3. A top-k over <= k candidates must
    // return every candidate, so the filtered ANN search is a logical no-op: the short-circuit gate
    // skips it and scores the candidates exactly. Expect all candidates and NO search issued
    // (vector_search_timer accumulates only inside the search block -> exactly 0 when skipped).
    std::unique_ptr<ColumnPredicate> pred;
    std::vector<int64_t> ids;
    int64_t search_ns = -1;
    run_residual_case(make_ge_tree(pred, "6"), /*above_predicate=*/false, &ids, AnnFilterStrategy::AUTO,
                      /*pred_col_late_mat=*/false, &search_ns);
    EXPECT_EQ(ids, (std::vector<int64_t>{6, 7}));
    EXPECT_EQ(search_ns, 0) << "filtered ANN search ran despite cardinality < k";
}

TEST_F(VectorResidualPrefilterTest, cardinality_equal_k_short_circuits_search) {
    // Boundary: filter_col >= 5 -> cardinality 3 == k=3. The gate is <=, so the search is still a no-op
    // (all 3 candidates must be returned) and must be skipped.
    std::unique_ptr<ColumnPredicate> pred;
    std::vector<int64_t> ids;
    int64_t search_ns = -1;
    run_residual_case(make_ge_tree(pred, "5"), /*above_predicate=*/false, &ids, AnnFilterStrategy::AUTO,
                      /*pred_col_late_mat=*/false, &search_ns);
    EXPECT_EQ(ids, (std::vector<int64_t>{5, 6, 7}));
    EXPECT_EQ(search_ns, 0) << "filtered ANN search ran despite cardinality == k";
}

TEST_F(VectorResidualPrefilterTest, sparse_ratio_short_circuits_search) {
    // Ratio leg: threshold 0.5 over the 8-row segment allows up to 4 candidates. filter_col >= 4 ->
    // cardinality 4 > k=3 (the <=k leg does NOT fire) but 4 <= 0.5*8 -> the ratio leg skips the search.
    // The short-circuit path has no segment-level k-limit, so ALL candidates {4,5,6,7} come back (the
    // upstream TopN cuts k in a real query); the search path would return only {4,5,6}. Both are exact.
    double saved = config::vector_index_brute_selectivity_threshold;
    config::vector_index_brute_selectivity_threshold = 0.5;
    DeferOp restore([&] { config::vector_index_brute_selectivity_threshold = saved; });
    std::unique_ptr<ColumnPredicate> pred;
    std::vector<int64_t> ids;
    int64_t search_ns = -1;
    run_residual_case(make_ge_tree(pred, "4"), /*above_predicate=*/false, &ids, AnnFilterStrategy::AUTO,
                      /*pred_col_late_mat=*/false, &search_ns);
    EXPECT_EQ(ids, (std::vector<int64_t>{4, 5, 6, 7}));
    EXPECT_EQ(search_ns, 0) << "filtered ANN search ran despite ratio gate";
}

TEST_F(VectorResidualPrefilterTest, cosine_metric_survives_exact_rescan) {
    // Regression for the PRE-path metric bug: is_cosine_similarity used to be initialized ONLY by
    // _setup_brute_force_fallback (the BRUTE route), so the exact-rescan paths reached from PRE (the
    // cardinality short-circuit and the under-return count gate) scored a cosine index as L2.
    //
    // The data makes L2 and cosine DISAGREE so the wrong metric cannot hide:
    //   row 6 = [100,0,0,0]: huge-norm, parallel to the query -> cosine 1.0 (best), l2^2 9801 (worst)
    //   row 7 = [1,1,0,0]:   cosine 1/sqrt(2) ~ 0.707, l2^2 1
    // filter_col >= 6 -> bitmap {6,7}, cardinality 2 < k=3 -> short-circuit -> exact rescan. The
    // appended distance column must carry cosine similarities; with the bug it carried 9801 and 1.
    ResidualCaseConfig cfg;
    cfg.metric = "cosine_similarity";
    cfg.query = {1.f, 0.f, 0.f, 0.f};
    cfg.vecs.resize(8);
    for (int i = 0; i < 6; i++) {
        cfg.vecs[i] = {1.f, static_cast<float>(i + 2), 0.f, 0.f}; // non-zero, pairwise-distinct angles
    }
    cfg.vecs[6] = {100.f, 0.f, 0.f, 0.f};
    cfg.vecs[7] = {1.f, 1.f, 0.f, 0.f};

    std::unique_ptr<ColumnPredicate> pred;
    std::vector<int64_t> ids;
    int64_t search_ns = -1;
    std::vector<float> dist;
    run_residual_case(make_ge_tree(pred, "6"), /*above_predicate=*/false, &ids, AnnFilterStrategy::AUTO,
                      /*pred_col_late_mat=*/false, &search_ns, &cfg, &dist);
    EXPECT_EQ(ids, (std::vector<int64_t>{6, 7}));
    EXPECT_EQ(search_ns, 0) << "short-circuit did not fire";
    ASSERT_EQ(dist.size(), 2u);
    EXPECT_NEAR(dist[0], 1.0f, 1e-4) << "row 6 must score cosine 1.0, not l2 9801";
    EXPECT_NEAR(dist[1], 1.0f / std::sqrt(2.0f), 1e-4) << "row 7 must score cosine ~0.707, not l2 1";
}

TEST_F(VectorResidualPrefilterTest, above_gate_runs_search_path) {
    // Anti-overfire regression: filter_col >= 4 -> cardinality 4 > k=3, and the default ratio threshold
    // (0.01 -> 0.08 rows on this 8-row segment) cannot fire. The filtered ANN search must actually run
    // (timer > 0) and keep the segment-level k-limit: the k=3 nearest matching rows, not all 4.
    std::unique_ptr<ColumnPredicate> pred;
    std::vector<int64_t> ids;
    int64_t search_ns = -1;
    run_residual_case(make_ge_tree(pred, "4"), /*above_predicate=*/false, &ids, AnnFilterStrategy::AUTO,
                      /*pred_col_late_mat=*/false, &search_ns);
    EXPECT_EQ(ids, (std::vector<int64_t>{4, 5, 6}));
    EXPECT_GT(search_ns, 0) << "short-circuit gate fired above both thresholds";
}
#endif // WITH_TENANN

} // namespace starrocks
