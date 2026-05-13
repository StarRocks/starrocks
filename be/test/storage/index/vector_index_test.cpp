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

#include <limits>

#include "fs/fs_factory.h"

#ifdef WITH_TENANN
#include <tenann/factory/ann_searcher_factory.h>
#include <tenann/factory/index_factory.h>
#endif

#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "column/column_helper.h"
#include "common/config_vector_index_fwd.h"
#include "fs/fs_memory.h"
#include "runtime/mem_pool.h"
#include "storage/index/index_descriptor.h"
#include "storage/index/vector/tenann/tenann_index_utils.h"
#include "storage/index/vector/vector_index_writer.h"
#include "storage/rowset/bitmap_index_reader.h"
#include "storage/rowset/bitmap_index_writer.h"
#include "storage/rowset/column_writer.h"
#include "storage/tablet_schema_helper.h"

namespace starrocks {

class VectorIndexWriterTest : public testing::Test {
public:
    VectorIndexWriterTest() = default;

protected:
    void SetUp() override {
        srand(GetCurrentTimeMicros());
        CHECK_OK(fs::remove_all(test_vector_index_dir));
        CHECK_OK(fs::create_directories(test_vector_index_dir));
        ASSIGN_OR_ABORT(_fs, FileSystemFactory::CreateSharedFromString(test_vector_index_dir));
    }

    void TearDown() override { fs::remove_all(test_vector_index_dir); }

    std::shared_ptr<FileSystem> _fs;
    const std::string test_vector_index_dir = "vector_tenann_builder_test";
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

    // Drives 11 rows through the writer. Caller picks expectations based on whether
    // they configured the index to land above or below the build threshold.
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

    // Threshold met: a real .vi file (or stub mark_word under WITH_TENANN=OFF) is produced.
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

    // Threshold not met: finish() short-circuits without writing a file. Readers
    // surface the missing file as NotFound and fall back to brute-force scan.
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

    void check_empty(const std::string& index_path) {
        auto res = _fs->new_random_access_file(index_path);
        CHECK_OK(res);
        const auto& index_file = res.value();
        auto data_res = index_file->read_all();
        CHECK_OK(data_res);
        ASSERT_EQ(data_res.value(), IndexDescriptor::mark_word);
    }
};

TEST_F(VectorIndexWriterTest, test_write_vector_index) {
    auto tablet_index = prepare_tablet_index();
    tablet_index->add_common_properties("index_type", "hnsw");
    tablet_index->add_common_properties("dim", "3");
    tablet_index->add_common_properties("is_vector_normed", "false");
    tablet_index->add_common_properties("metric_type", "l2_distance");
    // Force the writer to build the index immediately rather than wait for the
    // default (config_vector_index_default_build_threshold) row count. Without
    // this the 11 rows below would fall under the threshold and finish() would
    // short-circuit without producing a file.
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
        const auto& meta = status.value();

        // read and search index
        tenann::IndexReaderRef index_reader = tenann::IndexFactory::CreateReaderFromMeta(meta);
        auto ann_searcher = tenann::AnnSearcherFactory::CreateSearcherFromMeta(meta);
        ann_searcher->ReadIndex(index_path);
        ASSERT_TRUE(ann_searcher->is_index_loaded());
    } catch (tenann::Error& e) {
        LOG(WARNING) << e.what();
    }
#else
    check_empty(index_path);
#endif
}

// IVFPQ + threshold not met: finish() short-circuits and no .vi file is created.
// Readers surface the missing file as NotFound (handled by the brute-force fallback
// in segment_iterator); vacuum sees no vector_index_id recorded in segment_meta and
// has nothing to delete.
TEST_F(VectorIndexWriterTest, testwrite_with_empty_mark) {
    config::config_vector_index_default_build_threshold = 100;
    auto tablet_index = prepare_tablet_index();

    tablet_index->add_common_properties("index_type", "ivfpq");
    tablet_index->add_common_properties("dim", "3");
    tablet_index->add_common_properties("is_vector_normed", "false");
    tablet_index->add_common_properties("metric_type", "l2_distance");

    auto index_path = test_vector_index_dir + "/" + empty_index_name;
    write_vector_index_below_threshold(index_path, tablet_index);
}

// Helper: build an ArrayColumn with the given per-row float vectors.
static MutableColumnPtr make_array_column(const std::vector<std::vector<float>>& rows) {
    auto element = FixedLengthColumn<float>::create();
    auto offsets = UInt32Column::create();
    offsets->append(0);
    uint32_t cursor = 0;
    for (const auto& row : rows) {
        for (float v : row) element->append(v);
        cursor += static_cast<uint32_t>(row.size());
        offsets->append(cursor);
    }
    auto null_column = NullColumn::create(element->size(), 0);
    auto nullable = NullableColumn::create(std::move(element), std::move(null_column));
    return ArrayColumn::create(std::move(nullable), std::move(offsets));
}

// Regression for StarRocksTest issue 11268 case 1: a non-normalized vector
// must be rejected at INSERT time when cosine_similarity + is_vector_normed.
// validate_vector_index_input is the single check called from
// ArrayColumnWriter::append covering both sync and async write paths.
TEST_F(VectorIndexWriterTest, validate_rejects_non_normalized_cosine) {
    auto col = make_array_column({{1.0f, 2.0f, 3.0f, 4.0f, 5.0f}}); // sum² = 55
    auto st = validate_vector_index_input(*down_cast<ArrayColumn*>(col.get()), /*dim=*/5,
                                          /*is_input_normalized=*/true);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_invalid_argument()) << "expected InvalidArgument, got " << st.to_string();
    ASSERT_NE(st.message().find("not normalized"), std::string_view::npos)
            << "expected normalization message, got: " << st.message();
}

TEST_F(VectorIndexWriterTest, validate_accepts_normalized_cosine) {
    // Vector with sum² ~= 1.
    auto col = make_array_column({{0.1f, -0.3f, 0.4f, 0.5f, -0.7f}});
    CHECK_OK(validate_vector_index_input(*down_cast<ArrayColumn*>(col.get()), 5, true));
}

TEST_F(VectorIndexWriterTest, validate_rejects_dim_mismatch) {
    auto col = make_array_column({{1.0f, 2.0f, 3.0f}}); // 3 elems, expected 5
    auto st = validate_vector_index_input(*down_cast<ArrayColumn*>(col.get()), 5, false);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_invalid_argument()) << "expected InvalidArgument, got " << st.to_string();
    ASSERT_NE(st.message().find("dimensions of the vector"), std::string_view::npos)
            << "expected dim mismatch message, got: " << st.message();
}

// Without the cosine + normalized combination, sum² is not checked.
TEST_F(VectorIndexWriterTest, validate_skips_normalization_when_flag_off) {
    auto col = make_array_column({{1.0f, 2.0f, 3.0f, 4.0f, 5.0f}});
    CHECK_OK(validate_vector_index_input(*down_cast<ArrayColumn*>(col.get()), 5, false));
}

// Empty input is a no-op (mirrors original valid_input_vector behavior).
TEST_F(VectorIndexWriterTest, validate_empty_is_noop) {
    auto col = make_array_column({});
    CHECK_OK(validate_vector_index_input(*down_cast<ArrayColumn*>(col.get()), 5, true));
}

// Holds the wfile + meta backing an ArrayColumnWriter so the writer's
// borrowed references stay alive for the lifetime of the test body.
struct ArrayWriterFixture {
    std::shared_ptr<MemoryFileSystem> fs;
    std::unique_ptr<WritableFile> wfile;
    TabletColumn array_column;
    ColumnMetaPB meta;
    ColumnWriterOptions opts;
    std::unique_ptr<ColumnWriter> writer;
};

// Build an ArrayColumnWriter fixture with the given vector-index common
// properties. need_vector_index is left false so we exercise the validation
// path without instantiating VectorIndexWriter (which would need a
// standalone_index_file_paths entry and a real file).
static std::unique_ptr<ArrayWriterFixture> make_array_writer_with_vector_index(
        const std::vector<std::pair<std::string, std::string>>& common_props) {
    auto fx = std::make_unique<ArrayWriterFixture>();
    fx->fs = std::make_shared<MemoryFileSystem>();
    CHECK(fx->fs->create_dir("/vi_test").ok());
    auto wfile_status = fx->fs->new_writable_file("/vi_test/array.dat");
    CHECK(wfile_status.ok()) << wfile_status.status().to_string();
    fx->wfile = std::move(wfile_status.value());

    fx->array_column = create_array(0, /*is_nullable=*/false, /*length=*/24);
    TabletColumn float_column;
    float_column.set_unique_id(1);
    float_column.set_name("element");
    float_column.set_type(TYPE_FLOAT);
    float_column.set_is_nullable(true);
    float_column.set_length(4);
    float_column.set_index_length(4);
    fx->array_column.add_sub_column(float_column);

    fx->meta.set_column_id(0);
    fx->meta.set_unique_id(0);
    fx->meta.set_type(TYPE_ARRAY);
    fx->meta.set_length(24);
    fx->meta.set_encoding(DEFAULT_ENCODING);
    fx->meta.set_compression(LZ4_FRAME);
    fx->meta.set_is_nullable(false);

    auto* element_meta = fx->meta.add_children_columns();
    element_meta->set_column_id(1);
    element_meta->set_unique_id(1);
    element_meta->set_type(TYPE_FLOAT);
    element_meta->set_length(4);
    element_meta->set_encoding(DEFAULT_ENCODING);
    element_meta->set_compression(LZ4_FRAME);
    element_meta->set_is_nullable(true);

    TabletIndex tablet_index;
    TabletIndexPB index_pb;
    index_pb.set_index_id(0);
    index_pb.set_index_name("vector_index");
    index_pb.set_index_type(IndexType::VECTOR);
    index_pb.add_col_unique_id(1);
    tablet_index.init_from_pb(index_pb);
    for (const auto& [k, v] : common_props) {
        tablet_index.add_common_properties(k, v);
    }

    fx->opts.meta = &fx->meta;
    fx->opts.need_vector_index = false;
    fx->opts.tablet_index[IndexType::VECTOR] = std::move(tablet_index);

    auto writer_st = ColumnWriter::create(fx->opts, &fx->array_column, fx->wfile.get());
    CHECK(writer_st.ok()) << writer_st.status().to_string();
    fx->writer = std::move(writer_st.value());
    return fx;
}

// End-to-end regression for case 1 of PR #72382: a row written below the
// build threshold must still fail validation. The pre-fix code would have
// silently accepted bad data because TenAnnIndexBuilderProxy::add (the only
// validation site) was never reached.
TEST_F(VectorIndexWriterTest, array_column_writer_rejects_dim_mismatch) {
    auto fx = make_array_writer_with_vector_index({
            {"index_type", "hnsw"},
            {"dim", "5"},
            {"is_vector_normed", "false"},
            {"metric_type", "l2_distance"},
    });
    CHECK_OK(fx->writer->init());

    auto col = make_array_column({{1.0f, 2.0f, 3.0f}}); // dim=3, expected 5
    auto st = fx->writer->append(*col);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_invalid_argument()) << st.to_string();
    ASSERT_NE(st.message().find("dimensions of the vector"), std::string_view::npos) << st.message();
}

// End-to-end regression for case 1 of PR #72382: cosine + is_vector_normed=true
// must reject a non-normalized vector at ArrayColumnWriter::append regardless
// of the build threshold.
TEST_F(VectorIndexWriterTest, array_column_writer_rejects_non_normalized_cosine) {
    auto fx = make_array_writer_with_vector_index({
            {"index_type", "hnsw"},
            {"dim", "5"},
            {"is_vector_normed", "true"},
            {"metric_type", "cosine_similarity"},
    });
    CHECK_OK(fx->writer->init());

    auto col = make_array_column({{1.0f, 2.0f, 3.0f, 4.0f, 5.0f}}); // sum² = 55
    auto st = fx->writer->append(*col);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_invalid_argument()) << st.to_string();
    ASSERT_NE(st.message().find("not normalized"), std::string_view::npos) << st.message();
}

// boost::iequals lets the writer accept properties saved with mixed case
// (e.g. user enters `Cosine_Similarity` in DDL) so the writer and the index
// builder agree on whether normalization applies. Verifies normalization is
// still enforced under non-canonical casing.
TEST_F(VectorIndexWriterTest, array_column_writer_property_parsing_is_case_insensitive) {
    auto fx = make_array_writer_with_vector_index({
            {"index_type", "hnsw"},
            {"dim", "5"},
            {"is_vector_normed", "TRUE"},
            {"metric_type", "Cosine_Similarity"},
    });
    CHECK_OK(fx->writer->init());

    auto col = make_array_column({{1.0f, 2.0f, 3.0f, 4.0f, 5.0f}});
    auto st = fx->writer->append(*col);
    ASSERT_FALSE(st.ok()) << "expected normalization check to fire for mixed-case props";
    ASSERT_TRUE(st.is_invalid_argument()) << st.to_string();
    ASSERT_NE(st.message().find("not normalized"), std::string_view::npos) << st.message();
}

// is_vector_normed=true alone is not enough — normalization is only enforced
// when metric_type is also cosine_similarity. With l2_distance the writer
// must accept any vector of the right dim.
TEST_F(VectorIndexWriterTest, array_column_writer_accepts_unnormalized_when_metric_is_l2) {
    auto fx = make_array_writer_with_vector_index({
            {"index_type", "hnsw"},
            {"dim", "5"},
            {"is_vector_normed", "true"},
            {"metric_type", "l2_distance"},
    });
    CHECK_OK(fx->writer->init());

    auto col = make_array_column({{1.0f, 2.0f, 3.0f, 4.0f, 5.0f}});
    CHECK_OK(fx->writer->append(*col));
}

// Missing dim is fatal: without it the writer cannot validate input. The
// schema layer should never produce a vector index without dim, but the
// writer fails fast rather than silently skipping validation.
TEST_F(VectorIndexWriterTest, array_column_writer_rejects_missing_dim) {
    auto fx = make_array_writer_with_vector_index({
            {"index_type", "hnsw"},
            {"is_vector_normed", "false"},
            {"metric_type", "l2_distance"},
    });
    auto st = fx->writer->init();
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_invalid_argument()) << st.to_string();
    ASSERT_NE(st.message().find("dim"), std::string_view::npos) << st.message();
}

#ifdef WITH_TENANN
// --------------------------------------------------------------------------
// Adaptive ef_search
// --------------------------------------------------------------------------

class AdaptiveEfSearchTest : public testing::Test {
protected:
    void SetUp() override {
        _saved_enable = config::enable_vector_adaptive_search;
        _saved_alpha = config::vector_adaptive_ef_alpha;
        _saved_cap = config::vector_adaptive_ef_cap;
        _saved_baseline = config::vector_adaptive_ef_baseline_rows;
        config::enable_vector_adaptive_search = true;
        config::vector_adaptive_ef_alpha = 1.0;
        config::vector_adaptive_ef_cap = 8.0;
        config::vector_adaptive_ef_baseline_rows = 100000;
    }

    void TearDown() override {
        config::enable_vector_adaptive_search = _saved_enable;
        config::vector_adaptive_ef_alpha = _saved_alpha;
        config::vector_adaptive_ef_cap = _saved_cap;
        config::vector_adaptive_ef_baseline_rows = _saved_baseline;
    }

    bool _saved_enable = true;
    double _saved_alpha = 1.0;
    double _saved_cap = 8.0;
    int64_t _saved_baseline = 100000;
};

TEST_F(AdaptiveEfSearchTest, below_baseline_not_scaled) {
    // rows <= baseline: use max(user_ef, k) without scaling.
    EXPECT_EQ(100, compute_adaptive_ef_search(100, 100, 50000));
    EXPECT_EQ(100, compute_adaptive_ef_search(100, 100, 100000));
    // user_ef < k: floor at k.
    EXPECT_EQ(100, compute_adaptive_ef_search(40, 100, 50000));
    // user_ef > k: honor user_ef as the base.
    EXPECT_EQ(200, compute_adaptive_ef_search(200, 100, 50000));
}

TEST_F(AdaptiveEfSearchTest, log_scaling_above_baseline) {
    // 1M rows, baseline 100K, alpha=1.0 -> factor = 1 + log2(10) = 4.32.
    // With base max(100,100)=100, expect ef ~ 432.
    EXPECT_EQ(432, compute_adaptive_ef_search(100, 100, 1000000));
    // 200K rows -> factor = 1 + log2(2) = 2.0 -> ef = 200.
    EXPECT_EQ(200, compute_adaptive_ef_search(100, 100, 200000));
    // 500K rows -> factor = 1 + log2(5) ~ 3.32 -> ef = 332.
    EXPECT_EQ(332, compute_adaptive_ef_search(100, 100, 500000));
}

TEST_F(AdaptiveEfSearchTest, cap_applied_on_huge_segments) {
    // 12.8M rows: factor = 1 + log2(128) = 8.0 -> at cap, ef = 800.
    EXPECT_EQ(800, compute_adaptive_ef_search(100, 100, 12800000));
    // 100M rows: factor would exceed cap (= 1 + ~10), clamped to 8, ef = 800.
    EXPECT_EQ(800, compute_adaptive_ef_search(100, 100, 100000000));
}

TEST_F(AdaptiveEfSearchTest, result_clamped_to_int_max) {
    // Pathological config: extremely large user_ef combined with a high cap
    // could push `ef_base * factor` past INT_MAX. The cast must clamp instead
    // of relying on UB float-to-int conversion.
    config::vector_adaptive_ef_cap = 1e9;
    const int huge_ef = std::numeric_limits<int>::max() / 2;
    int result = compute_adaptive_ef_search(huge_ef, 1, /*rows=*/100000000);
    EXPECT_EQ(std::numeric_limits<int>::max(), result);
}

TEST_F(AdaptiveEfSearchTest, nonfinite_config_falls_back_to_base) {
    // `vector_adaptive_ef_alpha` / `vector_adaptive_ef_cap` are mutable doubles
    // and `strtod` accepts "nan"/"inf". A non-finite factor must short-circuit
    // before the float-to-int cast (which is UB on NaN).
    config::vector_adaptive_ef_alpha = std::numeric_limits<double>::quiet_NaN();
    EXPECT_EQ(100, compute_adaptive_ef_search(100, 100, 1000000));

    config::vector_adaptive_ef_alpha = std::numeric_limits<double>::infinity();
    EXPECT_EQ(100, compute_adaptive_ef_search(100, 100, 1000000));

    config::vector_adaptive_ef_alpha = 1.0;
    config::vector_adaptive_ef_cap = std::numeric_limits<double>::quiet_NaN();
    EXPECT_EQ(100, compute_adaptive_ef_search(100, 100, 1000000));
}

TEST_F(AdaptiveEfSearchTest, respects_max_ef_k_floor) {
    // query_k greater than user_ef: scaling base is query_k.
    // 1M rows, base=100, factor=4.32 -> ef=432 regardless of user_ef=40.
    EXPECT_EQ(432, compute_adaptive_ef_search(40, 100, 1000000));
    // user_ef=200, k=100 -> base=200, 1M rows -> ef=200 * 4.32 = 864.
    EXPECT_EQ(864, compute_adaptive_ef_search(200, 100, 1000000));
}

TEST_F(AdaptiveEfSearchTest, config_overrides_alpha_and_cap) {
    config::vector_adaptive_ef_alpha = 1.5;
    EXPECT_EQ(598, compute_adaptive_ef_search(100, 100, 1000000)); // 1 + 1.5*3.32 = 5.98

    config::vector_adaptive_ef_alpha = 1.0;
    config::vector_adaptive_ef_cap = 4.0;
    EXPECT_EQ(400, compute_adaptive_ef_search(100, 100, 1000000)); // capped at 4.0
}

TEST_F(AdaptiveEfSearchTest, apply_writes_scaled_value_to_meta) {
    tenann::IndexMeta meta;
    meta.search_params()[starrocks::index::vector::EF_SEARCH] = 100;

    apply_adaptive_ef_search(&meta, /*rows=*/1000000, /*k=*/100, /*user_set_ef=*/false);

    EXPECT_EQ(432, meta.search_params()[starrocks::index::vector::EF_SEARCH].get<int>());
}

TEST_F(AdaptiveEfSearchTest, apply_skipped_when_disabled) {
    config::enable_vector_adaptive_search = false;
    tenann::IndexMeta meta;
    meta.search_params()[starrocks::index::vector::EF_SEARCH] = 100;

    apply_adaptive_ef_search(&meta, 1000000, 100, false);

    EXPECT_EQ(100, meta.search_params()[starrocks::index::vector::EF_SEARCH].get<int>());
}

TEST_F(AdaptiveEfSearchTest, apply_skipped_when_user_set_ef) {
    tenann::IndexMeta meta;
    meta.search_params()[starrocks::index::vector::EF_SEARCH] = 100;

    apply_adaptive_ef_search(&meta, 1000000, 100, /*user_set_ef=*/true);

    EXPECT_EQ(100, meta.search_params()[starrocks::index::vector::EF_SEARCH].get<int>());
}

TEST_F(AdaptiveEfSearchTest, apply_noop_when_no_ef_in_meta) {
    tenann::IndexMeta meta;
    // No efSearch key set (non-HNSW index).
    apply_adaptive_ef_search(&meta, 1000000, 100, false);
    EXPECT_EQ(meta.search_params().find(starrocks::index::vector::EF_SEARCH), meta.search_params().end());
}

TEST_F(AdaptiveEfSearchTest, apply_noop_when_below_baseline) {
    tenann::IndexMeta meta;
    meta.search_params()[starrocks::index::vector::EF_SEARCH] = 100;

    apply_adaptive_ef_search(&meta, /*rows=*/50000, 100, false);

    EXPECT_EQ(100, meta.search_params()[starrocks::index::vector::EF_SEARCH].get<int>());
}

// ef_base = max(user_ef, query_k); when both are non-positive the helper must
// short-circuit to 0 so downstream `static_cast<int>` never sees a negative
// scaled value (which would then be clamped to INT_MAX by the cap check).
TEST_F(AdaptiveEfSearchTest, compute_returns_zero_when_ef_base_nonpositive) {
    EXPECT_EQ(0, compute_adaptive_ef_search(0, 0, 1000000));
    EXPECT_EQ(0, compute_adaptive_ef_search(-5, -3, 1000000));
    // Row count below baseline doesn't matter: the guard fires before baseline.
    EXPECT_EQ(0, compute_adaptive_ef_search(0, 0, 50));
}

// A non-positive baseline disables scaling entirely. Defends against
// pathological online retuning (`ADMIN SET FRONTEND CONFIG`-style edits to BE
// configs that set baseline to 0 or a negative value).
TEST_F(AdaptiveEfSearchTest, compute_no_scale_when_baseline_nonpositive) {
    config::vector_adaptive_ef_baseline_rows = 0;
    EXPECT_EQ(100, compute_adaptive_ef_search(100, 100, 10000000));

    config::vector_adaptive_ef_baseline_rows = -1;
    EXPECT_EQ(100, compute_adaptive_ef_search(100, 100, 10000000));
}

// alpha <= 0 produces factor <= 1.0 above baseline; we must NOT shrink ef
// below the base (faiss already guarantees `ef = max(efSearch, k)` internally,
// so shrinking would be silently overridden anyway).
TEST_F(AdaptiveEfSearchTest, compute_no_scale_when_alpha_zero) {
    config::vector_adaptive_ef_alpha = 0.0;
    EXPECT_EQ(100, compute_adaptive_ef_search(100, 100, 1000000));
    EXPECT_EQ(200, compute_adaptive_ef_search(200, 50, 100000000));
}

TEST_F(AdaptiveEfSearchTest, compute_no_scale_when_alpha_negative) {
    // A negative alpha would mathematically yield factor < 1.0 above baseline.
    // The `factor <= 1.0` guard must keep us at the floor.
    config::vector_adaptive_ef_alpha = -1.0;
    EXPECT_EQ(100, compute_adaptive_ef_search(100, 100, 1000000));
    EXPECT_EQ(100, compute_adaptive_ef_search(100, 100, 100000000));
}

// segment_num_rows = 0 is unusual but legal: the baseline check (rows <=
// baseline) short-circuits before any log2 is evaluated, avoiding log2(0) = -inf.
TEST_F(AdaptiveEfSearchTest, compute_no_scale_when_rows_zero) {
    EXPECT_EQ(100, compute_adaptive_ef_search(100, 100, 0));
}

// efSearch stored as a JSON string ("100") must still drive adaptive scaling.
// tenann meta serialization can round-trip integers as strings depending on the
// caller; apply_adaptive_ef_search must accept both shapes.
TEST_F(AdaptiveEfSearchTest, apply_scales_when_ef_is_string) {
    tenann::IndexMeta meta;
    meta.search_params()[starrocks::index::vector::EF_SEARCH] = std::string("100");

    apply_adaptive_ef_search(&meta, /*rows=*/1000000, /*k=*/100, /*user_set_ef=*/false);

    // Effective ef is rewritten as an int (matches compute_adaptive_ef_search return).
    EXPECT_EQ(432, meta.search_params()[starrocks::index::vector::EF_SEARCH].get<int>());
}

// A malformed string in efSearch must be tolerated: stoi throws, we swallow and
// leave the original value alone rather than crashing the query path.
TEST_F(AdaptiveEfSearchTest, apply_skipped_when_ef_string_invalid) {
    tenann::IndexMeta meta;
    meta.search_params()[starrocks::index::vector::EF_SEARCH] = std::string("not-a-number");

    apply_adaptive_ef_search(&meta, 1000000, 100, false);

    EXPECT_EQ("not-a-number", meta.search_params()[starrocks::index::vector::EF_SEARCH].get<std::string>());
}

// JSON value of an unexpected shape (float, bool, array) must be ignored so we
// don't silently coerce non-integer types into ef_search.
TEST_F(AdaptiveEfSearchTest, apply_skipped_when_ef_is_unsupported_type) {
    {
        // Float: is_number_integer() == false, is_string() == false.
        tenann::IndexMeta meta;
        meta.search_params()[starrocks::index::vector::EF_SEARCH] = 100.5;
        apply_adaptive_ef_search(&meta, 1000000, 100, false);
        EXPECT_DOUBLE_EQ(100.5, meta.search_params()[starrocks::index::vector::EF_SEARCH].get<double>());
    }
    {
        tenann::IndexMeta meta;
        meta.search_params()[starrocks::index::vector::EF_SEARCH] = true;
        apply_adaptive_ef_search(&meta, 1000000, 100, false);
        EXPECT_EQ(true, meta.search_params()[starrocks::index::vector::EF_SEARCH].get<bool>());
    }
    {
        tenann::IndexMeta meta;
        meta.search_params()[starrocks::index::vector::EF_SEARCH] = std::vector<int>{1, 2, 3};
        apply_adaptive_ef_search(&meta, 1000000, 100, false);
        EXPECT_TRUE(meta.search_params()[starrocks::index::vector::EF_SEARCH].is_array());
    }
}

// efSearch present but non-positive: skip scaling and leave value alone. A
// non-positive ef wouldn't survive faiss anyway, but the helper must not
// rewrite it (e.g. into a scaled value that hides the real misconfiguration).
TEST_F(AdaptiveEfSearchTest, apply_skipped_when_user_ef_nonpositive) {
    {
        tenann::IndexMeta meta;
        meta.search_params()[starrocks::index::vector::EF_SEARCH] = 0;
        apply_adaptive_ef_search(&meta, 1000000, 100, false);
        EXPECT_EQ(0, meta.search_params()[starrocks::index::vector::EF_SEARCH].get<int>());
    }
    {
        tenann::IndexMeta meta;
        meta.search_params()[starrocks::index::vector::EF_SEARCH] = -10;
        apply_adaptive_ef_search(&meta, 1000000, 100, false);
        EXPECT_EQ(-10, meta.search_params()[starrocks::index::vector::EF_SEARCH].get<int>());
    }
}

// When the formula produces a value identical to user_ef the helper must skip
// the assignment entirely. We force this with alpha=0 + above-baseline rows so
// factor==1.0 and compute returns the unmodified ef_base. The distinguishing
// signal is the JSON value's type stability: we initialize with a string and
// verify it stays a string (a redundant write would coerce it to int).
TEST_F(AdaptiveEfSearchTest, apply_no_rewrite_when_effective_equals_user) {
    config::vector_adaptive_ef_alpha = 0.0;
    tenann::IndexMeta meta;
    meta.search_params()[starrocks::index::vector::EF_SEARCH] = std::string("100");

    apply_adaptive_ef_search(&meta, /*rows=*/10000000, /*k=*/100, /*user_set_ef=*/false);

    // Still a string — the helper exited via the `eff_ef == user_ef` branch
    // before assigning an int back to params.
    EXPECT_TRUE(meta.search_params()[starrocks::index::vector::EF_SEARCH].is_string());
    EXPECT_EQ("100", meta.search_params()[starrocks::index::vector::EF_SEARCH].get<std::string>());
}

#endif // WITH_TENANN

} // namespace starrocks
