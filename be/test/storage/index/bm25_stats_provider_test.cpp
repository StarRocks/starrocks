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

// End-to-end tests for build_tablet_bm25_stats: the storage-agnostic BM25 Phase-1 helper. Unlike
// bm25_scoring_test (which drives TabletLocalProvider directly off hand-opened GIN readers), these build
// REAL segments through SegmentWriter from a TabletSchema that declares a builtin GIN index, then let the
// helper resolve the column, tokenize with the index analyzer, open each segment's GIN reader, and fold
// N / avgdl / idf. Covers the happy path (values), k1/b passthrough, multi-segment folding + null-segment
// skip, empty-query early return, and the missing-column / no-GIN / docs-only error paths.

#include "storage/index/inverted/builtin/bm25_stats_provider.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "base/string/slice.h"
#include "base/testutil/assert.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "fmt/format.h"
#include "fs/fs_memory.h"
#include "storage/chunk_helper.h"
#include "storage/index/inverted/builtin/bm25_scoring.h"
#include "storage/index/inverted/inverted_index_common.h"
#include "storage/options.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_writer.h"
#include "storage/tablet_index.h"
#include "storage/tablet_schema.h"
#include "storage_primitive/bm25_search_option.h"
#include "types/datum.h"

namespace starrocks {

class Bm25StatsProviderTest : public testing::Test {
public:
    const std::string kTestDir = "/bm25_stats_provider_test";

protected:
    void SetUp() override {
        _fs = std::make_shared<MemoryFileSystem>();
        ASSERT_TRUE(_fs->create_dir(kTestDir).ok());
    }

    // (k1 INT key, doc VARCHAR) DUP_KEYS. When with_gin, `doc` (uid 2) carries a builtin GIN index whose
    // index_options is `options_value` (docs_and_freqs by default; pass "docs" for a freqs-less index) and
    // whose tokenizer is `parser_value` (english by default; pass "standard" for the Lucene StandardAnalyzer).
    std::shared_ptr<TabletSchema> make_schema(bool with_gin,
                                              const std::string& options_value = INVERTED_INDEX_OPTIONS_DOCS_AND_FREQS,
                                              const std::string& parser_value = INVERTED_INDEX_PARSER_ENGLISH) {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(DUP_KEYS);
        schema_pb.set_num_short_key_columns(1);
        schema_pb.set_num_rows_per_row_block(1024);
        schema_pb.set_next_column_unique_id(3);

        ColumnPB* k1 = schema_pb.add_column();
        k1->set_unique_id(1);
        k1->set_name("k1");
        k1->set_type("INT");
        k1->set_is_key(true);
        k1->set_length(4);
        k1->set_index_length(4);
        k1->set_is_nullable(false);

        ColumnPB* doc = schema_pb.add_column();
        doc->set_unique_id(2);
        doc->set_name("doc");
        doc->set_type("VARCHAR");
        doc->set_is_key(false);
        doc->set_length(1024);
        doc->set_index_length(4);
        doc->set_is_nullable(true);
        doc->set_aggregation("NONE");

        if (with_gin) {
            // Build the serialized index_properties JSON the same way a real schema does: populate a
            // TabletIndex and reuse its properties_to_json(); index_type + col_unique_id are separate PB fields.
            TabletIndex props;
            props.add_common_properties(INVERTED_IMP_KEY, TYPE_BUILTIN);
            props.add_index_properties(INVERTED_INDEX_PARSER_KEY, parser_value);
            if (!options_value.empty()) {
                props.add_index_properties(INVERTED_INDEX_OPTIONS_KEY, options_value);
            }
            TabletIndexPB* idx = schema_pb.add_table_indices();
            idx->set_index_id(1);
            idx->set_index_name("gin_doc");
            idx->set_index_type(GIN);
            idx->add_col_unique_id(2);
            idx->set_index_properties(props.properties_to_json());
        }
        return std::make_shared<TabletSchema>(schema_pb);
    }

    // Write a full segment (k1 = row index, doc = docs[i]) via SegmentWriter, then reopen it. A unique file
    // name per call avoids the (filename,offset) page-cache returning a stale segment.
    std::shared_ptr<Segment> build_segment(const std::shared_ptr<TabletSchema>& schema,
                                           const std::vector<std::string>& docs) {
        std::string filename = fmt::format("{}/seg_{}.dat", kTestDir, _seg_id++);
        auto wfile_or = _fs->new_writable_file(filename);
        CHECK(wfile_or.ok());
        SegmentWriterOptions opts;
        SegmentWriter writer(std::move(wfile_or.value()), 0, schema, opts);
        CHECK_OK(writer.init());

        auto chunk_schema = ChunkHelper::convert_schema(schema);
        auto chunk = ChunkFactory::new_chunk(chunk_schema, docs.size());
        auto cols = chunk->columns();
        for (size_t rid = 0; rid < docs.size(); ++rid) {
            cols[0]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(rid)));
            cols[1]->as_mutable_ptr()->append_datum(Datum(Slice(docs[rid])));
        }
        CHECK_OK(writer.append_chunk(*chunk));
        uint64_t file_size = 0, index_size = 0, footer_position = 0;
        CHECK_OK(writer.finalize(&file_size, &index_size, &footer_position));

        auto seg_or = Segment::open(_fs, FileInfo{filename}, 0, schema);
        CHECK(seg_or.ok()) << seg_or.status();
        return std::move(seg_or.value());
    }

    static BM25SearchOption option(const std::string& query, double k1 = 1.2, double b = 0.75,
                                   const std::string& column_id = "doc") {
        BM25SearchOption opt;
        opt.enable = true;
        opt.query = query;
        opt.column_id = column_id;
        opt.k1 = k1;
        opt.b = b;
        return opt;
    }

    std::shared_ptr<MemoryFileSystem> _fs;
    int _seg_id = 0;
};

// Happy path: one segment, corpus mirrors bm25_scoring's; the helper must resolve the column, tokenize
// "apple cherry", and return exact N / avgdl / idf / terms plus a fresh zero-seeded shared_threshold.
TEST_F(Bm25StatsProviderTest, single_segment_stats_are_exact) {
    auto schema = make_schema(/*with_gin=*/true);
    auto seg = build_segment(schema, {"apple banana apple", "banana cherry", "apple cherry cherry cherry"});
    ASSERT_EQ(3u, seg->num_rows());

    OlapReaderStatistics stats;
    auto opt = option("apple cherry");
    ASSIGN_OR_ABORT(auto result,
                    build_tablet_bm25_stats(*schema, opt, {seg}, LakeIOOptions{}, /*use_page_cache=*/true, &stats));
    ASSERT_NE(nullptr, result);

    EXPECT_EQ(3, result->N);
    EXPECT_NEAR(9.0 / 3.0, result->avgdl, 1e-9);
    ASSERT_EQ((std::vector<std::string>{"apple", "cherry"}), result->terms);
    ASSERT_EQ(2u, result->idf.size());
    EXPECT_NEAR(bm25_idf(3, 2), result->idf[0], 1e-9); // apple df 2 (rows 0,2)
    EXPECT_NEAR(bm25_idf(3, 2), result->idf[1], 1e-9); // cherry df 2 (rows 1,2)
    EXPECT_DOUBLE_EQ(1.2, result->k1);
    EXPECT_DOUBLE_EQ(0.75, result->b);
    // A tablet scan shares one WAND threshold across its segments; Phase-1 must hand back a fresh 0.0 one.
    ASSERT_NE(nullptr, result->shared_threshold);
    EXPECT_DOUBLE_EQ(0.0, result->shared_threshold->load());
}

// The option's k1/b are corpus-independent tuning knobs; they must reach the stats verbatim.
TEST_F(Bm25StatsProviderTest, k1_and_b_are_passed_through) {
    auto schema = make_schema(/*with_gin=*/true);
    auto seg = build_segment(schema, {"apple", "apple banana"});

    OlapReaderStatistics stats;
    auto opt = option("apple", /*k1=*/1.5, /*b=*/0.6);
    ASSIGN_OR_ABORT(auto result,
                    build_tablet_bm25_stats(*schema, opt, {seg}, LakeIOOptions{}, /*use_page_cache=*/true, &stats));
    EXPECT_DOUBLE_EQ(1.5, result->k1);
    EXPECT_DOUBLE_EQ(0.6, result->b);
}

// N / sum_len / per-term df must fold across the whole segment set, and a null (dropped) segment in the
// list is silently skipped. The tablet idf therefore differs from either segment computed alone.
TEST_F(Bm25StatsProviderTest, folds_across_segments_and_skips_null) {
    auto schema = make_schema(/*with_gin=*/true);
    auto seg1 = build_segment(schema, {"apple", "apple banana", "banana"}); // sum_len 4
    auto seg2 = build_segment(schema, {"apple cherry", "cherry"});          // sum_len 3

    OlapReaderStatistics stats;
    auto opt = option("apple banana cherry zzz");
    // A null segment (e.g. concurrently compacted away) between the two real ones must be dropped.
    ASSIGN_OR_ABORT(auto result, build_tablet_bm25_stats(*schema, opt, {seg1, nullptr, seg2}, LakeIOOptions{},
                                                         /*use_page_cache=*/true, &stats));

    EXPECT_EQ(5, result->N);
    EXPECT_NEAR(7.0 / 5.0, result->avgdl, 1e-9);
    ASSERT_EQ(4u, result->idf.size());
    EXPECT_NEAR(bm25_idf(5, 3), result->idf[0], 1e-9); // apple  df 2+1
    EXPECT_NEAR(bm25_idf(5, 2), result->idf[1], 1e-9); // banana df 2+0
    EXPECT_NEAR(bm25_idf(5, 2), result->idf[2], 1e-9); // cherry df 0+2
    EXPECT_NEAR(bm25_idf(5, 0), result->idf[3], 1e-9); // zzz    absent everywhere
    // Real folding happened: the tablet idf is not the single-segment idf.
    EXPECT_NE(bm25_idf(3, 2), result->idf[0]);
}

// Empty / stopword-only query tokenizes to nothing: the helper returns OK with empty N/avgdl/idf/terms
// (every score is then 0) and, per the early-return contract, does NOT allocate a shared_threshold.
TEST_F(Bm25StatsProviderTest, empty_query_returns_empty_stats) {
    auto schema = make_schema(/*with_gin=*/true);
    auto seg = build_segment(schema, {"apple banana", "cherry"});

    OlapReaderStatistics stats;
    auto opt = option("");
    ASSIGN_OR_ABORT(auto result,
                    build_tablet_bm25_stats(*schema, opt, {seg}, LakeIOOptions{}, /*use_page_cache=*/true, &stats));
    ASSERT_NE(nullptr, result);
    EXPECT_EQ(0, result->N);
    EXPECT_DOUBLE_EQ(0.0, result->avgdl);
    EXPECT_TRUE(result->idf.empty());
    EXPECT_TRUE(result->terms.empty());
    EXPECT_DOUBLE_EQ(1.2, result->k1);
    EXPECT_EQ(nullptr, result->shared_threshold);
}

// A column_id absent from the schema fails fast (field_index -> npos) rather than mis-scoring.
TEST_F(Bm25StatsProviderTest, unknown_column_id_errors) {
    auto schema = make_schema(/*with_gin=*/true);
    auto seg = build_segment(schema, {"apple", "banana"});

    OlapReaderStatistics stats;
    auto opt = option("apple", 1.2, 0.75, /*column_id=*/"does_not_exist");
    auto st = build_tablet_bm25_stats(*schema, opt, {seg}, LakeIOOptions{}, /*use_page_cache=*/true, &stats);
    ASSERT_FALSE(st.ok());
    EXPECT_TRUE(st.status().is_internal_error()) << st.status();
}

// A real column that carries no builtin GIN index cannot be BM25-scored: fail fast.
TEST_F(Bm25StatsProviderTest, column_without_gin_index_errors) {
    auto schema = make_schema(/*with_gin=*/false);

    OlapReaderStatistics stats;
    auto opt = option("apple");
    // The error fires during column/index resolution, before any segment is opened.
    auto st = build_tablet_bm25_stats(*schema, opt, {}, LakeIOOptions{}, /*use_page_cache=*/true, &stats);
    ASSERT_FALSE(st.ok());
    EXPECT_TRUE(st.status().is_internal_error()) << st.status();
}

// A GIN index built DOCS-only (no freqs) cannot be scored: the provider must reject it end-to-end rather
// than fold partial statistics. Exercises the has_freqs() gate through the full segment path.
TEST_F(Bm25StatsProviderTest, docs_only_index_errors) {
    auto schema = make_schema(/*with_gin=*/true, /*options_value=*/INVERTED_INDEX_OPTIONS_DOCS);
    auto seg = build_segment(schema, {"apple banana", "cherry"});

    OlapReaderStatistics stats;
    auto opt = option("apple");
    auto st = build_tablet_bm25_stats(*schema, opt, {seg}, LakeIOOptions{}, /*use_page_cache=*/true, &stats);
    ASSERT_FALSE(st.ok());
}

// A GIN index built with parser=standard tokenizes both the corpus (at write time) and the query (in
// Phase-1) through the Lucene StandardAnalyzer. Same simple corpus/query as the happy path, so the folded
// N / avgdl / idf are identical -- what this pins is that the standard-analyzer tokenizer branch is taken.
TEST_F(Bm25StatsProviderTest, standard_parser_tokenizes_query) {
    auto schema = make_schema(/*with_gin=*/true, INVERTED_INDEX_OPTIONS_DOCS_AND_FREQS, INVERTED_INDEX_PARSER_STANDARD);
    auto seg = build_segment(schema, {"apple banana apple", "banana cherry", "apple cherry cherry cherry"});

    OlapReaderStatistics stats;
    auto opt = option("apple cherry");
    ASSIGN_OR_ABORT(auto result,
                    build_tablet_bm25_stats(*schema, opt, {seg}, LakeIOOptions{}, /*use_page_cache=*/true, &stats));
    ASSERT_NE(nullptr, result);

    EXPECT_EQ(3, result->N);
    EXPECT_NEAR(9.0 / 3.0, result->avgdl, 1e-9);
    ASSERT_EQ((std::vector<std::string>{"apple", "cherry"}), result->terms);
    ASSERT_EQ(2u, result->idf.size());
    EXPECT_NEAR(bm25_idf(3, 2), result->idf[0], 1e-9); // apple df 2 (rows 0,2)
    EXPECT_NEAR(bm25_idf(3, 2), result->idf[1], 1e-9); // cherry df 2 (rows 1,2)
}

} // namespace starrocks
