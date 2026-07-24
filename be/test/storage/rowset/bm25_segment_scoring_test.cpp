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

// Integration tests for BM25 Phase-2 in the SegmentIterator (_apply_bm25_scoring + score-column emission +
// top-k scan narrowing + delete pre-narrow). Builds a real duplicate-key segment carrying a builtin GIN
// DOCS_AND_FREQS index, runs Phase-1 (build_tablet_bm25_stats) to obtain the stats, then drives a full
// segment scan with bm25 options set and reads the synthesized __bm25_score column back. Covers:
//   - score-all (topk=0): every matched row is emitted with its exact BM25 score;
//   - top-k pushdown (topk>0): the scan is narrowed to this segment's top-k by score;
//   - delete pre-narrow: a deleted row never occupies a top-k slot.

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "base/string/slice.h"
#include "base/testutil/assert.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/datum_tuple.h"
#include "column/vectorized_fwd.h"
#include "fmt/format.h"
#include "fs/fs_memory.h"
#include "storage/chunk_helper.h"
#include "storage/index/inverted/builtin/bm25_scoring.h"
#include "storage/index/inverted/builtin/bm25_stats_provider.h"
#include "storage/index/inverted/inverted_index_common.h"
#include "storage/options.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_writer.h"
#include "storage/tablet_index.h"
#include "storage/tablet_schema.h"
#include "storage_primitive/bm25_search_option.h"
#include "storage_primitive/chunk_iterator.h"
#include "storage_primitive/column_predicate.h"
#include "storage_primitive/column_predicate_factory.h"
#include "storage_primitive/conjunctive_predicates.h"
#include "storage_primitive/disjunctive_predicates.h"
#include "types/datum.h"

namespace starrocks {

class Bm25SegmentScoringTest : public testing::Test {
public:
    const std::string kTestDir = "/bm25_segment_scoring_test";
    static constexpr SlotId kScoreSlot = 5;

protected:
    void SetUp() override {
        _fs = std::make_shared<MemoryFileSystem>();
        ASSERT_TRUE(_fs->create_dir(kTestDir).ok());
    }

    // (k1 INT key, doc VARCHAR) DUP_KEYS with a builtin GIN index on `doc` (uid 2). index_options defaults to
    // docs_and_freqs (scorable); pass "docs" for a freqs-less index that Phase-2 must reject.
    std::shared_ptr<TabletSchema> make_schema(
            const std::string& options_value = INVERTED_INDEX_OPTIONS_DOCS_AND_FREQS) {
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

        TabletIndex props;
        props.add_common_properties(INVERTED_IMP_KEY, TYPE_BUILTIN);
        props.add_index_properties(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_ENGLISH);
        props.add_index_properties(INVERTED_INDEX_OPTIONS_KEY, options_value);
        TabletIndexPB* idx = schema_pb.add_table_indices();
        idx->set_index_id(1);
        idx->set_index_name("gin_doc");
        idx->set_index_type(GIN);
        idx->add_col_unique_id(2);
        idx->set_index_properties(props.properties_to_json());
        return std::make_shared<TabletSchema>(schema_pb);
    }

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
        uint64_t a = 0, b = 0, c = 0;
        CHECK_OK(writer.finalize(&a, &b, &c));
        auto seg_or = Segment::open(_fs, FileInfo{filename}, 0, schema);
        CHECK(seg_or.ok()) << seg_or.status();
        return std::move(seg_or.value());
    }

    BM25SearchOptionPtr make_option(const std::string& query, int64_t topk) {
        auto opt = std::make_shared<BM25SearchOption>();
        opt->enable = true;
        opt->query = query;
        opt->column_id = "doc";
        opt->score_column_name = "__bm25_score";
        opt->score_slot_id = kScoreSlot;
        opt->topk = topk;
        opt->k1 = 1.2;
        opt->b = 0.75;
        return opt;
    }

    // Run a full bm25 scan and collect (k1 -> score) for every emitted row.
    std::unordered_map<int32_t, double> run_scan(const std::shared_ptr<TabletSchema>& schema,
                                                 const std::shared_ptr<Segment>& seg, const BM25SearchOptionPtr& opt,
                                                 const BM25StatsPtr& stats, DisjunctivePredicates delete_preds = {},
                                                 bool enable_gin_filter = true) {
        SegmentReadOptions seg_opts;
        seg_opts.fs = _fs;
        seg_opts.stats = &_stats;
        seg_opts.tablet_schema = schema;
        seg_opts.bm25_search_option = opt;
        seg_opts.bm25_stats = stats;
        seg_opts.delete_predicates = std::move(delete_preds);
        // A real BM25 top-k scan always carries a MATCH predicate the GIN index narrows _scan_range with, so
        // the pushdown path runs with gin filter on; the gin-off fallback is exercised by its own test.
        seg_opts.enable_gin_filter = enable_gin_filter;

        auto out_schema = ChunkHelper::convert_schema(schema);
        auto iter_or = seg->new_iterator(out_schema, seg_opts);
        CHECK(iter_or.ok()) << iter_or.status();
        auto iter = std::move(iter_or.value());

        std::unordered_map<int32_t, double> k1_to_score;
        auto chunk = ChunkFactory::new_chunk(out_schema, 1024);
        while (true) {
            chunk->reset();
            auto st = iter->get_next(chunk.get());
            if (st.is_end_of_file()) {
                break;
            }
            CHECK(st.ok()) << st;
            CHECK(chunk->is_slot_exist(kScoreSlot)) << "score column not emitted";
            const auto& score_col = chunk->get_column_by_slot_id(kScoreSlot);
            for (size_t i = 0; i < chunk->num_rows(); ++i) {
                int32_t k1 = chunk->get(i)[0].get_int32();
                k1_to_score[k1] = score_col->get(i).get_double();
            }
        }
        return k1_to_score;
    }

    std::shared_ptr<MemoryFileSystem> _fs;
    OlapReaderStatistics _stats;
    std::vector<std::unique_ptr<ColumnPredicate>> _pred_pool;
    int _seg_id = 0;
};

// topk=0 (score-all): every row is emitted and carries its exact BM25 score. Corpus mirrors the scorer
// unit test: row0 apple x2, row1 cherry x1, row2 apple x1 + cherry x3.
TEST_F(Bm25SegmentScoringTest, score_all_emits_exact_scores) {
    auto schema = make_schema();
    auto seg = build_segment(schema, {"apple banana apple", "banana cherry", "apple cherry cherry cherry"});

    auto opt = make_option("apple cherry", /*topk=*/0);
    ASSIGN_OR_ABORT(auto stats,
                    build_tablet_bm25_stats(*schema, *opt, {seg}, LakeIOOptions{}, /*use_page_cache=*/true, &_stats));

    auto scores = run_scan(schema, seg, opt, stats);
    ASSERT_EQ(3u, scores.size());

    // Expected scores computed from the Phase-1 stats + known tf/doc_len, term order {apple, cherry}.
    const double e0 = bm25_term(2, 3, stats->idf[0], *stats); // row0: apple tf2
    const double e1 = bm25_term(1, 2, stats->idf[1], *stats); // row1: cherry tf1
    const double e2 = bm25_term(1, 4, stats->idf[0], *stats) + bm25_term(3, 4, stats->idf[1], *stats); // row2
    EXPECT_NEAR(e0, scores[0], 1e-9);
    EXPECT_NEAR(e1, scores[1], 1e-9);
    EXPECT_NEAR(e2, scores[2], 1e-9);
    // row2 carries both terms (cherry tf3) -> it must be the top scorer.
    EXPECT_GT(scores[2], scores[0]);
    EXPECT_GT(scores[2], scores[1]);
}

// topk pushdown narrows the scan to this segment's top-k by score; lower-scoring rows are not emitted.
TEST_F(Bm25SegmentScoringTest, topk_narrows_scan_to_best_rows) {
    auto schema = make_schema();
    auto seg = build_segment(schema, {"apple banana apple", "banana cherry", "apple cherry cherry cherry"});

    auto opt = make_option("apple cherry", /*topk=*/2);
    ASSIGN_OR_ABORT(auto stats,
                    build_tablet_bm25_stats(*schema, *opt, {seg}, LakeIOOptions{}, /*use_page_cache=*/true, &_stats));

    auto scores = run_scan(schema, seg, opt, stats);
    ASSERT_EQ(2u, scores.size());
    // row2 is the unique top-1 and must survive; the emitted rows are the two highest scorers.
    EXPECT_TRUE(scores.count(2));
    for (const auto& [k1, sc] : scores) {
        EXPECT_GT(sc, 0.0);
    }
}

// enable_gin_filter off: _apply_inverted_index does not narrow _scan_range, so the MATCH predicate stays
// residual (evaluated per-chunk). A top-k pushdown would then let a partial MATCH_ALL match take a slot and be
// dropped afterwards -> under-return. BM25 must fall back to score-all: with topk=2 but gin off, all 3 rows are
// still scored (no per-segment top-k trim), and the coordinator TopN applies the limit.
TEST_F(Bm25SegmentScoringTest, gin_filter_off_falls_back_to_score_all) {
    auto schema = make_schema();
    auto seg = build_segment(schema, {"apple banana apple", "banana cherry", "apple cherry cherry cherry"});

    auto opt = make_option("apple cherry", /*topk=*/2);
    ASSIGN_OR_ABORT(auto stats,
                    build_tablet_bm25_stats(*schema, *opt, {seg}, LakeIOOptions{}, /*use_page_cache=*/true, &_stats));

    auto scores = run_scan(schema, seg, opt, stats, /*delete_preds=*/{}, /*enable_gin_filter=*/false);
    ASSERT_EQ(3u, scores.size()) << "gin off must score every matched row, not narrow to top-k";
    // Every row carries its exact score (same values as the score-all test), proving no top-k trim happened.
    const double e0 = bm25_term(2, 3, stats->idf[0], *stats);
    const double e1 = bm25_term(1, 2, stats->idf[1], *stats);
    const double e2 = bm25_term(1, 4, stats->idf[0], *stats) + bm25_term(3, 4, stats->idf[1], *stats);
    EXPECT_NEAR(e0, scores[0], 1e-9);
    EXPECT_NEAR(e1, scores[1], 1e-9);
    EXPECT_NEAR(e2, scores[2], 1e-9);
}

// Delete pre-narrow: a row removed by a storage delete predicate must not take a top-k slot. Delete row 2
// (the top scorer); with topk=2 the output is the next two live rows and never includes row 2.
TEST_F(Bm25SegmentScoringTest, delete_predicate_excluded_from_topk) {
    auto schema = make_schema();
    auto seg = build_segment(schema,
                             {"apple banana apple", "banana cherry", "apple cherry cherry cherry", "apple", "cherry"});

    auto opt = make_option("apple cherry", /*topk=*/2);
    ASSIGN_OR_ABORT(auto stats,
                    build_tablet_bm25_stats(*schema, *opt, {seg}, LakeIOOptions{}, /*use_page_cache=*/true, &_stats));

    // DELETE WHERE k1 == 2 (the top scorer). cid 0 is k1 in the query schema.
    auto out_schema = ChunkHelper::convert_schema(schema);
    const ColumnId k1_cid = out_schema.field(0)->id();
    auto* pred = new_column_eq_predicate(get_type_info(TYPE_INT), k1_cid, "2");
    _pred_pool.emplace_back(pred);
    ConjunctivePredicates conj;
    conj.add(pred);
    DisjunctivePredicates disj;
    disj.add(conj);

    auto scores = run_scan(schema, seg, opt, stats, std::move(disj));
    ASSERT_EQ(2u, scores.size());
    EXPECT_FALSE(scores.count(2)) << "deleted row 2 must not occupy a top-k slot";
}

// A DOCS-only GIN index has no term frequencies, so BM25 scoring is impossible. Phase-1 already rejects such
// an index, so hand-build a minimal stats object to push the scan into Phase-2, where _apply_bm25_scoring's
// has_freqs() gate must reject the freqs-less segment rather than silently mis-score.
TEST_F(Bm25SegmentScoringTest, docs_only_index_scan_is_rejected) {
    auto schema = make_schema(INVERTED_INDEX_OPTIONS_DOCS);
    auto seg = build_segment(schema, {"apple banana", "cherry"});
    auto opt = make_option("apple", /*topk=*/0);

    auto stats = std::make_shared<BM25Stats>();
    stats->N = 2;
    stats->avgdl = 2.0;
    stats->terms = {"apple"};
    stats->idf = {bm25_idf(2, 1)};

    SegmentReadOptions seg_opts;
    seg_opts.fs = _fs;
    seg_opts.stats = &_stats;
    seg_opts.tablet_schema = schema;
    seg_opts.bm25_search_option = opt;
    seg_opts.bm25_stats = stats;

    auto out_schema = ChunkHelper::convert_schema(schema);
    ASSIGN_OR_ABORT(auto iter, seg->new_iterator(out_schema, seg_opts));

    // Phase-2 runs during scan init, so the rejection surfaces on the first get_next.
    auto chunk = ChunkFactory::new_chunk(out_schema, 1024);
    auto st = iter->get_next(chunk.get());
    ASSERT_FALSE(st.ok());
    EXPECT_NE(std::string::npos, st.to_string().find("DOCS only")) << st.to_string();
}

} // namespace starrocks
