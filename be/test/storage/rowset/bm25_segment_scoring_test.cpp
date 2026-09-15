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
#include "common/object_pool.h"
#include "exprs/expr_context.h"
#include "exprs/expr_executor.h"
#include "exprs/match_expr.h"
#include "fmt/format.h"
#include "fs/fs_memory.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
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
#include "storage_primitive/column_expr_predicate.h"
#include "storage_primitive/column_predicate.h"
#include "storage_primitive/column_predicate_factory.h"
#include "storage_primitive/conjunctive_predicates.h"
#include "storage_primitive/disjunctive_predicates.h"
#include "storage_primitive/predicate_tree/predicate_tree.h"
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

    // A scalar predicate on k1 (cid 0 in the query schema), the shape a `WHERE ... AND k1 >= 3` conjunct
    // lands in after predicate pushdown.
    ColumnPredicate* k1_pred(PredicateType op, const std::string& operand) {
        auto* pred = new_column_cmp_predicate(op, get_type_info(TYPE_INT), /*cid=*/0, operand);
        _pred_pool.emplace_back(pred);
        return pred;
    }

    // Same predicate flagged index-only. The read loop discards these, so they cannot drop a scored row;
    // the pre-fold must leave them alone because evaluating one against a dict-code-read column is a type
    // mismatch.
    ColumnPredicate* k1_index_only_pred(PredicateType op, const std::string& operand) {
        auto* pred = k1_pred(op, operand);
        pred->set_index_filter_only(true);
        return pred;
    }

    // A MATCH predicate on a column that carries no inverted index: _apply_inverted_index finds no
    // iterator for that cid and leaves the predicate in the tree, with _scan_range never narrowed by it --
    // the same state a failed seek_inverted_index leaves behind. MatchExpr::prepare is a no-op, so no live
    // index or slot machinery is needed to build one.
    ColumnPredicate* unconsumed_match_pred(ColumnId cid) {
        _runtime_state.init_instance_mem_tracker();
        // The predicate binds its chunk column to a slot id when it evaluates, so it needs a real slot.
        TDescriptorTableBuilder table_builder;
        TTupleDescriptorBuilder tuple_builder;
        TSlotDescriptorBuilder slot_builder;
        tuple_builder.add_slot(slot_builder.type(TYPE_INT).column_name("k1").column_pos(0).nullable(false).build());
        tuple_builder.build(&table_builder);
        DescriptorTbl* tbl = nullptr;
        CHECK_OK(DescriptorTbl::create(&_runtime_state, &_expr_pool, table_builder.desc_tbl(), &tbl,
                                       /*chunk_size=*/4096));
        SlotDescriptor* slot = tbl->get_tuple_descriptor(0)->slots()[0];

        TExprNode match_node;
        match_node.node_type = TExprNodeType::MATCH_EXPR;
        match_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
        match_node.num_children = 0;
        match_node.is_nullable = true;
        Expr* root = _expr_pool.add(new MatchExpr(match_node));
        // ColumnExprPredicate::_add_expr_ctx CHECKs that the context is already open.
        auto* ctx = _expr_pool.add(new ExprContext(root));
        CHECK_OK(ExprExecutor::prepare(ctx, &_runtime_state));
        CHECK_OK(ExprExecutor::open(ctx, &_runtime_state));
        auto pred_or = ColumnExprPredicate::make_column_expr_predicate(get_type_info(TYPE_INT), cid, &_runtime_state,
                                                                       ctx, slot);
        CHECK(pred_or.ok()) << pred_or.status();
        _pred_pool.emplace_back(pred_or.value());
        return pred_or.value();
    }

    static PredicateTree tree_of(const std::vector<ColumnPredicate*>& preds) {
        PredicateAndNode root;
        for (auto* pred : preds) {
            root.add_child(PredicateColumnNode(pred));
        }
        return PredicateTree::create(std::move(root));
    }

    // A root AND holding one OR child: the shape `MATCH 'x' AND (a = 1 OR b = 2)` lands in.
    static PredicateTree or_tree_of(const std::vector<ColumnPredicate*>& preds) {
        PredicateOrNode disjunction;
        for (auto* pred : preds) {
            disjunction.add_child(PredicateColumnNode(pred));
        }
        PredicateAndNode root;
        root.add_child(std::move(disjunction));
        return PredicateTree::create(std::move(root));
    }

    // Run a full bm25 scan and collect (k1 -> score) for every emitted row.
    std::unordered_map<int32_t, double> run_scan(const std::shared_ptr<TabletSchema>& schema,
                                                 const std::shared_ptr<Segment>& seg, const BM25SearchOptionPtr& opt,
                                                 const BM25StatsPtr& stats, DisjunctivePredicates delete_preds = {},
                                                 bool enable_gin_filter = true, PredicateTree pred_tree = {},
                                                 SparseRangePtr precomputed_range = nullptr,
                                                 bool has_predicate_above_iterator = false) {
        SegmentReadOptions seg_opts;
        seg_opts.fs = _fs;
        seg_opts.stats = &_stats;
        seg_opts.tablet_schema = schema;
        seg_opts.bm25_search_option = opt;
        seg_opts.bm25_stats = stats;
        seg_opts.delete_predicates = std::move(delete_preds);
        seg_opts.pred_tree = std::move(pred_tree);
        // A non-null prepared range takes the reuse path: _apply_precomputed_scan_range intersects it in and
        // every page filter, including the inverted index, is skipped.
        seg_opts.read_state_cache.scan_range = std::move(precomputed_range);
        // A real BM25 top-k scan always carries a MATCH predicate the GIN index narrows _scan_range with, so
        // the pushdown path runs with gin filter on; the gin-off fallback is exercised by its own test.
        seg_opts.enable_gin_filter = enable_gin_filter;
        // Set by the chunk source / scanner when a predicate is evaluated above the iterator. The scan
        // cannot fold those, so it must not truncate by rank.
        seg_opts.has_predicate_above_iterator = has_predicate_above_iterator;

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

    // Same setup as run_scan, but hands back the scan status instead of CHECK-failing: a MATCH the index
    // did not consume is fatal in the read loop, and that is precisely the shape this exercises.
    Status run_scan_status(const std::shared_ptr<TabletSchema>& schema, const std::shared_ptr<Segment>& seg,
                           const BM25SearchOptionPtr& opt, const BM25StatsPtr& stats, PredicateTree pred_tree) {
        SegmentReadOptions seg_opts;
        seg_opts.fs = _fs;
        seg_opts.stats = &_stats;
        seg_opts.tablet_schema = schema;
        seg_opts.bm25_search_option = opt;
        seg_opts.bm25_stats = stats;
        seg_opts.pred_tree = std::move(pred_tree);
        seg_opts.enable_gin_filter = true;

        auto out_schema = ChunkHelper::convert_schema(schema);
        auto iter_or = seg->new_iterator(out_schema, seg_opts);
        if (!iter_or.ok()) {
            return iter_or.status();
        }
        auto iter = std::move(iter_or.value());
        auto chunk = ChunkFactory::new_chunk(out_schema, 1024);
        while (true) {
            chunk->reset();
            auto st = iter->get_next(chunk.get());
            if (st.is_end_of_file()) {
                return Status::OK();
            }
            if (!st.ok()) {
                return st;
            }
        }
    }

    std::shared_ptr<MemoryFileSystem> _fs;
    OlapReaderStatistics _stats;
    ObjectPool _expr_pool;
    RuntimeState _runtime_state;
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

// The pruning-ratio counters must survive the WAND path. It reads its candidates straight off _scan_range
// instead of materializing a bitmap, so a candidate count taken from that bitmap would report 0 candidates
// beside N scored rows -- and only the WAND path prunes, so the ratio would be lost exactly where it matters.
TEST_F(Bm25SegmentScoringTest, topk_reports_candidate_and_scored_rows) {
    auto schema = make_schema();
    auto seg = build_segment(schema, {"apple banana apple", "banana cherry", "apple cherry cherry cherry"});

    auto opt = make_option("apple cherry", /*topk=*/2);
    ASSIGN_OR_ABORT(auto stats,
                    build_tablet_bm25_stats(*schema, *opt, {seg}, LakeIOOptions{}, /*use_page_cache=*/true, &_stats));

    auto scores = run_scan(schema, seg, opt, stats);
    ASSERT_EQ(2u, scores.size());
    // No MATCH predicate narrowed the scan here, so the candidate set is the whole segment.
    EXPECT_EQ(3, _stats.bm25_candidate_rows);
    EXPECT_GT(_stats.bm25_scored_rows, 0);
    EXPECT_LE(_stats.bm25_scored_rows, _stats.bm25_candidate_rows);
    EXPECT_EQ(1, _stats.bm25_segments_scored);
    EXPECT_EQ(0, _stats.bm25_segments_no_pruning);
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

// ---------------------------------------------------------------------------------------------------
// Scalar residual pre-fold. A scalar conjunct beside the full-text predicate used to be evaluated only in
// the read loop, i.e. after the per-segment top-k truncation, so a row that won a slot by score and then
// failed the conjunct left a hole and the LIMIT under-returned. The pre-fold evaluates that conjunct into
// the scan range before scoring instead, which both removes the hole and stops the scorer from paying for
// rows that cannot be returned.
//
// The corpus is the five-row one from the delete test, so the rows the predicates reject are the top
// scorers: row2 (apple + cherry x3) is the highest, row0 (apple x2) is next. Rejecting them is what makes
// these cases catch a gate that lets truncation happen too early.
// ---------------------------------------------------------------------------------------------------

namespace {
const std::vector<std::string> kFiveDocs = {"apple banana apple", "banana cherry", "apple cherry cherry cherry",
                                            "apple", "cherry"};
} // namespace

// The core case. `k1 >= 3` rejects the two best scorers, so the top-2 must come from {3, 4} and the count
// must still be 2. Without the pre-fold the scorer picks {2, 0} and the read loop drops both -> zero rows.
TEST_F(Bm25SegmentScoringTest, residual_excluded_from_topk) {
    auto schema = make_schema();
    auto seg = build_segment(schema, kFiveDocs);

    auto opt = make_option("apple cherry", /*topk=*/2);
    ASSIGN_OR_ABORT(auto stats,
                    build_tablet_bm25_stats(*schema, *opt, {seg}, LakeIOOptions{}, /*use_page_cache=*/true, &_stats));

    auto scores = run_scan(schema, seg, opt, stats, /*delete_preds=*/{}, /*enable_gin_filter=*/true,
                           tree_of({k1_pred(PredicateType::kGE, "3")}));
    ASSERT_EQ(2u, scores.size()) << "the residual must not leave holes in the top-k";
    EXPECT_TRUE(scores.count(3));
    EXPECT_TRUE(scores.count(4));
    EXPECT_FALSE(scores.count(2)) << "row 2 fails k1 >= 3 and must not occupy a top-k slot";
}

// Fewer survivors than the limit is not an error; the scan returns what there is.
TEST_F(Bm25SegmentScoringTest, residual_survivors_fewer_than_topk) {
    auto schema = make_schema();
    auto seg = build_segment(schema, kFiveDocs);

    auto opt = make_option("apple cherry", /*topk=*/3);
    ASSIGN_OR_ABORT(auto stats,
                    build_tablet_bm25_stats(*schema, *opt, {seg}, LakeIOOptions{}, /*use_page_cache=*/true, &_stats));

    auto scores = run_scan(schema, seg, opt, stats, /*delete_preds=*/{}, /*enable_gin_filter=*/true,
                           tree_of({k1_pred(PredicateType::kEQ, "4")}));
    ASSERT_EQ(1u, scores.size());
    EXPECT_TRUE(scores.count(4));
}

TEST_F(Bm25SegmentScoringTest, residual_matches_nothing) {
    auto schema = make_schema();
    auto seg = build_segment(schema, kFiveDocs);

    auto opt = make_option("apple cherry", /*topk=*/2);
    ASSIGN_OR_ABORT(auto stats,
                    build_tablet_bm25_stats(*schema, *opt, {seg}, LakeIOOptions{}, /*use_page_cache=*/true, &_stats));

    auto scores = run_scan(schema, seg, opt, stats, /*delete_preds=*/{}, /*enable_gin_filter=*/true,
                           tree_of({k1_pred(PredicateType::kEQ, "99")}));
    EXPECT_TRUE(scores.empty());
}

// A residual that rejects nothing must not change the answer. Compared against a run without it rather
// than a hard-coded set, because rows 3 and 4 carry identical scores and the tie-break between them is not
// part of the contract.
//
// Each arm needs its own stats object: BM25Stats::shared_threshold is a per-tablet atomic that seeds the
// next segment's WAND pruning bound from the k-th best seen so far, so reusing one stats object across two
// scans lets the first arm's threshold prune the second arm down to the rows that strictly beat it.
TEST_F(Bm25SegmentScoringTest, residual_matching_all_rows_keeps_the_same_topk) {
    auto schema = make_schema();
    auto seg = build_segment(schema, kFiveDocs);

    auto opt = make_option("apple cherry", /*topk=*/2);
    ASSIGN_OR_ABORT(auto baseline_stats,
                    build_tablet_bm25_stats(*schema, *opt, {seg}, LakeIOOptions{}, /*use_page_cache=*/true, &_stats));
    ASSIGN_OR_ABORT(auto residual_stats,
                    build_tablet_bm25_stats(*schema, *opt, {seg}, LakeIOOptions{}, /*use_page_cache=*/true, &_stats));

    auto baseline = run_scan(schema, seg, opt, baseline_stats);
    auto with_residual = run_scan(schema, seg, opt, residual_stats, /*delete_preds=*/{}, /*enable_gin_filter=*/true,
                                  tree_of({k1_pred(PredicateType::kGE, "0")}));
    EXPECT_EQ(baseline, with_residual);
}

// Both folds stack: row 2 is deleted and row 0 fails the residual, so the top-2 is {3, 4}.
TEST_F(Bm25SegmentScoringTest, residual_and_delete_both_excluded_from_topk) {
    auto schema = make_schema();
    auto seg = build_segment(schema, kFiveDocs);

    auto opt = make_option("apple cherry", /*topk=*/2);
    ASSIGN_OR_ABORT(auto stats,
                    build_tablet_bm25_stats(*schema, *opt, {seg}, LakeIOOptions{}, /*use_page_cache=*/true, &_stats));

    auto* del = new_column_eq_predicate(get_type_info(TYPE_INT), /*cid=*/0, "2");
    _pred_pool.emplace_back(del);
    ConjunctivePredicates conj;
    conj.add(del);
    DisjunctivePredicates disj;
    disj.add(conj);

    auto scores = run_scan(schema, seg, opt, stats, std::move(disj), /*enable_gin_filter=*/true,
                           tree_of({k1_pred(PredicateType::kGE, "1")}));
    ASSERT_EQ(2u, scores.size());
    EXPECT_TRUE(scores.count(3));
    EXPECT_TRUE(scores.count(4));
    EXPECT_FALSE(scores.count(2)) << "deleted row must not occupy a slot";
    EXPECT_FALSE(scores.count(0)) << "row rejected by the residual must not occupy a slot";
}

// Several conjuncts are folded together, not one at a time.
TEST_F(Bm25SegmentScoringTest, multiple_residual_predicates_all_folded) {
    auto schema = make_schema();
    auto seg = build_segment(schema, kFiveDocs);

    auto opt = make_option("apple cherry", /*topk=*/2);
    ASSIGN_OR_ABORT(auto stats,
                    build_tablet_bm25_stats(*schema, *opt, {seg}, LakeIOOptions{}, /*use_page_cache=*/true, &_stats));

    auto scores = run_scan(schema, seg, opt, stats, /*delete_preds=*/{}, /*enable_gin_filter=*/true,
                           tree_of({k1_pred(PredicateType::kGE, "3"), k1_pred(PredicateType::kLE, "3")}));
    ASSERT_EQ(1u, scores.size());
    EXPECT_TRUE(scores.count(3));
}

// With the gin filter off the candidate gate does not enforce the full-text semantics, so scoring stays on
// the score-all path. The residual still filters in the read loop, and nothing is truncated by score.
TEST_F(Bm25SegmentScoringTest, residual_with_gin_filter_off_scores_all) {
    auto schema = make_schema();
    auto seg = build_segment(schema, kFiveDocs);

    auto opt = make_option("apple cherry", /*topk=*/2);
    ASSIGN_OR_ABORT(auto stats,
                    build_tablet_bm25_stats(*schema, *opt, {seg}, LakeIOOptions{}, /*use_page_cache=*/true, &_stats));

    auto scores = run_scan(schema, seg, opt, stats, /*delete_preds=*/{}, /*enable_gin_filter=*/false,
                           tree_of({k1_pred(PredicateType::kGE, "3")}));
    ASSERT_EQ(2u, scores.size());
    EXPECT_TRUE(scores.count(3));
    EXPECT_TRUE(scores.count(4));
}

// The gate has to be consulted, not assumed. A mixed OR subtree cannot be folded whole -- a bitmap cannot
// be built from one arm of a disjunction -- so the residual survives into the read loop and truncating by
// score would drop rows afterwards. Scoring must fall back to every matched row: with topk=2 the answer is
// the three rows the disjunction keeps (k1 = 2 or k1 >= 3), which is more than the limit, proving no
// per-segment truncation happened.
TEST_F(Bm25SegmentScoringTest, unfoldable_residual_falls_back_to_score_all) {
    auto schema = make_schema();
    auto seg = build_segment(schema, kFiveDocs);

    auto opt = make_option("apple cherry", /*topk=*/2);
    ASSIGN_OR_ABORT(auto stats,
                    build_tablet_bm25_stats(*schema, *opt, {seg}, LakeIOOptions{}, /*use_page_cache=*/true, &_stats));

    auto scores = run_scan(schema, seg, opt, stats, /*delete_preds=*/{}, /*enable_gin_filter=*/true,
                           or_tree_of({k1_index_only_pred(PredicateType::kEQ, "2"), k1_pred(PredicateType::kGE, "3")}));
    ASSERT_EQ(3u, scores.size()) << "an unfoldable residual must disable the top-k truncation, not survive it";
    EXPECT_TRUE(scores.count(2));
    EXPECT_TRUE(scores.count(3));
    EXPECT_TRUE(scores.count(4));
}

// An index-only predicate is safe to leave in the tree: the read loop discards it, so it cannot drop a
// scored row and the limit is still honoured.
TEST_F(Bm25SegmentScoringTest, index_only_residual_keeps_topk_pushdown) {
    auto schema = make_schema();
    auto seg = build_segment(schema, kFiveDocs);

    auto opt = make_option("apple cherry", /*topk=*/2);
    ASSIGN_OR_ABORT(auto stats,
                    build_tablet_bm25_stats(*schema, *opt, {seg}, LakeIOOptions{}, /*use_page_cache=*/true, &_stats));

    auto scores = run_scan(schema, seg, opt, stats, /*delete_preds=*/{}, /*enable_gin_filter=*/true,
                           tree_of({k1_index_only_pred(PredicateType::kEQ, "99")}));
    ASSERT_EQ(2u, scores.size()) << "an index-only predicate must not disable the top-k truncation";
    EXPECT_TRUE(scores.count(2)) << "the top scorer must still be there: the predicate is never evaluated";
}

// ---------------------------------------------------------------------------------------------------
// The prepared-split reuse path: the scan range arrives precomputed from the seed and every page filter,
// including the inverted index, is skipped, so the full-text predicate is never erased from the tree. The
// top-k pushdown has to keep working there (the candidate gate is the seed's range, which the seed did
// narrow with the GIN filter), and a scalar conjunct still has to be folded before scoring.
// ---------------------------------------------------------------------------------------------------

// A MATCH the inverted index did not consume stays in the tree while _scan_range was never narrowed by
// it (gin filter off, a failed seek_inverted_index, or -- as here -- a column with no index at all). The
// pre-fold cannot fold it (MatchExpr::evaluate_checked always fails), so the tree stays non-empty and the
// gate must refuse to truncate. The read loop then fails on that same predicate, which is today's
// behaviour for this shape; what matters is that no rank truncation happened before it.
TEST_F(Bm25SegmentScoringTest, unconsumed_match_blocks_topk_pushdown) {
    auto schema = make_schema();
    auto seg = build_segment(schema, kFiveDocs);

    auto opt = make_option("apple cherry", /*topk=*/2);
    ASSIGN_OR_ABORT(auto stats,
                    build_tablet_bm25_stats(*schema, *opt, {seg}, LakeIOOptions{}, /*use_page_cache=*/true, &_stats));

    const int64_t before = _stats.bm25_segments_topk_not_pushed;
    auto st = run_scan_status(schema, seg, opt, stats, tree_of({unconsumed_match_pred(/*cid=*/0)}));

    EXPECT_EQ(before + 1, _stats.bm25_segments_topk_not_pushed)
            << "the gate must record that this segment could not honour the pushed limit";
    EXPECT_EQ(0, _stats.rows_bm25_prefold_filtered) << "an unfoldable residual must not narrow the scan range";
    ASSERT_FALSE(st.ok()) << "evaluating a MATCH outside the index is expected to fail in the read loop";
    EXPECT_TRUE(st.message().find("Match can only used as a pushdown predicate") != std::string::npos) << st.message();
}

TEST_F(Bm25SegmentScoringTest, above_iterator_predicate_falls_back_to_score_all) {
    auto schema = make_schema();
    auto seg = build_segment(schema, kFiveDocs);

    auto opt = make_option("apple cherry", /*topk=*/2);
    ASSIGN_OR_ABORT(auto stats,
                    build_tablet_bm25_stats(*schema, *opt, {seg}, LakeIOOptions{}, /*use_page_cache=*/true, &_stats));

    // A conjunct the scan could not push down (a function call, a two-column comparison) is evaluated above
    // this iterator, so a row kept by rank here can still be dropped later: no truncation.
    auto scores = run_scan(schema, seg, opt, stats, /*delete_preds=*/{}, /*enable_gin_filter=*/true,
                           /*pred_tree=*/{}, /*precomputed_range=*/nullptr,
                           /*has_predicate_above_iterator=*/true);
    ASSERT_EQ(5u, scores.size()) << "a predicate above the iterator must disable the top-k truncation";
}

TEST_F(Bm25SegmentScoringTest, prepared_range_still_narrows_to_topk) {
    auto schema = make_schema();
    auto seg = build_segment(schema, kFiveDocs);

    auto opt = make_option("apple cherry", /*topk=*/2);
    ASSIGN_OR_ABORT(auto stats,
                    build_tablet_bm25_stats(*schema, *opt, {seg}, LakeIOOptions{}, /*use_page_cache=*/true, &_stats));

    auto prepared = std::make_shared<SparseRange<>>();
    prepared->add(Range<>(0, 5));
    auto scores = run_scan(schema, seg, opt, stats, /*delete_preds=*/{}, /*enable_gin_filter=*/true,
                           /*pred_tree=*/{}, prepared);
    ASSERT_EQ(2u, scores.size()) << "the reuse path must keep its top-k pushdown";
    EXPECT_TRUE(scores.count(2));
}

TEST_F(Bm25SegmentScoringTest, prepared_range_folds_scalar_residual) {
    auto schema = make_schema();
    auto seg = build_segment(schema, kFiveDocs);

    auto opt = make_option("apple cherry", /*topk=*/2);
    ASSIGN_OR_ABORT(auto stats,
                    build_tablet_bm25_stats(*schema, *opt, {seg}, LakeIOOptions{}, /*use_page_cache=*/true, &_stats));

    auto prepared = std::make_shared<SparseRange<>>();
    prepared->add(Range<>(0, 5));
    auto scores = run_scan(schema, seg, opt, stats, /*delete_preds=*/{}, /*enable_gin_filter=*/true,
                           tree_of({k1_pred(PredicateType::kGE, "3")}), prepared);
    ASSERT_EQ(2u, scores.size()) << "the residual must be folded on the reuse path too";
    EXPECT_TRUE(scores.count(3));
    EXPECT_TRUE(scores.count(4));
    EXPECT_FALSE(scores.count(2));
}

} // namespace starrocks
