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

// Unit tests for the BM25 query execution layer (Phase-1 stats + Phase-2 scoring), built on real
// DOCS_AND_FREQS segments written through the GIN writer (same round-trip harness as
// builtin_gin_freqs_test): kernel numerics, ScoreAllScorer accumulation, TabletLocalProvider folding.

#include "storage/index/inverted/builtin/bm25_scoring.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <functional>
#include <memory>
#include <random>
#include <roaring/roaring.hh>
#include <string>
#include <unordered_map>
#include <vector>

#include "base/string/slice.h"
#include "base/testutil/assert.h"
#include "fs/fs_memory.h"
#include "gen_cpp/segment.pb.h"
#include "gutil/casts.h"
#include "storage/index/inverted/builtin/bm25_scorer.h"
#include "storage/index/inverted/builtin/bm25_stats_provider.h"
#include "storage/index/inverted/builtin/bm25_wand_scorer.h"
#include "storage/index/inverted/builtin/builtin_inverted_reader.h"
#include "storage/index/inverted/builtin/builtin_inverted_writer.h"
#include "storage/index/inverted/inverted_index_common.h"
#include "storage/index/inverted/inverted_index_option.h"
#include "storage/rowset/options.h"
#include "storage/tablet_index.h"
#include "storage/types.h"

namespace starrocks {

class Bm25ScoringTest : public testing::Test {
public:
    const std::string kTestDir = "/bm25_scoring_test";

protected:
    void SetUp() override {
        _fs = std::make_shared<MemoryFileSystem>();
        ASSERT_TRUE(_fs->create_dir(kTestDir).ok());
    }

    struct Row {
        std::string value;
        bool is_null = false;
    };

    // A fully-opened DOCS_AND_FREQS segment: keeps every object the reads depend on alive.
    struct OpenedSeg {
        std::unique_ptr<InvertedReader> reader;
        std::unique_ptr<RandomAccessFile> rfile;
        std::unique_ptr<OlapReaderStatistics> stats;
        std::unique_ptr<IndexReadOptions> opts;
        BuiltinInvertedIndexPB meta;
        int64_t num_rows = 0;

        BuiltinInvertedReader* br() const { return down_cast<BuiltinInvertedReader*>(reader.get()); }
    };

    ColumnMetaPB write(const std::string& file, const std::string& parser, const std::vector<Row>& rows) {
        TabletIndex tablet_index;
        tablet_index.add_index_properties(INVERTED_INDEX_PARSER_KEY, parser);
        tablet_index.add_index_properties(INVERTED_INDEX_OPTIONS_KEY, INVERTED_INDEX_OPTIONS_DOCS_AND_FREQS);
        TypeInfoPtr type_info = get_type_info(TYPE_VARCHAR);
        ColumnMetaPB meta;
        auto wfile_or = _fs->new_writable_file(file);
        CHECK(wfile_or.ok());
        auto wfile = std::move(wfile_or.value());
        std::unique_ptr<InvertedWriter> writer;
        CHECK_OK(BuiltinInvertedWriter::create(type_info, &tablet_index, &writer));
        CHECK_OK(writer->init());
        for (const auto& r : rows) {
            if (r.is_null) {
                writer->add_nulls(1);
            } else {
                Slice s(r.value);
                writer->add_values(&s, 1);
            }
        }
        CHECK_OK(writer->finish(wfile.get(), &meta));
        CHECK_OK(wfile->close());
        return meta;
    }

    std::unique_ptr<OpenedSeg> open_segment(const std::string& file, const std::string& parser,
                                            const std::vector<Row>& rows) {
        ColumnMetaPB col_meta = write(file, parser, rows);
        auto seg = std::make_unique<OpenedSeg>();
        seg->num_rows = static_cast<int64_t>(rows.size());
        seg->meta = col_meta.indexes(0).builtin_inverted_index();
        seg->stats = std::make_unique<OlapReaderStatistics>();
        auto rf = _fs->new_random_access_file(file);
        CHECK(rf.ok());
        seg->rfile = std::move(rf.value());
        seg->opts = std::make_unique<IndexReadOptions>();
        seg->opts->stats = seg->stats.get();
        seg->opts->read_file = seg->rfile.get();
        seg->opts->segment_rows = rows.size();
        auto tablet_index_sp = std::make_shared<TabletIndex>();
        tablet_index_sp->add_index_properties(INVERTED_INDEX_PARSER_KEY, parser);
        CHECK_OK(BuiltinInvertedReader::create(tablet_index_sp, TYPE_VARCHAR, &seg->reader));
        CHECK_OK(seg->reader->load(*seg->opts, &seg->meta));
        return seg;
    }

    std::shared_ptr<MemoryFileSystem> _fs;
};

namespace {
// Descending top-k score multiset. WAND may swap equal-score docs at the top-k boundary (admission
// needs strictly-greater), so equivalence asserts compare score values, not rowid sets.
std::vector<double> top_scores(const std::unordered_map<rowid_t, double>& m, size_t k) {
    std::vector<double> v;
    v.reserve(m.size());
    for (const auto& [id, s] : m) {
        v.push_back(s);
    }
    std::sort(v.begin(), v.end(), std::greater<>());
    if (v.size() > k) {
        v.resize(k);
    }
    return v;
}
} // namespace

// ---- Task 2: pure kernel ----

TEST_F(Bm25ScoringTest, kernel_matches_reference_formula) {
    BM25Stats s;
    s.N = 10;
    s.avgdl = 5.0;
    s.k1 = 1.2;
    s.b = 0.75;
    const double idf = bm25_idf(10, 3);
    const double denom = 2 + 1.2 * (1 - 0.75 + 0.75 * 4.0 / 5.0);
    const double expect = idf * (2 * 2.2) / denom;
    EXPECT_NEAR(bm25_term(2, 4, idf, s), expect, 1e-9);
    EXPECT_DOUBLE_EQ(bm25_term(0, 4, idf, s), 0.0); // tf 0 contributes nothing
}

TEST_F(Bm25ScoringTest, idf_is_nonnegative_and_decreasing_in_df) {
    EXPECT_GT(bm25_idf(1000, 1), bm25_idf(1000, 500));
    EXPECT_GE(bm25_idf(1000, 999), 0.0);
    EXPECT_GT(bm25_idf(1000, 0), bm25_idf(1000, 1));
}

// ---- Task 3: ScoreAllScorer over a real segment ----
// corpus: row0 "apple banana apple"(len3) row1 "banana cherry"(len2) row2 "apple cherry cherry cherry"(len4)
// dict ordinals: apple 0, banana 1, cherry 2.

TEST_F(Bm25ScoringTest, score_all_accumulates_across_terms) {
    auto seg = open_segment(kTestDir + "/scorer", INVERTED_INDEX_PARSER_ENGLISH,
                            {{"apple banana apple"}, {"banana cherry"}, {"apple cherry cherry cherry"}});
    ASSERT_TRUE(seg->br()->has_freqs());
    ASSIGN_OR_ABORT(auto freqs, seg->br()->new_freqs_iterator(*seg->opts));

    std::vector<Slice> terms{Slice("apple"), Slice("cherry")};
    std::vector<int64_t> ords;
    ASSERT_OK(seg->br()->lookup_term_ordinals(*seg->opts, terms, &ords));
    ASSERT_EQ((std::vector<int64_t>{0, 2}), ords);

    BM25Stats s;
    s.N = 3;
    s.avgdl = 3.0; // sum_len 9 / 3
    s.k1 = 1.2;
    s.b = 0.75;
    s.idf = {bm25_idf(3, 2), bm25_idf(3, 2)}; // df apple=2, cherry=2

    ScoreAllScorer scorer(s, freqs.get(), *seg->opts, ords, /*candidates=*/nullptr, /*topk=*/0);
    std::unordered_map<rowid_t, double> out;
    ASSERT_OK(scorer.run(&out));

    ASSERT_EQ(3u, out.size());
    EXPECT_NEAR(out[0], bm25_term(2, 3, s.idf[0], s), 1e-9);                                // apple only
    EXPECT_NEAR(out[1], bm25_term(1, 2, s.idf[1], s), 1e-9);                                // cherry only
    EXPECT_NEAR(out[2], bm25_term(1, 4, s.idf[0], s) + bm25_term(3, 4, s.idf[1], s), 1e-9); // both
    // row2 has both terms (cherry tf3) -> should outrank the single-term rows.
    EXPECT_GT(out[2], out[0]);
    EXPECT_GT(out[2], out[1]);
}

TEST_F(Bm25ScoringTest, score_all_respects_candidate_bitmap) {
    auto seg = open_segment(kTestDir + "/scorer_cand", INVERTED_INDEX_PARSER_ENGLISH,
                            {{"apple banana apple"}, {"banana cherry"}, {"apple cherry cherry cherry"}});
    ASSIGN_OR_ABORT(auto freqs, seg->br()->new_freqs_iterator(*seg->opts));
    std::vector<Slice> terms{Slice("apple"), Slice("cherry")};
    std::vector<int64_t> ords;
    ASSERT_OK(seg->br()->lookup_term_ordinals(*seg->opts, terms, &ords));

    BM25Stats s;
    s.N = 3;
    s.avgdl = 3.0;
    s.idf = {bm25_idf(3, 2), bm25_idf(3, 2)};

    roaring::Roaring candidates; // only score row 2
    candidates.add(2);
    ScoreAllScorer scorer(s, freqs.get(), *seg->opts, ords, &candidates, /*topk=*/0);
    std::unordered_map<rowid_t, double> out;
    ASSERT_OK(scorer.run(&out));

    ASSERT_EQ(1u, out.size());
    EXPECT_TRUE(out.count(2));
    EXPECT_FALSE(out.count(0));
    EXPECT_FALSE(out.count(1));
}

TEST_F(Bm25ScoringTest, score_all_applies_topk) {
    auto seg = open_segment(kTestDir + "/scorer_topk", INVERTED_INDEX_PARSER_ENGLISH,
                            {{"apple banana apple"}, {"banana cherry"}, {"apple cherry cherry cherry"}});
    std::vector<Slice> terms{Slice("apple"), Slice("cherry")};
    std::vector<int64_t> ords;
    ASSERT_OK(seg->br()->lookup_term_ordinals(*seg->opts, terms, &ords));
    BM25Stats s;
    s.N = 3;
    s.avgdl = 3.0;
    s.idf = {bm25_idf(3, 2), bm25_idf(3, 2)};

    // topk=1: only the single highest-scoring row survives. row2 carries both query terms (cherry tf3),
    // so it outranks the single-term rows (see score_all_accumulates_across_terms).
    {
        ASSIGN_OR_ABORT(auto freqs, seg->br()->new_freqs_iterator(*seg->opts));
        ScoreAllScorer scorer(s, freqs.get(), *seg->opts, ords, /*candidates=*/nullptr, /*topk=*/1);
        std::unordered_map<rowid_t, double> out;
        ASSERT_OK(scorer.run(&out));
        ASSERT_EQ(1u, out.size());
        EXPECT_TRUE(out.count(2));
    }
    // topk larger than the matched set keeps every row (no-op trim).
    {
        ASSIGN_OR_ABORT(auto freqs, seg->br()->new_freqs_iterator(*seg->opts));
        ScoreAllScorer scorer(s, freqs.get(), *seg->opts, ords, /*candidates=*/nullptr, /*topk=*/10);
        std::unordered_map<rowid_t, double> out;
        ASSERT_OK(scorer.run(&out));
        ASSERT_EQ(3u, out.size());
    }
}

// ---- Task 4: TabletLocalProvider folds df/N/avgdl across the tablet's segments ----
// seg1(3 rows): "apple"(1) "apple banana"(2) "banana"(1)   sum_len 4
// seg2(2 rows): "apple cherry"(2) "cherry"(1)              sum_len 3
// tablet: N=5, sum_len=7, avgdl=1.4; df apple=3, banana=2, cherry=2, zzz=0.

TEST_F(Bm25ScoringTest, tablet_local_folds_across_segments) {
    auto seg1 =
            open_segment(kTestDir + "/prov1", INVERTED_INDEX_PARSER_ENGLISH, {{"apple"}, {"apple banana"}, {"banana"}});
    auto seg2 = open_segment(kTestDir + "/prov2", INVERTED_INDEX_PARSER_ENGLISH, {{"apple cherry"}, {"cherry"}});

    std::vector<Bm25SegmentHandle> handles;
    handles.push_back({seg1->br(), seg1->num_rows, seg1->opts.get()});
    handles.push_back({seg2->br(), seg2->num_rows, seg2->opts.get()});
    TabletLocalProvider provider(std::move(handles), 1.2, 0.75);

    std::vector<Slice> terms{Slice("apple"), Slice("banana"), Slice("cherry"), Slice("zzz")};
    ASSIGN_OR_ABORT(auto s, provider.get_stats(terms));

    EXPECT_EQ(5, s.N);
    EXPECT_NEAR(s.avgdl, 7.0 / 5.0, 1e-9);
    ASSERT_EQ(4u, s.idf.size());
    EXPECT_NEAR(s.idf[0], bm25_idf(5, 3), 1e-9); // apple  df 2+1
    EXPECT_NEAR(s.idf[1], bm25_idf(5, 2), 1e-9); // banana df 2+0 (absent in seg2)
    EXPECT_NEAR(s.idf[2], bm25_idf(5, 2), 1e-9); // cherry df 0+2 (absent in seg1)
    EXPECT_NEAR(s.idf[3], bm25_idf(5, 0), 1e-9); // zzz absent everywhere
    // Tablet-local folding really happened: the tablet idf differs from computing it on seg1 alone.
    EXPECT_NE(s.idf[0], bm25_idf(3, 2));
}

TEST_F(Bm25ScoringTest, tablet_local_rejects_docs_only_segment) {
    // Write a DOCS-only segment (no freqs) and confirm the provider fails fast rather than mis-scoring.
    TabletIndex tablet_index;
    tablet_index.add_index_properties(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_ENGLISH);
    TypeInfoPtr type_info = get_type_info(TYPE_VARCHAR);
    const std::string file = kTestDir + "/docs_only";
    ColumnMetaPB col_meta;
    {
        auto wfile_or = _fs->new_writable_file(file);
        CHECK(wfile_or.ok());
        auto wfile = std::move(wfile_or.value());
        std::unique_ptr<InvertedWriter> writer;
        CHECK_OK(BuiltinInvertedWriter::create(type_info, &tablet_index, &writer));
        CHECK_OK(writer->init());
        Slice s("apple");
        writer->add_values(&s, 1);
        CHECK_OK(writer->finish(wfile.get(), &col_meta));
        CHECK_OK(wfile->close());
    }
    BuiltinInvertedIndexPB meta = col_meta.indexes(0).builtin_inverted_index();
    OlapReaderStatistics stats;
    auto rf = _fs->new_random_access_file(file);
    CHECK(rf.ok());
    auto rfile = std::move(rf.value());
    IndexReadOptions opts;
    opts.stats = &stats;
    opts.read_file = rfile.get();
    opts.segment_rows = 1;
    auto tablet_index_sp = std::make_shared<TabletIndex>();
    tablet_index_sp->add_index_properties(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_ENGLISH);
    std::unique_ptr<InvertedReader> reader;
    CHECK_OK(BuiltinInvertedReader::create(tablet_index_sp, TYPE_VARCHAR, &reader));
    CHECK_OK(reader->load(opts, &meta));
    auto* br = down_cast<BuiltinInvertedReader*>(reader.get());
    ASSERT_FALSE(br->has_freqs());

    std::vector<Bm25SegmentHandle> handles;
    handles.push_back({br, 1, &opts});
    TabletLocalProvider provider(std::move(handles), 1.2, 0.75);
    std::vector<Slice> terms{Slice("apple")};
    auto st = provider.get_stats(terms);
    EXPECT_FALSE(st.ok());
}

// ---- WandScorer: block-max WAND top-k equals ScoreAll's top-k, every returned score exact ----

TEST_F(Bm25ScoringTest, wand_matches_score_all_topk) {
    auto seg = open_segment(kTestDir + "/wand_eq", INVERTED_INDEX_PARSER_ENGLISH,
                            {{"apple banana apple"},
                             {"banana cherry"},
                             {"apple cherry cherry cherry"},
                             {"banana"},
                             {"apple apple cherry"}});
    ASSIGN_OR_ABORT(auto freqs_ref, seg->br()->new_freqs_iterator(*seg->opts));
    ASSIGN_OR_ABORT(auto freqs_wand, seg->br()->new_freqs_iterator(*seg->opts));

    std::vector<Slice> terms{Slice("apple"), Slice("cherry")};
    std::vector<int64_t> ords;
    ASSERT_OK(seg->br()->lookup_term_ordinals(*seg->opts, terms, &ords));

    BM25Stats s;
    s.N = 5;
    s.avgdl = 13.0 / 5;                       // doc lens 3+2+4+1+3
    s.idf = {bm25_idf(5, 3), bm25_idf(5, 3)}; // df apple=3 (rows 0,2,4), cherry=3 (rows 1,2,4)

    std::unordered_map<rowid_t, double> full;
    ScoreAllScorer ref(s, freqs_ref.get(), *seg->opts, ords, /*candidates=*/nullptr, /*topk=*/0);
    ASSERT_OK(ref.run(&full));

    std::unordered_map<rowid_t, double> got;
    WandScorer wand(s, freqs_wand.get(), *seg->opts, ords, /*candidates=*/nullptr, /*topk=*/2);
    ASSERT_OK(wand.run(&got));

    ASSERT_EQ(2u, got.size());
    for (const auto& [id, score] : got) {
        ASSERT_TRUE(full.count(id));
        EXPECT_NEAR(full[id], score, 1e-9);
    }
    auto expect = top_scores(full, 2);
    auto actual = top_scores(got, 2);
    ASSERT_EQ(expect.size(), actual.size());
    for (size_t i = 0; i < expect.size(); ++i) {
        EXPECT_NEAR(expect[i], actual[i], 1e-9);
    }
}

TEST_F(Bm25ScoringTest, wand_prunes_below_threshold_docs) {
    // 300 one-token docs "alpha"; every 40th doc also repeats "beta" 4x. With k=5 the heap fills with
    // beta docs whose scores exceed the alpha-only upper bound (an alpha-only doc's bound EQUALS its
    // exact score here, and admission needs strictly-greater), so alpha-only docs must be skipped.
    std::vector<Row> rows;
    for (int i = 0; i < 300; ++i) {
        if (i % 40 == 0) {
            rows.push_back({"alpha beta beta beta beta"});
        } else {
            rows.push_back({"alpha"});
        }
    }
    auto seg = open_segment(kTestDir + "/wand_prune", INVERTED_INDEX_PARSER_ENGLISH, rows);
    ASSIGN_OR_ABORT(auto freqs_ref, seg->br()->new_freqs_iterator(*seg->opts));
    ASSIGN_OR_ABORT(auto freqs_wand, seg->br()->new_freqs_iterator(*seg->opts));

    std::vector<Slice> terms{Slice("alpha"), Slice("beta")};
    std::vector<int64_t> ords;
    ASSERT_OK(seg->br()->lookup_term_ordinals(*seg->opts, terms, &ords));

    BM25Stats s;
    s.N = 300;
    s.avgdl = (292.0 * 1 + 8.0 * 5) / 300;
    s.idf = {bm25_idf(300, 300), bm25_idf(300, 8)};

    std::unordered_map<rowid_t, double> full;
    ScoreAllScorer ref(s, freqs_ref.get(), *seg->opts, ords, /*candidates=*/nullptr, /*topk=*/0);
    ASSERT_OK(ref.run(&full));
    ASSERT_EQ(300u, full.size());

    std::unordered_map<rowid_t, double> got;
    WandScorer wand(s, freqs_wand.get(), *seg->opts, ords, /*candidates=*/nullptr, /*topk=*/5);
    ASSERT_OK(wand.run(&got));

    ASSERT_EQ(5u, got.size());
    for (const auto& [id, score] : got) {
        ASSERT_TRUE(full.count(id));
        EXPECT_NEAR(full[id], score, 1e-9);
    }
    auto expect = top_scores(full, 5);
    auto actual = top_scores(got, 5);
    for (size_t i = 0; i < expect.size(); ++i) {
        EXPECT_NEAR(expect[i], actual[i], 1e-9);
    }
    // The pruning teeth: ~5 DAAT docs before the heap fills + the 8 beta docs. A score-all
    // implementation (or an >=-threshold regression) would score all 300.
    EXPECT_LE(wand.docs_scored(), 20);
}

TEST_F(Bm25ScoringTest, wand_block_check_skips_outlier_term) {
    // 299 docs of "alpha" (tf=1, dl=1); the last doc repeats alpha x50. The outlier lifts the term's
    // GLOBAL bound above any threshold (a classic-WAND regression would decode and score all 300),
    // but blocks 0/1 keep local max_tf=1, so the block-max recheck skips them without decoding.
    std::vector<Row> rows;
    for (int i = 0; i < 299; ++i) {
        rows.push_back({"alpha"});
    }
    std::string outlier = "alpha";
    for (int j = 0; j < 49; ++j) {
        outlier += " alpha";
    }
    rows.push_back({outlier}); // doc 299: tf=50, dl=50 -> the true top-1
    auto seg = open_segment(kTestDir + "/wand_outlier", INVERTED_INDEX_PARSER_ENGLISH, rows);
    ASSIGN_OR_ABORT(auto freqs_ref, seg->br()->new_freqs_iterator(*seg->opts));
    ASSIGN_OR_ABORT(auto freqs_wand, seg->br()->new_freqs_iterator(*seg->opts));

    std::vector<Slice> terms{Slice("alpha")};
    std::vector<int64_t> ords;
    ASSERT_OK(seg->br()->lookup_term_ordinals(*seg->opts, terms, &ords));

    BM25Stats s;
    s.N = 300;
    s.avgdl = (299.0 * 1 + 50.0) / 300;
    s.idf = {bm25_idf(300, 300)};

    std::unordered_map<rowid_t, double> full;
    ScoreAllScorer ref(s, freqs_ref.get(), *seg->opts, ords, /*candidates=*/nullptr, /*topk=*/0);
    ASSERT_OK(ref.run(&full));

    std::unordered_map<rowid_t, double> got;
    WandScorer wand(s, freqs_wand.get(), *seg->opts, ords, /*candidates=*/nullptr, /*topk=*/1);
    ASSERT_OK(wand.run(&got));

    ASSERT_EQ(1u, got.size());
    ASSERT_TRUE(got.count(299)); // the outlier doc is the unique top-1
    EXPECT_NEAR(full[299], got[299], 1e-9);
    // Blocks 0/1 (256 docs) must be skipped via the directory recheck; only the heap-filling prefix
    // and the outlier's block get scored. A classic-WAND regression scores all 300.
    EXPECT_LE(wand.docs_scored(), 50);
}

TEST_F(Bm25ScoringTest, wand_respects_candidates_absent_and_dup_terms) {
    auto seg = open_segment(kTestDir + "/wand_cand", INVERTED_INDEX_PARSER_ENGLISH,
                            {{"apple banana apple"},
                             {"banana cherry"},
                             {"apple cherry cherry cherry"},
                             {"banana"},
                             {"apple apple cherry"}});
    ASSIGN_OR_ABORT(auto freqs_ref, seg->br()->new_freqs_iterator(*seg->opts));
    ASSIGN_OR_ABORT(auto freqs_wand, seg->br()->new_freqs_iterator(*seg->opts));

    // "zzz" is absent (ordinal -1); "cherry" appears twice and contributes once per position, exactly
    // like ScoreAllScorer.
    std::vector<Slice> terms{Slice("apple"), Slice("zzz"), Slice("cherry"), Slice("cherry")};
    std::vector<int64_t> ords;
    ASSERT_OK(seg->br()->lookup_term_ordinals(*seg->opts, terms, &ords));
    ASSERT_EQ(-1, ords[1]);

    BM25Stats s;
    s.N = 5;
    s.avgdl = 13.0 / 5;
    const double idf_ac = bm25_idf(5, 3);
    s.idf = {idf_ac, 0.0, idf_ac, idf_ac};

    roaring::Roaring candidates; // exclude rows 2 and 4 -- the global top scorers
    candidates.add(0);
    candidates.add(1);
    candidates.add(3);

    std::unordered_map<rowid_t, double> full;
    ScoreAllScorer ref(s, freqs_ref.get(), *seg->opts, ords, &candidates, /*topk=*/0);
    ASSERT_OK(ref.run(&full));

    std::unordered_map<rowid_t, double> got;
    WandScorer wand(s, freqs_wand.get(), *seg->opts, ords, &candidates, /*topk=*/2);
    ASSERT_OK(wand.run(&got));

    ASSERT_EQ(2u, got.size());
    for (const auto& [id, score] : got) {
        EXPECT_TRUE(candidates.contains(id));
        ASSERT_TRUE(full.count(id));
        EXPECT_NEAR(full[id], score, 1e-9);
    }
    auto expect = top_scores(full, 2);
    auto actual = top_scores(got, 2);
    for (size_t i = 0; i < expect.size(); ++i) {
        EXPECT_NEAR(expect[i], actual[i], 1e-9);
    }
}

TEST_F(Bm25ScoringTest, wand_returns_all_when_topk_exceeds_matches) {
    auto seg = open_segment(kTestDir + "/wand_bigk", INVERTED_INDEX_PARSER_ENGLISH,
                            {{"apple banana apple"}, {"banana cherry"}, {"apple cherry cherry cherry"}});
    ASSIGN_OR_ABORT(auto freqs_ref, seg->br()->new_freqs_iterator(*seg->opts));
    ASSIGN_OR_ABORT(auto freqs_wand, seg->br()->new_freqs_iterator(*seg->opts));

    std::vector<Slice> terms{Slice("apple"), Slice("cherry")};
    std::vector<int64_t> ords;
    ASSERT_OK(seg->br()->lookup_term_ordinals(*seg->opts, terms, &ords));

    BM25Stats s;
    s.N = 3;
    s.avgdl = 3.0;
    s.idf = {bm25_idf(3, 2), bm25_idf(3, 2)};

    std::unordered_map<rowid_t, double> full;
    ScoreAllScorer ref(s, freqs_ref.get(), *seg->opts, ords, /*candidates=*/nullptr, /*topk=*/0);
    ASSERT_OK(ref.run(&full));

    std::unordered_map<rowid_t, double> got;
    WandScorer wand(s, freqs_wand.get(), *seg->opts, ords, /*candidates=*/nullptr, /*topk=*/100);
    ASSERT_OK(wand.run(&got));

    ASSERT_EQ(full.size(), got.size());
    for (const auto& [id, score] : full) {
        ASSERT_TRUE(got.count(id));
        EXPECT_NEAR(score, got[id], 1e-9);
    }

    // topk <= 0 is the caller's bug (selection must route it to ScoreAllScorer): fail fast.
    WandScorer bad(s, freqs_wand.get(), *seg->opts, ords, /*candidates=*/nullptr, /*topk=*/0);
    std::unordered_map<rowid_t, double> unused;
    ASSERT_FALSE(bad.run(&unused).ok());
}

TEST_F(Bm25ScoringTest, wand_shared_threshold_carries_across_scorers) {
    // Same corpus as the pruning test: the 8 beta docs all carry an identical total score and are the
    // global top, which makes the carry-over assertions exact.
    std::vector<Row> rows;
    for (int i = 0; i < 300; ++i) {
        if (i % 40 == 0) {
            rows.push_back({"alpha beta beta beta beta"});
        } else {
            rows.push_back({"alpha"});
        }
    }
    auto seg = open_segment(kTestDir + "/wand_carry", INVERTED_INDEX_PARSER_ENGLISH, rows);

    std::vector<Slice> terms{Slice("alpha"), Slice("beta")};
    std::vector<int64_t> ords;
    ASSERT_OK(seg->br()->lookup_term_ordinals(*seg->opts, terms, &ords));

    BM25Stats s;
    s.N = 300;
    s.avgdl = (292.0 * 1 + 8.0 * 5) / 300;
    s.idf = {bm25_idf(300, 300), bm25_idf(300, 8)};

    // Run 1 publishes its k-th best into the accumulator.
    std::atomic<double> shared(0.0);
    ASSIGN_OR_ABORT(auto freqs1, seg->br()->new_freqs_iterator(*seg->opts));
    std::unordered_map<rowid_t, double> got1;
    WandScorer w1(s, freqs1.get(), *seg->opts, ords, /*candidates=*/nullptr, /*topk=*/5, &shared);
    ASSERT_OK(w1.run(&got1));
    ASSERT_EQ(5u, got1.size());
    EXPECT_GT(shared.load(), 0.0);

    // Run 2 over the same corpus, seeded by run 1: every doc scores <= the published k-th best (the
    // top docs tie it exactly), so nothing is admitted; alpha-only docs are pruned before scoring.
    ASSIGN_OR_ABORT(auto freqs2, seg->br()->new_freqs_iterator(*seg->opts));
    std::unordered_map<rowid_t, double> got2;
    WandScorer w2(s, freqs2.get(), *seg->opts, ords, /*candidates=*/nullptr, /*topk=*/5, &shared);
    ASSERT_OK(w2.run(&got2));
    EXPECT_TRUE(got2.empty());
    EXPECT_LE(w2.docs_scored(), 8); // only beta docs survive admission, all rejected at the heap

    // A seed above every possible score prunes the whole segment without scoring a single doc --
    // exercises the seed participating in admission before the heap ever fills.
    std::atomic<double> sky(1e9);
    ASSIGN_OR_ABORT(auto freqs3, seg->br()->new_freqs_iterator(*seg->opts));
    std::unordered_map<rowid_t, double> got3;
    WandScorer w3(s, freqs3.get(), *seg->opts, ords, /*candidates=*/nullptr, /*topk=*/5, &sky);
    ASSERT_OK(w3.run(&got3));
    EXPECT_TRUE(got3.empty());
    EXPECT_EQ(0, w3.docs_scored());
}

TEST_F(Bm25ScoringTest, wand_randomized_matches_score_all) {
    // Fixed-seed random corpus + random candidates, checked at several k: differential testing is the
    // widest net for silent-ranking bugs (e.g. a missing boundary clamp) that targeted fixtures miss.
    const char* kVocab[] = {"alpha", "bravo", "carol", "delta", "eagle", "fox",  "golf",   "hotel", "india",  "julia",
                            "kilo",  "lima",  "mike",  "nancy", "oscar", "papa", "quebec", "romeo", "sierra", "tango"};
    std::mt19937 rng(12345);
    std::uniform_int_distribution<int> word_pick(0, 19);
    std::uniform_int_distribution<int> len_pick(1, 8);
    std::vector<Row> rows;
    for (int i = 0; i < 400; ++i) {
        std::string doc;
        const int len = len_pick(rng);
        for (int j = 0; j < len; ++j) {
            if (j > 0) {
                doc += ' ';
            }
            doc += kVocab[word_pick(rng)];
        }
        rows.push_back({doc});
    }
    auto seg = open_segment(kTestDir + "/wand_rand", INVERTED_INDEX_PARSER_ENGLISH, rows);

    std::vector<Slice> terms{Slice("alpha"), Slice("kilo"), Slice("tango")};
    std::vector<int64_t> ords;
    ASSERT_OK(seg->br()->lookup_term_ordinals(*seg->opts, terms, &ords));

    std::vector<Bm25SegmentHandle> handles;
    handles.push_back({seg->br(), seg->num_rows, seg->opts.get()});
    TabletLocalProvider provider(std::move(handles), 1.2, 0.75);
    ASSIGN_OR_ABORT(BM25Stats s, provider.get_stats(terms));

    roaring::Roaring candidates;
    std::uniform_real_distribution<double> coin(0.0, 1.0);
    for (uint32_t r = 0; r < 400; ++r) {
        if (coin(rng) < 0.7) {
            candidates.add(r);
        }
    }

    for (int64_t k : {int64_t(1), int64_t(7), int64_t(100)}) {
        ASSIGN_OR_ABORT(auto freqs_ref, seg->br()->new_freqs_iterator(*seg->opts));
        ASSIGN_OR_ABORT(auto freqs_wand, seg->br()->new_freqs_iterator(*seg->opts));
        std::unordered_map<rowid_t, double> full;
        ScoreAllScorer ref(s, freqs_ref.get(), *seg->opts, ords, &candidates, /*topk=*/0);
        ASSERT_OK(ref.run(&full));
        std::unordered_map<rowid_t, double> got;
        WandScorer wand(s, freqs_wand.get(), *seg->opts, ords, &candidates, k);
        ASSERT_OK(wand.run(&got));

        auto expect = top_scores(full, k);
        auto actual = top_scores(got, k);
        ASSERT_EQ(expect.size(), actual.size()) << "k=" << k;
        for (size_t i = 0; i < expect.size(); ++i) {
            EXPECT_NEAR(expect[i], actual[i], 1e-9) << "k=" << k << " rank=" << i;
        }
        for (const auto& [id, score] : got) {
            ASSERT_TRUE(full.count(id)) << "k=" << k;
            EXPECT_NEAR(full[id], score, 1e-9) << "k=" << k;
        }
    }
}

TEST_F(Bm25ScoringTest, wand_boundary_clamped_by_outsider_cursor) {
    // Deterministic fixture for the boundary's second clamp (Ding & Suel's GetNewCandidate). "x" spans
    // two blocks: block 0 (rows 0..127, tf=1) whose local bound EQUALS the running threshold, so the
    // block recheck keeps skipping it, while block 1 (rows 190..199, tf=4) lifts the global bound
    // above the threshold, so Level 1 keeps electing x's next row as pivot. "c" sits only on row 60 --
    // inside the skipped range -- and makes row 60 the true top-1; row 60 holds x too. With the clamp
    // the skip stops exactly at row 60 and it is scored with BOTH terms; without the clamp x leaps
    // past row 60 and its contribution is silently lost (wrong score for the top-1).
    std::vector<Row> rows;
    for (int i = 0; i < 200; ++i) {
        if (i == 60) {
            rows.push_back({"x c c c"});
        } else if (i < 128) {
            rows.push_back({"x"});
        } else if (i < 190) {
            rows.push_back({"pad"});
        } else {
            rows.push_back({"x x x x"});
        }
    }
    auto seg = open_segment(kTestDir + "/wand_clamp", INVERTED_INDEX_PARSER_ENGLISH, rows);

    std::vector<Slice> terms{Slice("x"), Slice("c")};
    std::vector<int64_t> ords;
    ASSERT_OK(seg->br()->lookup_term_ordinals(*seg->opts, terms, &ords));

    std::vector<Bm25SegmentHandle> handles;
    handles.push_back({seg->br(), seg->num_rows, seg->opts.get()});
    TabletLocalProvider provider(std::move(handles), 1.2, 0.75);
    ASSIGN_OR_ABORT(BM25Stats s, provider.get_stats(terms));

    ASSIGN_OR_ABORT(auto freqs_ref, seg->br()->new_freqs_iterator(*seg->opts));
    std::unordered_map<rowid_t, double> full;
    ScoreAllScorer ref(s, freqs_ref.get(), *seg->opts, ords, /*candidates=*/nullptr, /*topk=*/0);
    ASSERT_OK(ref.run(&full));

    ASSIGN_OR_ABORT(auto freqs_wand, seg->br()->new_freqs_iterator(*seg->opts));
    std::unordered_map<rowid_t, double> got;
    WandScorer wand(s, freqs_wand.get(), *seg->opts, ords, /*candidates=*/nullptr, /*topk=*/1);
    ASSERT_OK(wand.run(&got));

    ASSERT_EQ(1u, got.size());
    ASSERT_TRUE(got.count(60));           // the outsider's row must not be skipped over
    EXPECT_NEAR(full[60], got[60], 1e-9); // and it must carry x's share too, not just c's
    // Correct flow scores exactly two rows: the heap-filling row 0 and row 60. A broken clamp or a
    // broken skip makes x crawl or overshoot and this bound explodes.
    EXPECT_LE(wand.docs_scored(), 3);
}

TEST_F(Bm25ScoringTest, wand_block_check_sentinel_for_exhausted_term) {
    // Deterministic fixture for _block_ub_at's sentinel branch. "d" has a single posting on row 10, a
    // 100-token row, so d's bound stays below the threshold and it is never elected pivot; row 10
    // holds no "x", so x's catch-up hops from row 1 to row 11 and d's cursor is left standing on its
    // exhausted tail for the whole run. Every later recheck queries d's directory past its last
    // posting -> the sentinel: 0 contribution, no boundary constraint. Teeth: with the sentinel the
    // recheck keeps skipping whole x-blocks and only ONE row is ever scored; if the sentinel
    // regressed to any non-zero bound, block_sum would exceed the threshold, skipping would stop, and
    // x would score its way through ~140 rows.
    std::vector<Row> rows;
    std::string d_row = "d";
    for (int j = 0; j < 99; ++j) {
        d_row += " pad";
    }
    for (int i = 0; i < 200; ++i) {
        if (i == 10) {
            rows.push_back({d_row});
        } else if (i <= 140) {
            rows.push_back({"x"});
        } else {
            rows.push_back({"pad"});
        }
    }
    auto seg = open_segment(kTestDir + "/wand_sentinel", INVERTED_INDEX_PARSER_ENGLISH, rows);

    std::vector<Slice> terms{Slice("x"), Slice("d")};
    std::vector<int64_t> ords;
    ASSERT_OK(seg->br()->lookup_term_ordinals(*seg->opts, terms, &ords));

    std::vector<Bm25SegmentHandle> handles;
    handles.push_back({seg->br(), seg->num_rows, seg->opts.get()});
    TabletLocalProvider provider(std::move(handles), 1.2, 0.75);
    ASSIGN_OR_ABORT(BM25Stats s, provider.get_stats(terms));

    ASSIGN_OR_ABORT(auto freqs_ref, seg->br()->new_freqs_iterator(*seg->opts));
    std::unordered_map<rowid_t, double> full;
    ScoreAllScorer ref(s, freqs_ref.get(), *seg->opts, ords, /*candidates=*/nullptr, /*topk=*/0);
    ASSERT_OK(ref.run(&full));

    ASSIGN_OR_ABORT(auto freqs_wand, seg->br()->new_freqs_iterator(*seg->opts));
    std::unordered_map<rowid_t, double> got;
    WandScorer wand(s, freqs_wand.get(), *seg->opts, ords, /*candidates=*/nullptr, /*topk=*/1);
    ASSERT_OK(wand.run(&got));

    // Only the heap-filling row 0 is ever fully scored; the x rows all tie, so compare scores.
    ASSERT_EQ(1u, got.size());
    ASSERT_TRUE(got.count(0));
    EXPECT_NEAR(full[0], got[0], 1e-9);
    auto expect = top_scores(full, 1);
    auto actual = top_scores(got, 1);
    EXPECT_NEAR(expect[0], actual[0], 1e-9);
    EXPECT_LE(wand.docs_scored(), 3);
}

} // namespace starrocks
