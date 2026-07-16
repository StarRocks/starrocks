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

#include <memory>
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

} // namespace starrocks
