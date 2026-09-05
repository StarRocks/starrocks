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

#include <atomic>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "base/string/slice.h"
#include "base/testutil/assert.h"
#include "fs/fs_memory.h"
#include "gen_cpp/segment.pb.h"
#include "roaring/roaring.hh"
#include "storage/index/inverted/clucene/clucene_inverted_reader.h"
#include "storage/index/inverted/clucene/clucene_inverted_writer.h"
#include "storage/index/inverted/inverted_index_common.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/index/inverted/inverted_index_option.h"
#include "storage/olap_common.h"
#include "storage/rowset/options.h"
#include "storage/tablet_index.h"
#include "storage/types.h"

namespace starrocks {

// CLucene's IndexWriter writes through lucene::store::FSDirectory which talks to the local
// filesystem directly (it is not aware of StarRocks' fs::FileSystem abstraction). The reader
// likewise calls FSDirectory::getDirectory(path) and PhraseQuery::_search reads .prx files
// off of disk. Tests therefore have to use a real local temp directory rather than the
// MemoryFileSystem used by the builtin inverted index tests.
class CLuceneMatchPhraseTest : public testing::Test {
public:
    void SetUp() override {
        // Use a per-test unique sub-directory so cases do not collide when run in parallel and
        // failed runs do not poison subsequent ones.
        std::error_code ec;
        std::filesystem::path tmp =
                std::filesystem::temp_directory_path(ec) /
                ("clucene_match_phrase_ut_" + std::to_string(::getpid()) + "_" + std::to_string(++_counter));
        std::filesystem::remove_all(tmp, ec); // clean leftovers if any
        std::filesystem::create_directories(tmp);
        _index_dir = tmp.string();

        _opts.use_page_cache = false;
        _opts.stats = &_stats;

        _fs = std::make_shared<MemoryFileSystem>();
        ASSERT_TRUE(_fs->create_dir("/tmp_meta").ok());
    }

    void TearDown() override {
        std::error_code ec;
        std::filesystem::remove_all(_index_dir, ec);
    }

protected:
    // Build a TabletIndex with the given parser + support_phrase combination. CLucene only
    // honors the index_properties map, so we go through add_index_properties.
    static TabletIndex make_tablet_index(const std::string& parser, bool support_phrase) {
        TabletIndex idx;
        if (!parser.empty()) {
            idx.add_index_properties(INVERTED_INDEX_PARSER_KEY, parser);
        }
        idx.add_index_properties(INVERTED_INDEX_SUPPORT_PHRASE_KEY, support_phrase ? "true" : "false");
        return idx;
    }

    // Materialize an inverted index for `values` under `_index_dir` using the given properties.
    // Returns once the writer has been closed and the lucene segment files are on disk.
    void write_index(const std::vector<std::string>& values, const TabletIndex& tablet_index) {
        std::vector<Slice> slices;
        slices.reserve(values.size());
        for (const auto& v : values) {
            slices.emplace_back(v.data(), v.size());
        }

        TypeInfoPtr type_info = get_type_info(TYPE_VARCHAR);

        // We still need a (dummy) WritableFile for the null-bitmap side-channel that
        // CLuceneInvertedWriter::finish writes. The actual segment data lives in _index_dir.
        std::string meta_file = "/tmp_meta/null_bitmap_" + std::to_string(++_counter);
        ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(meta_file));

        // We pass a mutable copy because the writer interface takes a non-const pointer.
        // The writer only reads from index_properties() so the copy is safe.
        auto idx_copy = std::make_unique<TabletIndex>(tablet_index);
        std::unique_ptr<InvertedWriter> writer;
        ASSERT_OK(CLuceneInvertedWriter::create(type_info, "c0", _index_dir, idx_copy.get(), &writer));
        ASSERT_OK(writer->init());
        writer->add_values(slices.data(), slices.size());
        writer->add_nulls(0);

        ColumnMetaPB meta;
        ASSERT_OK(writer->finish(wfile.get(), &meta));
        ASSERT_TRUE(wfile->close().ok());
    }

    // Build a reader pointing at _index_dir. The `support_phrase_override` argument lets
    // tests intentionally desynchronise reader state from writer state to exercise the BE
    // defensive guard.
    std::unique_ptr<FullTextCLuceneInvertedReader> make_reader(InvertedIndexParserType parser,
                                                               bool support_phrase_override) {
        return std::make_unique<FullTextCLuceneInvertedReader>(_index_dir, /*index_id=*/1, parser,
                                                               support_phrase_override);
    }

    // Returns true if the temp directory contains at least one file whose extension matches
    // `ext` (e.g. ".prx"). CLucene writes segment files with deterministic suffixes when
    // setUseCompoundFile(false) is set (which CLuceneInvertedWriter does).
    bool dir_has_file_with_extension(const std::string& ext) const {
        std::error_code ec;
        for (auto& entry : std::filesystem::directory_iterator(_index_dir, ec)) {
            if (entry.path().extension().string() == ext) {
                return true;
            }
        }
        return false;
    }

    OlapReaderStatistics _stats;
    IndexReadOptions _opts;
    std::shared_ptr<MemoryFileSystem> _fs;
    std::string _index_dir;
    static std::atomic<int> _counter;
};

std::atomic<int> CLuceneMatchPhraseTest::_counter{0};

// ---------------------------------------------------------------------------
// Section 1: setOmitPositions decision matrix (writer side).
//
// The writer must only emit the .prx (term position) file when the user has opted in via
// `support_phrase=true` AND the column is being tokenized (parser != none). Any other
// combination has to fall back to the legacy on-disk layout (no .prx) so existing indexes
// remain byte-compatible with upgraded clusters.
// ---------------------------------------------------------------------------

TEST_F(CLuceneMatchPhraseTest, writer_emits_prx_only_when_support_phrase_and_tokenized) {
    auto idx = make_tablet_index(INVERTED_INDEX_PARSER_ENGLISH, /*support_phrase=*/true);
    write_index({"hello world", "the quick brown fox", "hello there"}, idx);
    ASSERT_TRUE(dir_has_file_with_extension(".prx"))
            << "support_phrase=true + tokenized parser must persist term positions";
}

TEST_F(CLuceneMatchPhraseTest, writer_omits_prx_when_support_phrase_false_with_english_parser) {
    auto idx = make_tablet_index(INVERTED_INDEX_PARSER_ENGLISH, /*support_phrase=*/false);
    write_index({"hello world", "the quick brown fox"}, idx);
    ASSERT_FALSE(dir_has_file_with_extension(".prx"))
            << "support_phrase=false must preserve the pre-feature on-disk layout (no .prx)";
}

TEST_F(CLuceneMatchPhraseTest, writer_omits_prx_when_support_phrase_true_but_parser_none) {
    // Defense-in-depth: FE rejects this combination but BE must still refuse to write
    // positions for an untokenized field, where positions are meaningless.
    auto idx = make_tablet_index(INVERTED_INDEX_PARSER_NONE, /*support_phrase=*/true);
    write_index({"hello world", "the quick brown fox"}, idx);
    ASSERT_FALSE(dir_has_file_with_extension(".prx"))
            << "parser=none must always omit term positions, even with support_phrase=true";
}

TEST_F(CLuceneMatchPhraseTest, writer_omits_prx_when_support_phrase_false_and_parser_none) {
    auto idx = make_tablet_index(INVERTED_INDEX_PARSER_NONE, /*support_phrase=*/false);
    write_index({"hello world", "the quick brown fox"}, idx);
    ASSERT_FALSE(dir_has_file_with_extension(".prx"));
}

// ---------------------------------------------------------------------------
// Section 2: reader-side defensive rejection of MATCH_PHRASE_QUERY.
//
// This is the BE's last line of defense for MATCH_PHRASE: even if the FE validator was
// bypassed (older FE, programmatic access to the storage layer, metadata round-tripped from
// an older version) we must NOT silently return empty results or, worse, crash inside
// PhraseQuery against an index that has no .prx file. Returning Status::NotSupported with a
// clear message ensures the user gets a deterministic error.
// ---------------------------------------------------------------------------

TEST_F(CLuceneMatchPhraseTest, reader_rejects_match_phrase_when_support_phrase_false) {
    auto idx = make_tablet_index(INVERTED_INDEX_PARSER_ENGLISH, /*support_phrase=*/false);
    write_index({"hello world", "the quick brown fox"}, idx);

    auto reader = make_reader(InvertedIndexParserType::PARSER_ENGLISH,
                              /*support_phrase_override=*/false);
    roaring::Roaring bitmap;
    Slice query("hello world");
    Status st = reader->query(&_stats, "c0", &query, InvertedIndexQueryType::MATCH_PHRASE_QUERY, &bitmap);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_not_supported()) << st;
    ASSERT_NE(std::string::npos, st.to_string().find("'support_phrase' = 'true'"))
            << "Error message should name the property the user needs to flip; got: " << st.to_string();
}

// MATCH/MATCH_ANY/MATCH_ALL/wildcard/EQUAL must remain available when support_phrase=false.
// This regression guards against a future change accidentally tying these query types to
// the support_phrase flag.
TEST_F(CLuceneMatchPhraseTest, reader_still_serves_non_phrase_queries_when_support_phrase_false) {
    auto idx = make_tablet_index(INVERTED_INDEX_PARSER_ENGLISH, /*support_phrase=*/false);
    write_index({"hello world", "hello there", "goodbye world", "unrelated"}, idx);

    auto reader = make_reader(InvertedIndexParserType::PARSER_ENGLISH,
                              /*support_phrase_override=*/false);

    {
        roaring::Roaring bitmap;
        Slice query("hello");
        ASSERT_OK(reader->query(&_stats, "c0", &query, InvertedIndexQueryType::EQUAL_QUERY, &bitmap));
        EXPECT_TRUE(bitmap.contains(0));
        EXPECT_TRUE(bitmap.contains(1));
        EXPECT_FALSE(bitmap.contains(2));
        EXPECT_FALSE(bitmap.contains(3));
    }
    {
        roaring::Roaring bitmap;
        Slice query("hello world");
        ASSERT_OK(reader->query(&_stats, "c0", &query, InvertedIndexQueryType::MATCH_ANY_QUERY, &bitmap));
        // MATCH_ANY: any row containing "hello" or "world".
        EXPECT_TRUE(bitmap.contains(0));
        EXPECT_TRUE(bitmap.contains(1));
        EXPECT_TRUE(bitmap.contains(2));
        EXPECT_FALSE(bitmap.contains(3));
    }
    {
        roaring::Roaring bitmap;
        Slice query("hello world");
        ASSERT_OK(reader->query(&_stats, "c0", &query, InvertedIndexQueryType::MATCH_ALL_QUERY, &bitmap));
        // MATCH_ALL: rows containing both "hello" and "world".
        EXPECT_EQ(1, bitmap.cardinality());
        EXPECT_TRUE(bitmap.contains(0));
    }
}

// ---------------------------------------------------------------------------
// Section 3: end-to-end MATCH_PHRASE behavior with support_phrase=true.
//
// The phrase query semantics we promise to users: tokens must appear in the document in
// the same order AND adjacent to each other (slop=0). These tests exercise that contract.
// ---------------------------------------------------------------------------

TEST_F(CLuceneMatchPhraseTest, match_phrase_multi_token_hits_only_when_adjacent_and_in_order) {
    auto idx = make_tablet_index(INVERTED_INDEX_PARSER_ENGLISH, /*support_phrase=*/true);
    write_index(
            {
                    "the quick brown fox jumps over the lazy dog", // row 0: contains "quick brown"
                    "the brown quick fox",                         // row 1: order reversed
                    "the quick green brown fox",                   // row 2: word in between
                    "no animals here",                             // row 3: unrelated
                    "another quick brown story",                   // row 4: contains "quick brown"
            },
            idx);
    auto reader = make_reader(InvertedIndexParserType::PARSER_ENGLISH, /*support_phrase_override=*/true);

    roaring::Roaring bitmap;
    Slice query("quick brown");
    ASSERT_OK(reader->query(&_stats, "c0", &query, InvertedIndexQueryType::MATCH_PHRASE_QUERY, &bitmap));
    EXPECT_TRUE(bitmap.contains(0)) << "exact adjacent in-order phrase";
    EXPECT_FALSE(bitmap.contains(1)) << "reversed order must NOT match";
    EXPECT_FALSE(bitmap.contains(2)) << "intervening token must NOT match";
    EXPECT_FALSE(bitmap.contains(3));
    EXPECT_TRUE(bitmap.contains(4));
    EXPECT_EQ(2, bitmap.cardinality());
}

// Regression for a previously observed BE segfault in lucene::index::SegmentTermPositions::lazySkip
// triggered by single-token phrase queries. Even though a 1-token "phrase" is semantically the
// same as a term query, MatchPhraseOperator still goes through PhraseQuery and must not crash.
TEST_F(CLuceneMatchPhraseTest, match_phrase_single_token_does_not_crash_and_matches_term_semantics) {
    auto idx = make_tablet_index(INVERTED_INDEX_PARSER_ENGLISH, /*support_phrase=*/true);
    write_index({"hello world", "hello there", "no greeting", "say hello again"}, idx);
    auto reader = make_reader(InvertedIndexParserType::PARSER_ENGLISH, /*support_phrase_override=*/true);

    roaring::Roaring bitmap;
    Slice query("hello");
    // The critical assertion is that this returns OK rather than crashing the process.
    ASSERT_OK(reader->query(&_stats, "c0", &query, InvertedIndexQueryType::MATCH_PHRASE_QUERY, &bitmap));
    EXPECT_TRUE(bitmap.contains(0));
    EXPECT_TRUE(bitmap.contains(1));
    EXPECT_FALSE(bitmap.contains(2));
    EXPECT_TRUE(bitmap.contains(3));
    EXPECT_EQ(3, bitmap.cardinality());
}

// The english SimpleAnalyzer lowercases tokens; phrase queries must therefore be
// case-insensitive end-to-end. This locks in the user-visible behavior of
// "english" parser + MATCH_PHRASE.
TEST_F(CLuceneMatchPhraseTest, match_phrase_english_parser_is_case_insensitive) {
    auto idx = make_tablet_index(INVERTED_INDEX_PARSER_ENGLISH, /*support_phrase=*/true);
    write_index({"Hello World", "HELLO WORLD", "hello world", "world hello"}, idx);
    auto reader = make_reader(InvertedIndexParserType::PARSER_ENGLISH, /*support_phrase_override=*/true);

    roaring::Roaring bitmap;
    Slice query("hello world");
    ASSERT_OK(reader->query(&_stats, "c0", &query, InvertedIndexQueryType::MATCH_PHRASE_QUERY, &bitmap));
    EXPECT_EQ(3, bitmap.cardinality());
    EXPECT_TRUE(bitmap.contains(0));
    EXPECT_TRUE(bitmap.contains(1));
    EXPECT_TRUE(bitmap.contains(2));
    EXPECT_FALSE(bitmap.contains(3)) << "order matters";
}

// StandardAnalyzer splits on punctuation. After tokenization "hello, world" and "hello world"
// should produce the same token stream "hello"/"world", and the phrase query should hit both.
TEST_F(CLuceneMatchPhraseTest, match_phrase_standard_parser_treats_punctuation_as_delimiter) {
    auto idx = make_tablet_index(INVERTED_INDEX_PARSER_STANDARD, /*support_phrase=*/true);
    write_index({"hello, world", "hello world", "hello-world", "world; hello"}, idx);
    auto reader = make_reader(InvertedIndexParserType::PARSER_STANDARD, /*support_phrase_override=*/true);

    roaring::Roaring bitmap;
    Slice query("hello world");
    ASSERT_OK(reader->query(&_stats, "c0", &query, InvertedIndexQueryType::MATCH_PHRASE_QUERY, &bitmap));
    EXPECT_TRUE(bitmap.contains(0));
    EXPECT_TRUE(bitmap.contains(1));
    // StandardAnalyzer keeps hyphenated words like "hello-world" as a single token in many
    // configurations; we don't pin its exact behavior here, but the order-mismatched row
    // must always be excluded.
    EXPECT_FALSE(bitmap.contains(3));
}

// An empty phrase string tokenizes to zero terms. PhraseQuery with zero terms should not
// crash the BE and should produce a deterministic (empty) result set. This is the second
// edge case (alongside single-token) that historically interacted poorly with CLucene's
// SegmentTermPositions.
TEST_F(CLuceneMatchPhraseTest, match_phrase_empty_phrase_does_not_crash) {
    auto idx = make_tablet_index(INVERTED_INDEX_PARSER_ENGLISH, /*support_phrase=*/true);
    write_index({"hello world", "any text"}, idx);
    auto reader = make_reader(InvertedIndexParserType::PARSER_ENGLISH, /*support_phrase_override=*/true);

    roaring::Roaring bitmap;
    Slice query("");
    Status st = reader->query(&_stats, "c0", &query, InvertedIndexQueryType::MATCH_PHRASE_QUERY, &bitmap);
    // The contract is "do not crash"; either OK with empty bitmap or a Status::* error is
    // acceptable. We assert the cardinality only in the OK path so the test stays meaningful
    // under either resolution.
    if (st.ok()) {
        EXPECT_EQ(0, bitmap.cardinality());
    }
}

// ---------------------------------------------------------------------------
// Section 4: cross-row / many-doc verification.
//
// CLucene only allocates a SegmentTermPositions stream when the term has a posting list of
// non-trivial length; the .prx access path that historically crashed is on the cold path
// of "term occurs across many docs". This test seeds enough rows that the term "quick"
// reaches that path.
// ---------------------------------------------------------------------------

TEST_F(CLuceneMatchPhraseTest, match_phrase_many_docs_returns_correct_bitmap) {
    std::vector<std::string> values;
    values.reserve(200);
    for (int i = 0; i < 100; ++i) {
        values.emplace_back("the quick brown fox " + std::to_string(i));
    }
    for (int i = 0; i < 100; ++i) {
        values.emplace_back("slow brown rabbit " + std::to_string(i));
    }

    auto idx = make_tablet_index(INVERTED_INDEX_PARSER_ENGLISH, /*support_phrase=*/true);
    write_index(values, idx);
    auto reader = make_reader(InvertedIndexParserType::PARSER_ENGLISH, /*support_phrase_override=*/true);

    roaring::Roaring bitmap;
    Slice query("quick brown");
    ASSERT_OK(reader->query(&_stats, "c0", &query, InvertedIndexQueryType::MATCH_PHRASE_QUERY, &bitmap));
    // First 100 rows contain "quick brown" as an adjacent phrase. The other 100 contain
    // "brown" but not "quick".
    EXPECT_EQ(100, bitmap.cardinality());
    for (uint32_t i = 0; i < 100; ++i) {
        EXPECT_TRUE(bitmap.contains(i)) << "row " << i << " expected to match";
    }
    for (uint32_t i = 100; i < 200; ++i) {
        EXPECT_FALSE(bitmap.contains(i)) << "row " << i << " must NOT match";
    }
}

// ---------------------------------------------------------------------------
// Section 5: writer/reader metadata desync.
//
// Even if the writer wrote .prx files (support_phrase=true at ingest time), the reader
// independently enforces the flag from the live TabletIndex metadata. This protects users
// who flip support_phrase back to false on existing data: MATCH_PHRASE should be rejected
// at query time rather than silently working off stale .prx files.
// ---------------------------------------------------------------------------

TEST_F(CLuceneMatchPhraseTest, reader_rejects_match_phrase_even_if_prx_exists_on_disk) {
    // Write with support_phrase=true so .prx files are emitted.
    auto write_idx = make_tablet_index(INVERTED_INDEX_PARSER_ENGLISH, /*support_phrase=*/true);
    write_index({"hello world", "the quick brown fox"}, write_idx);
    ASSERT_TRUE(dir_has_file_with_extension(".prx"));

    // But construct the reader with support_phrase=false (simulating metadata that was
    // edited after the data was written).
    auto reader = make_reader(InvertedIndexParserType::PARSER_ENGLISH,
                              /*support_phrase_override=*/false);
    roaring::Roaring bitmap;
    Slice query("hello world");
    Status st = reader->query(&_stats, "c0", &query, InvertedIndexQueryType::MATCH_PHRASE_QUERY, &bitmap);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_not_supported()) << st;

    // Non-phrase queries should still work against the same data.
    roaring::Roaring eq_bitmap;
    Slice eq_query("hello");
    ASSERT_OK(reader->query(&_stats, "c0", &eq_query, InvertedIndexQueryType::EQUAL_QUERY, &eq_bitmap));
    EXPECT_TRUE(eq_bitmap.contains(0));
}

} // namespace starrocks
