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

// Integration tests for the builtin GIN freqs storage path (index_options = DOCS_AND_FREQS):
// end-to-end capture (tokenize -> tf / doc_len / sum_len) on write and read-back of
// posting / doc_freq / doc_len / sum_len, plus the DOCS-unchanged behavior.

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "base/string/slice.h"
#include "base/testutil/assert.h"
#include "fs/fs_memory.h"
#include "gen_cpp/segment.pb.h"
#include "storage/index/inverted/builtin/block_posting_reader.h"
#include "storage/index/inverted/builtin/builtin_inverted_reader.h"
#include "storage/index/inverted/builtin/builtin_inverted_writer.h"
#include "storage/index/inverted/inverted_index_common.h"
#include "storage/index/inverted/inverted_index_option.h"
#include "storage/rowset/bitmap_index_reader.h"
#include "storage/rowset/options.h"
#include "storage/tablet_index.h"
#include "storage/types.h"

namespace starrocks {

class BuiltinGinFreqsTest : public testing::Test {
public:
    const std::string kTestDir = "/builtin_gin_freqs_test";

protected:
    void SetUp() override {
        _fs = std::make_shared<MemoryFileSystem>();
        ASSERT_TRUE(_fs->create_dir(kTestDir).ok());
        _opts.stats = &_stats;
    }

    // A row is either a value (one Slice) or a null. We model rows as (value, is_null).
    struct Row {
        std::string value;
        bool is_null = false;
    };

    // Write a DOCS_AND_FREQS (or DOCS) builtin GIN segment for the given rows and parser.
    ColumnMetaPB write(const std::string& file, const std::string& parser, bool with_freqs,
                       const std::vector<Row>& rows) {
        TabletIndex tablet_index;
        tablet_index.add_index_properties(INVERTED_INDEX_PARSER_KEY, parser);
        if (with_freqs) {
            tablet_index.add_index_properties(INVERTED_INDEX_OPTIONS_KEY, INVERTED_INDEX_OPTIONS_DOCS_AND_FREQS);
        }
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

    // Open a BuiltinInvertedReader on a written segment. Keeps the random-access file alive in *rfile.
    std::unique_ptr<InvertedReader> open_reader(const std::string& file, const std::string& parser, size_t segment_rows,
                                                BuiltinInvertedIndexPB* meta_copy,
                                                std::unique_ptr<RandomAccessFile>* rfile) {
        auto rf = _fs->new_random_access_file(file);
        CHECK(rf.ok());
        *rfile = std::move(rf.value());
        _opts.read_file = rfile->get();
        _opts.segment_rows = segment_rows;
        auto tablet_index_sp = std::make_shared<TabletIndex>();
        tablet_index_sp->add_index_properties(INVERTED_INDEX_PARSER_KEY, parser);
        std::unique_ptr<InvertedReader> reader;
        CHECK_OK(BuiltinInvertedReader::create(tablet_index_sp, TYPE_VARCHAR, &reader));
        CHECK_OK(reader->load(_opts, meta_copy));
        return reader;
    }

    // Collect a term's full (docid, tf) sequence across its blocks.
    void read_term(BlockPostingIterator* pr, uint32_t term, std::vector<uint32_t>* docids, std::vector<uint32_t>* tfs) {
        ASSERT_OK(pr->seek_to_term(term));
        docids->clear();
        tfs->clear();
        while (pr->has_next_block()) {
            ASSERT_OK(pr->next_block());
            for (size_t i = 0; i < pr->cur_block_size(); ++i) {
                docids->push_back(pr->docids()[i]);
                tfs->push_back(pr->tfs()[i]);
            }
        }
    }

    std::shared_ptr<MemoryFileSystem> _fs;
    IndexReadOptions _opts;
    OlapReaderStatistics _stats;
};

// English parser: tf within a doc, df across docs, doc_len, sum_len; dict ordinal alignment.
TEST_F(BuiltinGinFreqsTest, english_freqs_basic) {
    // row0: "apple banana apple"  -> apple:2 banana:1   len 3
    // row1: "banana cherry"       -> banana:1 cherry:1  len 2
    // row2: "apple cherry cherry cherry" -> apple:1 cherry:3 len 4
    // distinct terms sorted: apple(0), banana(1), cherry(2)
    const std::string file = kTestDir + "/english_basic";
    ColumnMetaPB meta = write(file, INVERTED_INDEX_PARSER_ENGLISH, true,
                              {{"apple banana apple"}, {"banana cherry"}, {"apple cherry cherry cherry"}});
    BuiltinInvertedIndexPB meta_copy = meta.indexes(0).builtin_inverted_index();
    ASSERT_EQ(BuiltinInvertedIndexPB::DOCS_AND_FREQS, meta_copy.index_options());
    ASSERT_TRUE(meta_copy.has_posting());
    ASSERT_TRUE(meta_copy.has_norms());

    std::unique_ptr<RandomAccessFile> rfile;
    auto reader = open_reader(file, INVERTED_INDEX_PARSER_ENGLISH, 3, &meta_copy, &rfile);
    auto* br = down_cast<BuiltinInvertedReader*>(reader.get());
    ASSERT_TRUE(br->has_freqs());

    ASSIGN_OR_ABORT(auto freqs, br->new_freqs_iterator(_opts));
    std::unique_ptr<BlockPostingIterator> pr;
    ASSERT_OK(freqs->new_posting_cursor(_opts, &pr));
    std::vector<uint32_t> d, f;
    read_term(pr.get(), 0, &d, &f); // apple
    EXPECT_EQ((std::vector<uint32_t>{0, 2}), d);
    EXPECT_EQ((std::vector<uint32_t>{2, 1}), f);
    read_term(pr.get(), 1, &d, &f); // banana
    EXPECT_EQ((std::vector<uint32_t>{0, 1}), d);
    EXPECT_EQ((std::vector<uint32_t>{1, 1}), f);
    read_term(pr.get(), 2, &d, &f); // cherry
    EXPECT_EQ((std::vector<uint32_t>{1, 2}), d);
    EXPECT_EQ((std::vector<uint32_t>{1, 3}), f);

    // df per term
    ASSIGN_OR_ABORT(auto df0, freqs->doc_freq(0));
    ASSIGN_OR_ABORT(auto df1, freqs->doc_freq(1));
    ASSIGN_OR_ABORT(auto df2, freqs->doc_freq(2));
    EXPECT_EQ(2u, df0);
    EXPECT_EQ(2u, df1);
    EXPECT_EQ(2u, df2);

    // doc_len per row + sum_len
    ASSIGN_OR_ABORT(auto dl0, freqs->doc_len(0));
    ASSIGN_OR_ABORT(auto dl1, freqs->doc_len(1));
    ASSIGN_OR_ABORT(auto dl2, freqs->doc_len(2));
    EXPECT_EQ(3u, dl0);
    EXPECT_EQ(2u, dl1);
    EXPECT_EQ(4u, dl2);
    EXPECT_EQ(9u, freqs->sum_len());
}

// parser=none: each whole value is a single term, tf=1, doc_len=1.
TEST_F(BuiltinGinFreqsTest, parser_none_freqs) {
    const std::string file = kTestDir + "/none_basic";
    ColumnMetaPB meta = write(file, INVERTED_INDEX_PARSER_NONE, true, {{"x"}, {"y"}, {"x"}});
    BuiltinInvertedIndexPB meta_copy = meta.indexes(0).builtin_inverted_index();

    std::unique_ptr<RandomAccessFile> rfile;
    auto reader = open_reader(file, INVERTED_INDEX_PARSER_NONE, 3, &meta_copy, &rfile);
    auto* br = down_cast<BuiltinInvertedReader*>(reader.get());
    ASSERT_TRUE(br->has_freqs());

    ASSIGN_OR_ABORT(auto freqs, br->new_freqs_iterator(_opts));
    std::unique_ptr<BlockPostingIterator> pr;
    ASSERT_OK(freqs->new_posting_cursor(_opts, &pr));
    std::vector<uint32_t> d, f;
    read_term(pr.get(), 0, &d, &f); // "x"
    EXPECT_EQ((std::vector<uint32_t>{0, 2}), d);
    EXPECT_EQ((std::vector<uint32_t>{1, 1}), f);
    read_term(pr.get(), 1, &d, &f); // "y"
    EXPECT_EQ((std::vector<uint32_t>{1}), d);
    EXPECT_EQ((std::vector<uint32_t>{1}), f);

    ASSIGN_OR_ABORT(auto df0, freqs->doc_freq(0));
    ASSIGN_OR_ABORT(auto df1, freqs->doc_freq(1));
    EXPECT_EQ(2u, df0);
    EXPECT_EQ(1u, df1);
    for (uint32_t r = 0; r < 3; ++r) {
        ASSIGN_OR_ABORT(auto dl, freqs->doc_len(r));
        EXPECT_EQ(1u, dl);
    }
    EXPECT_EQ(3u, freqs->sum_len());
}

// Default DOCS (index_options unset): no posting/norms, has_freqs() == false.
TEST_F(BuiltinGinFreqsTest, docs_default_has_no_freqs) {
    const std::string file = kTestDir + "/docs_only";
    ColumnMetaPB meta =
            write(file, INVERTED_INDEX_PARSER_ENGLISH, /*with_freqs=*/false, {{"apple banana"}, {"banana"}});
    BuiltinInvertedIndexPB meta_copy = meta.indexes(0).builtin_inverted_index();
    EXPECT_EQ(BuiltinInvertedIndexPB::DOCS, meta_copy.index_options());
    EXPECT_FALSE(meta_copy.has_posting());
    EXPECT_FALSE(meta_copy.has_norms());

    std::unique_ptr<RandomAccessFile> rfile;
    auto reader = open_reader(file, INVERTED_INDEX_PARSER_ENGLISH, 2, &meta_copy, &rfile);
    auto* br = down_cast<BuiltinInvertedReader*>(reader.get());
    EXPECT_FALSE(br->has_freqs());
    // No freqs side data on a DOCS index -> minting a freqs iterator fails.
    EXPECT_FALSE(br->new_freqs_iterator(_opts).ok());
}

// A term appearing in > 128 docs exercises the multi-block posting path through the GIN writer.
TEST_F(BuiltinGinFreqsTest, multi_block_posting) {
    const std::string file = kTestDir + "/multiblock";
    const uint32_t kN = 200;
    std::vector<Row> rows;
    rows.reserve(kN);
    for (uint32_t i = 0; i < kN; ++i) {
        // "common" is in every doc; "wN" varies. 'c' < 'w' so "common" is dict ordinal 0.
        rows.push_back({"common w" + std::to_string(i % 50)});
    }
    ColumnMetaPB meta = write(file, INVERTED_INDEX_PARSER_ENGLISH, true, rows);
    BuiltinInvertedIndexPB meta_copy = meta.indexes(0).builtin_inverted_index();

    std::unique_ptr<RandomAccessFile> rfile;
    auto reader = open_reader(file, INVERTED_INDEX_PARSER_ENGLISH, kN, &meta_copy, &rfile);
    auto* br = down_cast<BuiltinInvertedReader*>(reader.get());
    ASSERT_TRUE(br->has_freqs());

    ASSIGN_OR_ABORT(auto freqs, br->new_freqs_iterator(_opts));
    std::unique_ptr<BlockPostingIterator> pr;
    ASSERT_OK(freqs->new_posting_cursor(_opts, &pr));
    std::vector<uint32_t> d, f;
    read_term(pr.get(), 0, &d, &f); // "common"
    ASSERT_EQ(kN, d.size());
    for (uint32_t i = 0; i < kN; ++i) {
        EXPECT_EQ(i, d[i]);  // every doc, in order, across blocks
        EXPECT_EQ(1u, f[i]); // tf 1
    }
    ASSIGN_OR_ABORT(auto df0, freqs->doc_freq(0));
    EXPECT_EQ(kN, df0);
    ASSIGN_OR_ABORT(auto dl0, freqs->doc_len(0));
    EXPECT_EQ(2u, dl0); // "common" + "wN"
    EXPECT_EQ(2u * kN, freqs->sum_len());
}

// Null rows contribute doc_len 0 and are skipped in postings.
TEST_F(BuiltinGinFreqsTest, nulls_freqs) {
    const std::string file = kTestDir + "/nulls";
    // row0: "apple"  row1: NULL  row2: "apple"  row3: NULL
    ColumnMetaPB meta =
            write(file, INVERTED_INDEX_PARSER_ENGLISH, true, {{"apple"}, {"", true}, {"apple"}, {"", true}});
    BuiltinInvertedIndexPB meta_copy = meta.indexes(0).builtin_inverted_index();

    std::unique_ptr<RandomAccessFile> rfile;
    auto reader = open_reader(file, INVERTED_INDEX_PARSER_ENGLISH, 4, &meta_copy, &rfile);
    auto* br = down_cast<BuiltinInvertedReader*>(reader.get());
    ASSERT_TRUE(br->has_freqs());

    ASSIGN_OR_ABORT(auto freqs, br->new_freqs_iterator(_opts));
    std::unique_ptr<BlockPostingIterator> pr;
    ASSERT_OK(freqs->new_posting_cursor(_opts, &pr));
    std::vector<uint32_t> d, f;
    read_term(pr.get(), 0, &d, &f); // "apple": rows 0 and 2 only
    EXPECT_EQ((std::vector<uint32_t>{0, 2}), d);
    EXPECT_EQ((std::vector<uint32_t>{1, 1}), f);

    ASSIGN_OR_ABORT(auto dl0, freqs->doc_len(0));
    ASSIGN_OR_ABORT(auto dl1, freqs->doc_len(1));
    ASSIGN_OR_ABORT(auto dl3, freqs->doc_len(3));
    EXPECT_EQ(1u, dl0); // "apple"
    EXPECT_EQ(0u, dl1); // null
    EXPECT_EQ(0u, dl3); // null
    EXPECT_EQ(2u, freqs->sum_len());
}

// The whole feature rests on one invariant: the term ordinal the query path resolves via the presence
// bitmap dictionary (seek_dictionary -> current_ordinal) is the SAME ordinal that indexes the term's
// freqs posting + doc_freq. The other tests assert the writer is internally consistent (posting ordinal
// i is the i-th sorted term); this one asserts that ordinal actually matches the bitmap dictionary, so
// a future regression in either sort path (e.g. signed vs unsigned byte order) is caught here.
TEST_F(BuiltinGinFreqsTest, posting_ordinal_matches_bitmap_dictionary) {
    const std::string file = kTestDir + "/align";
    // Same corpus as english_freqs_basic: sorted distinct terms are apple, banana, cherry.
    ColumnMetaPB meta = write(file, INVERTED_INDEX_PARSER_ENGLISH, true,
                              {{"apple banana apple"}, {"banana cherry"}, {"apple cherry cherry cherry"}});
    BuiltinInvertedIndexPB meta_copy = meta.indexes(0).builtin_inverted_index();

    std::unique_ptr<RandomAccessFile> rfile;
    auto reader = open_reader(file, INVERTED_INDEX_PARSER_ENGLISH, 3, &meta_copy, &rfile);
    auto* br = down_cast<BuiltinInvertedReader*>(reader.get());
    ASSERT_TRUE(br->has_freqs());
    ASSIGN_OR_ABORT(auto freqs, br->new_freqs_iterator(_opts));
    std::unique_ptr<BlockPostingIterator> pr;
    ASSERT_OK(freqs->new_posting_cursor(_opts, &pr));

    // Resolve term ordinals exactly the way the query path does -- through the presence-bitmap
    // dictionary loaded from the same segment meta.
    TabletIndex tablet_index;
    tablet_index.add_index_properties(INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_ENGLISH);
    int32_t gram_num = get_gram_num_from_properties(tablet_index.index_properties());
    BitmapIndexReader bitmap_reader(gram_num, false);
    ASSIGN_OR_ABORT(auto first_load, bitmap_reader.load(_opts, meta_copy.bitmap_index()));
    ASSERT_TRUE(first_load);
    SegmentBitmapIndexIterator* raw_it = nullptr;
    ASSERT_OK(bitmap_reader.new_iterator(_opts, &raw_it));
    std::unique_ptr<SegmentBitmapIndexIterator> dict_it(raw_it);

    auto dict_ordinal = [&](const std::string& term) -> uint32_t {
        bool exact = false;
        Slice s(term);
        CHECK_OK(dict_it->seek_dictionary(&s, &exact));
        CHECK(exact) << "term missing from bitmap dictionary: " << term;
        return dict_it->current_ordinal();
    };
    const uint32_t apple = dict_ordinal("apple");
    const uint32_t banana = dict_ordinal("banana");
    const uint32_t cherry = dict_ordinal("cherry");
    EXPECT_EQ(0u, apple);
    EXPECT_EQ(1u, banana);
    EXPECT_EQ(2u, cherry);

    // Reading the posting / doc_freq at the dictionary ordinal must yield that exact term's data.
    std::vector<uint32_t> d, f;
    read_term(pr.get(), banana, &d, &f);
    EXPECT_EQ((std::vector<uint32_t>{0, 1}), d); // banana in rows 0 and 1
    EXPECT_EQ((std::vector<uint32_t>{1, 1}), f);
    ASSIGN_OR_ABORT(auto df_banana, freqs->doc_freq(banana));
    EXPECT_EQ(2u, df_banana);

    read_term(pr.get(), cherry, &d, &f);
    EXPECT_EQ((std::vector<uint32_t>{1, 2}), d); // cherry in rows 1 and 2
    EXPECT_EQ((std::vector<uint32_t>{1, 3}), f);
    ASSIGN_OR_ABORT(auto df_cherry, freqs->doc_freq(cherry));
    EXPECT_EQ(2u, df_cherry);
}

} // namespace starrocks
