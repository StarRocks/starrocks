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
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "base/string/slice.h"
#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "column/chunk_factory.h"
#include "common/config_rowset_fwd.h"
#include "fs/fs_memory.h"
#include "gen_cpp/segment.pb.h"
#include "storage/index/inverted/builtin/block_posting_reader.h"
#include "storage/index/inverted/builtin/builtin_inverted_reader.h"
#include "storage/index/inverted/builtin/builtin_inverted_writer.h"
#include "storage/index/inverted/inverted_index_common.h"
#include "storage/index/inverted/inverted_index_option.h"
#include "storage/rowset/bitmap_index_reader.h"
#include "storage/rowset/encoding_info.h"
#include "storage/rowset/indexed_column_reader.h"
#include "storage/rowset/indexed_column_writer.h"
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

    // `n` rows whose doc_len cycles 1..7, so a right-page/wrong-offset read cannot pass.
    std::vector<Row> varying_len_rows(uint32_t n, std::vector<uint32_t>* lens) {
        std::vector<Row> rows;
        rows.reserve(n);
        lens->clear();
        lens->reserve(n);
        for (uint32_t i = 0; i < n; ++i) {
            const uint32_t words = i % 7 + 1;
            std::string v;
            for (uint32_t w = 0; w < words; ++w) {
                v += "w" + std::to_string(w) + " ";
            }
            rows.push_back({v});
            lens->push_back(words);
        }
        return rows;
    }

    // A standalone u32 IndexedColumn of `n` values, value i = i * 3 + 1, in the given encoding.
    IndexedColumnMetaPB write_u32_column(const std::string& file, EncodingTypePB encoding, size_t n) {
        auto wfile = *_fs->new_writable_file(file);
        IndexedColumnWriterOptions options;
        options.write_ordinal_index = true;
        options.write_value_index = false;
        options.encoding = encoding;
        TypeInfoPtr typeinfo = get_type_info(TYPE_INT);
        IndexedColumnWriter writer(options, typeinfo, wfile.get());
        CHECK_OK(writer.init());
        for (size_t i = 0; i < n; ++i) {
            auto v = static_cast<int32_t>(i * 3 + 1);
            CHECK_OK(writer.add(&v));
        }
        IndexedColumnMetaPB meta;
        CHECK_OK(writer.finish(&meta));
        CHECK_OK(wfile->close());
        return meta;
    }

    // The whole doc_len column through next_batch on its own reader: the reference either fetch path
    // has to match, sharing no state with FreqsIterator.
    std::vector<uint32_t> read_doc_len_column(const BuiltinInvertedIndexPB& meta, size_t n) {
        IndexedColumnReader reader(meta.norms().doc_len_column());
        CHECK_OK(reader.load(_opts));
        std::unique_ptr<IndexedColumnIterator> iter;
        CHECK_OK(reader.new_iterator(_opts, &iter));
        CHECK_OK(iter->seek_to_ordinal(0));
        auto column = ChunkFactory::column_from_field_type(TYPE_INT, false);
        size_t read = 0;
        while (read < n) {
            size_t batch = n - read;
            CHECK_OK(iter->next_batch(&batch, column.get()));
            CHECK(batch > 0) << "short read on the doc_len column";
            read += batch;
        }
        std::vector<uint32_t> out;
        out.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            out.push_back(static_cast<uint32_t>(column->get(i).get_int32()));
        }
        return out;
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

// A segment this small is one doc_len page (1 MiB data pages, 262,144 u32 values), so this pins the
// offset arithmetic inside a view under orders that are not ascending. doc_len varies row to row, so
// landing at the wrong offset cannot pass.
TEST_F(BuiltinGinFreqsTest, doc_len_reads_across_page_boundaries) {
    const std::string file = kTestDir + "/doc_len_pages";
    const uint32_t kN = 200;
    std::vector<uint32_t> expected;
    ColumnMetaPB meta = write(file, INVERTED_INDEX_PARSER_ENGLISH, true, varying_len_rows(kN, &expected));
    BuiltinInvertedIndexPB meta_copy = meta.indexes(0).builtin_inverted_index();

    std::unique_ptr<RandomAccessFile> rfile;
    auto reader = open_reader(file, INVERTED_INDEX_PARSER_ENGLISH, kN, &meta_copy, &rfile);
    auto* br = down_cast<BuiltinInvertedReader*>(reader.get());
    ASSERT_TRUE(br->has_freqs());

    std::vector<uint32_t> ascending(kN);
    std::iota(ascending.begin(), ascending.end(), 0);
    std::vector<uint32_t> descending(ascending.rbegin(), ascending.rend());
    std::vector<uint32_t> strided;
    strided.reserve(kN);
    for (uint32_t i = 0; i < kN; ++i) {
        strided.push_back(i * 37 % kN); // 37 is coprime with 200, so this is a permutation
    }

    const std::vector<std::pair<const char*, std::vector<uint32_t>>> orders = {
            {"ascending", ascending}, {"descending", descending}, {"strided", strided}};
    for (const auto& [name, order] : orders) {
        SCOPED_TRACE(name);
        // A fresh iterator per order: the view lives in the iterator, so reuse would warm the next.
        ASSIGN_OR_ABORT(auto freqs, br->new_freqs_iterator(_opts));
        for (uint32_t rid : order) {
            ASSIGN_OR_ABORT(auto dl, freqs->doc_len(rid));
            EXPECT_EQ(expected[rid], dl) << "rid " << rid;
        }
    }
}

// Two independent implementations of the same lookup; this optimization may change which one runs,
// never the value. The reference uses its own IndexedColumnReader, sharing no state with the iterator
// under test.
TEST_F(BuiltinGinFreqsTest, doc_len_page_view_matches_a_next_batch_read) {
    const std::string file = kTestDir + "/doc_len_vs_next_batch";
    const uint32_t kN = 200;
    std::vector<uint32_t> expected;
    ColumnMetaPB meta = write(file, INVERTED_INDEX_PARSER_ENGLISH, true, varying_len_rows(kN, &expected));
    BuiltinInvertedIndexPB meta_copy = meta.indexes(0).builtin_inverted_index();

    std::unique_ptr<RandomAccessFile> rfile;
    auto reader = open_reader(file, INVERTED_INDEX_PARSER_ENGLISH, kN, &meta_copy, &rfile);
    auto* br = down_cast<BuiltinInvertedReader*>(reader.get());
    ASSERT_TRUE(br->has_freqs());

    const std::vector<uint32_t> reference = read_doc_len_column(meta_copy, kN);
    ASSERT_EQ(kN, reference.size());

    ASSIGN_OR_ABORT(auto freqs, br->new_freqs_iterator(_opts));
    for (uint32_t rid = 0; rid < kN; ++rid) {
        ASSIGN_OR_ABORT(auto dl, freqs->doc_len(rid));
        EXPECT_EQ(reference[rid], dl) << "rid " << rid;
        EXPECT_EQ(expected[rid], dl) << "rid " << rid;
    }
}

// The fast path needs the column's pages to be a flat array. Pin that premise: the column records
// bit-shuffle and the decoder hands out a view over it.
TEST_F(BuiltinGinFreqsTest, doc_len_column_layout_supports_the_page_view) {
    const std::string file = kTestDir + "/doc_len_layout";
    const uint32_t kN = 200;
    std::vector<uint32_t> expected;
    ColumnMetaPB meta = write(file, INVERTED_INDEX_PARSER_ENGLISH, true, varying_len_rows(kN, &expected));
    BuiltinInvertedIndexPB meta_copy = meta.indexes(0).builtin_inverted_index();

    std::unique_ptr<RandomAccessFile> rfile;
    auto reader = open_reader(file, INVERTED_INDEX_PARSER_ENGLISH, kN, &meta_copy, &rfile);
    ASSERT_TRUE(down_cast<BuiltinInvertedReader*>(reader.get())->has_freqs());

    IndexedColumnReader doc_len(meta_copy.norms().doc_len_column());
    ASSERT_OK(doc_len.load(_opts));
    // The writer asks for the default rather than pinning one, so assert it followed the default and
    // that the default is still an encoding this optimization can use.
    EXPECT_EQ(EncodingInfo::get_default_encoding(TYPE_INT, false), doc_len.encoding_info()->encoding());
    EXPECT_EQ(BIT_SHUFFLE, doc_len.encoding_info()->encoding());
    EXPECT_EQ(static_cast<int64_t>(kN), doc_len.num_values());

    std::unique_ptr<IndexedColumnIterator> iter;
    ASSERT_OK(doc_len.new_iterator(_opts, &iter));

    // One page covers every row here: data pages are 1 MiB (262,144 u32 values) and index_page_size
    // only bounds index pages. Asserting the count keeps that visible -- it is why a crossing needs a
    // segment past 262,144 rows, and why the offset math is covered at the decoder level.
    ASSERT_OK(iter->seek_to_ordinal(0));
    const uint8_t* data = nullptr;
    ordinal_t first = 0;
    size_t count = 0;
    ASSERT_OK(iter->current_page_view(&data, &first, &count));
    EXPECT_EQ(0u, first);
    EXPECT_EQ(static_cast<size_t>(kN), count);
    ASSERT_NE(nullptr, data);
    const auto* values = reinterpret_cast<const uint32_t*>(data);
    for (size_t i = 0; i < count; ++i) {
        EXPECT_EQ(expected[i], values[i]) << "ordinal " << i;
    }
}

// With dictionary_encoding_ratio_for_non_string_column set, get_default_encoding() returns
// DICT_ENCODING for INT, which an IndexedColumn cannot read back at all. Pin that this config cannot
// reach the two side columns.
TEST_F(BuiltinGinFreqsTest, doc_len_encoding_is_named_not_defaulted) {
    const double saved_ratio = config::dictionary_encoding_ratio_for_non_string_column;
    config::dictionary_encoding_ratio_for_non_string_column = 1.0;
    DeferOp restore([&]() { config::dictionary_encoding_ratio_for_non_string_column = saved_ratio; });
    // Guard the guard: if the gate stops flipping, this test would pass without proving anything.
    ASSERT_EQ(DICT_ENCODING, EncodingInfo::get_default_encoding(TYPE_INT, false));

    const std::string file = kTestDir + "/doc_len_named_encoding";
    const uint32_t kN = 200;
    std::vector<uint32_t> expected;
    ColumnMetaPB meta = write(file, INVERTED_INDEX_PARSER_ENGLISH, true, varying_len_rows(kN, &expected));
    BuiltinInvertedIndexPB meta_copy = meta.indexes(0).builtin_inverted_index();

    std::unique_ptr<RandomAccessFile> rfile;
    auto reader = open_reader(file, INVERTED_INDEX_PARSER_ENGLISH, kN, &meta_copy, &rfile);
    auto* br = down_cast<BuiltinInvertedReader*>(reader.get());
    ASSERT_TRUE(br->has_freqs());

    // Both side columns must still be bit-shuffle, not the (unreadable here) default.
    IndexedColumnReader doc_len(meta_copy.norms().doc_len_column());
    ASSERT_OK(doc_len.load(_opts));
    EXPECT_EQ(BIT_SHUFFLE, doc_len.encoding_info()->encoding());
    IndexedColumnReader doc_freq(meta_copy.posting().doc_freq_column());
    ASSERT_OK(doc_freq.load(_opts));
    EXPECT_EQ(BIT_SHUFFLE, doc_freq.encoding_info()->encoding());

    // And the column still reads back, both through the page view and through next_batch.
    const std::vector<uint32_t> reference = read_doc_len_column(meta_copy, kN);
    ASSIGN_OR_ABORT(auto freqs, br->new_freqs_iterator(_opts));
    for (uint32_t rid = 0; rid < kN; ++rid) {
        ASSIGN_OR_ABORT(auto dl, freqs->doc_len(rid));
        EXPECT_EQ(reference[rid], dl) << "rid " << rid;
        EXPECT_EQ(expected[rid], dl) << "rid " << rid;
    }
}

// current_page_view() is a general IndexedColumn API, and what it answers is decided by the page layout
// alone: a flat fixed-width page yields a view, anything else is refused, and before a seek there is no
// page to describe. PLAIN and FOR_ENCODING are both registered for INT and both read back fine, so they
// isolate the layout from everything else about the column.
TEST_F(BuiltinGinFreqsTest, current_page_view_follows_the_page_layout) {
    const size_t kN = 300;

    struct Arm {
        const char* name;
        EncodingTypePB encoding;
        bool expect_view;
    };
    for (const Arm& arm : {Arm{"plain", PLAIN_ENCODING, true}, Arm{"for", FOR_ENCODING, false}}) {
        SCOPED_TRACE(arm.name);
        const std::string file = kTestDir + "/page_view_" + arm.name;
        IndexedColumnMetaPB meta = write_u32_column(file, arm.encoding, kN);

        ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file));
        _opts.read_file = rfile.get();
        IndexedColumnReader reader(meta);
        ASSERT_OK(reader.load(_opts));
        ASSERT_EQ(arm.encoding, reader.encoding_info()->encoding());
        std::unique_ptr<IndexedColumnIterator> iter;
        ASSERT_OK(reader.new_iterator(_opts, &iter));

        const uint8_t* data = nullptr;
        ordinal_t first = 0;
        size_t count = 0;
        // Before any seek there is no page to describe, whatever the layout is.
        EXPECT_TRUE(iter->current_page_view(&data, &first, &count).is_internal_error());

        ASSERT_OK(iter->seek_to_ordinal(0));
        Status st = iter->current_page_view(&data, &first, &count);
        if (arm.expect_view) {
            ASSERT_OK(st);
            EXPECT_EQ(0u, first);
            ASSERT_EQ(kN, count);
            const auto* values = reinterpret_cast<const int32_t*>(data);
            for (size_t i = 0; i < kN; ++i) {
                EXPECT_EQ(static_cast<int32_t>(i * 3 + 1), values[i]) << "ordinal " << i;
            }
        } else {
            EXPECT_TRUE(st.is_not_supported());
        }

        // Either way the column itself reads back: the view is an optimization, not a requirement.
        ASSERT_OK(iter->seek_to_ordinal(0));
        auto column = ChunkFactory::column_from_field_type(TYPE_INT, false);
        size_t n = kN;
        ASSERT_OK(iter->next_batch(&n, column.get()));
        ASSERT_EQ(kN, n);
        EXPECT_EQ(1, column->get(0).get_int32());
        EXPECT_EQ(static_cast<int32_t>((kN - 1) * 3 + 1), column->get(kN - 1).get_int32());
    }
}

// A column whose pages offer no view must still serve every doc_len, through the same point read used
// before this change: the view is an optimization, so its absence may cost the optimization and nothing
// else. FreqsIterator is built directly here because the writer names an encoding that always provides
// a view, so this arm is not reachable through BuiltinInvertedWriter.
TEST_F(BuiltinGinFreqsTest, doc_len_falls_back_when_the_column_offers_no_view) {
    const size_t kN = 300;
    const std::string file = kTestDir + "/doc_len_no_view";
    IndexedColumnMetaPB meta = write_u32_column(file, FOR_ENCODING, kN);

    ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file));
    _opts.read_file = rfile.get();
    IndexedColumnReader reader(meta);
    ASSERT_OK(reader.load(_opts));
    std::unique_ptr<IndexedColumnIterator> df_iter;
    std::unique_ptr<IndexedColumnIterator> doc_len_iter;
    ASSERT_OK(reader.new_iterator(_opts, &df_iter));
    ASSERT_OK(reader.new_iterator(_opts, &doc_len_iter));

    // No posting loader: this exercises doc_len only, and nothing on that path touches it.
    FreqsIterator freqs(nullptr, std::move(df_iter), std::move(doc_len_iter), 0, static_cast<int64_t>(kN));
    for (uint32_t rid = 0; rid < kN; ++rid) {
        ASSIGN_OR_ABORT(auto dl, freqs.doc_len(rid));
        EXPECT_EQ(rid * 3 + 1, dl) << "rid " << rid;
    }
    // Descending too: the point read holds no state between lookups, so order cannot matter.
    for (uint32_t rid = kN; rid-- > 0;) {
        ASSIGN_OR_ABORT(auto dl, freqs.doc_len(rid));
        EXPECT_EQ(rid * 3 + 1, dl) << "rid " << rid;
    }
    // The range check still applies on this path.
    EXPECT_FALSE(freqs.doc_len(kN).ok());
    ASSIGN_OR_ABORT(auto dl0, freqs.doc_len(0));
    EXPECT_EQ(1u, dl0) << "a rejection left the iterator unusable";
}

// A document length is a property of the data, so the dict gate must not move it: same corpus written
// with the gate off and on, same doc_len for every row and same sum_len.
TEST_F(BuiltinGinFreqsTest, doc_len_agrees_regardless_of_the_dict_gate) {
    const uint32_t kN = 200;
    std::vector<uint32_t> expected;
    const std::vector<Row> rows = varying_len_rows(kN, &expected);

    auto lengths_under = [&](const char* tag, double dict_ratio) {
        const double saved = config::dictionary_encoding_ratio_for_non_string_column;
        config::dictionary_encoding_ratio_for_non_string_column = dict_ratio;
        DeferOp restore([&]() { config::dictionary_encoding_ratio_for_non_string_column = saved; });

        ColumnMetaPB meta = write(kTestDir + "/agree_" + tag, INVERTED_INDEX_PARSER_ENGLISH, true, rows);
        BuiltinInvertedIndexPB meta_copy = meta.indexes(0).builtin_inverted_index();
        std::unique_ptr<RandomAccessFile> rfile;
        auto reader = open_reader(kTestDir + "/agree_" + tag, INVERTED_INDEX_PARSER_ENGLISH, kN, &meta_copy, &rfile);
        auto* br = down_cast<BuiltinInvertedReader*>(reader.get());
        CHECK(br->has_freqs());
        ASSIGN_OR_ABORT(auto freqs, br->new_freqs_iterator(_opts));
        std::vector<uint32_t> out;
        out.reserve(kN);
        for (uint32_t rid = 0; rid < kN; ++rid) {
            ASSIGN_OR_ABORT(auto dl, freqs->doc_len(rid));
            out.push_back(dl);
        }
        out.push_back(static_cast<uint32_t>(freqs->sum_len())); // folded in so it is compared too
        return out;
    };

    const std::vector<uint32_t> gate_off = lengths_under("gate_off", 0.0);
    const std::vector<uint32_t> gate_on = lengths_under("gate_on", 1.0);
    EXPECT_EQ(gate_off, gate_on);
    ASSERT_EQ(kN + 1, gate_off.size());
    for (uint32_t rid = 0; rid < kN; ++rid) {
        EXPECT_EQ(expected[rid], gate_off[rid]) << "rid " << rid;
    }
}

// seek_to_ordinal(num_values) is a legal past-the-end seek that leaves the previously loaded page in
// place, so an out-of-range rowid has to be rejected before any page view is consulted -- otherwise the
// fast path would index the stale page and hand back a plausible-looking wrong length.
TEST_F(BuiltinGinFreqsTest, doc_len_rejects_out_of_range_rowid) {
    const std::string file = kTestDir + "/doc_len_out_of_range";
    ColumnMetaPB meta = write(file, INVERTED_INDEX_PARSER_ENGLISH, true,
                              {{"apple banana apple"}, {"banana cherry"}, {"apple cherry cherry cherry"}});
    BuiltinInvertedIndexPB meta_copy = meta.indexes(0).builtin_inverted_index();

    std::unique_ptr<RandomAccessFile> rfile;
    auto reader = open_reader(file, INVERTED_INDEX_PARSER_ENGLISH, 3, &meta_copy, &rfile);
    auto* br = down_cast<BuiltinInvertedReader*>(reader.get());
    ASSERT_TRUE(br->has_freqs());

    ASSIGN_OR_ABORT(auto freqs, br->new_freqs_iterator(_opts));
    // Load a page first: the stale-view hazard only exists once one is in place.
    ASSIGN_OR_ABORT(auto dl2, freqs->doc_len(2));
    EXPECT_EQ(4u, dl2);
    EXPECT_FALSE(freqs->doc_len(3).ok());   // exactly past the end
    EXPECT_FALSE(freqs->doc_len(100).ok()); // well past the end
    // A rejection must not leave the iterator unusable.
    ASSIGN_OR_ABORT(auto dl0, freqs->doc_len(0));
    EXPECT_EQ(3u, dl0);
}

} // namespace starrocks
