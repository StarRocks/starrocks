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

#include <string>
#include <vector>

#include "base/coding.h"
#include "base/string/slice.h"
#include "base/testutil/assert.h"
#include "fs/fs_memory.h"
#include "gen_cpp/segment.pb.h"
#include "storage/index/inverted/builtin/block_posting_reader.h"
#include "storage/index/inverted/builtin/block_posting_writer.h"
#include "storage/olap_common.h"
#include "storage/rowset/encoding_info.h"
#include "storage/rowset/indexed_column_writer.h"
#include "storage/rowset/options.h"
#include "storage/types.h"
#include "types/logical_type.h"

namespace starrocks {

class BlockPostingTest : public testing::Test {
protected:
    void SetUp() override {
        _fs = std::make_shared<MemoryFileSystem>();
        ASSERT_TRUE(_fs->create_dir(kTestDir).ok());
        _opts.stats = &_stats;
    }

    struct Term {
        std::vector<uint32_t> docids;
        std::vector<uint32_t> tfs;
        std::vector<uint32_t> doclens;
    };

    // Write the given terms (in order, term i -> ordinal i) and return the populated PostingIndexPB.
    PostingIndexPB write(const std::string& file, const std::vector<Term>& terms) {
        PostingIndexPB pb;
        ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file));
        BlockPostingWriter w(wfile.get());
        CHECK_OK(w.init());
        for (uint32_t t = 0; t < terms.size(); ++t) {
            w.start_term(t);
            for (size_t i = 0; i < terms[t].docids.size(); ++i) {
                w.add(terms[t].docids[i], terms[t].tfs[i], terms[t].doclens[i]);
            }
            CHECK_OK(w.finish_term());
        }
        CHECK_OK(w.finish(pb.mutable_posting_block_column(), pb.mutable_posting_index_column()));
        CHECK_OK(wfile->close());
        return pb;
    }

    // Write two IndexedColumns of raw VARCHAR blobs (block column + directory column), mirroring
    // BlockPostingWriter::init()'s options, so a test can inject a hand-crafted block the writer would
    // never emit (e.g. a zero doc_count).
    PostingIndexPB write_raw(const std::string& file, const std::vector<std::string>& block_blobs,
                             const std::vector<std::string>& dir_blobs) {
        PostingIndexPB pb;
        ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(file));
        TypeInfoPtr typeinfo = get_type_info(TYPE_VARCHAR);
        IndexedColumnWriterOptions options;
        options.write_ordinal_index = true;
        options.write_value_index = false;
        options.encoding = PLAIN_ENCODING;
        options.compression = NO_COMPRESSION;
        IndexedColumnWriter block_col(options, typeinfo, wfile.get());
        IndexedColumnWriter dir_col(options, typeinfo, wfile.get());
        CHECK_OK(block_col.init());
        CHECK_OK(dir_col.init());
        for (const auto& b : block_blobs) {
            Slice s(b);
            CHECK_OK(block_col.add(&s));
        }
        for (const auto& d : dir_blobs) {
            Slice s(d);
            CHECK_OK(dir_col.add(&s));
        }
        CHECK_OK(block_col.finish(pb.mutable_posting_block_column()));
        CHECK_OK(dir_col.finish(pb.mutable_posting_index_column()));
        CHECK_OK(wfile->close());
        return pb;
    }

    // Read back a whole term's (docid, tf) sequence by iterating its blocks.
    void read_all(BlockPostingIterator* it, uint32_t term, std::vector<uint32_t>* docids, std::vector<uint32_t>* tfs) {
        ASSERT_OK(it->seek_to_term(term));
        docids->clear();
        tfs->clear();
        while (it->has_next_block()) {
            ASSERT_OK(it->next_block());
            for (size_t i = 0; i < it->cur_block_size(); ++i) {
                docids->push_back(it->docids()[i]);
                tfs->push_back(it->tfs()[i]);
            }
        }
    }

    std::shared_ptr<MemoryFileSystem> _fs;
    IndexReadOptions _opts;
    OlapReaderStatistics _stats;
    const std::string kTestDir = "/block_posting_test";
};

// Single-block term: exact docid/tf round-trip + per-block max statistics.
TEST_F(BlockPostingTest, single_block) {
    const std::string file = kTestDir + "/single";
    Term t0{{1, 5, 300}, {2, 1, 3}, {10, 4, 7}};
    PostingIndexPB pb = write(file, {t0});

    ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file));
    _opts.read_file = rfile.get();
    BlockPostingReader r;
    ASSERT_OK(r.load(_opts, pb));
    std::unique_ptr<BlockPostingIterator> it;
    ASSERT_OK(r.new_iterator(_opts, &it));

    ASSERT_OK(it->seek_to_term(0));
    ASSERT_TRUE(it->has_next_block());
    ASSERT_OK(it->next_block());
    ASSERT_EQ(3u, it->cur_block_size());
    EXPECT_EQ(1u, it->docids()[0]);
    EXPECT_EQ(5u, it->docids()[1]);
    EXPECT_EQ(300u, it->docids()[2]);
    EXPECT_EQ(2u, it->tfs()[0]);
    EXPECT_EQ(1u, it->tfs()[1]);
    EXPECT_EQ(3u, it->tfs()[2]);
    // block max stats
    EXPECT_EQ(300u, it->cur_block_last_docid());
    EXPECT_EQ(3u, it->cur_block_max_tf());
    EXPECT_EQ(4u, it->cur_block_min_doclen());
    EXPECT_FALSE(it->has_next_block());
}

// Term spanning multiple blocks (>128 docids): full sequence is reconstructed across blocks.
TEST_F(BlockPostingTest, multi_block) {
    const std::string file = kTestDir + "/multi";
    Term t0;
    const uint32_t kN = 300; // 128 + 128 + 44 -> 3 blocks
    for (uint32_t i = 0; i < kN; ++i) {
        t0.docids.push_back(i * 3 + 1); // strictly increasing
        t0.tfs.push_back((i % 5) + 1);
        t0.doclens.push_back(8 + (i % 7));
    }
    PostingIndexPB pb = write(file, {t0});

    ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file));
    _opts.read_file = rfile.get();
    BlockPostingReader r;
    ASSERT_OK(r.load(_opts, pb));
    std::unique_ptr<BlockPostingIterator> it;
    ASSERT_OK(r.new_iterator(_opts, &it));

    std::vector<uint32_t> docids, tfs;
    read_all(it.get(), 0, &docids, &tfs);
    ASSERT_EQ(kN, docids.size());
    ASSERT_EQ(kN, tfs.size());
    for (uint32_t i = 0; i < kN; ++i) {
        EXPECT_EQ(i * 3 + 1, docids[i]) << "docid at " << i;
        EXPECT_EQ((i % 5) + 1, tfs[i]) << "tf at " << i;
    }
}

// Multiple terms with distinct ordinals, each independently readable.
TEST_F(BlockPostingTest, multi_term) {
    const std::string file = kTestDir + "/terms";
    Term t0{{2, 9}, {1, 4}, {3, 6}};
    Term t1{{0, 1, 2, 7, 100}, {5, 5, 5, 5, 5}, {2, 2, 2, 2, 2}};
    PostingIndexPB pb = write(file, {t0, t1});

    ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file));
    _opts.read_file = rfile.get();
    BlockPostingReader r;
    ASSERT_OK(r.load(_opts, pb));
    std::unique_ptr<BlockPostingIterator> it;
    ASSERT_OK(r.new_iterator(_opts, &it));

    std::vector<uint32_t> d, f;
    read_all(it.get(), 1, &d, &f);
    EXPECT_EQ((std::vector<uint32_t>{0, 1, 2, 7, 100}), d);
    EXPECT_EQ((std::vector<uint32_t>{5, 5, 5, 5, 5}), f);

    read_all(it.get(), 0, &d, &f);
    EXPECT_EQ((std::vector<uint32_t>{2, 9}), d);
    EXPECT_EQ((std::vector<uint32_t>{1, 4}), f);
}

// seek_block skips to the first block whose last_docid >= target.
TEST_F(BlockPostingTest, seek_block) {
    const std::string file = kTestDir + "/seek";
    Term t0;
    const uint32_t kN = 300;
    for (uint32_t i = 0; i < kN; ++i) {
        t0.docids.push_back(i); // 0..299, block0=[0..127], block1=[128..255], block2=[256..299]
        t0.tfs.push_back(1);
        t0.doclens.push_back(5);
    }
    PostingIndexPB pb = write(file, {t0});

    ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file));
    _opts.read_file = rfile.get();
    BlockPostingReader r;
    ASSERT_OK(r.load(_opts, pb));
    std::unique_ptr<BlockPostingIterator> it;
    ASSERT_OK(r.new_iterator(_opts, &it));

    ASSERT_OK(it->seek_to_term(0));
    ASSERT_OK(it->seek_block(200)); // -> block1 (last_docid 255 >= 200)
    EXPECT_EQ(255u, it->cur_block_last_docid());
    EXPECT_EQ(128u, it->docids()[0]);
    ASSERT_OK(it->seek_block(256)); // -> block2 (last_docid 299)
    EXPECT_EQ(299u, it->cur_block_last_docid());
    EXPECT_EQ(256u, it->docids()[0]);
}

// Single-doc term: the gap stream is empty (n-1 == 0) -- edge of the block encoding.
TEST_F(BlockPostingTest, single_doc_term) {
    const std::string file = kTestDir + "/single_doc";
    Term t0{{42}, {7}, {3}};
    PostingIndexPB pb = write(file, {t0});

    ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file));
    _opts.read_file = rfile.get();
    BlockPostingReader r;
    ASSERT_OK(r.load(_opts, pb));
    std::unique_ptr<BlockPostingIterator> it;
    ASSERT_OK(r.new_iterator(_opts, &it));

    ASSERT_OK(it->seek_to_term(0));
    ASSERT_TRUE(it->has_next_block());
    ASSERT_OK(it->next_block());
    ASSERT_EQ(1u, it->cur_block_size());
    EXPECT_EQ(42u, it->docids()[0]);
    EXPECT_EQ(7u, it->tfs()[0]);
    EXPECT_EQ(42u, it->cur_block_last_docid());
    EXPECT_EQ(7u, it->cur_block_max_tf());
    EXPECT_EQ(3u, it->cur_block_min_doclen());
    EXPECT_FALSE(it->has_next_block());
}

// Exact block-size boundaries: 128 (one full block) and 256 (two full blocks).
TEST_F(BlockPostingTest, block_boundaries) {
    for (uint32_t kN : {128u, 256u}) {
        const std::string file = kTestDir + "/boundary_" + std::to_string(kN);
        Term t0;
        for (uint32_t i = 0; i < kN; ++i) {
            t0.docids.push_back(i);
            t0.tfs.push_back((i % 4) + 1);
            t0.doclens.push_back(6);
        }
        PostingIndexPB pb = write(file, {t0});

        ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file));
        _opts.read_file = rfile.get();
        BlockPostingReader r;
        ASSERT_OK(r.load(_opts, pb));
        std::unique_ptr<BlockPostingIterator> it;
        ASSERT_OK(r.new_iterator(_opts, &it));

        std::vector<uint32_t> d, f;
        read_all(it.get(), 0, &d, &f);
        ASSERT_EQ(kN, d.size());
        for (uint32_t i = 0; i < kN; ++i) {
            EXPECT_EQ(i, d[i]);
        }
        uint32_t nblocks = 0;
        ASSERT_OK(it->seek_to_term(0));
        while (it->has_next_block()) {
            ASSERT_OK(it->next_block());
            ++nblocks;
        }
        EXPECT_EQ(kN / 128u, nblocks) << "N=" << kN;
    }
}

// Large irregular docid gaps + large tf values exercise the PFOR exception/patch path through the
// block layer (the codec itself is fuzzed in gin_pfor_test; here we verify it round-trips end to end).
TEST_F(BlockPostingTest, large_gaps_and_tf) {
    const std::string file = kTestDir + "/large";
    Term t0{{0, 5, 1000000, 1000003, 50000000}, {1, 65000, 2, 1, 100000}, {1, 9, 2, 7, 3}};
    PostingIndexPB pb = write(file, {t0});

    ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file));
    _opts.read_file = rfile.get();
    BlockPostingReader r;
    ASSERT_OK(r.load(_opts, pb));
    std::unique_ptr<BlockPostingIterator> it;
    ASSERT_OK(r.new_iterator(_opts, &it));

    std::vector<uint32_t> d, f;
    read_all(it.get(), 0, &d, &f);
    EXPECT_EQ((std::vector<uint32_t>{0, 5, 1000000, 1000003, 50000000}), d);
    EXPECT_EQ((std::vector<uint32_t>{1, 65000, 2, 1, 100000}), f);
}

// Each block's {max_tf, min_doclen, last_docid} are computed independently per block.
TEST_F(BlockPostingTest, per_block_stats) {
    const std::string file = kTestDir + "/perblock";
    Term t0;
    for (uint32_t i = 0; i < 128; ++i) { // block 0: max_tf 10, min_doclen 5
        t0.docids.push_back(i);
        t0.tfs.push_back((i % 10) + 1);
        t0.doclens.push_back(5 + (i % 3));
    }
    for (uint32_t i = 128; i < 200; ++i) { // block 1: max_tf 3, min_doclen 20
        t0.docids.push_back(i);
        t0.tfs.push_back((i % 3) + 1);
        t0.doclens.push_back(20 + (i % 4));
    }
    PostingIndexPB pb = write(file, {t0});

    ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file));
    _opts.read_file = rfile.get();
    BlockPostingReader r;
    ASSERT_OK(r.load(_opts, pb));
    std::unique_ptr<BlockPostingIterator> it;
    ASSERT_OK(r.new_iterator(_opts, &it));

    ASSERT_OK(it->seek_to_term(0));
    ASSERT_OK(it->next_block()); // block 0
    EXPECT_EQ(127u, it->cur_block_last_docid());
    EXPECT_EQ(10u, it->cur_block_max_tf());
    EXPECT_EQ(5u, it->cur_block_min_doclen());
    ASSERT_OK(it->next_block()); // block 1
    EXPECT_EQ(199u, it->cur_block_last_docid());
    EXPECT_EQ(3u, it->cur_block_max_tf());
    EXPECT_EQ(20u, it->cur_block_min_doclen());
    EXPECT_FALSE(it->has_next_block());
}

// seek_block boundary behavior: exact last_docid, cross-block, and overflow (NotFound).
TEST_F(BlockPostingTest, seek_block_edges) {
    const std::string file = kTestDir + "/seekedge";
    Term t0;
    for (uint32_t i = 0; i < 300; ++i) { // block0=[0..127] block1=[128..255] block2=[256..299]
        t0.docids.push_back(i);
        t0.tfs.push_back(1);
        t0.doclens.push_back(5);
    }
    PostingIndexPB pb = write(file, {t0});

    ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file));
    _opts.read_file = rfile.get();
    BlockPostingReader r;
    ASSERT_OK(r.load(_opts, pb));
    std::unique_ptr<BlockPostingIterator> it;
    ASSERT_OK(r.new_iterator(_opts, &it));

    ASSERT_OK(it->seek_to_term(0));
    ASSERT_OK(it->seek_block(127)); // last_docid 127 >= 127 -> block0
    EXPECT_EQ(127u, it->cur_block_last_docid());
    ASSERT_OK(it->seek_to_term(0));
    ASSERT_OK(it->seek_block(128)); // -> block1
    EXPECT_EQ(255u, it->cur_block_last_docid());
    ASSERT_OK(it->seek_to_term(0));
    ASSERT_OK(it->seek_block(299)); // -> block2
    EXPECT_EQ(299u, it->cur_block_last_docid());
    ASSERT_OK(it->seek_to_term(0));
    EXPECT_FALSE(it->seek_block(300).ok()); // beyond last docid -> NotFound
}

// A validly-written block always has doc_count in [1, kBlockSize]; a zero doc_count only comes from a
// corrupt/truncated blob and must be rejected, not silently accepted as an empty block.
TEST_F(BlockPostingTest, rejects_zero_doc_count_block) {
    const std::string file = kTestDir + "/zero_count";
    // block 0: doc_count byte = 0, then a 4-byte first_docid (blob is the minimum 5 bytes, so it
    // passes the size check and reaches the doc_count guard).
    std::string block0;
    block0.push_back('\0');
    put_fixed32_le(&block0, 0);
    // one term's directory: [num_blocks=1][first_block_id=0] then the block's {last_docid,max_tf,
    // min_doclen} (unused here -- decoding fails before they are read).
    std::string dir0;
    put_fixed32_le(&dir0, 1);
    put_fixed32_le(&dir0, 0);
    put_fixed32_le(&dir0, 0);
    put_fixed32_le(&dir0, 0);
    put_fixed32_le(&dir0, 0);

    PostingIndexPB pb = write_raw(file, {block0}, {dir0});

    ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file));
    _opts.read_file = rfile.get();
    BlockPostingReader r;
    ASSERT_OK(r.load(_opts, pb));
    std::unique_ptr<BlockPostingIterator> it;
    ASSERT_OK(r.new_iterator(_opts, &it));

    ASSERT_OK(it->seek_to_term(0)); // directory is well-formed
    ASSERT_TRUE(it->has_next_block());
    const Status st = it->next_block(); // loads block 0 -> doc_count == 0 -> Corruption
    EXPECT_TRUE(st.is_corruption()) << st;
}

TEST_F(BlockPostingTest, directory_stats_accessors) {
    const std::string file = kTestDir + "/dirstats";
    Term t0;
    for (uint32_t i = 0; i < 300; ++i) { // 3 blocks: [0..127] [128..255] [256..299]
        t0.docids.push_back(i);
        t0.tfs.push_back(1 + (i % 7));
        t0.doclens.push_back(3 + (i % 5));
    }
    PostingIndexPB pb = write(file, {t0});

    ASSIGN_OR_ABORT(auto rfile, _fs->new_random_access_file(file));
    _opts.read_file = rfile.get();
    BlockPostingReader r;
    ASSERT_OK(r.load(_opts, pb));
    std::unique_ptr<BlockPostingIterator> it;
    ASSERT_OK(r.new_iterator(_opts, &it));

    // Directory accessors are valid right after seek_to_term, before any block decode.
    ASSERT_OK(it->seek_to_term(0));
    ASSERT_EQ(3u, it->num_blocks());
    // Directory stats must match what cur_block_* reports while walking the blocks.
    for (uint32_t b = 0; b < it->num_blocks(); ++b) {
        ASSERT_OK(it->next_block());
        EXPECT_EQ(it->cur_block_last_docid(), it->block_last_docid(b));
        EXPECT_EQ(it->cur_block_max_tf(), it->block_max_tf(b));
        EXPECT_EQ(it->cur_block_min_doclen(), it->block_min_doclen(b));
    }
}

} // namespace starrocks
