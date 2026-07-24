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

// Covers the Segment presence vs. lazy-usability split for the full sort key index page
// (SegmentFooterPB field 11): presence at open, lazy validated load on first full-key request,
// the read-time gate, and the go-forward fallback to the legacy short key page for a
// read-OFF query or a malformed/geometry-mismatched/arity-mismatched/key-order-corrupt page.

#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/datum_tuple.h"
#include "common/config_rowset_fwd.h"
#include "fs/fs.h"
#include "fs/fs_memory.h"
#include "gen_cpp/segment.pb.h"
#include "gutil/strings/substitute.h"
#include "storage/base/short_key_index.h"
#include "storage/chunk_helper.h"
#include "storage/rowset/page_io.h"
#include "storage/rowset/page_pointer.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_writer.h"
#include "storage/tablet_schema.h"
#include "storage/tablet_schema_helper.h"

namespace starrocks {

// Two INT sort-key columns (arity 2) + one INT value column.
static std::shared_ptr<TabletSchema> make_two_int_key_schema() {
    std::vector<ColumnPB> cols;
    cols.push_back(create_int_key_pb(/*id=*/1));
    cols.push_back(create_int_key_pb(/*id=*/2));
    cols.push_back(create_int_value_pb(/*id=*/3));
    auto unique = TabletSchemaHelper::create_tablet_schema(cols);
    return std::shared_ptr<TabletSchema>(std::move(unique));
}

// A 4-byte big-endian encoding, so numeric order == lexicographic byte order.
static std::string be32(uint32_t v) {
    std::string s(4, '\0');
    s[0] = static_cast<char>((v >> 24) & 0xFF);
    s[1] = static_cast<char>((v >> 16) & 0xFF);
    s[2] = static_cast<char>((v >> 8) & 0xFF);
    s[3] = static_cast<char>(v & 0xFF);
    return s;
}

class SegmentFullSortKeyIndexTest : public ::testing::Test {
protected:
    void SetUp() override {
        _fs = std::make_shared<MemoryFileSystem>();
        ASSERT_OK(_fs->create_dir(kDir));
        _schema = make_two_int_key_schema();
        ASSERT_EQ(2U, _schema->sort_key_idxes().size());
        _saved_write = config::enable_full_sort_key_index;
        _saved_read = config::enable_full_sort_key_index_read;
    }

    void TearDown() override {
        config::enable_full_sort_key_index = _saved_write;
        config::enable_full_sort_key_index_read = _saved_read;
    }

    // Write a segment (dual-page when `write_full` is on) and return the file path (unopened).
    std::string write_segment(const std::string& name, bool write_full) {
        const bool old = config::enable_full_sort_key_index;
        config::enable_full_sort_key_index = write_full;
        DeferOp restore([&] { config::enable_full_sort_key_index = old; });

        std::string path = strings::Substitute("$0/$1.dat", kDir, name);
        ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(path));
        SegmentWriterOptions opts;
        opts.num_rows_per_block = kRowsPerBlock;
        SegmentWriter writer(std::move(wfile), 0, _schema, opts);
        CHECK_OK(writer.init(true));

        auto s = ChunkHelper::convert_schema(_schema);
        auto chunk = ChunkFactory::new_chunk(s, kNumRows);
        auto cols = chunk->columns();
        for (int64_t i = 0; i < kNumRows; ++i) {
            cols[0]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i / kRowsPerBlock)));
            cols[1]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i)));
            cols[2]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(0)));
        }
        CHECK_OK(writer.append_chunk(*chunk));
        uint64_t file_size = 0;
        uint64_t index_size = 0;
        uint64_t footer_position = 0;
        CHECK_OK(writer.finalize(&file_size, &index_size, &footer_position));
        return path;
    }

    std::shared_ptr<Segment> open_segment(const std::string& path) {
        ASSIGN_OR_ABORT(auto segment, Segment::open(_fs, FileInfo{path}, 0, _schema));
        return segment;
    }

    SegmentFooterPB read_seg_footer(const std::string& path) {
        ASSIGN_OR_ABORT(auto rf, _fs->new_random_access_file(path));
        SegmentFooterPB footer;
        CHECK(Segment::parse_segment_footer(rf.get(), &footer, nullptr, nullptr).ok());
        return footer;
    }

    ShortKeyFooterPB read_page_footer(const std::string& path, const PagePointerPB& page) {
        ASSIGN_OR_ABORT(auto rf, _fs->new_random_access_file(path));
        PageReadOptions opts;
        opts.read_file = rf.get();
        opts.page_pointer = PagePointer(page);
        opts.codec = nullptr;
        OlapReaderStatistics stats;
        opts.stats = &stats;
        PageHandle handle;
        Slice body;
        PageFooterPB pf;
        CHECK(PageIO::read_and_decompress_page(opts, &handle, &body, &pf).ok());
        return pf.short_key_page_footer();
    }

    // Copy `orig`'s data region (everything except its trailing footer) verbatim into `dst`, so
    // the legacy short key page + column pages keep their offsets, then return an open WritableFile
    // positioned at the end of that region plus the parsed footer to be rewritten by the caller.
    void copy_data_region(const std::string& orig, const std::string& dst, std::unique_ptr<WritableFile>* out_wf,
                          SegmentFooterPB* out_footer) {
        ASSIGN_OR_ABORT(auto rf, _fs->new_random_access_file(orig));
        ASSIGN_OR_ABORT(auto file_size, rf->get_size());
        ASSIGN_OR_ABORT(auto footer_total, Segment::parse_segment_footer(rf.get(), out_footer, nullptr, nullptr));
        const size_t data_len = static_cast<size_t>(file_size) - footer_total;
        std::string data;
        data.resize(data_len);
        CHECK(rf->read_at_fully(0, data.data(), data_len).ok());

        ASSIGN_OR_ABORT(auto wf, _fs->new_writable_file(dst));
        CHECK_OK(wf->append(Slice(data)));
        *out_wf = std::move(wf);
    }

    // Rewrite `orig` -> `dst` replacing field 11 with a well-formed short key page built from the
    // given geometry/encoding/keys.
    std::string rewrite_with_full_page(const std::string& name, const std::string& orig, uint32_t rows_per_block,
                                       uint32_t seg_rows, ShortKeyEncodingPB encoding, uint32_t arity,
                                       const std::vector<std::string>& keys) {
        std::string dst = strings::Substitute("$0/$1.dat", kDir, name);
        std::unique_ptr<WritableFile> wf;
        SegmentFooterPB footer;
        copy_data_region(orig, dst, &wf, &footer);

        ShortKeyIndexBuilder builder(/*segment_id=*/0, rows_per_block);
        for (const auto& k : keys) {
            CHECK_OK(builder.add_item(k));
        }
        std::vector<Slice> body;
        PageFooterPB pf;
        CHECK_OK(builder.finalize(seg_rows, &body, &pf, encoding, arity));
        PagePointer pp;
        CHECK_OK(PageIO::write_page(wf.get(), body, pf, &pp));
        pp.to_proto(footer.mutable_full_sort_key_index_page());
        CHECK_OK(Segment::write_segment_footer(wf.get(), footer));
        CHECK_OK(wf->close());
        return dst;
    }

    // Rewrite `orig` -> `dst` pointing field 11 at raw, unreadable bytes (fails page parse/CRC).
    std::string rewrite_with_raw_full_page(const std::string& name, const std::string& orig) {
        std::string dst = strings::Substitute("$0/$1.dat", kDir, name);
        std::unique_ptr<WritableFile> wf;
        SegmentFooterPB footer;
        copy_data_region(orig, dst, &wf, &footer);

        const uint64_t page_off = wf->size();
        std::string garbage(64, static_cast<char>(0xAB));
        CHECK_OK(wf->append(Slice(garbage)));
        auto* pp = footer.mutable_full_sort_key_index_page();
        pp->set_offset(page_off);
        pp->set_size(garbage.size());
        CHECK_OK(Segment::write_segment_footer(wf.get(), footer));
        CHECK_OK(wf->close());
        return dst;
    }

    // Increasing (sorted) keys, one per legacy index item.
    std::vector<std::string> sorted_keys(uint32_t n) {
        std::vector<std::string> keys;
        for (uint32_t i = 0; i < n; ++i) {
            keys.push_back(be32(i));
        }
        return keys;
    }

    std::shared_ptr<MemoryFileSystem> _fs;
    std::shared_ptr<TabletSchema> _schema;
    bool _saved_write = true;
    bool _saved_read = true;
    static constexpr int64_t kNumRows = 100;
    static constexpr uint32_t kRowsPerBlock = 10; // => 10 index items
    const std::string kDir = "/full_sk_test";
};

// (a) Dual-page segment: presence true; the full decoder is NOT loaded before any full-key
// request; use_full_sort_key_index() (read on) lazily loads + validates it; num_sort_key_columns()
// == arity; num_rows_per_block() reads the legacy decoder.
TEST_F(SegmentFullSortKeyIndexTest, dual_page_presence_then_lazy_usable) {
    config::enable_full_sort_key_index_read = true;
    auto path = write_segment("a_dual", /*write_full=*/true);
    auto segment = open_segment(path);

    EXPECT_TRUE(segment->has_full_sort_key_index_page());
    // Presence must not have loaded the full decoder yet.
    EXPECT_EQ(nullptr, segment->full_sort_key_index_decoder());

    ASSERT_OK(segment->load_index());
    // Legacy load alone still must not touch the full page.
    EXPECT_EQ(nullptr, segment->full_sort_key_index_decoder());

    EXPECT_TRUE(segment->use_full_sort_key_index());
    ASSERT_NE(nullptr, segment->full_sort_key_index_decoder());
    EXPECT_EQ(2U, segment->num_sort_key_columns());
    // Shared geometry: rows-per-block comes from the legacy decoder.
    EXPECT_EQ(static_cast<uint32_t>(kRowsPerBlock), segment->num_rows_per_block());
}

// (b) Legacy-only segment: presence false; use_full_sort_key_index() false.
TEST_F(SegmentFullSortKeyIndexTest, legacy_only_not_present_not_usable) {
    config::enable_full_sort_key_index_read = true;
    auto path = write_segment("b_legacy", /*write_full=*/false);
    auto segment = open_segment(path);

    EXPECT_FALSE(segment->has_full_sort_key_index_page());
    EXPECT_FALSE(segment->use_full_sort_key_index());
    EXPECT_EQ(nullptr, segment->full_sort_key_index_decoder());
}

// (c) Read config OFF never touches field 11. A malformed field-11 page is injected; with the read
// gate off, load_index() + legacy reads still succeed (the full page is never read). Turning the
// read gate on makes the same segment permanently unusable (parse fails) and falls back to legacy
// with no crash.
TEST_F(SegmentFullSortKeyIndexTest, read_off_never_reads_field11_and_malformed_falls_back) {
    auto base = write_segment("c_base", /*write_full=*/true);
    auto bad = rewrite_with_raw_full_page("c_malformed", base);

    // Read OFF: full page (even malformed) is never consulted; legacy path works.
    {
        config::enable_full_sort_key_index_read = false;
        auto segment = open_segment(bad);
        EXPECT_TRUE(segment->has_full_sort_key_index_page()); // presence is footer-only
        EXPECT_FALSE(segment->use_full_sort_key_index());
        ASSERT_OK(segment->load_index()); // reads field 9 only
        std::vector<std::string> sk;
        ASSERT_OK(segment->get_short_key_index(&sk));
        EXPECT_FALSE(sk.empty());
        EXPECT_EQ(nullptr, segment->full_sort_key_index_decoder());
    }
    // Read ON: malformed field 11 -> permanently unusable -> legacy, no crash.
    {
        config::enable_full_sort_key_index_read = true;
        auto segment = open_segment(bad);
        EXPECT_FALSE(segment->use_full_sort_key_index());
        EXPECT_FALSE(segment->ensure_full_sort_key_index_usable()); // permanent
        EXPECT_EQ(nullptr, segment->full_sort_key_index_decoder());
        ASSERT_OK(segment->load_index()); // legacy still fine
    }
}

// (d) A geometry-mismatched full page and an arity-mismatched full page are each permanently
// unusable -> legacy.
TEST_F(SegmentFullSortKeyIndexTest, geometry_or_arity_mismatch_unusable) {
    config::enable_full_sort_key_index_read = true;
    auto base = write_segment("d_base", /*write_full=*/true);
    ShortKeyFooterPB legacy = read_page_footer(base, read_seg_footer(base).short_key_index_page());
    const uint32_t items = legacy.num_items();
    const uint32_t rpb = legacy.num_rows_per_block();
    const uint32_t seg_rows = legacy.num_segment_rows();

    // Geometry mismatch: wrong num_rows_per_block.
    {
        auto p = rewrite_with_full_page("d_geo", base, /*rows_per_block=*/rpb + 1, seg_rows,
                                        SHORT_KEY_ENCODING_FULL_SORT_KEY, /*arity=*/2, sorted_keys(items));
        auto segment = open_segment(p);
        EXPECT_TRUE(segment->has_full_sort_key_index_page());
        EXPECT_FALSE(segment->use_full_sort_key_index());
        EXPECT_EQ(nullptr, segment->full_sort_key_index_decoder());
    }
    // Arity mismatch: num_sort_key_columns != schema sort-key arity (2).
    {
        auto p = rewrite_with_full_page("d_arity", base, rpb, seg_rows, SHORT_KEY_ENCODING_FULL_SORT_KEY,
                                        /*arity=*/1, sorted_keys(items));
        auto segment = open_segment(p);
        EXPECT_TRUE(segment->has_full_sort_key_index_page());
        EXPECT_FALSE(segment->use_full_sort_key_index());
        EXPECT_EQ(nullptr, segment->full_sort_key_index_decoder());
    }
    // Also: a page tagged TRUNCATED (wrong encoding) is unusable.
    {
        auto p = rewrite_with_full_page("d_enc", base, rpb, seg_rows, SHORT_KEY_ENCODING_TRUNCATED,
                                        /*arity=*/2, sorted_keys(items));
        auto segment = open_segment(p);
        EXPECT_FALSE(segment->use_full_sort_key_index());
    }
}

// (f) A geometry-valid, arity-valid full page whose entries are NOT byte-wise non-decreasing is
// rejected by the order validation -> permanently unusable -> legacy.
TEST_F(SegmentFullSortKeyIndexTest, key_order_corrupt_unusable) {
    config::enable_full_sort_key_index_read = true;
    auto base = write_segment("f_base", /*write_full=*/true);
    ShortKeyFooterPB legacy = read_page_footer(base, read_seg_footer(base).short_key_index_page());
    const uint32_t items = legacy.num_items();
    ASSERT_GE(items, 2U);

    // First entry is the largest, so key(1) < key(0) -> not non-decreasing.
    std::vector<std::string> keys;
    keys.push_back(be32(items)); // largest, out of order
    for (uint32_t i = 1; i < items; ++i) {
        keys.push_back(be32(i));
    }
    auto p = rewrite_with_full_page("f_corrupt", base, legacy.num_rows_per_block(), legacy.num_segment_rows(),
                                    SHORT_KEY_ENCODING_FULL_SORT_KEY, /*arity=*/2, keys);
    auto segment = open_segment(p);
    EXPECT_TRUE(segment->has_full_sort_key_index_page());
    EXPECT_FALSE(segment->use_full_sort_key_index());
    EXPECT_EQ(nullptr, segment->full_sort_key_index_decoder());
}

// (e) Concurrent first-use from two threads publishes exactly one result and races cleanly.
TEST_F(SegmentFullSortKeyIndexTest, concurrent_first_use_publishes_once) {
    config::enable_full_sort_key_index_read = true;
    auto path = write_segment("e_base", /*write_full=*/true);
    auto segment = open_segment(path);
    ASSERT_OK(segment->load_index());

    std::atomic<int> usable_count{0};
    auto worker = [&] {
        if (segment->use_full_sort_key_index()) {
            usable_count.fetch_add(1);
        }
    };
    std::thread t1(worker);
    std::thread t2(worker);
    t1.join();
    t2.join();

    EXPECT_EQ(2, usable_count.load()); // both observe the same published USABLE result
    ASSERT_NE(nullptr, segment->full_sort_key_index_decoder());
    EXPECT_EQ(2U, segment->num_sort_key_columns());
}

} // namespace starrocks
