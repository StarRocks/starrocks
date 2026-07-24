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

#include <memory>
#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/datum_tuple.h"
#include "common/config_rowset_fwd.h"
#include "fs/fs_memory.h"
#include "gen_cpp/segment.pb.h"
#include "gutil/strings/substitute.h"
#include "storage/chunk_helper.h"
#include "storage/rowset/page_io.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_file_info.h"
#include "storage/rowset/segment_writer.h"
#include "storage/tablet_schema.h"
#include "storage/tablet_schema_helper.h"
#include "storage_primitive/storage_stats.h"

namespace starrocks {

// Matches the default of config::segment_sort_key_sample_row_interval
// (declared in be/src/common/config.h). Kept in sync manually; if the config
// default changes, update this test constant too.
static constexpr int64_t kInterval = 65536;

// Build a DUP_KEYS-style tablet schema with a single INT sort-key column and
// one INT value column. INT (4 bytes) is sufficient to exercise the sampler;
// the sampler behavior is independent of sort-key data type.
static std::shared_ptr<TabletSchema> make_int_key_schema() {
    std::vector<ColumnPB> cols;
    cols.push_back(create_int_key_pb(/*id=*/1));
    cols.push_back(create_int_value_pb(/*id=*/2));
    auto unique = TabletSchemaHelper::create_tablet_schema(cols);
    return std::shared_ptr<TabletSchema>(std::move(unique));
}

// Build a schema with 2 sort-key (is_key) INT columns but only 1 short key column, so
// sort_key_idxes().size() (2) != num_short_key_columns() (1). Used to distinguish the
// full-sort-key-index encoding (must cover ALL sort columns) from the legacy short-key
// encoding (truncated to num_short_key_columns()).
static std::shared_ptr<TabletSchema> make_two_int_key_schema_short_key_override() {
    std::vector<ColumnPB> cols;
    cols.push_back(create_int_key_pb(/*id=*/1));
    cols.push_back(create_int_key_pb(/*id=*/2));
    cols.push_back(create_int_value_pb(/*id=*/3));
    auto unique = TabletSchemaHelper::create_tablet_schema(cols, /*num_short_key_columns=*/1);
    return std::shared_ptr<TabletSchema>(std::move(unique));
}

inline ColumnPB create_float_key_pb(int32_t id) {
    ColumnPB col;
    col.set_unique_id(id);
    col.set_name(std::to_string(id));
    col.set_type("FLOAT");
    col.set_is_key(true);
    col.set_is_nullable(true);
    col.set_length(4);
    col.set_index_length(4);
    return col;
}

// Build a schema with an INT sort-key column followed by a FLOAT sort-key column, with
// num_short_key_columns capped at 1 so the FLOAT column is excluded from the short-key PREFIX --
// mirroring GlobalStateMgr.calcShortKeyColumnCount (FE), which stops accumulating the short key at
// the first floating-point/complex sort column. get_key_coder(TYPE_FLOAT) is nullptr, so the full
// sort key index must not be enabled for this sort key even when config::enable_full_sort_key_index
// is on; the writer must fall back to the legacy short-key index (which never touches this column,
// since it's outside num_short_key_columns).
static std::shared_ptr<TabletSchema> make_int_float_key_schema_short_key_excludes_float() {
    std::vector<ColumnPB> cols;
    cols.push_back(create_int_key_pb(/*id=*/1));
    cols.push_back(create_float_key_pb(/*id=*/2));
    cols.push_back(create_int_value_pb(/*id=*/3));
    auto unique = TabletSchemaHelper::create_tablet_schema(cols, /*num_short_key_columns=*/1);
    return std::shared_ptr<TabletSchema>(std::move(unique));
}

// Fixture that spins up an in-memory writable file and builds SegmentWriters.
class SegmentWriterSortKeySamplingTest : public ::testing::Test {
protected:
    void SetUp() override {
        // These tests exercise the legacy metadata sort-key sampler, which only runs when the
        // full sort key index is off. Pin it off as the baseline (config now defaults to on);
        // the few tests that cover the full-key path opt back in explicitly.
        _saved_full_sort_key_index = config::enable_full_sort_key_index;
        config::enable_full_sort_key_index = false;
        _fs = std::make_shared<MemoryFileSystem>();
        ASSERT_OK(_fs->create_dir(kDir));
        _schema = make_int_key_schema();
    }

    void TearDown() override { config::enable_full_sort_key_index = _saved_full_sort_key_index; }

    std::unique_ptr<SegmentWriter> new_writer(uint32_t seg_id, const SegmentWriterOptions& opts = {}) {
        return new_writer_for_schema(seg_id, _schema, opts);
    }

    std::unique_ptr<SegmentWriter> new_writer_for_schema(uint32_t seg_id, const std::shared_ptr<TabletSchema>& schema,
                                                         const SegmentWriterOptions& opts = {}) {
        std::string filename = strings::Substitute("$0/seg_$1.dat", kDir, seg_id);
        ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(filename));
        return std::make_unique<SegmentWriter>(std::move(wfile), seg_id, schema, opts);
    }

    // Create a chunk with `n` rows for a 2-int-key + 1-int-value schema (see
    // make_two_int_key_schema_short_key_override): both key columns are a monotonic
    // int32 starting at `start_key`, the value column is a constant 0.
    ChunkUniquePtr make_two_key_increasing_chunk(const std::shared_ptr<TabletSchema>& schema, int64_t n,
                                                 int32_t start_key) {
        auto s = ChunkHelper::convert_schema(schema);
        auto chunk = ChunkFactory::new_chunk(s, n);
        auto cols = chunk->columns();
        for (int64_t i = 0; i < n; ++i) {
            cols[0]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(start_key + i)));
            cols[1]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(start_key + i)));
            cols[2]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(0)));
        }
        return chunk;
    }

    // Create a chunk with `n` rows for the int-key + float-key + int-value schema (see
    // make_int_float_key_schema_short_key_excludes_float): column 0 is a monotonic int32 key,
    // column 1 is a monotonic float key, column 2 is a constant int32 value.
    ChunkUniquePtr make_int_float_key_increasing_chunk(const std::shared_ptr<TabletSchema>& schema, int64_t n,
                                                       int32_t start_key) {
        auto s = ChunkHelper::convert_schema(schema);
        auto chunk = ChunkFactory::new_chunk(s, n);
        auto cols = chunk->columns();
        for (int64_t i = 0; i < n; ++i) {
            cols[0]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(start_key + i)));
            cols[1]->as_mutable_ptr()->append_datum(Datum(static_cast<float>(start_key + i)));
            cols[2]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(0)));
        }
        return chunk;
    }

    // Parse the SegmentFooterPB from the written segment file.
    SegmentFooterPB read_segment_footer(const std::string& filename) {
        ASSIGN_OR_ABORT(auto read_file, _fs->new_random_access_file(filename));
        SegmentFooterPB seg_footer;
        auto footer_size_or = Segment::parse_segment_footer(read_file.get(), &seg_footer, nullptr, nullptr);
        CHECK(footer_size_or.ok()) << footer_size_or.status().to_string();
        return seg_footer;
    }

    // Read the ShortKeyFooterPB of a short key index page given its page pointer.
    ShortKeyFooterPB read_short_key_page_footer(const std::string& filename, const PagePointerPB& page) {
        ASSIGN_OR_ABORT(auto read_file, _fs->new_random_access_file(filename));
        PageReadOptions opts;
        opts.read_file = read_file.get();
        opts.page_pointer = PagePointer(page);
        opts.codec = nullptr; // short key index page is not compressed
        OlapReaderStatistics stats;
        opts.stats = &stats;

        PageHandle handle;
        Slice body;
        PageFooterPB page_footer;
        CHECK(PageIO::read_and_decompress_page(opts, &handle, &body, &page_footer).ok());
        return page_footer.short_key_page_footer();
    }

    // Read the legacy short key index page's ShortKeyFooterPB (footer field 9).
    ShortKeyFooterPB read_short_key_footer(const std::string& filename) {
        return read_short_key_page_footer(filename, read_segment_footer(filename).short_key_index_page());
    }

    // Create a chunk with `n` rows where column 0 (key) is a monotonic int32
    // starting at `start_key`, and column 1 (value) is a constant 0.
    ChunkUniquePtr make_increasing_chunk(int64_t n, int32_t start_key) {
        auto s = ChunkHelper::convert_schema(_schema);
        auto chunk = ChunkFactory::new_chunk(s, n);
        auto cols = chunk->columns();
        for (int64_t i = 0; i < n; ++i) {
            cols[0]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(start_key + i)));
            cols[1]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(0)));
        }
        return chunk;
    }

    std::shared_ptr<MemoryFileSystem> _fs;
    std::shared_ptr<TabletSchema> _schema;
    bool _saved_full_sort_key_index = false;
    const std::string kDir = "/sampling_test";
};

// ---------------------------------------------------------------------------
// Row count sweep: for each N, verify sample count matches (N-1)/interval and
// the invariant samples.size() * interval < num_rows (strict) holds.
// ---------------------------------------------------------------------------
TEST_F(SegmentWriterSortKeySamplingTest, row_count_sweep_produces_expected_sample_counts) {
    const int64_t row_interval = kInterval;
    // Mix of edge cases: below interval, at interval, just over, 2*interval
    // boundaries. Use small row counts relative to kInterval for test speed --
    // the sampler's behavior is identical at any scale.
    std::vector<int64_t> row_counts = {1,
                                       row_interval - 1,
                                       row_interval,
                                       row_interval + 1,
                                       2 * row_interval - 1,
                                       2 * row_interval,
                                       2 * row_interval + 1};
    for (int64_t num_rows : row_counts) {
        auto w = new_writer(/*seg_id=*/static_cast<uint32_t>(num_rows));
        ASSERT_OK(w->init(true));
        auto chunk = make_increasing_chunk(num_rows, /*start_key=*/0);
        ASSERT_OK(w->append_chunk(*chunk));

        const auto& samples = w->get_sort_key_samples();
        // Expected: samples fire whenever _num_rows_written (pre-increment)
        // equals k*row_interval, so the largest k with
        // k*row_interval <= num_rows - 1 wins.
        const int64_t expected = (num_rows >= 1) ? ((num_rows - 1) / row_interval) : 0;
        EXPECT_EQ(static_cast<size_t>(expected), samples.size())
                << "num_rows=" << num_rows << " row_interval=" << row_interval;
        if (!samples.empty()) {
            EXPECT_LT(static_cast<int64_t>(samples.size()) * row_interval, num_rows);
        }
        EXPECT_EQ(row_interval, w->get_sort_key_sample_row_interval());
    }
}

// ---------------------------------------------------------------------------
// Samples are non-decreasing on ordered input.
// ---------------------------------------------------------------------------
TEST_F(SegmentWriterSortKeySamplingTest, samples_are_non_decreasing_on_ordered_input) {
    const int64_t num_rows = 3 * kInterval + 123;
    auto w = new_writer(0);
    ASSERT_OK(w->init(true));
    auto chunk = make_increasing_chunk(num_rows, 0);
    ASSERT_OK(w->append_chunk(*chunk));

    const auto& samples = w->get_sort_key_samples();
    ASSERT_FALSE(samples.empty());
    for (size_t i = 1; i < samples.size(); ++i) {
        EXPECT_LE(samples[i - 1].compare(samples[i]), 0);
    }
}

// ---------------------------------------------------------------------------
// Off-by-one: feed interval+1 rows where last row's key equals max_key.
// Expected: samples.size() == 1 and samples[0] == max_key.
// ---------------------------------------------------------------------------
TEST_F(SegmentWriterSortKeySamplingTest, off_by_one_trailing_max_sample) {
    const int64_t num_rows = kInterval + 1;
    auto w = new_writer(0);
    ASSERT_OK(w->init(true));
    auto chunk = make_increasing_chunk(num_rows, 0);
    ASSERT_OK(w->append_chunk(*chunk));

    const auto& samples = w->get_sort_key_samples();
    ASSERT_EQ(1U, samples.size());
    EXPECT_EQ(0, samples[0].compare(w->get_sort_key_max()));
}

// ---------------------------------------------------------------------------
// Leading-min duplicates: feed `interval + 1` rows of key 0 followed by
// further monotonic rows. samples[0] should equal min (0).
// ---------------------------------------------------------------------------
TEST_F(SegmentWriterSortKeySamplingTest, leading_min_duplicate_samples) {
    const int64_t leading = kInterval + 1;
    const int64_t trailing = kInterval + 5;
    const int64_t num_rows = leading + trailing;

    auto w = new_writer(0);
    ASSERT_OK(w->init(true));

    auto s = ChunkHelper::convert_schema(_schema);
    auto chunk = ChunkFactory::new_chunk(s, num_rows);
    auto cols = chunk->columns();
    for (int64_t i = 0; i < leading; ++i) {
        cols[0]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(0)));
        cols[1]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(0)));
    }
    for (int64_t i = 0; i < trailing; ++i) {
        cols[0]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(1 + i)));
        cols[1]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(0)));
    }
    ASSERT_OK(w->append_chunk(*chunk));

    const auto& samples = w->get_sort_key_samples();
    ASSERT_FALSE(samples.empty());
    EXPECT_EQ(0, samples[0].compare(w->get_sort_key_min()));
}

// ---------------------------------------------------------------------------
// All-identical-key segment: samples all equal the single key; min == max ==
// every sample.
// ---------------------------------------------------------------------------
TEST_F(SegmentWriterSortKeySamplingTest, all_identical_key) {
    const int64_t num_rows = 2 * kInterval + 7;
    auto w = new_writer(0);
    ASSERT_OK(w->init(true));

    auto s = ChunkHelper::convert_schema(_schema);
    auto chunk = ChunkFactory::new_chunk(s, num_rows);
    auto cols = chunk->columns();
    for (int64_t i = 0; i < num_rows; ++i) {
        cols[0]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(42)));
        cols[1]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(0)));
    }
    ASSERT_OK(w->append_chunk(*chunk));

    const auto& samples = w->get_sort_key_samples();
    ASSERT_FALSE(samples.empty());
    for (const auto& sample : samples) {
        EXPECT_EQ(0, sample.compare(w->get_sort_key_min()));
        EXPECT_EQ(0, sample.compare(w->get_sort_key_max()));
    }
}

// ---------------------------------------------------------------------------
// Vertical writer re-entry: init() with is_key=true produces samples, then a
// subsequent init({non_key_cols}, is_key=false) MUST NOT wipe the sampler
// state. This is the vertical-compaction-compat contract.
// ---------------------------------------------------------------------------
TEST_F(SegmentWriterSortKeySamplingTest, vertical_writer_reinit_preserves_sampler_state) {
    const int64_t num_rows = kInterval + 5;
    auto w = new_writer(0);

    // First init: column 0 only (the key column). is_key=true arms the sampler.
    std::vector<uint32_t> key_cols{0};
    ASSERT_OK(w->init(key_cols, true));

    // Append key-column-only chunks.
    auto key_schema_view = ChunkHelper::convert_schema(_schema, key_cols);
    auto key_chunk = ChunkFactory::new_chunk(key_schema_view, num_rows);
    auto* key_col = key_chunk->columns()[0]->as_mutable_ptr().get();
    for (int64_t i = 0; i < num_rows; ++i) {
        key_col->append_datum(Datum(static_cast<int32_t>(i)));
    }
    ASSERT_OK(w->append_chunk(*key_chunk));

    const auto samples_after_key = w->get_sort_key_samples();
    const int64_t row_interval_after_key = w->get_sort_key_sample_row_interval();
    ASSERT_EQ(1U, samples_after_key.size());
    ASSERT_EQ(kInterval, row_interval_after_key);

    // Vertical writer calls finalize_columns between column-group passes; this
    // clears _column_writers/_column_indexes so the next init() does not
    // assert. We mirror that sequence here.
    uint64_t index_size = 0;
    ASSERT_OK(w->finalize_columns(&index_size));

    // Second init: column 1 only (value column), is_key=false.
    std::vector<uint32_t> non_key_cols{1};
    ASSERT_OK(w->init(non_key_cols, false));

    // Sampler state must survive re-init.
    EXPECT_EQ(row_interval_after_key, w->get_sort_key_sample_row_interval());
    ASSERT_EQ(samples_after_key.size(), w->get_sort_key_samples().size());
    for (size_t i = 0; i < samples_after_key.size(); ++i) {
        EXPECT_EQ(0, samples_after_key[i].compare(w->get_sort_key_samples()[i]));
    }
}

// ---------------------------------------------------------------------------
// write_sort_key_fields_to(SegmentFileInfo&) transfers min, max, samples, and
// interval into a SegmentFileInfo. Preserves the carrier invariant
// (samples.empty() <=> 0).
// ---------------------------------------------------------------------------
TEST_F(SegmentWriterSortKeySamplingTest, write_sort_key_fields_to_transfers_ownership) {
    const int64_t num_rows = 2 * kInterval + 3;
    auto w = new_writer(0);
    ASSERT_OK(w->init(true));
    auto chunk = make_increasing_chunk(num_rows, 0);
    ASSERT_OK(w->append_chunk(*chunk));

    SegmentFileInfo file_info;
    w->write_sort_key_fields_to(file_info);
    EXPECT_EQ(2U, file_info.sort_key_samples.size());
    EXPECT_EQ(kInterval, file_info.sort_key_sample_row_interval);
    EXPECT_FALSE(file_info.sort_key_min.empty());
    EXPECT_FALSE(file_info.sort_key_max.empty());
    // After fill, writer's samples should be moved out.
    EXPECT_TRUE(w->get_sort_key_samples().empty());
}

// ---------------------------------------------------------------------------
// config::enable_full_sort_key_index gates the dual-page write and the metadata sampler. When
// enabled: the segment carries BOTH the always-written legacy truncated short key page (footer
// field 9, short_key_encoding==TRUNCATED) AND an additional full sort key index page (footer
// field 11, short_key_encoding==FULL_SORT_KEY, num_sort_key_columns==sort key arity -- ALL sort
// columns, not num_short_key_columns()). The two pages share block geometry (equal num_items,
// num_rows_per_block, num_segment_rows), and no deprecated sort-key samples are produced even
// though num_rows exceeds the sample interval.
// ---------------------------------------------------------------------------
TEST_F(SegmentWriterSortKeySamplingTest, full_sort_key_index_enabled_writes_dual_page_and_no_samples) {
    const bool old_enable = config::enable_full_sort_key_index;
    config::enable_full_sort_key_index = true;
    DeferOp restore([&] { config::enable_full_sort_key_index = old_enable; });

    auto schema = make_two_int_key_schema_short_key_override();
    ASSERT_EQ(2U, schema->sort_key_idxes().size());
    ASSERT_EQ(1U, schema->num_short_key_columns());

    auto w = new_writer_for_schema(0, schema);
    ASSERT_OK(w->init(true));

    // Exceeds the sample interval, so config-off would have produced a sample.
    const int64_t num_rows = kInterval + 5;
    auto chunk = make_two_key_increasing_chunk(schema, num_rows, 0);
    ASSERT_OK(w->append_chunk(*chunk));
    EXPECT_TRUE(w->get_sort_key_samples().empty());

    uint64_t file_size = 0;
    uint64_t index_size = 0;
    uint64_t footer_position = 0;
    ASSERT_OK(w->finalize(&file_size, &index_size, &footer_position));

    // Both pages are present.
    SegmentFooterPB seg_footer = read_segment_footer(w->segment_path());
    ASSERT_TRUE(seg_footer.has_short_key_index_page());
    ASSERT_TRUE(seg_footer.has_full_sort_key_index_page());

    // The legacy page (field 9) stays truncated; the full page (field 11) is full-sort-key encoded.
    ShortKeyFooterPB legacy_footer = read_short_key_page_footer(w->segment_path(), seg_footer.short_key_index_page());
    ShortKeyFooterPB full_footer = read_short_key_page_footer(w->segment_path(), seg_footer.full_sort_key_index_page());
    EXPECT_EQ(SHORT_KEY_ENCODING_TRUNCATED, legacy_footer.short_key_encoding());
    EXPECT_EQ(0U, legacy_footer.num_sort_key_columns());
    EXPECT_EQ(SHORT_KEY_ENCODING_FULL_SORT_KEY, full_footer.short_key_encoding());
    EXPECT_EQ(2U, full_footer.num_sort_key_columns());

    // Shared block geometry between the two pages.
    EXPECT_EQ(legacy_footer.num_items(), full_footer.num_items());
    EXPECT_EQ(legacy_footer.num_rows_per_block(), full_footer.num_rows_per_block());
    EXPECT_EQ(legacy_footer.num_segment_rows(), full_footer.num_segment_rows());

    SegmentFileInfo file_info;
    w->write_sort_key_fields_to(file_info);
    SegmentMetadataPB meta;
    file_info.to_proto(/*segment_idx=*/0, &meta);
    EXPECT_EQ(0, meta.deprecated_sort_key_samples_size());
}

// ---------------------------------------------------------------------------
// config::enable_full_sort_key_index == false preserves existing behavior: only the legacy short
// key page (footer field 9) is written -- short_key_encoding TRUNCATED, num_sort_key_columns 0,
// NO full sort key index page (footer field 11), and metadata sort-key samples are still produced.
// ---------------------------------------------------------------------------
TEST_F(SegmentWriterSortKeySamplingTest, full_sort_key_index_disabled_writes_legacy_only_and_samples_present) {
    const bool old_enable = config::enable_full_sort_key_index;
    config::enable_full_sort_key_index = false;
    DeferOp restore([&] { config::enable_full_sort_key_index = old_enable; });

    auto schema = make_two_int_key_schema_short_key_override();
    auto w = new_writer_for_schema(0, schema);
    ASSERT_OK(w->init(true));

    const int64_t num_rows = kInterval + 5;
    auto chunk = make_two_key_increasing_chunk(schema, num_rows, 0);
    ASSERT_OK(w->append_chunk(*chunk));
    EXPECT_FALSE(w->get_sort_key_samples().empty());

    uint64_t file_size = 0;
    uint64_t index_size = 0;
    uint64_t footer_position = 0;
    ASSERT_OK(w->finalize(&file_size, &index_size, &footer_position));

    SegmentFooterPB seg_footer = read_segment_footer(w->segment_path());
    ASSERT_TRUE(seg_footer.has_short_key_index_page());
    ASSERT_FALSE(seg_footer.has_full_sort_key_index_page());

    ShortKeyFooterPB sk_footer = read_short_key_page_footer(w->segment_path(), seg_footer.short_key_index_page());
    EXPECT_EQ(SHORT_KEY_ENCODING_TRUNCATED, sk_footer.short_key_encoding());
    EXPECT_EQ(0U, sk_footer.num_sort_key_columns());

    SegmentFileInfo file_info;
    w->write_sort_key_fields_to(file_info);
    SegmentMetadataPB meta;
    file_info.to_proto(/*segment_idx=*/0, &meta);
    EXPECT_GT(meta.deprecated_sort_key_samples_size(), 0);
}

// ---------------------------------------------------------------------------
// A sort key containing a FLOAT column has no registered KeyCoder
// (get_key_coder(TYPE_FLOAT) == nullptr), so the full sort key index must not be enabled for it
// even when config::enable_full_sort_key_index is on: the writer must fall back to the legacy
// short-key index (short_key_encoding==TRUNCATED) and keep writing metadata sort-key samples. Writing must
// not crash (this is the regression the codec-support gate protects against).
// ---------------------------------------------------------------------------
TEST_F(SegmentWriterSortKeySamplingTest, float_sort_key_falls_back_to_legacy_index) {
    const bool old_enable = config::enable_full_sort_key_index;
    config::enable_full_sort_key_index = true;
    DeferOp restore([&] { config::enable_full_sort_key_index = old_enable; });

    auto schema = make_int_float_key_schema_short_key_excludes_float();
    ASSERT_EQ(2U, schema->sort_key_idxes().size());
    ASSERT_EQ(1U, schema->num_short_key_columns());

    auto w = new_writer_for_schema(0, schema);
    ASSERT_OK(w->init(true));

    // Exceeds the sample interval, so the legacy sampler should fire.
    const int64_t num_rows = kInterval + 5;
    auto chunk = make_int_float_key_increasing_chunk(schema, num_rows, 0);
    ASSERT_OK(w->append_chunk(*chunk)); // must not crash
    EXPECT_FALSE(w->get_sort_key_samples().empty());

    uint64_t file_size = 0;
    uint64_t index_size = 0;
    uint64_t footer_position = 0;
    ASSERT_OK(w->finalize(&file_size, &index_size, &footer_position));

    // Codec-unsupported sort key -> legacy-only write, no full sort key index page.
    SegmentFooterPB seg_footer = read_segment_footer(w->segment_path());
    ASSERT_TRUE(seg_footer.has_short_key_index_page());
    ASSERT_FALSE(seg_footer.has_full_sort_key_index_page());

    ShortKeyFooterPB sk_footer = read_short_key_page_footer(w->segment_path(), seg_footer.short_key_index_page());
    EXPECT_EQ(SHORT_KEY_ENCODING_TRUNCATED, sk_footer.short_key_encoding());
    EXPECT_EQ(0U, sk_footer.num_sort_key_columns());

    SegmentFileInfo file_info;
    w->write_sort_key_fields_to(file_info);
    SegmentMetadataPB meta;
    file_info.to_proto(/*segment_idx=*/0, &meta);
    EXPECT_GT(meta.deprecated_sort_key_samples_size(), 0);
}

// ---------------------------------------------------------------------------
// estimate_segment_size() counts the full sort key index builder when the write gate is on. For
// identical schema + data, the legacy short key builder content is byte-for-byte identical whether
// the gate is on or off (both encode the same truncated legacy entries), so the only difference is
// the additional full sort key index builder -> the ON estimate must exceed the OFF estimate.
// ---------------------------------------------------------------------------
TEST_F(SegmentWriterSortKeySamplingTest, estimate_segment_size_counts_full_sort_key_index_builder) {
    auto schema = make_two_int_key_schema_short_key_override();
    // Multiple blocks (default num_rows_per_block=100 in BE_TEST) so both builders hold >1 entry.
    const int64_t num_rows = 1000;

    auto estimate_with = [&](bool full_on, uint32_t seg_id) {
        const bool old_enable = config::enable_full_sort_key_index;
        config::enable_full_sort_key_index = full_on;
        DeferOp restore([&] { config::enable_full_sort_key_index = old_enable; });

        auto w = new_writer_for_schema(seg_id, schema);
        CHECK_OK(w->init(true));
        auto chunk = make_two_key_increasing_chunk(schema, num_rows, 0);
        CHECK_OK(w->append_chunk(*chunk));
        return w->estimate_segment_size();
    };

    const uint64_t off_size = estimate_with(/*full_on=*/false, /*seg_id=*/100);
    const uint64_t on_size = estimate_with(/*full_on=*/true, /*seg_id=*/101);
    EXPECT_GT(on_size, off_size);
}

} // namespace starrocks
