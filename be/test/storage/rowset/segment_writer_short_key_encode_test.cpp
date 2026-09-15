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
#include <new>
#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/datum_tuple.h"
#include "column/fixed_length_column.h"
#include "common/config_rowset_fwd.h"
#include "fs/fs_memory.h"
#include "gen_cpp/segment.pb.h"
#include "gutil/strings/substitute.h"
#include "storage/base/short_key_index.h"
#include "storage/chunk_helper.h"
#include "storage/rowset/page_io.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_writer.h"
#include "storage/seek_tuple.h"
#include "storage/tablet_schema.h"
#include "storage/tablet_schema_helper.h"
#include "storage_primitive/chunk_iterator.h"
#include "storage_primitive/storage_stats.h"

namespace starrocks {

// Materializing this column throws, standing in for the huge ARRAY value whose Datum vector used to
// abort BE from SegmentWriter::append_chunk(). Every other Column method behaves like an INT column,
// so it survives the column writers; ColumnFactory::accept() resolves to the
// FixedLengthColumnBase<int32_t> visitor overload.
class ThrowOnGetInt32Column final
        : public CowFactory<ColumnFactory<FixedLengthColumnBase<int32_t>, ThrowOnGetInt32Column>, ThrowOnGetInt32Column,
                            Column> {
public:
    using SuperClass = CowFactory<ColumnFactory<FixedLengthColumnBase<int32_t>, ThrowOnGetInt32Column>,
                                  ThrowOnGetInt32Column, Column>;
    ThrowOnGetInt32Column() = default;

    Datum get(size_t) const override { throw std::bad_alloc(); }

    MutableColumnPtr clone_empty() const override { return this->create(); }

    MutableColumnPtr clone() const override {
        auto p = clone_empty();
        p->append(*this, 0, this->size());
        return p;
    }
};

static ColumnPB create_varchar_key_pb(int32_t id, int length, int index_length) {
    ColumnPB col;
    col.set_unique_id(id);
    col.set_name(std::to_string(id));
    col.set_type("VARCHAR");
    col.set_is_key(true);
    col.set_is_nullable(true);
    col.set_length(length);
    col.set_index_length(index_length);
    return col;
}

// One index entry per block; a small block keeps the fixtures tiny while still covering several blocks.
static constexpr uint32_t kRowsPerBlock = 4;
static constexpr int64_t kNumRows = 14;

class SegmentWriterShortKeyEncodeTest : public ::testing::Test {
protected:
    void SetUp() override {
        _fs = std::make_shared<MemoryFileSystem>();
        ASSERT_OK(_fs->create_dir(kDir));
    }

    std::unique_ptr<SegmentWriter> new_writer(uint32_t seg_id, const std::shared_ptr<TabletSchema>& schema) {
        SegmentWriterOptions opts;
        opts.num_rows_per_block = kRowsPerBlock;
        std::string filename = strings::Substitute("$0/seg_$1.dat", kDir, seg_id);
        ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(filename));
        return std::make_unique<SegmentWriter>(std::move(wfile), seg_id, schema, opts);
    }

    SegmentFooterPB read_segment_footer(const std::string& filename) {
        ASSIGN_OR_ABORT(auto read_file, _fs->new_random_access_file(filename));
        SegmentFooterPB seg_footer;
        auto footer_size_or = Segment::parse_segment_footer(read_file.get(), &seg_footer, nullptr, nullptr);
        CHECK(footer_size_or.ok()) << footer_size_or.status().to_string();
        return seg_footer;
    }

    // Decode all key entries of an index page. |full| selects the SORT_KEY_PAGE footer variant.
    std::vector<std::string> read_index_entries(const std::string& filename, const PagePointerPB& page, bool full) {
        ASSIGN_OR_ABORT(auto read_file, _fs->new_random_access_file(filename));
        PageReadOptions opts;
        opts.read_file = read_file.get();
        opts.page_pointer = PagePointer(page);
        opts.codec = nullptr; // index pages are not compressed
        OlapReaderStatistics stats;
        opts.stats = &stats;

        PageHandle handle;
        Slice body;
        PageFooterPB page_footer;
        CHECK(PageIO::read_and_decompress_page(opts, &handle, &body, &page_footer).ok());

        ShortKeyIndexDecoder decoder;
        if (full) {
            CHECK(decoder.parse(body, page_footer.sort_key_page_footer()).ok());
        } else {
            CHECK(decoder.parse(body, page_footer.short_key_page_footer()).ok());
        }
        std::vector<std::string> keys;
        for (uint32_t i = 0; i < decoder.num_items(); ++i) {
            keys.emplace_back(decoder.key(i).to_string());
        }
        return keys;
    }

    // Golden encodings produced the way the writer used to: materialize the WHOLE row, then encode.
    std::vector<std::string> golden_entries(const Chunk& chunk, const TabletSchema& schema,
                                            const std::vector<uint32_t>& sort_column_indexes, bool full) {
        std::vector<std::string> keys;
        for (size_t row = 0; row < chunk.num_rows(); row += kRowsPerBlock) {
            SeekTuple tuple(*chunk.schema(), chunk.get(row).datums());
            keys.emplace_back(full ? tuple.full_sort_key_encode(sort_column_indexes, 0)
                                   : tuple.short_key_encode(schema.num_short_key_columns(), sort_column_indexes, 0));
        }
        return keys;
    }

    // Write |chunk| through append_chunk() and assert every index entry equals the whole-row golden.
    void expect_entries_match_golden(uint32_t seg_id, const std::shared_ptr<TabletSchema>& schema, const Chunk& chunk,
                                     const std::vector<uint32_t>& expected_sort_column_indexes) {
        auto w = new_writer(seg_id, schema);
        ASSERT_OK(w->init(true));
        // The writer indexes chunk columns with its own local indexes, never tablet schema indexes.
        ASSERT_EQ(expected_sort_column_indexes, w->_sort_column_indexes);
        ASSERT_OK(w->append_chunk(chunk));
        uint64_t file_size = 0;
        uint64_t index_size = 0;
        uint64_t footer_position = 0;
        ASSERT_OK(w->finalize(&file_size, &index_size, &footer_position));

        SegmentFooterPB seg_footer = read_segment_footer(w->segment_path());
        ASSERT_TRUE(seg_footer.has_short_key_index_page());
        EXPECT_EQ(golden_entries(chunk, *schema, w->_sort_column_indexes, /*full=*/false),
                  read_index_entries(w->segment_path(), seg_footer.short_key_index_page(), /*full=*/false));

        ASSERT_EQ(config::enable_full_sort_key_index, seg_footer.has_full_sort_key_index_page());
        if (seg_footer.has_full_sort_key_index_page()) {
            EXPECT_EQ(golden_entries(chunk, *schema, w->_sort_column_indexes, /*full=*/true),
                      read_index_entries(w->segment_path(), seg_footer.full_sort_key_index_page(), /*full=*/true));
        }
    }

    std::shared_ptr<TabletSchema> make_schema(const std::vector<ColumnPB>& cols, int num_short_key_columns) {
        auto unique = TabletSchemaHelper::create_tablet_schema(cols, num_short_key_columns);
        return std::shared_ptr<TabletSchema>(std::move(unique));
    }

    // Row |i| is NULL whenever i % 4 == 0, so every block boundary of a single-key schema is NULL.
    ChunkUniquePtr make_int_key_chunk(const std::shared_ptr<TabletSchema>& schema, bool null_at_block_start) {
        auto s = ChunkHelper::convert_schema(schema);
        auto chunk = ChunkFactory::new_chunk(s, kNumRows);
        auto cols = chunk->columns();
        for (int64_t i = 0; i < kNumRows; ++i) {
            const bool null = null_at_block_start && (i % kRowsPerBlock == 0);
            cols[0]->as_mutable_ptr()->append_datum(null ? Datum() : Datum(static_cast<int32_t>(i)));
            for (size_t c = 1; c < cols.size(); ++c) {
                cols[c]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i * 10)));
            }
        }
        return chunk;
    }

    std::shared_ptr<MemoryFileSystem> _fs;
    const std::string kDir = "/short_key_encode_test";
};

// ---------------------------------------------------------------------------
// Encoding equivalence: the entries written from the sort-key columns alone must be byte-identical to
// the ones the whole-row SeekTuple produced, for every key shape -- including NULL keys, VARCHAR
// truncation, a short key shorter than the sort key, and a sort key that is not a physical prefix.
// ---------------------------------------------------------------------------
TEST_F(SegmentWriterShortKeyEncodeTest, entries_match_whole_row_golden_single_int_key) {
    auto schema = make_schema({create_int_key_pb(1), create_int_value_pb(2)}, /*num_short_key_columns=*/1);
    auto chunk = make_int_key_chunk(schema, /*null_at_block_start=*/false);
    expect_entries_match_golden(0, schema, *chunk, {0});
}

TEST_F(SegmentWriterShortKeyEncodeTest, entries_match_whole_row_golden_null_keys) {
    auto schema = make_schema({create_int_key_pb(1), create_int_value_pb(2)}, /*num_short_key_columns=*/1);
    auto chunk = make_int_key_chunk(schema, /*null_at_block_start=*/true);
    expect_entries_match_golden(0, schema, *chunk, {0});
}

TEST_F(SegmentWriterShortKeyEncodeTest, entries_match_whole_row_golden_short_key_shorter_than_sort_key) {
    auto schema = make_schema({create_int_key_pb(1), create_int_key_pb(2), create_int_value_pb(3)},
                              /*num_short_key_columns=*/1);
    auto chunk = make_int_key_chunk(schema, /*null_at_block_start=*/false);
    expect_entries_match_golden(0, schema, *chunk, {0, 1});
}

TEST_F(SegmentWriterShortKeyEncodeTest, entries_match_whole_row_golden_non_prefix_sort_key) {
    auto schema = make_schema({create_int_key_pb(1), create_int_key_pb(2), create_int_value_pb(3)},
                              /*num_short_key_columns=*/2);
    schema->set_sort_key_idxes({1, 0});
    auto chunk = make_int_key_chunk(schema, /*null_at_block_start=*/false);
    expect_entries_match_golden(0, schema, *chunk, {1, 0});
}

TEST_F(SegmentWriterShortKeyEncodeTest, entries_match_whole_row_golden_varchar_key_truncated) {
    auto schema = make_schema({create_varchar_key_pb(1, /*length=*/32, /*index_length=*/4), create_int_value_pb(2)},
                              /*num_short_key_columns=*/1);
    auto s = ChunkHelper::convert_schema(schema);
    auto chunk = ChunkFactory::new_chunk(s, kNumRows);
    auto cols = chunk->columns();
    for (int64_t i = 0; i < kNumRows; ++i) {
        // Longer than index_length, so the short key is truncated while the full sort key is not.
        std::string v = strings::Substitute("key_that_is_long_$0", i);
        cols[0]->as_mutable_ptr()->append_datum(Datum(Slice(v)));
        cols[1]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i)));
    }
    expect_entries_match_golden(0, schema, *chunk, {0});
}

// The full sort key index (footer field 11) is encoded from the same SeekTuple, so it must match the
// whole-row golden too. Runs the shapes whose full and truncated encodings differ.
TEST_F(SegmentWriterShortKeyEncodeTest, entries_match_whole_row_golden_with_full_sort_key_index) {
    const bool old_enable = config::enable_full_sort_key_index;
    config::enable_full_sort_key_index = true;
    DeferOp restore([&] { config::enable_full_sort_key_index = old_enable; });

    auto int_schema = make_schema({create_int_key_pb(1), create_int_key_pb(2), create_int_value_pb(3)},
                                  /*num_short_key_columns=*/1);
    expect_entries_match_golden(0, int_schema, *make_int_key_chunk(int_schema, /*null_at_block_start=*/true), {0, 1});

    auto varchar_schema =
            make_schema({create_varchar_key_pb(1, /*length=*/32, /*index_length=*/4), create_int_value_pb(2)},
                        /*num_short_key_columns=*/1);
    auto s = ChunkHelper::convert_schema(varchar_schema);
    auto chunk = ChunkFactory::new_chunk(s, kNumRows);
    auto cols = chunk->columns();
    for (int64_t i = 0; i < kNumRows; ++i) {
        std::string v = strings::Substitute("key_that_is_long_$0", i);
        cols[0]->as_mutable_ptr()->append_datum(Datum(Slice(v)));
        cols[1]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i)));
    }
    expect_entries_match_golden(1, varchar_schema, *chunk, {0});
}

// ---------------------------------------------------------------------------
// Vertical writer, key-columns pass: the sort key is the third tablet column, but the pass writes it
// alone, so the writer must index the chunk with its local index 0 -- never the tablet index 2.
// ---------------------------------------------------------------------------
TEST_F(SegmentWriterShortKeyEncodeTest, vertical_writer_key_pass_uses_chunk_local_indexes) {
    auto schema = make_schema({create_int_key_pb(1), create_int_key_pb(2), create_int_value_pb(3)},
                              /*num_short_key_columns=*/1);
    schema->set_sort_key_idxes({2});

    auto w = new_writer(0, schema);
    ASSERT_OK(w->init(std::vector<uint32_t>{2}, true));
    ASSERT_EQ(std::vector<uint32_t>({0}), w->_sort_column_indexes);

    auto sort_key_schema = ChunkHelper::convert_schema(schema, std::vector<ColumnId>{2});
    auto sort_key_chunk = ChunkFactory::new_chunk(sort_key_schema, kNumRows);
    for (int64_t i = 0; i < kNumRows; ++i) {
        sort_key_chunk->columns()[0]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i)));
    }
    ASSERT_OK(w->append_chunk(*sort_key_chunk));
    uint64_t index_size = 0;
    ASSERT_OK(w->finalize_columns(&index_size));

    // Second pass: the remaining columns, written without keys.
    ASSERT_OK(w->init(std::vector<uint32_t>{0, 1}, false));
    auto value_schema = ChunkHelper::convert_schema(schema, std::vector<ColumnId>{0, 1});
    auto value_chunk = ChunkFactory::new_chunk(value_schema, kNumRows);
    for (int64_t i = 0; i < kNumRows; ++i) {
        value_chunk->columns()[0]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i)));
        value_chunk->columns()[1]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i)));
    }
    ASSERT_OK(w->append_chunk(*value_chunk));
    ASSERT_OK(w->finalize_columns(&index_size));
    uint64_t file_size = 0;
    ASSERT_OK(w->finalize_footer(&file_size));

    SegmentFooterPB seg_footer = read_segment_footer(w->segment_path());
    ASSERT_TRUE(seg_footer.has_short_key_index_page());
    EXPECT_EQ(golden_entries(*sort_key_chunk, *schema, {0}, /*full=*/false),
              read_index_entries(w->segment_path(), seg_footer.short_key_index_page(), /*full=*/false));
    if (seg_footer.has_full_sort_key_index_page()) {
        EXPECT_EQ(golden_entries(*sort_key_chunk, *schema, {0}, /*full=*/true),
                  read_index_entries(w->segment_path(), seg_footer.full_sort_key_index_page(), /*full=*/true));
    }
}

// ---------------------------------------------------------------------------
// Value columns are never materialized: a column that throws when materialized sits at a value
// position, so the pre-fix whole-row path (asserted to throw below) would have died here.
// ---------------------------------------------------------------------------
TEST_F(SegmentWriterShortKeyEncodeTest, value_column_is_never_materialized) {
    auto schema = make_schema({create_int_key_pb(1), create_int_value_pb(2)}, /*num_short_key_columns=*/1);
    auto s = ChunkHelper::convert_schema(schema);
    auto chunk = ChunkFactory::new_chunk(s, 1);
    chunk->columns()[0]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(7)));
    auto trap = ThrowOnGetInt32Column::create();
    trap->append_datum(Datum(static_cast<int32_t>(7)));
    chunk->columns()[1] = std::move(trap);
    ASSERT_EQ(1U, chunk->num_rows());
    ASSERT_THROW(chunk->get(0), std::bad_alloc);

    auto w = new_writer(0, schema);
    ASSERT_OK(w->init(true));
    ASSERT_EQ(std::vector<uint32_t>({0}), w->_sort_column_indexes);
    ASSERT_OK(w->_append_sort_key_index_entry(*chunk, 0));
}

// ---------------------------------------------------------------------------
// A bad_alloc raised while materializing a sort-key column must come back as a Status, not kill BE.
// ---------------------------------------------------------------------------
TEST_F(SegmentWriterShortKeyEncodeTest, bad_alloc_on_sort_key_column_returns_memory_limit_exceeded) {
    auto schema = make_schema({create_int_key_pb(1), create_int_value_pb(2)}, /*num_short_key_columns=*/1);
    auto s = ChunkHelper::convert_schema(schema);
    auto chunk = ChunkFactory::new_chunk(s, 1);
    auto trap = ThrowOnGetInt32Column::create();
    trap->append_datum(Datum(static_cast<int32_t>(7)));
    chunk->columns()[0] = std::move(trap);
    chunk->columns()[1]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(7)));

    auto w = new_writer(0, schema);
    ASSERT_OK(w->init(true));
    ASSERT_EQ(std::vector<uint32_t>({0}), w->_sort_column_indexes);
    Status st = w->_append_sort_key_index_entry(*chunk, 0);
    EXPECT_TRUE(st.is_mem_limit_exceeded()) << st.to_string();
}

// ---------------------------------------------------------------------------
// Reading back what was written: the short key index still resolves block boundaries, so
// lower_bound/upper_bound land on the same blocks the whole-row encoding produced.
// ---------------------------------------------------------------------------
TEST_F(SegmentWriterShortKeyEncodeTest, short_key_index_stays_seekable) {
    auto schema = make_schema({create_int_key_pb(1), create_int_value_pb(2)}, /*num_short_key_columns=*/1);
    auto chunk = make_int_key_chunk(schema, /*null_at_block_start=*/false);

    auto w = new_writer(0, schema);
    ASSERT_OK(w->init(true));
    ASSERT_OK(w->append_chunk(*chunk));
    uint64_t file_size = 0;
    uint64_t index_size = 0;
    uint64_t footer_position = 0;
    ASSERT_OK(w->finalize(&file_size, &index_size, &footer_position));

    std::vector<std::string> entries =
            read_index_entries(w->segment_path(), read_segment_footer(w->segment_path()).short_key_index_page(),
                               /*full=*/false);
    ASSERT_EQ(static_cast<size_t>((kNumRows + kRowsPerBlock - 1) / kRowsPerBlock), entries.size());
    // Entries are the keys of rows 0, 4, 8, 12 and must stay strictly increasing for seeks to work.
    for (size_t i = 1; i < entries.size(); ++i) {
        EXPECT_LT(entries[i - 1], entries[i]);
    }

    ASSIGN_OR_ABORT(auto segment, Segment::open(_fs, FileInfo{w->segment_path()}, 0, schema));
    ASSERT_EQ(kNumRows, segment->num_rows());
    SegmentReadOptions read_opts;
    read_opts.fs = _fs;
    OlapReaderStatistics stats;
    read_opts.stats = &stats;
    auto read_schema = ChunkHelper::convert_schema(schema);
    ASSIGN_OR_ABORT(auto iter, segment->new_iterator(read_schema, read_opts));
    auto read_chunk = ChunkFactory::new_chunk(read_schema, kNumRows);
    ASSERT_OK(iter->get_next(read_chunk.get()));
    ASSERT_EQ(kNumRows, read_chunk->num_rows());
    for (int64_t i = 0; i < kNumRows; ++i) {
        EXPECT_EQ(i, read_chunk->get(i)[0].get_int32());
        EXPECT_EQ(i * 10, read_chunk->get(i)[1].get_int32());
    }
    iter->close();
}

} // namespace starrocks
