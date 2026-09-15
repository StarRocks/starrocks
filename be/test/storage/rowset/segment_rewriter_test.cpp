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

#include "storage/rowset/segment_rewriter.h"

#include <gtest/gtest.h>

#include <functional>
#include <iostream>

#include "column/datum_tuple.h"
#include "common/logging.h"
#include "fs/fs_util.h"
#include "fs/key_cache.h"
#include "gen_cpp/olap_file.pb.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"
#include "storage/olap_common.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_writer.h"
#include "storage/tablet_schema.h"
#include "storage/tablet_schema_helper.h"
#include "testutil/assert.h"

namespace starrocks {

using std::string;
using std::shared_ptr;

using std::vector;

class SegmentRewriterTest : public ::testing::Test {
protected:
    void SetUp() override {
        _fs = FileSystem::CreateSharedFromString("posix://").value();
        ASSERT_OK(_fs->create_dir_recursive(kSegmentDir));
    }

    void TearDown() override { ASSERT_TRUE(fs::remove_all(kSegmentDir).ok()); }

    const std::string kSegmentDir = "./segment_rewriter_test";

    std::shared_ptr<FileSystem> _fs;
};

TEST_F(SegmentRewriterTest, rewrite_test) {
    std::shared_ptr<TabletSchema> partial_tablet_schema = TabletSchemaHelper::create_tablet_schema(
            {create_int_key_pb(1), create_int_key_pb(2), create_int_value_pb(4)});

    SegmentWriterOptions opts;
    opts.num_rows_per_block = 10;

    auto encryption_pair = KeyCache::instance().create_plain_random_encryption_meta_pair().value();
    std::string file_name = kSegmentDir + "/partial_rowset";
    WritableFileOptions wopts{.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE,
                              .encryption_info = encryption_pair.info};
    ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(wopts, file_name));

    SegmentWriter writer(std::move(wfile), 0, partial_tablet_schema, opts);
    ASSERT_OK(writer.init());

    int32_t chunk_size = config::vector_chunk_size;
    size_t num_rows = 10000;
    auto partial_schema = ChunkHelper::convert_schema(partial_tablet_schema);
    auto partial_chunk = ChunkHelper::new_chunk(partial_schema, chunk_size);

    for (auto i = 0; i < num_rows % chunk_size; ++i) {
        partial_chunk->reset();
        auto cols = partial_chunk->columns();
        for (auto j = 0; j < chunk_size && i * chunk_size + j < num_rows; ++j) {
            cols[0]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j)));
            cols[1]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j + 1)));
            cols[2]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j + 3)));
        }
        ASSERT_OK(writer.append_chunk(*partial_chunk));
    }

    uint64_t file_size = 0;
    uint64_t index_size;
    uint64_t footer_position;
    ASSERT_OK(writer.finalize(&file_size, &index_size, &footer_position));

    FooterPointerPB partial_rowset_footer;
    partial_rowset_footer.set_position(footer_position);
    partial_rowset_footer.set_size(file_size - footer_position);

    FileInfo src_file_info{.path = file_name, .encryption_meta = encryption_pair.encryption_meta};
    auto partial_segment = *Segment::open(_fs, src_file_info, 0, partial_tablet_schema);
    ASSERT_EQ(partial_segment->num_rows(), num_rows);

    std::shared_ptr<TabletSchema> tablet_schema = TabletSchemaHelper::create_tablet_schema(
            {create_int_key_pb(1), create_int_key_pb(2), create_int_value_pb(3), create_int_value_pb(4),
             create_int_value_pb(5)});
    std::string dst_file_name = kSegmentDir + "/rewrite_rowset";
    std::vector<uint32_t> read_column_ids{2, 4};
    MutableColumns write_columns(read_column_ids.size());
    for (auto i = 0; i < read_column_ids.size(); ++i) {
        const auto read_column_id = read_column_ids[i];
        auto tablet_column = tablet_schema->column(read_column_id);
        auto column = ChunkHelper::column_from_field_type(tablet_column.type(), tablet_column.is_nullable());
        write_columns[i] = column->clone_empty();
        for (auto j = 0; j < num_rows; ++j) {
            write_columns[i]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(j + read_column_ids[i])));
        }
    }

    FileInfo file_info{.path = dst_file_name};
    ASSERT_OK(SegmentRewriter::rewrite_partial_update(src_file_info, &file_info, tablet_schema, read_column_ids,
                                                      write_columns, partial_segment->id(), partial_rowset_footer));

    auto segment = *Segment::open(_fs, FileInfo{.path = dst_file_name, .encryption_meta = file_info.encryption_meta}, 0,
                                  tablet_schema);
    ASSERT_EQ(segment->num_rows(), num_rows);

    SegmentReadOptions seg_options;
    seg_options.fs = _fs;
    OlapReaderStatistics stats;
    seg_options.stats = &stats;
    auto schema = ChunkHelper::convert_schema(tablet_schema);
    auto res = segment->new_iterator(schema, seg_options);
    ASSERT_FALSE(res.status().is_end_of_file() || !res.ok() || res.value() == nullptr);
    const auto& seg_iterator = res.value();

    size_t count = 0;
    auto chunk = ChunkHelper::new_chunk(schema, chunk_size);
    while (true) {
        chunk->reset();
        auto st = seg_iterator->get_next(chunk.get());
        if (st.is_end_of_file()) {
            break;
        }
        ASSERT_FALSE(!st.ok());
        for (auto i = 0; i < chunk->num_rows(); ++i) {
            EXPECT_EQ(count, chunk->get(i)[0].get_int32());
            EXPECT_EQ(count + 1, chunk->get(i)[1].get_int32());
            EXPECT_EQ(count + 2, chunk->get(i)[2].get_int32());
            EXPECT_EQ(count + 3, chunk->get(i)[3].get_int32());
            EXPECT_EQ(count + 4, chunk->get(i)[4].get_int32());
            ++count;
        }
    }
    EXPECT_EQ(count, num_rows);
}

// Second way the copy-only path can hand back an unreadable segment, independent of encryption:
// when the source is a BUNDLED segment (one slice of a physical file shared with the other tablets of
// the same load), that copy must take the slice, not the file. Copying the whole file leaves the
// destination holding the entire bundle while the rowset metadata records only the slice's size and,
// the rewrite having unbundled the rowset, no offset. A reader that trusts the recorded size then
// looks for the footer 12 bytes before the slice length inside the bundle and fails the magic number
// check -- permanently, since compaction has to read the segment too.
TEST_F(SegmentRewriterTest, rewrite_copy_only_copies_just_the_bundled_slice) {
    std::shared_ptr<TabletSchema> tablet_schema =
            TabletSchemaHelper::create_tablet_schema({create_int_key_pb(1), create_int_value_pb(2)});
    constexpr size_t kNumRows = 100;
    // The bundle holds another tablet's slice first, so this segment starts at a non-zero offset.
    constexpr int64_t kLeadingSliceSize = 4096;

    // Write the segment on its own first: its bytes are what the bundle carries at kLeadingSliceSize,
    // and its length is the `size` the rowset metadata records for the slice.
    const std::string segment_path = kSegmentDir + "/bundled_slice";
    {
        WritableFileOptions wopts{.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        ASSIGN_OR_ABORT(auto wfile, _fs->new_writable_file(wopts, segment_path));
        SegmentWriter writer(std::move(wfile), 0, tablet_schema, SegmentWriterOptions{});
        ASSERT_OK(writer.init());
        auto schema = ChunkHelper::convert_schema(tablet_schema);
        auto chunk = ChunkHelper::new_chunk(schema, kNumRows);
        auto cols = chunk->columns();
        for (size_t i = 0; i < kNumRows; ++i) {
            cols[0]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i)));
            cols[1]->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i + 1)));
        }
        ASSERT_OK(writer.append_chunk(*chunk));
        uint64_t file_size = 0;
        uint64_t index_size = 0;
        uint64_t footer_position = 0;
        ASSERT_OK(writer.finalize(&file_size, &index_size, &footer_position));
    }
    ASSIGN_OR_ABORT(const uint64_t slice_size, _fs->get_file_size(segment_path));

    const std::string bundle_path = kSegmentDir + "/bundle";
    {
        WritableFileOptions wopts{.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        ASSIGN_OR_ABORT(auto bundle_file, _fs->new_writable_file(wopts, bundle_path));
        const std::string leading_slice(kLeadingSliceSize, 'x');
        ASSERT_OK(bundle_file->append(leading_slice));
        ASSERT_OK(fs::copy_append_file(segment_path, bundle_file.get()));
        ASSERT_OK(bundle_file->close());
    }

    FileInfo src{
            .path = bundle_path, .size = static_cast<int64_t>(slice_size), .bundle_file_offset = kLeadingSliceSize};
    const std::string dest_path = kSegmentDir + "/rewritten";
    FileInfo dest{.path = dest_path};
    // No unmodified column left to backfill: the copy-only path.
    std::vector<uint32_t> no_column_ids;
    MutableColumns no_columns;
    ASSERT_OK(SegmentRewriter::rewrite_partial_update(src, &dest, tablet_schema, no_column_ids, no_columns, 0,
                                                      FooterPointerPB()));

    // The destination is the slice alone, not the bundle it came out of.
    ASSERT_TRUE(dest.size.has_value());
    EXPECT_EQ(static_cast<int64_t>(slice_size), dest.size.value());
    ASSIGN_OR_ABORT(const uint64_t dest_file_size, _fs->get_file_size(dest_path));
    EXPECT_EQ(slice_size, dest_file_size);

    // ... and it is a readable segment, which is what the recorded size buys the reader.
    ASSIGN_OR_ABORT(auto segment,
                    Segment::open(_fs, FileInfo{.path = dest_path, .size = static_cast<int64_t>(slice_size)}, 0,
                                  tablet_schema));
    ASSERT_EQ(kNumRows, segment->num_rows());

    SegmentReadOptions seg_options;
    seg_options.fs = _fs;
    OlapReaderStatistics stats;
    seg_options.stats = &stats;
    auto schema = ChunkHelper::convert_schema(tablet_schema);
    ASSIGN_OR_ABORT(auto iter, segment->new_iterator(schema, seg_options));
    auto chunk = ChunkHelper::new_chunk(schema, kNumRows);
    size_t count = 0;
    while (true) {
        chunk->reset();
        auto st = iter->get_next(chunk.get());
        if (st.is_end_of_file()) {
            break;
        }
        ASSERT_OK(st);
        for (size_t i = 0; i < chunk->num_rows(); ++i) {
            EXPECT_EQ(static_cast<int32_t>(count), chunk->get(i)[0].get_int32());
            EXPECT_EQ(static_cast<int32_t>(count + 1), chunk->get(i)[1].get_int32());
            ++count;
        }
    }
    EXPECT_EQ(kNumRows, count);
}

// A partial update can reach publish with nothing left to backfill: the unmodified columns are
// resolved against the schema the metadata carries AT PUBLISH, and a DROP COLUMN landing between the
// write and its publish takes them with it. The rewrite then degenerates to copying the source
// segment out under the new name.
//
// That copy is raw bytes, so an encrypted source stays encrypted under the source's key -- and the
// destination is only readable if the key travels with it. Losing the meta here is not a silent
// downgrade to plaintext: apply_opwrite persists the empty meta over the segment's own, the reader
// opens ciphertext with no key, and its last 12 bytes fail the magic number check for good.
TEST_F(SegmentRewriterTest, rewrite_copy_only_keeps_the_source_encryption) {
    std::shared_ptr<TabletSchema> tablet_schema =
            TabletSchemaHelper::create_tablet_schema({create_int_key_pb(1), create_int_value_pb(2)});
    constexpr size_t kNumRows = 100;

    auto encryption_pair = KeyCache::instance().create_plain_random_encryption_meta_pair().value();
    const std::string src_path = kSegmentDir + "/copy_only_encrypted_src";
    ASSIGN_OR_ABORT(auto wfile,
                    _fs->new_writable_file(WritableFileOptions{.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE,
                                                               .encryption_info = encryption_pair.info},
                                           src_path));
    SegmentWriter writer(std::move(wfile), 0, tablet_schema, SegmentWriterOptions{});
    ASSERT_OK(writer.init());
    auto schema = ChunkHelper::convert_schema(tablet_schema);
    auto src_chunk = ChunkHelper::new_chunk(schema, kNumRows);
    for (size_t i = 0; i < kNumRows; ++i) {
        src_chunk->get_column_by_index(0)->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i)));
        src_chunk->get_column_by_index(1)->as_mutable_ptr()->append_datum(Datum(static_cast<int32_t>(i + 1)));
    }
    ASSERT_OK(writer.append_chunk(*src_chunk));
    uint64_t file_size = 0;
    uint64_t index_size = 0;
    uint64_t footer_position = 0;
    ASSERT_OK(writer.finalize(&file_size, &index_size, &footer_position));

    FooterPointerPB partial_rowset_footer;
    partial_rowset_footer.set_position(footer_position);
    partial_rowset_footer.set_size(file_size - footer_position);

    FileInfo src{.path = src_path,
                 .size = static_cast<int64_t>(file_size),
                 .encryption_meta = encryption_pair.encryption_meta};
    const std::string dest_path = kSegmentDir + "/copy_only_encrypted_dst";
    FileInfo dest{.path = dest_path};
    // No unmodified column left to backfill: the copy-only path.
    std::vector<uint32_t> no_column_ids;
    MutableColumns no_columns;
    ASSERT_OK(SegmentRewriter::rewrite_partial_update(src, &dest, tablet_schema, no_column_ids, no_columns, 0,
                                                      partial_rowset_footer));

    EXPECT_EQ(encryption_pair.encryption_meta, dest.encryption_meta)
            << "the copy is readable only with the source's key, so the meta must travel with it";
    ASSERT_TRUE(dest.size.has_value());

    auto segment_or =
            Segment::open(_fs, FileInfo{.path = dest_path, .size = dest.size, .encryption_meta = dest.encryption_meta},
                          0, tablet_schema);
    ASSERT_TRUE(segment_or.ok()) << segment_or.status();
    auto segment = std::move(segment_or).value();
    ASSERT_EQ(kNumRows, segment->num_rows());

    SegmentReadOptions seg_options;
    seg_options.fs = _fs;
    OlapReaderStatistics stats;
    seg_options.stats = &stats;
    ASSIGN_OR_ABORT(auto iter, segment->new_iterator(schema, seg_options));
    auto chunk = ChunkHelper::new_chunk(schema, kNumRows);
    size_t count = 0;
    while (true) {
        chunk->reset();
        auto st = iter->get_next(chunk.get());
        if (st.is_end_of_file()) {
            break;
        }
        ASSERT_OK(st);
        for (size_t i = 0; i < chunk->num_rows(); ++i) {
            EXPECT_EQ(static_cast<int32_t>(count), chunk->get(i)[0].get_int32());
            EXPECT_EQ(static_cast<int32_t>(count + 1), chunk->get(i)[1].get_int32());
            ++count;
        }
    }
    EXPECT_EQ(kNumRows, count);
}

} // namespace starrocks
