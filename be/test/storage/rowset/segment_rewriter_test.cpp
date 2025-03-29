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

    void TearDown() override {
        ASSERT_TRUE(fs::remove_all(kSegmentDir).ok());
        StoragePageCache::instance()->prune();
    }

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
        auto& cols = partial_chunk->columns();
        for (auto j = 0; j < chunk_size && i * chunk_size + j < num_rows; ++j) {
            cols[0]->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j)));
            cols[1]->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j + 1)));
            cols[2]->append_datum(Datum(static_cast<int32_t>(i * chunk_size + j + 3)));
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
    std::vector<MutableColumnPtr> write_columns(read_column_ids.size());
    for (auto i = 0; i < read_column_ids.size(); ++i) {
        const auto read_column_id = read_column_ids[i];
        auto tablet_column = tablet_schema->column(read_column_id);
        auto column = ChunkHelper::column_from_field_type(tablet_column.type(), tablet_column.is_nullable());
        write_columns[i] = column->clone_empty();
        for (auto j = 0; j < num_rows; ++j) {
            write_columns[i]->append_datum(Datum(static_cast<int32_t>(j + read_column_ids[i])));
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
    auto seg_iterator = res.value();

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

} // namespace starrocks
