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

#include "storage/segment_stream_converter.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "base/uuid/uuid_generator.h"
#include "column/chunk_factory.h"
#include "column/datum_tuple.h"
#include "fs/fs.h"
#include "fs/fs_memory.h"
#include "storage/chunk_helper.h"
#include "storage/lake/replication_schema_utils.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_writer.h"
#include "storage_primitive/chunk_iterator.h"

namespace starrocks {

extern std::atomic<size_t> s_segment_footer_buffer_size;

class SegmentStreamConverterTest : public testing::Test {
public:
    SegmentStreamConverterTest() = default;
    ~SegmentStreamConverterTest() override = default;

    void SetUp() override {}

    void TearDown() override {}
};

TEST_F(SegmentStreamConverterTest, test_no_convert) {
    s_segment_footer_buffer_size.store(1024);

    MemoryFileSystem memory_fs;
    auto status_or = memory_fs.new_writable_file("/test_url");
    EXPECT_TRUE(status_or.ok());

    auto wfile = std::move(status_or.value());
    SegmentStreamConverter segment_stream_converter("test_name", 1024, std::move(wfile), nullptr);

    char data[1024];
    auto status = segment_stream_converter.append(data, sizeof(data));
    EXPECT_TRUE(status.ok());

    EXPECT_EQ(segment_stream_converter._output_file->size(), sizeof(data));
    EXPECT_TRUE(segment_stream_converter._segment_footer_buffer.empty());
}

TEST_F(SegmentStreamConverterTest, test_convert_small_segment) {
    s_segment_footer_buffer_size.store(1024);

    MemoryFileSystem memory_fs;
    auto status_or = memory_fs.new_writable_file("/test_url");
    EXPECT_TRUE(status_or.ok());

    auto wfile = std::move(status_or.value());
    std::unordered_map<uint32_t, uint32_t> unique_id_map = {{1, 2}};
    SegmentStreamConverter segment_stream_converter("test_name", 1024, std::move(wfile), &unique_id_map);

    char data[512];
    auto status = segment_stream_converter.append(data, sizeof(data));
    EXPECT_TRUE(status.ok());

    EXPECT_EQ(segment_stream_converter._output_file->size(), 0);
    EXPECT_EQ(segment_stream_converter._segment_footer_buffer.size(), 512);

    status = segment_stream_converter.append(data, sizeof(data));
    EXPECT_TRUE(status.ok());

    EXPECT_EQ(segment_stream_converter._output_file->size(), 0);
    EXPECT_EQ(segment_stream_converter._segment_footer_buffer.size(), 1024);
}

TEST_F(SegmentStreamConverterTest, test_convert_large_segment) {
    s_segment_footer_buffer_size.store(512);

    MemoryFileSystem memory_fs;
    auto status_or = memory_fs.new_writable_file("/test_url");
    EXPECT_TRUE(status_or.ok());

    auto wfile = std::move(status_or.value());
    std::unordered_map<uint32_t, uint32_t> unique_id_map = {{1, 2}};
    SegmentStreamConverter segment_stream_converter("test_name", 1024, std::move(wfile), &unique_id_map);

    char data[256];
    auto status = segment_stream_converter.append(data, sizeof(data));
    EXPECT_TRUE(status.ok());

    EXPECT_EQ(segment_stream_converter._output_file->size(), 256);
    EXPECT_EQ(segment_stream_converter._segment_footer_buffer.size(), 0);

    status = segment_stream_converter.append(data, sizeof(data));
    EXPECT_TRUE(status.ok());

    EXPECT_EQ(segment_stream_converter._output_file->size(), 512);
    EXPECT_EQ(segment_stream_converter._segment_footer_buffer.size(), 0);

    status = segment_stream_converter.append(data, sizeof(data));
    EXPECT_TRUE(status.ok());

    EXPECT_EQ(segment_stream_converter._output_file->size(), 512);
    EXPECT_EQ(segment_stream_converter._segment_footer_buffer.size(), 256);

    status = segment_stream_converter.append(data, sizeof(data));
    EXPECT_TRUE(status.ok());

    EXPECT_EQ(segment_stream_converter._output_file->size(), 512);
    EXPECT_EQ(segment_stream_converter._segment_footer_buffer.size(), 512);
}

TEST_F(SegmentStreamConverterTest, test_convert_large_segment2) {
    s_segment_footer_buffer_size.store(512);

    MemoryFileSystem memory_fs;
    auto status_or = memory_fs.new_writable_file("/test_url");
    EXPECT_TRUE(status_or.ok());

    auto wfile = std::move(status_or.value());
    std::unordered_map<uint32_t, uint32_t> unique_id_map = {{1, 2}};
    SegmentStreamConverter segment_stream_converter("test_name", 1024, std::move(wfile), &unique_id_map);

    char data[512];
    auto status = segment_stream_converter.append(data, 256);
    EXPECT_TRUE(status.ok());

    EXPECT_EQ(segment_stream_converter._output_file->size(), 256);
    EXPECT_EQ(segment_stream_converter._segment_footer_buffer.size(), 0);

    status = segment_stream_converter.append(data, 512);
    EXPECT_TRUE(status.ok());

    EXPECT_EQ(segment_stream_converter._output_file->size(), 512);
    EXPECT_EQ(segment_stream_converter._segment_footer_buffer.size(), 256);

    status = segment_stream_converter.append(data, 256);
    EXPECT_TRUE(status.ok());

    EXPECT_EQ(segment_stream_converter._output_file->size(), 512);
    EXPECT_EQ(segment_stream_converter._segment_footer_buffer.size(), 512);
}

TEST_F(SegmentStreamConverterTest, test_strict_mode_rejects_empty_mapping) {
    MemoryFileSystem fs;
    ASSIGN_OR_ABORT(auto output, fs.new_writable_file("/empty-mapping"));
    std::unordered_map<uint32_t, uint32_t> mapping;
    SegmentStreamConverter converter("empty", 0, std::move(output), &mapping, true);
    EXPECT_TRUE(converter.append("", 0).is_corruption());
    EXPECT_TRUE(converter.close().is_corruption());
}

TEST_F(SegmentStreamConverterTest, StrictFooterRejectsMissingAndDuplicateUids) {
    for (bool duplicate : {false, true}) {
        MemoryFileSystem fs;
        ASSIGN_OR_ABORT(auto input_file, fs.new_writable_file("/source"));
        SegmentFooterPB footer;
        footer.add_columns()->set_unique_id(1);
        auto* second = footer.add_columns();
        if (duplicate) second->set_unique_id(1);
        ASSERT_OK(Segment::write_segment_footer(input_file.get(), footer));
        ASSERT_OK(input_file->close());
        ASSIGN_OR_ABORT(auto input, fs.new_random_access_file("/source"));
        ASSIGN_OR_ABORT(auto bytes, input->read_all());
        ASSIGN_OR_ABORT(auto output, fs.new_writable_file("/target"));
        std::unordered_map<uint32_t, uint32_t> mapping = {{2, 3}};
        std::unordered_set<uint32_t> dropped_uids = {1};
        SegmentStreamConverter converter("/source", bytes.size(), std::move(output), &mapping, true, &dropped_uids);
        ASSERT_OK(converter.append(bytes.data(), bytes.size()));
        EXPECT_TRUE(converter.close().is_corruption());
        EXPECT_EQ(1, mapping.size());
    }
}

TEST_F(SegmentStreamConverterTest, LightSchemaChangeReadsOldAndNewSegmentsAfterTwoHops) {
    s_segment_footer_buffer_size.store(4 * 1024 * 1024);
    TabletMetadataPB source;
    auto* current = source.mutable_schema();
    current->set_id(902101);
    current->set_keys_type(DUP_KEYS);
    current->set_num_short_key_columns(1);
    for (int i = 0; i < 3; ++i) {
        auto* c = current->add_column();
        c->set_unique_id(i * 2);
        c->set_name(i == 0 ? "k" : (i == 1 ? "stable" : "readded"));
        c->set_type("INT");
        c->set_is_key(i == 0);
        c->set_is_nullable(false);
        if (i == 2) c->set_default_value("7");
    }
    auto& old = (*source.mutable_historical_schemas())[902100];
    old.CopyFrom(*current);
    old.set_id(902100);
    old.mutable_column(2)->set_unique_id(1);
    // Also drop a middle column so retained footer column_id values have a gap.
    auto* dropped_middle = old.add_column();
    dropped_middle->set_unique_id(9);
    dropped_middle->set_name("dropped_middle");
    dropped_middle->set_type("INT");
    dropped_middle->set_is_nullable(true);
    old.mutable_column()->SwapElements(1, 3);
    old.mutable_column()->SwapElements(2, 3);
    source.add_rowsets()->set_id(1);
    (*source.mutable_rowset_to_schema())[1] = old.id();
    TabletMetadataPB target;
    target.mutable_schema()->CopyFrom(*current);
    target.mutable_schema()->set_id(902102);
    target.mutable_schema()->mutable_column(1)->set_unique_id(1);
    target.mutable_schema()->mutable_column(2)->set_unique_id(2);
    std::unordered_map<uint32_t, uint32_t> mapping;
    std::unordered_set<uint32_t> dropped_uids;
    ASSERT_OK(lake::build_light_schema_change_uid_map(source, target, &mapping, &dropped_uids));
    auto fs = std::make_shared<MemoryFileSystem>();
    auto target_schema = TabletSchema::create(target.schema());
    auto read_schema = ChunkHelper::convert_schema(target_schema);
    // The old UID 1 must become neither target.stable (also UID 1) nor target.readded.
    // Old rows use the default 7; new rows keep their actual values.
    for (bool historical : {true, false}) {
        const auto name = "/" + ThreadLocalUUIDGenerator::next_uuid_string();
        const auto output_name = name + ".converted";
        auto write_schema = TabletSchema::create(historical ? old : source.schema());
        ASSIGN_OR_ABORT(auto output, fs->new_writable_file(name));
        SegmentWriter writer(std::move(output), 0, write_schema, SegmentWriterOptions{});
        ASSERT_OK(writer.init());
        auto chunk = ChunkFactory::new_chunk(ChunkHelper::convert_schema(write_schema), 2);
        for (int row = 0; row < 2; ++row) {
            chunk->columns()[0]->as_mutable_ptr()->append_datum(Datum(row));
            if (historical) chunk->columns()[1]->as_mutable_ptr()->append_datum(Datum(600 + row));
            chunk->columns()[historical ? 2 : 1]->as_mutable_ptr()->append_datum(Datum(100 + row));
            chunk->columns()[historical ? 3 : 2]->as_mutable_ptr()->append_datum(Datum(900 + row));
        }
        ASSERT_OK(writer.append_chunk(*chunk));
        uint64_t size, index_size, footer_position;
        ASSERT_OK(writer.finalize(&size, &index_size, &footer_position));
        ASSIGN_OR_ABORT(auto input, fs->new_random_access_file(name));
        ASSIGN_OR_ABORT(auto bytes, input->read_all());
        ASSIGN_OR_ABORT(auto destination, fs->new_writable_file(output_name));
        SegmentStreamConverter converter(name, size, std::move(destination), &mapping, true, &dropped_uids);
        ASSERT_OK(converter.append(bytes.data(), bytes.size()));
        ASSERT_OK(converter.close());
        EXPECT_EQ(converter.final_output_file_size(), fs->get_file_size(output_name).value());
        ASSIGN_OR_ABORT(auto segment, Segment::open(fs, FileInfo{output_name}, 0, target_schema));
        SegmentReadOptions options;
        OlapReaderStatistics stats;
        options.fs = fs;
        options.stats = &stats;
        options.use_page_cache = false;
        ASSIGN_OR_ABORT(auto iterator, segment->new_iterator(read_schema, options));
        auto result = ChunkFactory::new_chunk(read_schema, 2);
        ASSERT_OK(iterator->get_next(result.get()));
        ASSERT_EQ(2, result->num_rows());
        for (int row = 0; row < 2; ++row) {
            EXPECT_EQ(row, result->get(row)[0].get_int32());
            EXPECT_EQ(100 + row, result->get(row)[1].get_int32());
            EXPECT_EQ(historical ? 7 : 900 + row, result->get(row)[2].get_int32());
        }
        iterator->close();

        // B now becomes a source: normalize its first snapshot, then add another
        // nullable scalar column while preserving the first-hop physical segments.
        // Its historical schemas no longer describe A's dropped column, so that
        // column's footer entry must have been removed by the first conversion.
        TabletMetadataPB second_source = target;
        auto* second_source_rowset = second_source.add_rowsets();
        second_source_rowset->set_id(1);
        second_source_rowset->add_segment_metas()->set_filename(output_name);
        lake::normalize_replication_rowset_schemas(&second_source);
        second_source.mutable_schema()->set_id(902103);
        auto* added = second_source.mutable_schema()->add_column();
        added->set_unique_id(3);
        added->set_name("added_again");
        added->set_type("INT");
        added->set_is_key(false);
        added->set_is_nullable(true);
        TabletMetadataPB second_target;
        second_target.mutable_schema()->CopyFrom(second_source.schema());
        second_target.mutable_schema()->set_id(902104);
        for (int i = 1; i < second_target.schema().column_size(); ++i) {
            second_target.mutable_schema()->mutable_column(i)->set_unique_id(9 + i);
        }
        std::unordered_map<uint32_t, uint32_t> second_mapping;
        std::unordered_set<uint32_t> second_dropped_uids;
        ASSERT_OK(lake::build_light_schema_change_uid_map(second_source, second_target, &second_mapping,
                                                          &second_dropped_uids));
        EXPECT_TRUE(second_dropped_uids.empty());
        ASSIGN_OR_ABORT(auto first_hop_input, fs->new_random_access_file(output_name));
        SegmentFooterPB first_hop_footer;
        size_t footer_hint = 4096;
        ASSERT_OK(Segment::parse_segment_footer(first_hop_input.get(), &first_hop_footer, &footer_hint, nullptr)
                          .status());
        ASSERT_EQ(historical ? 2 : 3, first_hop_footer.columns_size());
        EXPECT_EQ(0, first_hop_footer.columns(0).column_id());
        EXPECT_EQ(historical ? 2 : 1, first_hop_footer.columns(1).column_id());
        ASSIGN_OR_ABORT(auto first_hop_bytes, first_hop_input->read_all());
        const auto second_output_name = name + ".second-hop";
        ASSIGN_OR_ABORT(auto second_output, fs->new_writable_file(second_output_name));
        SegmentStreamConverter second_converter(output_name, first_hop_bytes.size(), std::move(second_output),
                                                &second_mapping, true, &second_dropped_uids);
        ASSERT_OK(second_converter.append(first_hop_bytes.data(), first_hop_bytes.size()));
        ASSERT_OK(second_converter.close());
        auto second_schema = TabletSchema::create(second_target.schema());
        auto second_read_schema = ChunkHelper::convert_schema(second_schema);
        ASSIGN_OR_ABORT(auto second_segment, Segment::open(fs, FileInfo{second_output_name}, 0, second_schema));
        ASSIGN_OR_ABORT(auto second_iterator, second_segment->new_iterator(second_read_schema, options));
        auto second_result = ChunkFactory::new_chunk(second_read_schema, 2);
        ASSERT_OK(second_iterator->get_next(second_result.get()));
        ASSERT_EQ(2, second_result->num_rows());
        for (int row = 0; row < 2; ++row) {
            EXPECT_EQ(row, second_result->get(row)[0].get_int32());
            EXPECT_EQ(100 + row, second_result->get(row)[1].get_int32());
            EXPECT_EQ(historical ? 7 : 900 + row, second_result->get(row)[2].get_int32());
            EXPECT_TRUE(second_result->get(row)[3].is_null());
        }
        second_iterator->close();
        auto incomplete = mapping;
        incomplete.erase(2);
        ASSIGN_OR_ABORT(auto bad_output, fs->new_writable_file(name + ".invalid"));
        SegmentStreamConverter invalid(name, size, std::move(bad_output), &incomplete, true, &dropped_uids);
        ASSERT_OK(invalid.append(bytes.data(), bytes.size()));
        EXPECT_TRUE(invalid.close().is_corruption());
    }
}

} // namespace starrocks
