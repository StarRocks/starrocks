// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/tools/meta_tool.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gflags/gflags.h>

#include <iostream>
#include <string>

#include "column/column.h"
#include "common/status.h"
#include "env/env.h"
#include "gen_cpp/segment.pb.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/substitute.h"
#include "json2pb/pb_to_json.h"
#include "runtime/memory/chunk_allocator.h"
#include "runtime/vectorized/time_types.h"
#include "storage/fs/block_manager.h"
#include "storage/fs/fs_util.h"
#include "storage/key_coder.h"
#include "storage/rowset/page_handle.h"
#include "storage/rowset/segment.h"
#include "storage/rowset/vectorized/segment_options.h"
#include "storage/tablet_meta.h"
#include "storage/vectorized/chunk_helper.h"
#include "util/block_compression.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace starrocks {

DEFINE_string(operation, "get_meta", "valid operation: flag");
DEFINE_string(file, "", "segment file path");

std::string get_usage(const std::string& progname) {
    std::stringstream ss;
    ss << progname << " is the StarRocks BE Meta tool.\n";
    ss << "Usage:\n";

    ss << "./meta_tool --operation=show_segment_footer --file=/path/to/segment/file\n";
    ss << "./meta_tool --operation=dump_data --file=/path/to/segment/file\n";
    ss << "./meta_tool --operation=dump_data2 --file=/path/to/segment/file\n";

    ss << "./meta_tool --operation=check_row_count --file=/path/to/segment/file\n";
    ss << "./meta_tool --operation=check_short_key_index --file=/path/to/segment/file\n";
    ss << "./meta_tool --operation=check_row_data --file=/path/to/segment/file\n";
    ss << "./meta_tool --operation=check_page --file=/path/to/segment/file\n";
    ss << "./meta_tool --operation=check_search --file=/path/to/segment/file\n";
    ss << "./meta_tool --operation=check_varchar_length --file=/path/to/segment/file\n";
    ss << "./meta_tool --operation=check_null --file=/path/to/segment/file\n";

    return ss.str();
}

Status get_segment_footer(RandomAccessFile* input_file, SegmentFooterPB* footer) {
    // Footer := SegmentFooterPB, FooterPBSize(4), FooterPBChecksum(4), MagicNumber(4)
    const std::string& file_name = input_file->filename();
    uint64_t file_size;
    RETURN_IF_ERROR(input_file->size(&file_size));

    if (file_size < 12) {
        return Status::Corruption(strings::Substitute("Bad segment file $0: file size $1 < 12", file_name, file_size));
    }

    uint8_t fixed_buf[12];
    RETURN_IF_ERROR(input_file->read_at_fully(file_size - 12, fixed_buf, 12));

    // validate magic number
    const char* k_segment_magic = "D0R1";
    const uint32_t k_segment_magic_length = 4;
    if (memcmp(fixed_buf + 8, k_segment_magic, k_segment_magic_length) != 0) {
        return Status::Corruption(strings::Substitute("Bad segment file $0: magic number not match", file_name));
    }

    // read footer PB
    uint32_t footer_length = decode_fixed32_le(fixed_buf);
    if (file_size < 12 + footer_length) {
        return Status::Corruption(strings::Substitute("Bad segment file $0: file size $1 < $2", file_name, file_size,
                                                      12 + footer_length));
    }
    std::string footer_buf;
    footer_buf.resize(footer_length);
    RETURN_IF_ERROR(input_file->read_at_fully(file_size - 12 - footer_length, footer_buf.data(), footer_buf.size()));

    // validate footer PB's checksum
    uint32_t expect_checksum = decode_fixed32_le(fixed_buf + 4);
    uint32_t actual_checksum = crc32c::Value(footer_buf.data(), footer_buf.size());
    if (actual_checksum != expect_checksum) {
        return Status::Corruption(
                strings::Substitute("Bad segment file $0: footer checksum not match, actual=$1 vs expect=$2", file_name,
                                    actual_checksum, expect_checksum));
    }

    // deserialize footer PB
    if (!footer->ParseFromString(footer_buf)) {
        return Status::Corruption(
                strings::Substitute("Bad segment file $0: failed to parse SegmentFooterPB", file_name));
    }
    return Status::OK();
}

struct ReadFileOpt {
    RandomAccessFile* file = nullptr;
    BlockCompressionCodec* codec = nullptr;
    EncodingTypePB encoding_type = UNKNOWN_ENCODING;
    int64_t offset = 0; // file offset
    int64_t size = 0;   // read size
};

void show_segment_footer(const std::string& file_name) {
    auto res = Env::Default()->new_random_access_file(file_name);
    if (!res.ok()) {
        std::cout << "open file failed: " << res.status() << std::endl;
        return;
    }
    auto input_file = std::move(res).value();
    SegmentFooterPB footer;
    auto status = get_segment_footer(input_file.get(), &footer);
    if (!status.ok()) {
        std::cout << "get footer failed: " << status.to_string() << std::endl;
        return;
    }
    std::string json_footer;
    json2pb::Pb2JsonOptions json_options;
    json_options.pretty_json = true;
    bool ret = json2pb::ProtoMessageToJson(footer, &json_footer, json_options);
    if (!ret) {
        std::cout << "Convert PB to json failed" << std::endl;
        return;
    }
    std::cout << json_footer << std::endl;
}

TabletSchemaPB create_tablet_schema() {
    TabletSchemaPB tablet_schema;

    ColumnPB* col1 = tablet_schema.add_column();
    col1->set_name("user_id");
    col1->set_type(type_to_string(PrimitiveType::TYPE_VARCHAR));
    col1->set_length(64);
    col1->set_is_key(true);
    col1->set_is_nullable(false);
    col1->set_unique_id(0);

    ColumnPB* col2 = tablet_schema.add_column();
    col2->set_name("enterprise_id");
    col2->set_type(type_to_string(PrimitiveType::TYPE_VARCHAR));
    col2->set_length(64);
    col2->set_is_key(true);
    col2->set_is_nullable(false);
    col2->set_unique_id(1);

    ColumnPB* col3 = tablet_schema.add_column();
    col3->set_name("stat_date");
    col3->set_type(type_to_string(PrimitiveType::TYPE_DATE));
    col3->set_is_key(true);
    col3->set_is_nullable(true);
    col3->set_unique_id(2);

    tablet_schema.set_num_short_key_columns(1);
    tablet_schema.set_keys_type(DUP_KEYS);
    tablet_schema.set_num_rows_per_row_block(1024);

    return tablet_schema;
}

void check_row_count(const std::string& file_name) {
    fs::BlockManager* blk_mgr = fs::fs_util::block_mgr_for_tool();
    auto mem_tracker = std::make_unique<MemTracker>();

    // create schema
    TabletSchemaPB schema_pb = create_tablet_schema();
    auto schema = TabletSchema::create(mem_tracker.get(), schema_pb);
    auto read_schema = vectorized::ChunkHelper::convert_schema_to_format_v2(*schema);

    // open segment
    size_t footer_length_hint = 16 * 1024;
    auto ret = Segment::open(mem_tracker.get(), blk_mgr, file_name, 0, schema.get(), &footer_length_hint, nullptr);
    if (!ret.ok()) {
        std::cout << "Segment open failed: " << ret.status() << std::endl;
        return;
    }
    auto segment = ret.value();

    // meta_row_count
    int64_t meta_row_count = segment->num_rows();

    // real_row_count
    vectorized::SegmentReadOptions opts(blk_mgr);
    OlapReaderStatistics stats;
    opts.stats = &stats;
    auto res = segment->new_iterator(read_schema, opts);
    if (!res.ok()) {
        std::cout << "New segment iterator failed: " << res.ok() << std::endl;
        return;
    }
    auto iter = res.value();

    // output data
    int64_t data_row_count = 0;
    do {
        auto chunk = vectorized::ChunkHelper::new_chunk(read_schema, 0);
        Status st = res.value()->get_next(chunk.get());
        if (!st.ok()) {
            if (!st.is_end_of_file()) {
                std::cout << "get next chunk failed: " << st.to_string() << std::endl;
            }
            break;
        }
        if (chunk->num_rows() <= 0) {
            continue;
        }

        data_row_count += chunk->num_rows();
    } while (true);

    std::cout << "meta_row_count=" << meta_row_count << std::endl;
    std::cout << "data_row_count=" << data_row_count << std::endl;
    if (meta_row_count != data_row_count) {
        std::cout << "FAIL" << std::endl;
    } else {
        std::cout << "SUCCESS" << std::endl;
    }
}

void check_short_key_index(const std::string& file_name) {
    fs::BlockManager* blk_mgr = fs::fs_util::block_mgr_for_tool();
    auto mem_tracker = std::make_unique<MemTracker>();

    // create schema
    TabletSchemaPB schema_pb = create_tablet_schema();
    auto schema = TabletSchema::create(mem_tracker.get(), schema_pb);
    auto read_schema = vectorized::ChunkHelper::convert_schema_to_format_v2(*schema);

    // open segment
    size_t footer_length_hint = 16 * 1024;
    auto ret = Segment::open(mem_tracker.get(), blk_mgr, file_name, 0, schema.get(), &footer_length_hint, nullptr);
    if (!ret.ok()) {
        std::cout << "Segment open failed: " << ret.status() << std::endl;
        return;
    }
    auto segment = ret.value();

    // load short key index
    auto st = segment->_load_index(mem_tracker.get());
    if (!st.ok()) {
        std::cout << "Load index failed: " << st.to_string() << std::endl;
        return;
    }

    int64_t real_key_count = segment->_sk_index_decoder->num_items();
    int64_t expect_key_count = segment->num_rows() / 1024 + 1;
    std::cout << "REAL_SHORT_KEY_COUNT: " << segment->_sk_index_decoder->num_items() << std::endl;
    std::cout << "EXPECT_SHORT_KEY_COUNT: " << expect_key_count << std::endl;
    if (real_key_count == expect_key_count) {
        std::cout << "SHORT KEY COUNT CHECK SUCCESS" << std::endl;
    } else {
        std::cout << "SHORT KEY COUNT CHECK FAILED" << std::endl;
    }

    size_t num_keys = segment->_sk_index_decoder->num_items();
    for (size_t i = 1; i < num_keys; i++) {
        Slice pre_key = segment->_sk_index_decoder->key(i - 1);
        Slice cur_key = segment->_sk_index_decoder->key(i);

        bool check = pre_key <= cur_key;
        if (!check) {
            std::cout << "CHECK SORT KEY FAILED: " << i << ":" << pre_key << ":" << cur_key << std::endl;
            return;
        }
    }
    std::cout << "CHECK SORT KEY SUCCESS" << std::endl;
}

void dump_data2(const std::string& file_name) {
    fs::BlockManager* blk_mgr = fs::fs_util::block_mgr_for_tool();
    auto mem_tracker = std::make_unique<MemTracker>();

    // create schema
    TabletSchemaPB schema_pb = create_tablet_schema();
    auto schema = TabletSchema::create(mem_tracker.get(), schema_pb);
    auto read_schema = vectorized::ChunkHelper::convert_schema_to_format_v2(*schema);

    // open segment
    size_t footer_length_hint = 16 * 1024;
    auto ret = Segment::open(mem_tracker.get(), blk_mgr, file_name, 0, schema.get(), &footer_length_hint, nullptr);
    if (!ret.ok()) {
        std::cout << "Segment open failed: " << ret.status() << std::endl;
        return;
    }
    auto segment = ret.value();

    // new segment iterator
    vectorized::SegmentReadOptions opts(blk_mgr);
    OlapReaderStatistics stats;
    opts.stats = &stats;
    auto res = segment->new_iterator(read_schema, opts);
    if (!res.ok()) {
        std::cout << "New segment iterator failed: " << res.ok() << std::endl;
        return;
    }
    auto iter = res.value();

    // output data
    int64_t remain = 0;
    size_t page_index = 0;
    vectorized::ChunkPtr pre_chunk;
    vectorized::ChunkPtr cur_chunk;
    do {
        pre_chunk = cur_chunk;
        cur_chunk = vectorized::ChunkHelper::new_chunk(read_schema, 0);
        Status st = res.value()->get_next(cur_chunk.get());
        if (!st.ok()) {
            std::cout << "get next chunk failed: " << st.to_string() << std::endl;

            if (remain < pre_chunk->num_rows()) {
                std::cout << "CHUNK:" << page_index << ":" << pre_chunk->debug_row(pre_chunk->num_rows() - 1)
                          << std::endl;
            }
            break;
        }
        if (cur_chunk->num_rows() <= 0) {
            continue;
        }

        int64_t tmp_index = remain;
        int64_t num_rows = cur_chunk->num_rows();
        while (tmp_index < num_rows) {
            std::cout << "CHUNK:" << page_index << ":" << cur_chunk->debug_row(tmp_index) << std::endl;
            tmp_index += 1024;
            page_index++;
        }
        remain += num_rows - tmp_index;
    } while (true);
}

void dump_data(const std::string& file_name) {
    fs::BlockManager* blk_mgr = fs::fs_util::block_mgr_for_tool();
    auto mem_tracker = std::make_unique<MemTracker>();

    // create schema
    TabletSchemaPB schema_pb = create_tablet_schema();
    auto schema = TabletSchema::create(mem_tracker.get(), schema_pb);
    auto read_schema = vectorized::ChunkHelper::convert_schema_to_format_v2(*schema);

    // open segment
    size_t footer_length_hint = 16 * 1024;
    auto ret = Segment::open(mem_tracker.get(), blk_mgr, file_name, 0, schema.get(), &footer_length_hint, nullptr);
    if (!ret.ok()) {
        std::cout << "Segment open failed: " << ret.status() << std::endl;
        return;
    }
    auto segment = ret.value();

    // load short key index
    auto st = segment->_load_index(mem_tracker.get());
    if (!st.ok()) {
        std::cout << "Load index failed: " << st.to_string() << std::endl;
        return;
    }

    std::cout << "KEY_COUNT: " << segment->_sk_index_decoder->num_items() << std::endl;
    size_t num_keys = segment->_sk_index_decoder->num_items();
    for (size_t i = 0; i < num_keys; i++) {
        std::cout << "INDEX: " << i << ":" << segment->_sk_index_decoder->key(i).to_string() << std::endl;
    }
}

} // namespace starrocks

int meta_tool_main(int argc, char** argv) {
    starrocks::ChunkAllocator::init_instance(nullptr, 4096);
    starrocks::vectorized::date::init_date_cache();

    std::string usage = starrocks::get_usage(argv[0]);
    gflags::SetUsageMessage(usage);
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (starrocks::FLAGS_operation == "check_row_count") {
        starrocks::check_row_count(starrocks::FLAGS_file);
    } else if (starrocks::FLAGS_operation == "check_short_key_index") {
        starrocks::check_short_key_index(starrocks::FLAGS_file);
    } else if (starrocks::FLAGS_operation == "dump_data") {
        starrocks::dump_data(starrocks::FLAGS_file);
    } else if (starrocks::FLAGS_operation == "dump_data2") {
        starrocks::dump_data2(starrocks::FLAGS_file);
    } else if (starrocks::FLAGS_operation == "show_segment_footer") {
        if (starrocks::FLAGS_file.empty()) {
            std::cout << "no file flag for show dict" << std::endl;
            return -1;
        }
        starrocks::show_segment_footer(starrocks::FLAGS_file);
    } else {
        std::cout << "do nothing" << std::endl;
        return 0;
    }
    gflags::ShutDownCommandLineFlags();
    return 0;
}