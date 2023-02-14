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

#include "common/status.h"
#include "env/env.h"
#include "gen_cpp/segment.pb.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/substitute.h"
#include "json2pb/pb_to_json.h"
#include "runtime/memory/chunk_allocator.h"
#include "storage/key_coder.h"
#include "storage/rowset/binary_plain_page.h"
#include "storage/short_key_index.h"
#include "storage/tablet_meta.h"
#include "storage/tablet_schema_map.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace starrocks {

DEFINE_string(operation, "get_meta", "valid operation: flag");
DEFINE_string(file, "", "segment file path");

std::string get_usage(const std::string& progname) {
    std::stringstream ss;
    ss << progname << " is the StarRocks BE Meta tool.\n";
    ss << "Stop BE first before use this tool.\n";
    ss << "Usage:\n";
    ss << "./meta_tool --operation=show_segment_footer --file=/path/to/segment/file\n";
    ss << "./meta_tool --operation=dump_data --file=/path/to/segment/file\n";
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

Status read_page(RandomAccessFile* file, size_t offset, size_t size, Slice* body, PageFooterPB* footer) {
    char* page_mem = new char[size];
    auto res = file->read_at_fully(offset, page_mem, size);
    if (!res.ok()) {
        std::cout << "Read page failed: " << res.to_string() << std::endl;
        return res;
    }
    size -= 4;
}

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

void dump_data(const std::string& file_name) {
    using VarcharDecode = KeyCoderTraits<OLAP_FIELD_TYPE_VARCHAR>;

    auto res = Env::Default()->new_random_access_file(file_name);
    if (!res.ok()) {
        std::cout << "open file failed: " << res.status() << std::endl;
        return;
    }

    auto input_file = std::move(res).value();
    SegmentFooterPB footer;
    auto st = get_segment_footer(input_file.get(), &footer);
    if (!st.ok()) {
        std::cout << "get segment footer failed: " << st.to_string() << std::endl;
        return;
    }

    const PagePointerPB& page = footer.short_key_index_page();
    std::cout << "OFFSET: " << page.offset() << ":" << page.size() << std::endl;
    char* sk_ptr = new char[page.size()];

    st = input_file->read_at_fully(static_cast<int64_t>(page.offset()), sk_ptr, page.size());
    if (!st.ok()) {
        std::cout << "read short key index failed: " << st.to_string() << std::endl;
        return;
    }
    Slice sk_page{sk_ptr, page.size()};

    uint32_t expect = decode_fixed32_le((uint8_t*)sk_page.data + sk_page.size - 4);
    uint32_t actual = crc32c::Value(sk_page.data, sk_page.size - 4);
    if (expect != actual) {
        std::cout << "invalid checksum" << std::endl;
        return;
    } else {
        std::cout << "checksum success" << std::endl;
    }

    PageFooterPB page_footer;
    sk_page.size = sk_page.size - 4;
    uint32_t footer_size = decode_fixed32_le((uint8_t*)sk_page.data + sk_page.size - 4);
    std::cout << "Short key footer size: " << footer_size << std::endl;

    bool succ =
            page_footer.ParseFromArray(sk_page.data + sk_page.size - 4 - footer_size, static_cast<int>(footer_size));
    if (!succ) {
        std::cout << "Parse page footer failed" << std::endl;
        return;
    }

    sk_page.size = sk_page.size - 4 - footer_size;
    auto sk_index_decode = std::make_unique<ShortKeyIndexDecoder>();
    st = sk_index_decode->parse(sk_page, page_footer.short_key_page_footer());
    if (!st.ok()) {
        std::cout << "Short key page decode failed: " << st.to_string() << std::endl;
        return;
    } else {
        std::cout << "Decode ShortKeyIndex success" << std::endl;
    }

    std::cout << "KEY_COUNT: " << sk_index_decode->num_items() << std::endl;

    MemPool mem_pool;
    Slice key1;
    Slice key2;
    for (size_t i = 0; i < sk_index_decode->num_items(); i++) {
        Slice key = sk_index_decode->key(static_cast<ssize_t>(i));
        key1 = key2;
        st = VarcharDecode::decode_ascending(&key, 1024 * 1024, reinterpret_cast<uint8_t*>(&key2), &mem_pool);
        if (!st.ok()) {
            std::cout << "Decode short key failed" << std::endl;
            return;
        } else {
            std::cout << "RESULT:" << key2.to_string() << std::endl;
            if (i != 0 && key2 < key1) {
                std::cout << "CHECK FAILED: " << key1 << ":" << key2 << std::endl;
                return;
            }
        }
    }

    delete[] sk_ptr;
}

} // namespace starrocks

int meta_tool_main(int argc, char** argv) {
    starrocks::ChunkAllocator::init_instance(nullptr, 4096);

    std::string usage = starrocks::get_usage(argv[0]);
    gflags::SetUsageMessage(usage);
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (starrocks::FLAGS_operation == "dump_data") {
        starrocks::dump_data(starrocks::FLAGS_file);
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