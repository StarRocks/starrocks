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
#include "storage/rowset/binary_plain_page.h"
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

void dump_data(const std::string& file_name) {
    auto res = Env::Default()->new_random_access_file(file_name);

    /*
    size_t file_size = std::filesystem::file_size(path);
    size_t footer_offset = file_size - 12;
    uint32_t footer_length;
    uint32_t checksum;
    uint32_t magic_number;

    std::ifstream fs(path, std::ios::in);
    if (!fs.is_open()) {
        std::cout << "open failed: " << path << std::endl;
        return;
    }

    char footer_header_buf[12];
    fs.seekg(static_cast<long>(footer_offset), std::ios::beg);
    fs.read(footer_header_buf, 12);

    footer_length = UNALIGNED_LOAD32(footer_header_buf);
    checksum = UNALIGNED_LOAD32(footer_header_buf + 4);
    magic_number = UNALIGNED_LOAD32(footer_header_buf + 8);

    std::cout<<"RESULT: "<< footer_length <<":"<<checksum<<":"<<magic_number<<std::endl;
    */
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
} // namespace starrocks

int meta_tool_main(int argc, char** argv) {
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