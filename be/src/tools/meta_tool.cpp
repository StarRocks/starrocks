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
#include "storage/key_coder.h"
#include "storage/rowset/page_handle.h"
#include "storage/rowset/storage_page_decoder.h"
#include "storage/short_key_index.h"
#include "storage/tablet_meta.h"
#include "util/block_compression.h"
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

struct ReadFileOpt {
    RandomAccessFile* file = nullptr;
    BlockCompressionCodec* codec = nullptr;
    EncodingTypePB encoding_type = UNKNOWN_ENCODING;
    int64_t offset = 0; // file offset
    int64_t size = 0;   // read size
};

Status read_page(ReadFileOpt* opt, Slice* body, PageFooterPB* footer, PageHandle* handle) {
    // Read page
    std::unique_ptr<char[]> page(new char[opt->size + vectorized::Column::APPEND_OVERFLOW_MAX_SIZE]);
    Slice page_slice(page.get(), opt->size);
    auto res = opt->file->read_at_fully(opt->offset, page.get(), opt->size);
    if (!res.ok()) {
        std::cout << "Read page failed: " << res << std::endl;
        return res;
    }

    // Parse page footer size
    page_slice.size -= 4;
    uint32_t footer_size = decode_fixed32_le((uint8_t*)page.get() + page_slice.size - 4);

    // Parse page footer
    if (!footer->ParseFromArray(page.get() + page_slice.size - 4 - footer_size, static_cast<int>(footer_size))) {
        return Status::Corruption("Parse page footer failed");
    }

    // Decompress page
    uint32_t body_size = page_slice.size - 4 - footer_size;
    if (body_size != footer->uncompressed_size()) {
        std::unique_ptr<char[]> decompressed_page(
                new char[footer->uncompressed_size() + footer_size + 4 + vectorized::Column::APPEND_OVERFLOW_MAX_SIZE]);

        Slice compressed_body(page.get(), body_size);
        Slice decompressed_body(decompressed_page.get(), footer->uncompressed_size());
        RETURN_IF_ERROR(opt->codec->decompress(compressed_body, &decompressed_body));
        if (decompressed_body.size != footer->uncompressed_size()) {
            return Status::Corruption("decompress failed");
        }
        memcpy(decompressed_body.data + decompressed_body.size, page.get() + body_size, footer_size + 4);
        page = std::move(decompressed_page);
        page_slice = Slice(page.get(), footer->uncompressed_size() + footer_size + 4);
    }

    // Decode page
    RETURN_IF_ERROR(StoragePageDecoder::decode_page(footer, footer_size + 4, opt->encoding_type, &page, &page_slice));
    *body = Slice(page_slice.data, page_slice.size - 4 - footer_size);
    *handle = PageHandle(page_slice);
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

    ReadFileOpt opt;
    opt.file = input_file.get();
    opt.codec = nullptr;
    opt.encoding_type = UNKNOWN_ENCODING;
    opt.offset = static_cast<int64_t>(page.offset());
    opt.size = page.size();

    Slice page_body;
    PageFooterPB page_footer;
    PageHandle page_handle;

    st = read_page(&opt, &page_body, &page_footer, &page_handle);
    if (!st.ok()) {
        std::cout << "Read page failed: " << st << std::endl;
        return;
    }

    auto sk_index_decode = std::make_unique<ShortKeyIndexDecoder>();
    st = sk_index_decode->parse(page_body, page_footer.short_key_page_footer());
    if (!st.ok()) {
        std::cout << "Short key page decode failed: " << st.to_string() << std::endl;
        return;
    } else {
        std::cout << "Decode ShortKeyIndex success" << std::endl;
    }

    std::cout << "KEY_COUNT: " << sk_index_decode->num_items() << std::endl;
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