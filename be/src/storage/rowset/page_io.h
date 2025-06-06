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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/page_io.h

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

#pragma once

#include <vector>

#include "common/logging.h"
#include "common/status.h"
#include "gen_cpp/segment.pb.h"
#include "io/seekable_input_stream.h"
#include "storage/rowset/page_handle.h"
#include "storage/rowset/page_pointer.h"
#include "util/slice.h"
namespace starrocks {

class BlockCompressionCodec;
class RandomAccessFile;
class WritableFile;
struct OlapReaderStatistics;

struct PageReadOptions {
    // block to read page
    //RandomAccessFile* read_file = nullptr;
    io::SeekableInputStream* read_file = nullptr;
    // location of the page
    PagePointer page_pointer;
    // decompressor for page body (null means page body is not compressed)
    const BlockCompressionCodec* codec = nullptr;
    // used to collect IO metrics
    OlapReaderStatistics* stats = nullptr;
    // whether to verify page checksum
    bool verify_checksum = true;
    // whether to use page cache in read path
    bool use_page_cache = true;
    // page encoding type
    EncodingTypePB encoding_type = UNKNOWN_ENCODING;

    void sanity_check() const {
        CHECK_NOTNULL(read_file);
        CHECK_NOTNULL(stats);
    }
};

// Utility class for read and write page. All types of page share the same general layout:
//     Page := PageBody, PageFooter, FooterSize(4), Checksum(4)
//     - PageBody is defined by page type and may be compressed
//     - PageFooter is serialized PageFooterPB. It contains page_type, uncompressed_body_size,
//       and other custom metadata. PageBody is not compressed when its size is equal to
//       uncompressed_body_size
//     - FooterSize stores the size of PageFooter
//     - Checksum is the crc32c checksum of all previous part
class PageIO {
public:
    // Compress `body' using `codec' into `compressed_body'.
    // The size of returned `compressed_body' is 0 when the body is not compressed, this
    // could happen when `codec' is null or space saving is less than `min_space_saving'.
    static Status compress_page_body(const BlockCompressionCodec* codec, double min_space_saving,
                                     const std::vector<Slice>& body, faststring* compressed_body);

    // Encode page from `body' and `footer' and write to `file'.
    // `body' could be either uncompressed or compressed.
    // On success, the file pointer to the written page is stored in `result'.
    static Status write_page(WritableFile* wfile, const std::vector<Slice>& body, const PageFooterPB& footer,
                             PagePointer* result);

    // Convenient function to compress page body and write page in one go.
    static Status compress_and_write_page(const BlockCompressionCodec* codec, double min_space_saving,
                                          WritableFile* wfile, const std::vector<Slice>& body,
                                          const PageFooterPB& footer, PagePointer* result) {
        DCHECK_EQ(footer.uncompressed_size(), Slice::compute_total_size(body));
        faststring compressed_body;
        RETURN_IF_ERROR(compress_page_body(codec, min_space_saving, body, &compressed_body));
        if (compressed_body.size() == 0) { // uncompressed
            return write_page(wfile, body, footer, result);
        }
        return write_page(wfile, {Slice(compressed_body)}, footer, result);
    }

    // Read and parse a page according to `opts'.
    // On success
    //     `handle' holds the memory of page data,
    //     `body' points to page body,
    //     `footer' stores the page footer.
    static Status read_and_decompress_page(const PageReadOptions& opts, PageHandle* handle, Slice* body,
                                           PageFooterPB* footer);
};

} // namespace starrocks
