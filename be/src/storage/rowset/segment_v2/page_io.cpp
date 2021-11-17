// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/page_io.cpp

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

#include "storage/rowset/segment_v2/page_io.h"

#include <cstring>
#include <string>

#include "column/column.h"
#include "common/logging.h"
#include "env/env.h"
#include "gutil/strings/substitute.h"
#include "storage/fs/block_manager.h"
#include "storage/page_cache.h"
#include "util/block_compression.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/faststring.h"
#include "util/runtime_profile.h"
#include "util/scoped_cleanup.h"
#include "storage/rowset/segment_v2/bitshuffle_wrapper.h"

namespace starrocks::segment_v2 {

using strings::Substitute;

Status PageIO::compress_page_body(const BlockCompressionCodec* codec, double min_space_saving,
                                  const std::vector<Slice>& body, faststring* compressed_body) {
    size_t uncompressed_size = Slice::compute_total_size(body);
    auto cleanup = MakeScopedCleanup([&]() { compressed_body->clear(); });
    if (codec != nullptr && codec->exceed_max_input_size(uncompressed_size)) {
        compressed_body->clear();
        return Status::OK();
    }
    if (codec != nullptr && uncompressed_size > 0) {
        compressed_body->resize(codec->max_compressed_len(uncompressed_size));
        Slice compressed_slice(*compressed_body);
        RETURN_IF_ERROR(codec->compress(body, &compressed_slice));
        compressed_body->resize(compressed_slice.get_size());

        double space_saving = 1.0 - static_cast<double>(compressed_body->size()) / uncompressed_size;
        // return compressed body only when it saves more than min_space_saving
        if (space_saving > 0 && space_saving >= min_space_saving) {
            compressed_body->shrink_to_fit();
            cleanup.cancel();
            return Status::OK();
        }
    }
    return Status::OK();
}

Status PageIO::write_page(fs::WritableBlock* wblock, const std::vector<Slice>& body, const PageFooterPB& footer,
                          PagePointer* result) {
    // sanity check of page footer
    CHECK(footer.has_type()) << "type must be set";
    CHECK(footer.has_uncompressed_size()) << "uncompressed_size must be set";
    switch (footer.type()) {
    case DATA_PAGE:
        CHECK(footer.has_data_page_footer());
        break;
    case INDEX_PAGE:
        CHECK(footer.has_index_page_footer());
        break;
    case DICTIONARY_PAGE:
        CHECK(footer.has_dict_page_footer());
        break;
    case SHORT_KEY_PAGE:
        CHECK(footer.has_short_key_page_footer());
        break;
    default:
        CHECK(false) << "Invalid page footer type: " << footer.type();
        break;
    }

    std::string footer_buf; // serialized footer + footer size
    footer.SerializeToString(&footer_buf);
    put_fixed32_le(&footer_buf, static_cast<uint32_t>(footer_buf.size()));

    std::vector<Slice> page = body;
    page.emplace_back(footer_buf);

    // checksum
    uint8_t checksum_buf[sizeof(uint32_t)];
    uint32_t checksum = crc32c::Value(page);
    encode_fixed32_le(checksum_buf, checksum);
    page.emplace_back(checksum_buf, sizeof(uint32_t));

    uint64_t offset = wblock->bytes_appended();
    RETURN_IF_ERROR(wblock->appendv(&page[0], page.size()));

    result->offset = offset;
    result->size = wblock->bytes_appended() - offset;
    return Status::OK();
}

Status PageIO::read_and_decompress_page(const PageReadOptions& opts, PageHandle* handle, Slice* body,
                                        PageFooterPB* footer) {
    opts.sanity_check();
    opts.stats->total_pages_num++;

    auto cache = StoragePageCache::instance();
    PageCacheHandle cache_handle;
    StoragePageCache::CacheKey cache_key(opts.rblock->path(), opts.page_pointer.offset);
    if (opts.use_page_cache && cache->lookup(cache_key, &cache_handle)) {
        // we find page in cache, use it
        *handle = PageHandle(std::move(cache_handle));
        opts.stats->cached_pages_num++;
        // parse body and footer
        Slice page_slice = handle->data();
        uint32_t footer_size = decode_fixed32_le((uint8_t*)page_slice.data + page_slice.size - 4);
        std::string footer_buf(page_slice.data + page_slice.size - 4 - footer_size, footer_size);
        if (!footer->ParseFromString(footer_buf)) {
            return Status::Corruption("Bad page: invalid footer");
        }
        *body = Slice(page_slice.data, page_slice.size - 4 - footer_size);
        return Status::OK();
    }

    // every page contains 4 bytes footer length and 4 bytes checksum
    const uint32_t page_size = opts.page_pointer.size;
    if (page_size < 8) {
        return Status::Corruption(strings::Substitute("Bad page: too small size ($0)", page_size));
    }

    // hold compressed page at first, reset to decompressed page later
    // Allocate APPEND_OVERFLOW_MAX_SIZE more bytes to make append_strings_overflow work
    std::unique_ptr<char[]> page(new char[page_size + vectorized::Column::APPEND_OVERFLOW_MAX_SIZE]);
    Slice page_slice(page.get(), page_size);
    {
        SCOPED_RAW_TIMER(&opts.stats->io_ns);
        RETURN_IF_ERROR(opts.rblock->read(opts.page_pointer.offset, page_slice));
        opts.stats->compressed_bytes_read += page_size;
    }

    if (opts.verify_checksum) {
        uint32_t expect = decode_fixed32_le((uint8_t*)page_slice.data + page_slice.size - 4);
        uint32_t actual = crc32c::Value(page_slice.data, page_slice.size - 4);
        if (expect != actual) {
            return Status::Corruption(
                    strings::Substitute("Bad page: checksum mismatch (actual=$0 vs expect=$1)", actual, expect));
        }
    }

    // remove checksum suffix
    page_slice.size -= 4;
    // parse and set footer
    uint32_t footer_size = decode_fixed32_le((uint8_t*)page_slice.data + page_slice.size - 4);
    if (!footer->ParseFromArray(page_slice.data + page_slice.size - 4 - footer_size, footer_size)) {
        return Status::Corruption("Bad page: invalid footer");
    }

    uint32_t body_size = page_slice.size - 4 - footer_size;
    if (body_size != footer->uncompressed_size()) { // need decompress body
        if (opts.codec == nullptr) {
            return Status::Corruption("Bad page: page is compressed but codec is NO_COMPRESSION");
        }
        SCOPED_RAW_TIMER(&opts.stats->decompress_ns);
        // Allocate APPEND_OVERFLOW_MAX_SIZE more bytes to make append_strings_overflow work
        std::unique_ptr<char[]> decompressed_page(
                new char[footer->uncompressed_size() + footer_size + 4 + vectorized::Column::APPEND_OVERFLOW_MAX_SIZE]);

        // decompress page body
        Slice compressed_body(page_slice.data, body_size);
        Slice decompressed_body(decompressed_page.get(), footer->uncompressed_size());
        RETURN_IF_ERROR(opts.codec->decompress(compressed_body, &decompressed_body));
        if (decompressed_body.size != footer->uncompressed_size()) {
            return Status::Corruption(
                    strings::Substitute("Bad page: record uncompressed size=$0 vs real decompressed size=$1",
                                        footer->uncompressed_size(), decompressed_body.size));
        }
        // append footer and footer size
        memcpy(decompressed_body.data + decompressed_body.size, page_slice.data + body_size, footer_size + 4);
        // free memory of compressed page
        page = std::move(decompressed_page);
        page_slice = Slice(page.get(), footer->uncompressed_size() + footer_size + 4);
        opts.stats->uncompressed_bytes_read += page_slice.size;
    } else {
        opts.stats->uncompressed_bytes_read += body_size;
    }

    if (opts.use_page_cache && (opts.encoding_type == EncodingTypePB::DICT_ENCODING || opts.encoding_type == EncodingTypePB::BIT_SHUFFLE)) {
        size_t dict_header_size = 4;
        size_t bitshuffle_header_size = 16;
        size_t type = EncodingTypePB::BIT_SHUFFLE;
        if (opts.encoding_type == EncodingTypePB::DICT_ENCODING) {
            //LOG(INFO) << "page_silice size before decompress is " << page_slice.size;
            type = decode_fixed32_le((const uint8_t*)&page_slice.data[0]);
            //LOG(INFO) << "encode type is " << encoding_type << ", decoded type is " << type;
            if (type != EncodingTypePB::DICT_ENCODING && type != EncodingTypePB::PLAIN_ENCODING) {
                return Status::InternalError(strings::Substitute("error encoding type, expected is:$0, actual is:$1", DICT_ENCODING, type));
            }
        } else  {
            dict_header_size = 0;
        }

        if (type == opts.encoding_type) {
            size_t num_elements = decode_fixed32_le((const uint8_t*)&page_slice[0 + dict_header_size]);
            size_t compressed_size = decode_fixed32_le((const uint8_t*)&page_slice[4 + dict_header_size]);
            size_t num_element_after_padding = decode_fixed32_le((const uint8_t*)&page_slice[8 + dict_header_size]);
            size_t size_of_element = decode_fixed32_le((const uint8_t*)&page_slice[12 + dict_header_size]);
            //LOG(INFO) << "num_elements is " << num_elements << ", compressed_size is " << compressed_size << ", num_element_after_padding is " << num_element_after_padding << ", size_of_element is " << size_of_element;
        
            size_t header_size = dict_header_size + bitshuffle_header_size;
            size_t data_size = num_element_after_padding * size_of_element;
            size_t null_size = footer->data_page_footer().nullmap_size();

            std::unique_ptr<char[]> decompressed_page(
                    new char[header_size + data_size + null_size + footer_size + 4 + vectorized::Column::APPEND_OVERFLOW_MAX_SIZE]);
            // append dict_Header and bitshuffle_header
            memcpy(decompressed_page.get(), page_slice.data, header_size);
            Slice compressed_body(page_slice.data + header_size, page_slice.size - 4 - footer_size - header_size - null_size);
            Slice decompressed_body(&(decompressed_page.get()[header_size]), data_size);
            {
                SCOPED_RAW_TIMER(&opts.stats->decompress_ns);
                int64_t bytes = bitshuffle::decompress_lz4(compressed_body.data, decompressed_body.data,
                                               num_element_after_padding, size_of_element, 0);
                if (bytes != compressed_body.size) {
                    return Status::Corruption(
                        strings::Substitute("decompress failed: expected number of bytes consumed=&0 vs real consumed=$1",
                                            compressed_body.size, bytes));
                }
            }
            // append null_flag and footer and footer size
            memcpy(decompressed_body.data + decompressed_body.size, page_slice.data + body_size - null_size, null_size + footer_size + 4);
            // free memory of compressed page
            page = std::move(decompressed_page);
            page_slice = Slice(page.get(), header_size + data_size + null_size + footer_size + 4);
        }
    }

    *body = Slice(page_slice.data, page_slice.size - 4 - footer_size);
    if (opts.use_page_cache) {
        // insert this page into cache and return the cache handle
        cache->insert(cache_key, page_slice, &cache_handle, opts.kept_in_memory);
        *handle = PageHandle(std::move(cache_handle));
    } else {
        *handle = PageHandle(page_slice);
    }
    page.release(); // memory now managed by handle
    return Status::OK();
}

} // namespace starrocks::segment_v2
