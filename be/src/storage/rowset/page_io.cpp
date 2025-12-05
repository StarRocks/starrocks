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

#include "storage/rowset/page_io.h"

#include <cstring>
#include <memory>
#include <string>
#include <string_view>

#include "cache/mem_cache/page_cache.h"
#include "column/column.h"
#include "common/logging.h"
#include "common/status.h"
#include "fs/fs.h"
#include "fs/fs_starlet.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "runtime/raw_container_checked.h"
#include "storage/rowset/storage_page_decoder.h"
#include "util/coding.h"
#include "util/compression/block_compression.h"
#include "util/crc32c.h"
#include "util/faststring.h"
#include "util/runtime_profile.h"
#include "util/scoped_cleanup.h"

namespace starrocks {

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
        if (use_compression_pool(codec->type())) {
            Slice compressed_slice;
            RETURN_IF_ERROR(
                    codec->compress(body, &compressed_slice, true, uncompressed_size, compressed_body, nullptr));
        } else {
            compressed_body->resize(codec->max_compressed_len(uncompressed_size));
            Slice compressed_slice(*compressed_body);
            RETURN_IF_ERROR(codec->compress(body, &compressed_slice));
            compressed_body->resize(compressed_slice.get_size());
        }
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

Status PageIO::write_page(WritableFile* wfile, const std::vector<Slice>& body, const PageFooterPB& footer,
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

    uint64_t offset = wfile->size();
    RETURN_IF_ERROR(wfile->appendv(&page[0], page.size()));

    result->offset = offset;
    result->size = wfile->size() - offset;
    return Status::OK();
}

// The unique key identifying entries in the page cache.
// Each cached page corresponds to a specific offset within
// a file.
//
// TODO(zc): Now we use file name(std::string) as a part of key,
//  which is not efficient. We should make it better later
std::string encode_cache_key(const std::string& fname, int64_t offset) {
    std::string str;
    str.reserve(fname.size() + sizeof(offset));
    str.append(fname);
    str.append((char*)&offset, sizeof(offset));
    return str;
}

#if defined(USE_STAROS) && !defined(BUILD_FORMAT_LIB)
Status drop_local_cache_data(const std::string& fname) {
    if (!config::lake_clear_corrupted_cache_data) {
        return Status::NotSupported("lake_clear_corrupted_cache_data is turned off");
    }
    if (!is_starlet_uri(fname)) {
        return Status::NotSupported("only support starlet file");
    }
    auto fs_or = FileSystem::CreateSharedFromString(fname);
    if (!fs_or.ok()) {
        LOG(INFO) << "clear corrupted cache for " << fname << ", error:" << fs_or.status();
        return fs_or.status();
    }
    auto s = (*fs_or)->drop_local_cache(fname);
    LOG(INFO) << "clear corrupted cache for " << fname << ", error:" << s;
    return s;
}
#endif

static Status parse_page_from_cache(PageHandle* handle, Slice* body, PageFooterPB* footer,
                                    PageCacheHandle* cache_handle, const PageReadOptions& opts) {
    *handle = PageHandle(std::move(*cache_handle));
    opts.stats->cached_pages_num++;

    const auto* page = handle->data();
    const uint32_t footer_length_offset = page->size() - 4;
    const uint32_t footer_size = decode_fixed32_le(page->data() + footer_length_offset);
    const uint32_t footer_offset = footer_length_offset - footer_size;

    std::string_view footer_buf(reinterpret_cast<const char*>(page->data() + footer_offset), footer_size);
    if (!footer->ParseFromArray(footer_buf.data(), footer_buf.size())) {
        return Status::Corruption(strings::Substitute("Bad page: invalid footer (cache), file=$0, footer_size=$1",
                                                      opts.read_file->filename(), footer_size));
    }
    *body = Slice(page->data(), footer_offset);
    return Status::OK();
}

static Status read_page_from_file(const PageReadOptions& opts, std::unique_ptr<std::vector<uint8_t>>* page_out) {
    // every page contains 4 bytes footer length and 4 bytes checksum
    const uint32_t page_size = opts.page_pointer.size;
    if (page_size < 8) {
        return Status::Corruption(
                strings::Substitute("Bad page: too small ($0), file($1)", page_size, opts.read_file->filename()));
    }

    auto page = std::make_unique<std::vector<uint8_t>>();
    // Allocate APPEND_OVERFLOW_MAX_SIZE more bytes to make append_strings_overflow work
    size_t reserve_size = page_size + Column::APPEND_OVERFLOW_MAX_SIZE;
    RETURN_IF_ERROR(raw::stl_vector_resize_uninitialized_checked(page.get(), reserve_size, page_size - 4));

    Slice slice(page->data(), page_size);

    {
        SCOPED_RAW_TIMER(&opts.stats->io_ns);
        RETURN_IF_ERROR(opts.read_file->read_at_fully(opts.page_pointer.offset, slice.data, slice.size));
        if (opts.read_file->is_cache_hit()) {
            ++opts.stats->pages_from_local_disk;
        }
        opts.stats->compressed_bytes_read_request += page_size;
        ++opts.stats->io_count_request;
    }

    *page_out = std::move(page);
    return Status::OK();
}
// Verify checksum and parse footer from page slice
static StatusOr<uint32_t> verify_and_parse_footer(Slice& page_slice, const PageReadOptions& opts,
                                                  PageFooterPB* footer) {
    if (opts.verify_checksum) {
        const uint32_t expect = decode_fixed32_le((uint8_t*)page_slice.data + page_slice.size - 4);
        const uint32_t actual = crc32c::Value(page_slice.data, page_slice.size - 4);
        if (expect != actual) {
            return Status::Corruption(
                    strings::Substitute("Bad page: checksum mismatch (actual=$0 vs expect=$1), file=$2", actual, expect,
                                        opts.read_file->filename()));
        }
    }

    // remove checksum suffix
    page_slice.size -= 4;

    // parse and set footer
    const uint32_t footer_size = decode_fixed32_le((uint8_t*)page_slice.data + page_slice.size - 4);
    if (!footer->ParseFromArray(page_slice.data + page_slice.size - 4 - footer_size, footer_size)) {
        return Status::Corruption(strings::Substitute("Bad page: invalid footer, file=$0, footer_size=$1",
                                                      opts.read_file->filename(), footer_size));
    }

    return footer_size;
}

static Status decompress_if_needed(const PageReadOptions& opts, const PageFooterPB* footer, uint32_t footer_size,
                                   std::unique_ptr<std::vector<uint8_t>>* page, Slice* page_slice) {
    const uint32_t body_size = page_slice->size - 4 - footer_size;

    if (body_size == footer->uncompressed_size()) {
        opts.stats->uncompressed_bytes_read += page_slice->size;
        return Status::OK();
    }

    // need decompress body
    if (!opts.codec) {
        return Status::Corruption(
                strings::Substitute("Bad page: compressed but codec=NONE, file=$0", opts.read_file->filename()));
    }

    SCOPED_RAW_TIMER(&opts.stats->decompress_ns);

    const uint32_t decompressed_size = footer->uncompressed_size();
    const uint32_t total_size = decompressed_size + footer_size + 4;
    auto decompressed = std::make_unique<std::vector<uint8_t>>();

    // Allocate APPEND_OVERFLOW_MAX_SIZE more bytes to make append_strings_overflow work
    size_t reserve_size = total_size + Column::APPEND_OVERFLOW_MAX_SIZE;
    RETURN_IF_ERROR(raw::stl_vector_resize_uninitialized_checked(decompressed.get(), reserve_size, total_size));

    Slice compressed_body(page_slice->data, body_size);
    Slice decompressed_body(decompressed->data(), decompressed_size);
    RETURN_IF_ERROR(opts.codec->decompress(compressed_body, &decompressed_body));

    if (decompressed_body.size != decompressed_size) {
        return Status::Corruption(strings::Substitute("Bad page: uncompressed size mismatch ($0 vs $1), file=$2",
                                                      decompressed_size, decompressed_body.size,
                                                      opts.read_file->filename()));
    }

    memcpy(decompressed_body.data + decompressed_body.size, page_slice->data + body_size, footer_size + 4);

    *page = std::move(decompressed);
    *page_slice = Slice((*page)->data(), total_size);
    opts.stats->uncompressed_bytes_read += total_size;

    return Status::OK();
}

Status insert_page_cache(bool cache_enabled, const PageReadOptions& opts, StoragePageCache* cache,
                         const std::string& cache_key, std::unique_ptr<std::vector<uint8_t>> page, PageHandle* handle) {
    // If cache is not enabled or use_page_cache is false, just return
    if (!cache_enabled || !opts.use_page_cache) {
        *handle = PageHandle(page.get());
        page.release();
        return Status::OK();
    }

    // Insert page into cache
    PageCacheHandle cache_handle;
    MemCacheWriteOptions wopts;
    Status st = cache->insert(cache_key, page.get(), wopts, &cache_handle);

    // Return page handle from cache if insert succeed, otherwise return page handle from memory
    *handle = st.ok() ? PageHandle(std::move(cache_handle)) : PageHandle(page.get());
    page.release();
    return Status::OK();
}

static Status read_and_decompress_page_internal(const PageReadOptions& opts, PageHandle* handle, Slice* body,
                                                PageFooterPB* footer) {
    // the function will be used by query or load, current load is not allowed to fail when memory reach the limit,
    // so don't check when tls_thread_state.check is set to false
    CHECK_MEM_LIMIT("read and decompress page");

    opts.sanity_check();
    opts.stats->total_pages_num++;

    auto cache = StoragePageCache::instance();
#ifdef __APPLE__
    bool page_cache_available = false;
#else
    bool page_cache_available = (cache != nullptr) && cache->available();
#endif
    PageCacheHandle cache_handle;
    std::string cache_key = encode_cache_key(opts.read_file->filename(), opts.page_pointer.offset);
    if (opts.use_page_cache && page_cache_available && cache->lookup(cache_key, &cache_handle)) {
        return parse_page_from_cache(handle, body, footer, &cache_handle, opts);
    }

    // hold compressed page at first, reset to decompressed page later
    std::unique_ptr<std::vector<uint8_t>> page;
    // not found from sr page cache. try to read from file stream
    RETURN_IF_ERROR(read_page_from_file(opts, &page));

    Slice page_slice(page->data(), opts.page_pointer.size);
    ASSIGN_OR_RETURN(uint32_t footer_size, verify_and_parse_footer(page_slice, opts, footer));

    RETURN_IF_ERROR(decompress_if_needed(opts, footer, footer_size, &page, &page_slice));

    RETURN_IF_ERROR(StoragePageDecoder::decode_page(footer, footer_size + 4, opts.encoding_type, &page, &page_slice));

    *body = Slice(page_slice.data, page_slice.size - 4 - footer_size);
    RETURN_IF_ERROR(insert_page_cache(page_cache_available, opts, cache, cache_key, std::move(page), handle));

    return Status::OK();
}

Status PageIO::read_and_decompress_page(const PageReadOptions& opts, PageHandle* handle, Slice* body,
                                        PageFooterPB* footer) {
    Status s = read_and_decompress_page_internal(opts, handle, body, footer);
    if (s.ok()) {
        return s;
    }
#if defined(USE_STAROS) && !defined(BUILD_FORMAT_LIB)
    if (s.is_corruption()) {
        auto drop_status = drop_local_cache_data(opts.read_file->filename());
        if (!drop_status.ok()) {
            return s; // return first read error
        }
        return read_and_decompress_page_internal(opts, handle, body, footer);
    } else {
        return s;
    }
#else
    return s;
#endif
}

} // namespace starrocks
