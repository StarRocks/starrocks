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

#include "formats/iceberg/iceberg_deletion_vector_reader.h"

#include <cstring>
#include <vector>

#include "cache/scan/cache_input_stream.h"
#include "cache/scan/shared_buffered_input_stream.h"
#include "common/runtime_profile.h"
#include "formats/deletion_bitmap.h"
#include "formats/file_input_stream.h"
#include "formats/puffin/deletion_vector_format.h"
#include "fs/fs.h"
#include "gutil/endian.h"
#include "gutil/strings/substitute.h"

namespace starrocks::formats {

StatusOr<roaring64_bitmap_t*> IcebergDeletionVectorReader::parse_dv_blob(const uint8_t* data, int64_t size,
                                                                         int64_t record_count,
                                                                         IcebergDVBuildStats* stats) {
    const int64_t min_size = kDvLengthPrefixBytes + kDvMagicBytes + kDvCrcBytes;
    if (size < min_size) {
        return Status::Corruption(strings::Substitute("Iceberg DV blob too small: $0 bytes", size));
    }

    // 1) length prefix == size - 8 (length covers magic + body, excludes the 4B prefix and 4B crc).
    uint32_t length = BigEndian::Load32(data);
    if (static_cast<int64_t>(length) != size - kDvLengthPrefixBytes - kDvCrcBytes) {
        return Status::Corruption(strings::Substitute("Iceberg DV length prefix mismatch: prefix=$0 expected=$1",
                                                      length, size - kDvLengthPrefixBytes - kDvCrcBytes));
    }

    // 2) magic.
    if (memcmp(data + kDvLengthPrefixBytes, kDvBlobMagic, kDvMagicBytes) != 0) {
        return Status::Corruption("Iceberg DV magic mismatch");
    }

    // 3) crc32 over magic + body == trailing crc.
    {
        int64_t crc_ns = 0;
        {
            SCOPED_RAW_TIMER(&crc_ns);
            uint32_t crc_expected = BigEndian::Load32(data + size - kDvCrcBytes);
            uint32_t crc = dv_blob_crc32(data + kDvLengthPrefixBytes, size - kDvLengthPrefixBytes - kDvCrcBytes);
            if (crc != crc_expected) {
                return Status::Corruption(
                        strings::Substitute("Iceberg DV crc mismatch: actual=$0 expected=$1", crc, crc_expected));
            }
        }
        if (stats != nullptr) {
            stats->checksum_ns += crc_ns;
        }
    }

    // 4) deserialize the roaring64 portable body (between magic and crc).
    const char* body = reinterpret_cast<const char*>(data + kDvLengthPrefixBytes + kDvMagicBytes);
    int64_t body_len = size - kDvLengthPrefixBytes - kDvMagicBytes - kDvCrcBytes;
    roaring64_bitmap_t* bitmap = nullptr;
    {
        int64_t deser_ns = 0;
        {
            SCOPED_RAW_TIMER(&deser_ns);
            bitmap = roaring64_bitmap_portable_deserialize_safe(body, static_cast<size_t>(body_len));
        }
        if (stats != nullptr) {
            stats->deserialize_ns += deser_ns;
        }
    }
    if (bitmap == nullptr) {
        return Status::Corruption("Iceberg DV roaring64 portable deserialize failed");
    }

    // 5) cardinality check against the manifest record_count.
    uint64_t card = roaring64_bitmap_get_cardinality(bitmap);
    if (record_count >= 0 && card != static_cast<uint64_t>(record_count)) {
        roaring::api::roaring64_bitmap_free(bitmap);
        return Status::Corruption(
                strings::Substitute("Iceberg DV cardinality mismatch: bitmap=$0 record_count=$1", card, record_count));
    }
    if (stats != nullptr) {
        stats->cardinality += static_cast<int64_t>(card);
        stats->build_count += 1;
    }
    return bitmap;
}

Status IcebergDeletionVectorReader::fill_row_indexes(const SkipRowsContextPtr& skip_rows_ctx) {
    const auto& descriptor = _options.descriptor;
    const std::string& path = descriptor.puffin_file_path;
    int64_t offset = descriptor.content_offset;
    int64_t size = descriptor.content_size_in_bytes;

    // Declared before the streams: the input-stream wrappers hold raw pointers to these stats.
    FormatScannerStats fs_stats;
    FormatScannerStats app_stats;
    std::shared_ptr<SharedBufferedInputStream> shared_buffered_input_stream;
    std::shared_ptr<CacheInputStream> cache_input_stream;

    std::vector<uint8_t> buffer(size);
    {
        SCOPED_RAW_TIMER(&_build_stats.read_ns);
        const FileInputStreamOptions options{
                .fs = _options.fs,
                .file_path = path,
                // FE ships the puffin length; without it the helper probes the size itself.
                .file_size = descriptor.__isset.puffin_file_size_in_bytes ? descriptor.puffin_file_size_in_bytes : -1,
                .fs_stats = &fs_stats,
                .app_stats = &app_stats,
                .datacache_options = _options.datacache_options};
        // A DV blob is one exact contiguous range, so no io_ranges are set: the shared buffer
        // stays pass-through and only the DataCache layer wraps the read.
        ASSIGN_OR_RETURN(auto file,
                         create_random_access_file(shared_buffered_input_stream, cache_input_stream, options));
        if (cache_input_stream != nullptr) {
            // Lets a local miss fall back to the node that cache select warmed, as the main data
            // stream does.
            cache_input_stream->set_peer_cache_node(_options.candidate_node);
        }
        RETURN_IF_ERROR(file->read_at_fully(offset, buffer.data(), size));
        _build_stats.read_bytes += size;
    }

    auto res = parse_dv_blob(buffer.data(), size, descriptor.record_count, &_build_stats);
    if (!res.ok()) {
        return Status::Corruption(strings::Substitute("$0 [puffin=$1 offset=$2 size=$3 referenced_data_file=$4]",
                                                      std::string(res.status().message()), path, offset, size,
                                                      descriptor.referenced_data_file));
    }
    skip_rows_ctx->deletion_bitmap = std::make_shared<DeletionBitmap>(res.value());
    if (_options.runtime_profile != nullptr) {
        update_counter(_options.runtime_profile, cache_input_stream);
    }
    return Status::OK();
}

void IcebergDeletionVectorReader::update_counter(RuntimeProfile* parent_profile,
                                                 const std::shared_ptr<CacheInputStream>& cache_input_stream) {
    static const char* kSection = "IcebergDeletionVector";
    ADD_COUNTER(parent_profile, kSection, TUnit::NONE);
    RuntimeProfile::Counter* read_bytes =
            ADD_CHILD_COUNTER(parent_profile, "IcebergDVReadBytes", TUnit::BYTES, kSection);
    RuntimeProfile::Counter* read_time = ADD_CHILD_TIMER(parent_profile, "IcebergDVReadTime", kSection);
    RuntimeProfile::Counter* deser_time = ADD_CHILD_TIMER(parent_profile, "IcebergDVDeserializeTime", kSection);
    RuntimeProfile::Counter* crc_time = ADD_CHILD_TIMER(parent_profile, "IcebergDVChecksumTime", kSection);
    RuntimeProfile::Counter* build_count =
            ADD_CHILD_COUNTER(parent_profile, "IcebergDVBuildCount", TUnit::UNIT, kSection);
    RuntimeProfile::Counter* cardinality =
            ADD_CHILD_COUNTER(parent_profile, "IcebergDVCardinality", TUnit::UNIT, kSection);
    COUNTER_UPDATE(read_bytes, _build_stats.read_bytes);
    COUNTER_UPDATE(read_time, _build_stats.read_ns);
    COUNTER_UPDATE(deser_time, _build_stats.deserialize_ns);
    COUNTER_UPDATE(crc_time, _build_stats.checksum_ns);
    COUNTER_UPDATE(build_count, _build_stats.build_count);
    COUNTER_UPDATE(cardinality, _build_stats.cardinality);

    if (cache_input_stream == nullptr) {
        return;
    }
    static const char* kCacheSection = "IcebergDV_DataCache";
    ADD_CHILD_COUNTER(parent_profile, kCacheSection, TUnit::NONE, kSection);
    RuntimeProfile::Counter* cache_read_counter =
            ADD_CHILD_COUNTER(parent_profile, "IcebergDV_DataCacheReadCounter", TUnit::UNIT, kCacheSection);
    RuntimeProfile::Counter* cache_read_bytes =
            ADD_CHILD_COUNTER(parent_profile, "IcebergDV_DataCacheReadBytes", TUnit::BYTES, kCacheSection);
    RuntimeProfile::Counter* cache_read_mem_bytes = ADD_CHILD_COUNTER(parent_profile, "IcebergDV_DataCacheReadMemBytes",
                                                                      TUnit::BYTES, "IcebergDV_DataCacheReadBytes");
    RuntimeProfile::Counter* cache_read_disk_bytes = ADD_CHILD_COUNTER(
            parent_profile, "IcebergDV_DataCacheReadDiskBytes", TUnit::BYTES, "IcebergDV_DataCacheReadBytes");
    RuntimeProfile::Counter* cache_read_timer =
            ADD_CHILD_TIMER(parent_profile, "IcebergDV_DataCacheReadTimer", kCacheSection);
    RuntimeProfile::Counter* cache_write_counter =
            ADD_CHILD_COUNTER(parent_profile, "IcebergDV_DataCacheWriteCounter", TUnit::UNIT, kCacheSection);
    RuntimeProfile::Counter* cache_write_bytes =
            ADD_CHILD_COUNTER(parent_profile, "IcebergDV_DataCacheWriteBytes", TUnit::BYTES, kCacheSection);
    RuntimeProfile::Counter* cache_skip_read_counter =
            ADD_CHILD_COUNTER(parent_profile, "IcebergDV_DataCacheSkipReadCounter", TUnit::UNIT, kCacheSection);
    RuntimeProfile::Counter* cache_skip_read_bytes =
            ADD_CHILD_COUNTER(parent_profile, "IcebergDV_DataCacheSkipReadBytes", TUnit::BYTES, kCacheSection);
    RuntimeProfile::Counter* cache_read_peer_counter =
            ADD_CHILD_COUNTER(parent_profile, "IcebergDV_DataCacheReadPeerCounter", TUnit::UNIT, kCacheSection);
    RuntimeProfile::Counter* cache_read_peer_bytes =
            ADD_CHILD_COUNTER(parent_profile, "IcebergDV_DataCacheReadPeerBytes", TUnit::BYTES, kCacheSection);
    RuntimeProfile::Counter* cache_read_peer_timer =
            ADD_CHILD_TIMER(parent_profile, "IcebergDV_DataCacheReadPeerTimer", kCacheSection);
    RuntimeProfile::Counter* cache_skip_read_peer_counter =
            ADD_CHILD_COUNTER(parent_profile, "IcebergDV_DataCacheSkipReadPeerCounter", TUnit::UNIT, kCacheSection);
    RuntimeProfile::Counter* cache_skip_read_peer_bytes =
            ADD_CHILD_COUNTER(parent_profile, "IcebergDV_DataCacheSkipReadPeerBytes", TUnit::BYTES, kCacheSection);

    const CacheInputStream::Stats& cache_stats = cache_input_stream->stats();
    COUNTER_UPDATE(cache_read_counter, cache_stats.read_block_cache_count);
    COUNTER_UPDATE(cache_read_bytes, cache_stats.read_block_cache_bytes);
    COUNTER_UPDATE(cache_read_mem_bytes, cache_stats.read_mem_cache_bytes);
    COUNTER_UPDATE(cache_read_disk_bytes, cache_stats.read_disk_cache_bytes);
    COUNTER_UPDATE(cache_read_timer, cache_stats.read_block_cache_ns);
    COUNTER_UPDATE(cache_write_counter, cache_stats.write_block_cache_count);
    COUNTER_UPDATE(cache_write_bytes, cache_stats.write_block_cache_bytes);
    COUNTER_UPDATE(cache_skip_read_counter, cache_stats.skip_read_cache_count);
    COUNTER_UPDATE(cache_skip_read_bytes, cache_stats.skip_read_cache_bytes);
    COUNTER_UPDATE(cache_read_peer_counter, cache_stats.read_peer_cache_count);
    COUNTER_UPDATE(cache_read_peer_bytes, cache_stats.read_peer_cache_bytes);
    COUNTER_UPDATE(cache_read_peer_timer, cache_stats.read_peer_cache_ns);
    COUNTER_UPDATE(cache_skip_read_peer_counter, cache_stats.skip_read_peer_cache_count);
    COUNTER_UPDATE(cache_skip_read_peer_bytes, cache_stats.skip_read_peer_cache_bytes);
}

} // namespace starrocks::formats
