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

#include "exec/iceberg/iceberg_deletion_vector_reader.h"

#include <zlib.h>

#include <cstring>
#include <vector>

#include "common/runtime_profile.h"
#include "formats/deletion_bitmap.h"
#include "fs/fs.h"
#include "gutil/endian.h"
#include "gutil/strings/substitute.h"

namespace starrocks {

const uint8_t IcebergDeletionVectorReader::MAGIC[4] = {0xD1, 0xD3, 0x39, 0x64};

StatusOr<roaring64_bitmap_t*> IcebergDeletionVectorReader::parse_dv_blob(const uint8_t* data, int64_t size,
                                                                         int64_t record_count,
                                                                         IcebergDVBuildStats* stats) {
    const int64_t min_size = LENGTH_PREFIX_BYTES + MAGIC_BYTES + CRC_BYTES;
    if (size < min_size) {
        return Status::Corruption(strings::Substitute("Iceberg DV blob too small: $0 bytes", size));
    }

    // 1) length prefix == size - 8 (length covers magic + body, excludes the 4B prefix and 4B crc).
    uint32_t length = BigEndian::Load32(data);
    if (static_cast<int64_t>(length) != size - LENGTH_PREFIX_BYTES - CRC_BYTES) {
        return Status::Corruption(strings::Substitute("Iceberg DV length prefix mismatch: prefix=$0 expected=$1",
                                                      length, size - LENGTH_PREFIX_BYTES - CRC_BYTES));
    }

    // 2) magic.
    if (memcmp(data + LENGTH_PREFIX_BYTES, MAGIC, MAGIC_BYTES) != 0) {
        return Status::Corruption("Iceberg DV magic mismatch");
    }

    // 3) crc32 over magic + body == trailing crc.
    {
        int64_t crc_ns = 0;
        {
            SCOPED_RAW_TIMER(&crc_ns);
            uint32_t crc_expected = BigEndian::Load32(data + size - CRC_BYTES);
            uLong crc = crc32(0L, Z_NULL, 0);
            crc = crc32(crc, reinterpret_cast<const Bytef*>(data + LENGTH_PREFIX_BYTES),
                        static_cast<uInt>(size - LENGTH_PREFIX_BYTES - CRC_BYTES));
            if (static_cast<uint32_t>(crc) != crc_expected) {
                return Status::Corruption(strings::Substitute("Iceberg DV crc mismatch: actual=$0 expected=$1",
                                                              static_cast<uint32_t>(crc), crc_expected));
            }
        }
        if (stats != nullptr) {
            stats->checksum_ns += crc_ns;
        }
    }

    // 4) deserialize the roaring64 portable body (between magic and crc).
    const char* body = reinterpret_cast<const char*>(data + LENGTH_PREFIX_BYTES + MAGIC_BYTES);
    int64_t body_len = size - LENGTH_PREFIX_BYTES - MAGIC_BYTES - CRC_BYTES;
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
    const std::string& path = _descriptor->puffin_file_path;
    int64_t offset = _descriptor->content_offset;
    int64_t size = _descriptor->content_size_in_bytes;

    std::vector<uint8_t> buffer(size);
    {
        SCOPED_RAW_TIMER(&_build_stats.read_ns);
        ASSIGN_OR_RETURN(auto file, _ctx.fs->new_random_access_file(path));
        RETURN_IF_ERROR(file->read_at_fully(offset, buffer.data(), size));
        _build_stats.read_bytes += size;
    }

    auto res = parse_dv_blob(buffer.data(), size, _descriptor->record_count, &_build_stats);
    if (!res.ok()) {
        return Status::Corruption(strings::Substitute("$0 [puffin=$1 offset=$2 size=$3 referenced_data_file=$4]",
                                                      std::string(res.status().message()), path, offset, size,
                                                      _descriptor->referenced_data_file));
    }
    skip_rows_ctx->deletion_bitmap = std::make_shared<DeletionBitmap>(res.value());
#ifndef BE_TEST
    update_counter(_ctx.profile.runtime_profile);
#endif
    return Status::OK();
}

void IcebergDeletionVectorReader::update_counter(RuntimeProfile* parent_profile) {
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
}

} // namespace starrocks