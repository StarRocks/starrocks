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

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "cache/cache_options.h"
#include "common/status.h"
#include "formats/deletion_bitmap.h"
#include "formats/scan_context.h"
#include "fs/fs.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"

namespace starrocks {
class CacheInputStream;
class RuntimeProfile;
class SharedBufferedInputStream;

namespace formats {

struct IcebergDeleteBuilderContext {
    const FormatScanContext* scan_context = nullptr;
    FileSystem* fs = nullptr;
    std::string data_file_path;
    DataCacheOptions datacache_options;
    RuntimeProfile* runtime_profile = nullptr;
    int32_t chunk_size = 0;
};

struct IcebergColumnMeta;

class IcebergDeleteBuilder {
public:
    explicit IcebergDeleteBuilder(IcebergDeleteBuilderContext ctx)
            : _ctx(std::move(ctx)), _deletion_bitmap(std::make_shared<DeletionBitmap>(roaring64_bitmap_create())) {}

    ~IcebergDeleteBuilder() = default;

    Status build_orc(const TIcebergDeleteFile& delete_file) const;

    Status build_parquet(const TIcebergDeleteFile& delete_file) const;

    Status build_deletion_vector(const TIcebergDeleteFile& delete_file) const;

    // Exposed for testing.
    static Status parse_deletion_vector_blob(const uint8_t* blob, int64_t content_size, DeletionBitmap* bitmap);

    DeletionBitmapPtr deletion_bitmap() const { return _deletion_bitmap; }

private:
    StatusOr<std::unique_ptr<RandomAccessFile>> open_random_access_file(
            const TIcebergDeleteFile& delete_file, FormatScannerStats& fs_stats, FormatScannerStats& app_stats,
            std::shared_ptr<SharedBufferedInputStream>& shared_buffered_input_stream,
            std::shared_ptr<CacheInputStream>& cache_input_stream) const;

    StatusOr<std::unique_ptr<RandomAccessFile>> open_deletion_vector_file(
            const TIcebergDeleteFile& delete_file, int64_t offset, int64_t size, FormatScannerStats& fs_stats,
            FormatScannerStats& app_stats, std::shared_ptr<SharedBufferedInputStream>& shared_buffered_input_stream,
            std::shared_ptr<CacheInputStream>& cache_input_stream) const;

    static void update_delete_file_io_counter(
            RuntimeProfile* parent_profile, const FormatScannerStats& app_stats, const FormatScannerStats& fs_stats,
            const std::shared_ptr<CacheInputStream>& cache_input_stream,
            const std::shared_ptr<SharedBufferedInputStream>& shared_buffered_input_stream);
    Status fill_skip_rowids(const ChunkPtr& chunk) const;

    IcebergDeleteBuilderContext _ctx;
    DeletionBitmapPtr _deletion_bitmap;
};

class IcebergDeleteFileMeta {
public:
    IcebergDeleteFileMeta() = default;
    ~IcebergDeleteFileMeta() = default;

    static SlotDescriptor& get_delete_file_path_slot();
    static SlotDescriptor& get_delete_file_pos_slot();

private:
    static SlotDescriptor gen_slot_helper(const IcebergColumnMeta& meta);
};
} // namespace formats
} // namespace starrocks
