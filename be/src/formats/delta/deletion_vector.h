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
#include <vector>

#include "cache/cache_options.h"
#include "common/status.h"
#include "formats/scan_context.h"
#include "fs/fs.h"
#include "gen_cpp/PlanNodes_types.h"

namespace starrocks {

class CacheInputStream;
class RuntimeProfile;
class SharedBufferedInputStream;

namespace formats {

struct DeletionVectorBuildStats {
    int64_t bitmap_deserialize_ns = 0;
};

struct DeletionVectorOptions {
    TDeletionVectorDescriptor descriptor;
    FileSystem* fs = nullptr;
    std::string table_location;
    DataCacheOptions datacache_options;
    RuntimeProfile* runtime_profile = nullptr;
};

class DeletionVector {
public:
    explicit DeletionVector(DeletionVectorOptions options) : _options(std::move(options)) {}

    Status fill_row_indexes(const SkipRowsContextPtr& skip_rows_ctx);
    Status deserialized_inline_dv(const std::string& encoded_bitmap_data, const SkipRowsContextPtr& skip_rows_ctx);
    StatusOr<std::string> get_absolute_path(const std::string& table_location) const;

    bool is_inline() const { return _options.descriptor.__isset.storageType && _options.descriptor.storageType == "i"; }

    static const int32_t DV_SIZE_LENGTH = 4;
    static const int32_t MAGIC_NUMBER_LENGTH = 4;
    static const uint32_t MAGIC_NUMBER = 1681511377;
    // UUIDs always encode into 20 characters.
    static const uint32_t ENCODED_UUID_LENGTH = 20;
    static const std::string DELETION_VECTOR;

private:
    StatusOr<std::unique_ptr<RandomAccessFile>> open_random_access_file(
            const std::string& file_path, FormatScannerStats& fs_stats, FormatScannerStats& app_stats,
            std::shared_ptr<SharedBufferedInputStream>& shared_buffered_input_stream,
            std::shared_ptr<CacheInputStream>& cache_input_stream) const;

    Status deserialized_deletion_vector(uint32_t magic_number, std::vector<char>& serialized_dv,
                                        int64_t serialized_bitmap_length, const SkipRowsContextPtr& skip_rows_ctx);

    std::string assemble_deletion_vector_path(const std::string& table_location, std::string&& uuid,
                                              const std::string& prefix) const;

    void update_dv_file_io_counter(RuntimeProfile* parent_profile, const FormatScannerStats& app_stats,
                                   const FormatScannerStats& fs_stats,
                                   const std::shared_ptr<CacheInputStream>& cache_input_stream,
                                   const std::shared_ptr<SharedBufferedInputStream>& shared_buffered_input_stream);

    void update_dv_build_counter(RuntimeProfile* parent_profile, const DeletionVectorBuildStats& build_stats);

    const DeletionVectorOptions _options;
    DeletionVectorBuildStats _build_stats;
};
} // namespace formats
} // namespace starrocks
