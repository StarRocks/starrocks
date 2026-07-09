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

#include <memory>
#include <string>

#include "cache/cache_options.h"
#include "common/statusor.h"
#include "gen_cpp/types.pb.h"

namespace starrocks {

class CacheInputStream;
class FileSystem;
struct FormatScannerStats;
class RandomAccessFile;
class SharedBufferedInputStream;

namespace connector {

struct ScanFileOpenOptions {
    FileSystem* fs = nullptr;
    std::string file_path;
    int64_t file_size = -1;
    FormatScannerStats* fs_stats = nullptr;
    FormatScannerStats* app_stats = nullptr;

    DataCacheOptions datacache_options;
    CompressionTypePB compression_type = CompressionTypePB::NO_COMPRESSION;
};

StatusOr<std::unique_ptr<RandomAccessFile>> open_scan_random_access_file(
        std::shared_ptr<SharedBufferedInputStream>& shared_buffered_input_stream,
        std::shared_ptr<CacheInputStream>& cache_input_stream, const ScanFileOpenOptions& options);

} // namespace connector
} // namespace starrocks
