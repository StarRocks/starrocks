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

#include <map>
#include <memory>
#include <string>

#include "common/statusor.h"
#include "fs/fs.h"
#include "vector_index_reader.h"

namespace starrocks {

struct OlapReaderStatistics;
class TabletIndex;
class VectorIndexCache;

#ifdef WITH_TENANN

struct VectorIndexReaderInitOptions {
    size_t segment_num_rows = 0;
    int query_k = 0;
    bool refine_distance = false;
    // Required. SegmentReadOptions rejects a null stats pointer before creating an iterator.
    OlapReaderStatistics& stats;
};

struct VectorIndexReaderCreateResult {
    VectorIndexReaderInitResult state = VectorIndexReaderInitResult::kFallback;
    std::shared_ptr<VectorIndexReader> reader;
};

// Binds reader creation to the SR-owned cache. create_and_init returns either a
// fully initialized reader (kReady) or a null reader with kFallback.
class VectorIndexReaderFactory {
public:
    explicit VectorIndexReaderFactory(VectorIndexCache& vector_index_cache) : _vector_index_cache(vector_index_cache) {}

    StatusOr<VectorIndexReaderCreateResult> create_and_init(FileInfo vi_file,
                                                            const std::shared_ptr<TabletIndex>& tablet_index,
                                                            const std::map<std::string, std::string>& query_params,
                                                            VectorIndexReaderInitOptions options);

private:
    VectorIndexCache& _vector_index_cache;
};

#endif

} // namespace starrocks
