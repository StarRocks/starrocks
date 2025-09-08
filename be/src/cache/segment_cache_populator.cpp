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

#include "cache/segment_cache_populator.h"

#include "cache/datacache.h"
#include "common/config.h"
#include "fs/fs.h"
#include "io/cache_input_stream.h"
#include "io/shared_buffered_input_stream.h"
#include "util/threadpool.h"
#include "gutil/strings/substitute.h"

namespace starrocks {

SegmentCachePopulator* SegmentCachePopulator::instance() {
    static SegmentCachePopulator instance;
    return &instance;
}

SegmentCachePopulator::SegmentCachePopulator() {
    Status st = ThreadPoolBuilder("segment_cache_populate")
                    .set_min_threads(1)
                    .set_max_threads(config::segment_cache_populate_thread_num)
                    .set_max_queue_size(1000)
                    .set_idle_timeout(MonoDelta::FromMilliseconds(10000))
                    .build(&_cache_populate_pool);
    if (!st.ok()) {
        LOG(WARNING) << "Failed to create segment cache populate thread pool: " << st;
    }
}

SegmentCachePopulator::~SegmentCachePopulator() {
    if (_cache_populate_pool) {
        _cache_populate_pool->shutdown();
    }
}

Status SegmentCachePopulator::populate_segment_to_cache(const std::string& segment_path) {

    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateUniqueFromString(segment_path));
    ASSIGN_OR_RETURN(auto file, fs->new_random_access_file(segment_path));
    ASSIGN_OR_RETURN(auto file_size, file->get_size());

    auto shared_buffered_input_stream = std::make_shared<io::SharedBufferedInputStream>(
        std::move(file), segment_path, file_size);

    auto cache_input_stream = std::make_shared<io::CacheInputStream>(
        shared_buffered_input_stream, segment_path, file_size, 0 /* modification_time */);

    constexpr size_t kChunkSize = 1024 * 1024;
    std::vector<uint8_t> buffer(kChunkSize);

    for (int64_t offset = 0; offset < file_size; offset += kChunkSize) {
        size_t to_read = std::min(kChunkSize, static_cast<size_t>(file_size - offset));
        RETURN_IF_ERROR(cache_input_stream->read_at_fully(offset, buffer.data(), to_read));
    }

    return Status::OK();
}

Status SegmentCachePopulator::populate_segment_to_cache_async(const std::string& segment_path) {

    if (!_cache_populate_pool) {
        return Status::InternalError("Cache populate thread pool not initialized");
    }

    auto task = [segment_path]() {
        Status st = populate_segment_to_cache(segment_path);
        if (!st.ok()) {
            LOG(WARNING) << "Failed to populate segment cache for " << segment_path << ": " << st;
        }
    };

    return _cache_populate_pool->submit_func(std::move(task));
}

}
