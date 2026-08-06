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

#ifdef WITH_TENANN
#include "storage/index/vector/vector_index_reader_factory.h"

#include "common/runtime_profile.h"
#include "fs/fs.h"
#include "storage/index/vector/empty_index_reader.h"
#include "storage/index/vector/tenann_index_reader.h"
#include "storage/index/vector/vector_index_reader.h"
#include "storage_primitive/storage_stats.h"
#include "tenann/index/index_cache.h"

namespace starrocks {

static Status create_from_file_impl(FileInfo* vi_file, const std::shared_ptr<tenann::IndexMeta>& /*index_meta*/,
                                    std::shared_ptr<VectorIndexReader>* vector_index_reader,
                                    OlapReaderStatistics* stats) {
    const std::string& index_path = vi_file->path;
    // Warm path: an entry in the cache means the .vi file exists and is not
    // an empty-mark placeholder, so we can skip the OSS/S3 HEAD round-trips
    // (path_exist + new_random_access_file + get_size) that the cold path runs.
    auto* cache = tenann::GetGlobalIndexCache();
    if (cache != nullptr) {
        tenann::IndexCacheHandle probe;
        bool cache_hit = false;
        {
            int64_t ignored_ns = 0;
            SCOPED_RAW_TIMER(stats != nullptr ? &stats->vector_index_cache_lookup_ns : &ignored_ns);
            cache_hit = cache->Lookup(tenann::CacheKey(index_path), &probe);
        }
        if (cache_hit) {
            (*vector_index_reader) = std::make_shared<TenANNReader>();
            return Status::OK();
        }
    }

    std::unique_ptr<RandomAccessFile> index_file;
    uint64_t file_size = 0;
    {
        int64_t ignored_ns = 0;
        SCOPED_RAW_TIMER(stats != nullptr ? &stats->vector_index_file_open_ns : &ignored_ns);
        if (vi_file->fs != nullptr) {
            // Remote FS: let new_random_access_file() be the single source of truth for
            // NotFound. Doing a separate path_exists() here would cost an extra round-trip.
            auto file_or = vi_file->fs->new_random_access_file(index_path);
            if (!file_or.ok()) {
                if (file_or.status().is_not_found()) {
                    return Status::NotFound(fmt::format("index path {} not found", index_path));
                }
                return file_or.status();
            }
            index_file = std::move(file_or).value();
        } else {
            if (!fs::path_exist(index_path)) {
                return Status::NotFound(fmt::format("index path {} not found", index_path));
            }
            ASSIGN_OR_RETURN(index_file, fs::new_random_access_file(index_path));
        }
        ASSIGN_OR_RETURN(file_size, index_file->get_size());
    }
    // Hand the resolved size to init_searcher so it does not HEAD/stat the same file again.
    vi_file->size = file_size;

    if (file_size == IndexDescriptor::mark_word_len) {
        auto buf = std::make_unique<unsigned char[]>(file_size);
        RETURN_IF_ERROR(index_file->read_fully(buf.get(), file_size));
        std::string_view buf_str = std::string_view(reinterpret_cast<char*>(buf.get()), file_size);
        if (buf_str == IndexDescriptor::mark_word) {
            (*vector_index_reader) = std::make_shared<EmptyIndexReader>();
            return Status::OK();
        }
    }
    (*vector_index_reader) = std::make_shared<TenANNReader>();
    return Status::OK();
}

Status VectorIndexReaderFactory::create_from_file(FileInfo* vi_file,
                                                  const std::shared_ptr<tenann::IndexMeta>& index_meta,
                                                  std::shared_ptr<VectorIndexReader>* vector_index_reader,
                                                  OlapReaderStatistics* stats) {
    return create_from_file_impl(vi_file, index_meta, vector_index_reader, stats);
}

} // namespace starrocks
#endif
