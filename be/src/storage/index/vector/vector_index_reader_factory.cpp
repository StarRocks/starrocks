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

#include <boost/algorithm/string/predicate.hpp>

#include "common/config_vector_index_fwd.h"
#include "common/runtime_profile.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "storage/index/index_descriptor.h"
#include "storage/index/vector/tenann/tenann_index_utils.h"
#include "storage/index/vector/tenann_index_reader.h"
#include "storage/index/vector/vector_index_cache.h"
#include "storage/index/vector/vector_index_reader.h"
#include "storage_primitive/storage_stats.h"

namespace starrocks {

StatusOr<VectorIndexReaderCreateResult> VectorIndexReaderFactory::create_and_init(
        FileInfo vi_file, const std::shared_ptr<TabletIndex>& tablet_index,
        const std::map<std::string, std::string>& query_params, VectorIndexReaderInitOptions options) {
    ASSIGN_OR_RETURN(auto meta, get_vector_meta(tablet_index, query_params));

    const std::string& index_path = vi_file.path;
    const bool async_load_on_miss = config::enable_vector_index_cache_async_load_on_miss &&
                                    _vector_index_cache.capacity() > 0 && !options.refine_distance;

    VectorIndexCacheProbeResult probe;
    {
        SCOPED_RAW_TIMER(&options.stats.vector_index_cache_lookup_ns);
        probe = _vector_index_cache.ProbeForQuery(tenann::CacheKey(index_path), !async_load_on_miss);
    }
    if (probe.state == VectorIndexCacheProbeState::kLoading ||
        probe.state == VectorIndexCacheProbeState::kWaitTimeout) {
        ++options.stats.vector_index_cache_miss_count;
        return VectorIndexReaderCreateResult{};
    }

    if (probe.state == VectorIndexCacheProbeState::kMiss) {
        std::unique_ptr<RandomAccessFile> index_file;
        uint64_t file_size = 0;
        {
            SCOPED_RAW_TIMER(&options.stats.vector_index_file_open_ns);
            if (vi_file.fs != nullptr) {
                auto file_or = vi_file.fs->new_random_access_file(index_path);
                if (!file_or.ok()) {
                    if (file_or.status().is_not_found()) {
                        return VectorIndexReaderCreateResult{};
                    }
                    return file_or.status();
                }
                index_file = std::move(file_or).value();
            } else {
                if (!fs::path_exist(index_path)) {
                    return VectorIndexReaderCreateResult{};
                }
                ASSIGN_OR_RETURN(index_file, fs::new_random_access_file(index_path));
            }
            ASSIGN_OR_RETURN(file_size, index_file->get_size());
        }
        vi_file.size = file_size;

        if (file_size == IndexDescriptor::mark_word_len) {
            auto buf = std::make_unique<unsigned char[]>(file_size);
            RETURN_IF_ERROR(index_file->read_fully(buf.get(), file_size));
            std::string_view buf_str = std::string_view(reinterpret_cast<char*>(buf.get()), file_size);
            if (buf_str == IndexDescriptor::mark_word) {
                return VectorIndexReaderCreateResult{};
            }
        }
    }

    bool user_set_ef = false;
    for (const auto& entry : query_params) {
        if (boost::iequals(entry.first, index::vector::EF_SEARCH)) {
            user_set_ef = true;
            break;
        }
    }
    apply_adaptive_ef_search(&meta, options.segment_num_rows, options.query_k, user_set_ef);

    std::shared_ptr<VectorIndexReader> reader =
            std::make_shared<TenANNReader>(_vector_index_cache, async_load_on_miss, std::move(probe.handle));
    ASSIGN_OR_RETURN(auto state, reader->init_searcher(std::move(meta), vi_file, options.stats));
    if (state == VectorIndexReaderInitResult::kFallback) {
        return VectorIndexReaderCreateResult{};
    }
    return VectorIndexReaderCreateResult{.state = state, .reader = std::move(reader)};
}

} // namespace starrocks
#endif
