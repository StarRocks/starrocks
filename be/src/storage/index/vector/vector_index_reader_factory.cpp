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

#include "storage/index/vector/vector_index_reader_factory.h"

#ifdef WITH_TENANN
#include <boost/algorithm/string/predicate.hpp>

#include "fs/fs.h"
#include "storage/index/vector/empty_index_reader.h"
#include "storage/index/vector/tenann/tenann_index_utils.h"
#include "storage/index/vector/tenann_index_reader.h"
#include "storage/index/vector/vector_index_reader.h"
#include "tenann/index/index_cache.h"
#endif

namespace starrocks {

#ifdef WITH_TENANN

static Status create_from_file_impl(const std::string& index_path,
                                    const std::shared_ptr<tenann::IndexMeta>& /*index_meta*/,
                                    std::shared_ptr<VectorIndexReader>* vector_index_reader, FileSystem* fs) {
    // Warm path: an entry in the cache means the .vi file exists and is not
    // an empty-mark placeholder, so we can skip the OSS/S3 HEAD round-trips
    // (path_exist + new_random_access_file + get_size) that the cold path runs.
    auto* cache = tenann::GetGlobalIndexCache();
    if (cache != nullptr) {
        tenann::IndexCacheHandle probe;
        if (cache->Lookup(tenann::CacheKey(index_path), &probe)) {
            (*vector_index_reader) = std::make_shared<TenANNReader>();
            return Status::OK();
        }
    }

    std::unique_ptr<RandomAccessFile> index_file;
    if (fs != nullptr) {
        // Remote FS: let new_random_access_file() be the single source of truth for
        // NotFound. Doing a separate path_exists() here would cost an extra round-trip.
        auto file_or = fs->new_random_access_file(index_path);
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
    ASSIGN_OR_RETURN(auto file_size, index_file->get_size());

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

Status VectorIndexReaderFactory::create_from_file(const std::string& index_path,
                                                  const std::shared_ptr<tenann::IndexMeta>& index_meta,
                                                  std::shared_ptr<VectorIndexReader>* vector_index_reader) {
    return create_from_file_impl(index_path, index_meta, vector_index_reader, nullptr);
}

Status VectorIndexReaderFactory::create_from_file(const std::string& index_path,
                                                  const std::shared_ptr<tenann::IndexMeta>& index_meta,
                                                  std::shared_ptr<VectorIndexReader>* vector_index_reader,
                                                  FileSystem* fs) {
    return create_from_file_impl(index_path, index_meta, vector_index_reader, fs);
}

Status VectorIndexReaderFactory::create_and_init(const std::string& index_path,
                                                 const std::shared_ptr<TabletIndex>& tablet_index,
                                                 const std::map<std::string, std::string>& query_params, FileSystem* fs,
                                                 size_t segment_num_rows, int query_k,
                                                 std::shared_ptr<VectorIndexReader>* reader) {
    ASSIGN_OR_RETURN(auto meta, get_vector_meta(tablet_index, query_params));
    auto index_meta = std::make_shared<tenann::IndexMeta>(std::move(meta));
    RETURN_IF_ERROR(create_from_file(index_path, index_meta, reader, fs));
    // Enable per-segment adaptive ef_search. query_params carries a user-explicit efSearch iff the
    // user set one via query hint / session var; that disables adaptive scaling so user intent is
    // honored. FE preserves the user-typed key casing (e.g. "efsearch", "EFSEARCH"), so the check
    // must be case-insensitive.
    bool user_set_ef = false;
    for (const auto& entry : query_params) {
        if (boost::iequals(entry.first, starrocks::index::vector::EF_SEARCH)) {
            user_set_ef = true;
            break;
        }
    }
    return (*reader)->init_searcher(*index_meta, index_path, fs, segment_num_rows, query_k, user_set_ef);
}

#else // !WITH_TENANN

Status VectorIndexReaderFactory::create_and_init(const std::string& /*index_path*/,
                                                 const std::shared_ptr<TabletIndex>& /*tablet_index*/,
                                                 const std::map<std::string, std::string>& /*query_params*/,
                                                 FileSystem* /*fs*/, size_t /*segment_num_rows*/, int /*query_k*/,
                                                 std::shared_ptr<VectorIndexReader>* /*reader*/) {
    return Status::NotSupported("vector index requires the TENANN build");
}

#endif

} // namespace starrocks
