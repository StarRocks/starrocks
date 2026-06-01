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

#include "fmt/format.h"
#include "fs/fs_util.h"
#include "storage/index/index_descriptor.h"
#include "vector_index_reader.h"

namespace starrocks {

class FileSystem;
class TabletIndex;

class VectorIndexReaderFactory {
public:
    // Build a ready-to-search reader for the .vi file at index_path: derives the TenANN index meta
    // from tablet_index + query_params, opens the reader, and initializes the searcher (including
    // per-segment adaptive ef_search). Returns NotFound when the .vi file is absent and NotSupported
    // when the index is an empty marker or the build has no TenANN; callers treat both as "fall back
    // to brute force". Keeps callers free of any TenANN type.
    static Status create_and_init(const std::string& index_path, const std::shared_ptr<TabletIndex>& tablet_index,
                                  const std::map<std::string, std::string>& query_params, FileSystem* fs,
                                  size_t segment_num_rows, int query_k, std::shared_ptr<VectorIndexReader>* reader);

#ifdef WITH_TENANN
    static Status create_from_file(const std::string& index_path, const std::shared_ptr<tenann::IndexMeta>& index_meta,
                                   std::shared_ptr<VectorIndexReader>* vector_index_reader);

    static Status create_from_file(const std::string& index_path, const std::shared_ptr<tenann::IndexMeta>& index_meta,
                                   std::shared_ptr<VectorIndexReader>* vector_index_reader, FileSystem* fs);
#endif
};

} // namespace starrocks
