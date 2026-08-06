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

#include "fmt/format.h"
#include "fs/fs_util.h"
#include "storage/index/index_descriptor.h"
#include "vector_index_reader.h"

namespace starrocks {

struct OlapReaderStatistics;
class VectorIndexCache;

class VectorIndexReaderFactory {
#ifdef WITH_TENANN
public:
    // `vi_file` is in/out: the caller supplies path (and fs, null for the local filesystem);
    // on the cold path this fills in vi_file->size from the get_size() it has to do anyway,
    // so the subsequent init_searcher does not repeat the HEAD/stat. On the warm path the
    // file is never opened and size stays unset — VectorIndexFileReader::open() resolves it
    // lazily if the entry happens to be evicted before init_searcher runs.
    static Status create_from_file(FileInfo* vi_file, const std::shared_ptr<tenann::IndexMeta>& index_meta,
                                   std::shared_ptr<VectorIndexReader>* vector_index_reader,
                                   OlapReaderStatistics* stats = nullptr,
                                   VectorIndexCache* vector_index_cache = nullptr);
#endif
};

} // namespace starrocks
