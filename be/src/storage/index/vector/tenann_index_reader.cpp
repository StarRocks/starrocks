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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/tablet.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifdef WITH_TENANN
#include "tenann_index_reader.h"

#include "common/config_vector_index_fwd.h"
#include "common/status.h"
#include "common/statusor.h"
#include "fs/fs.h"
#include "storage/index/vector/vector_index_file_reader.h"
#include "tenann/common/error.h"
#include "tenann/common/seq_view.h"
#include "tenann/searcher/id_filter.h"

namespace starrocks {

namespace {

void apply_index_reader_cache_options(tenann::IndexMeta* meta_copy) {
    if (meta_copy->index_type() == tenann::IndexType::kFaissIvfPq) {
        if (config::enable_vector_index_block_cache) {
            meta_copy->index_reader_options()[tenann::IndexReaderOptions::cache_index_file_key] = false;
            meta_copy->index_reader_options()[tenann::IndexReaderOptions::cache_index_block_key] = true;
        } else {
            meta_copy->index_reader_options()[tenann::IndexReaderOptions::cache_index_file_key] = true;
            meta_copy->index_reader_options()[tenann::IndexReaderOptions::cache_index_block_key] = false;
        }
    } else {
        meta_copy->index_reader_options()[tenann::IndexReaderOptions::cache_index_file_key] = true;
    }
}

} // namespace

Status TenANNReader::init_searcher(const tenann::IndexMeta& meta, const std::string& index_path) {
    try {
        auto meta_copy = meta;
        apply_index_reader_cache_options(&meta_copy);

        tenann::IndexCache::GetGlobalInstance()->SetCapacity(config::vector_query_cache_capacity);

        _searcher = tenann::AnnSearcherFactory::CreateSearcherFromMeta(meta_copy);
        _searcher->index_reader()->SetIndexCache(tenann::IndexCache::GetGlobalInstance());
        _searcher->ReadIndex(index_path);

        DCHECK(_searcher->is_index_loaded());
    } catch (tenann::Error& e) {
        return Status::InternalError(e.what());
    }
    return Status::OK();
}

Status TenANNReader::init_searcher(const tenann::IndexMeta& meta, const std::string& index_path, FileSystem* fs) {
    if (fs == nullptr) {
        return init_searcher(meta, index_path);
    }
    try {
        auto meta_copy = meta;
        apply_index_reader_cache_options(&meta_copy);

        tenann::IndexCache::GetGlobalInstance()->SetCapacity(config::vector_query_cache_capacity);

        _searcher = tenann::AnnSearcherFactory::CreateSearcherFromMeta(meta_copy);
        _searcher->index_reader()->SetIndexCache(tenann::IndexCache::GetGlobalInstance());
        ASSIGN_OR_RETURN(auto index_raf, fs->new_random_access_file(index_path));
        ASSIGN_OR_RETURN(auto file_size, index_raf->get_size());
        auto file_reader = std::make_shared<VectorIndexFileReader>(std::move(index_raf), file_size);
        _searcher->ReadIndex(file_reader);

        DCHECK(_searcher->is_index_loaded());
    } catch (tenann::Error& e) {
        return Status::InternalError(e.what());
    }
    return Status::OK();
}

Status TenANNReader::search(tenann::PrimitiveSeqView query_vector, int k, int64_t* result_ids,
                            uint8_t* result_distances, tenann::IdFilter* id_filter) {
    try {
        _searcher->AnnSearch(query_vector, k, result_ids, result_distances, id_filter);
    } catch (tenann::Error& e) {
        return Status::InternalError(e.what());
    }
    return Status::OK();
};

Status TenANNReader::range_search(tenann::PrimitiveSeqView query_vector, int k, std::vector<int64_t>* result_ids,
                                  std::vector<float>* result_distances, tenann::IdFilter* id_filter, float range,
                                  int order) {
    try {
        _searcher->RangeSearch(query_vector, range, k, tenann::AnnSearcher::ResultOrder(order), result_ids,
                               result_distances, id_filter);
    } catch (tenann::Error& e) {
        return Status::InternalError(e.what());
    }
    return Status::OK();
};

} // namespace starrocks
#endif