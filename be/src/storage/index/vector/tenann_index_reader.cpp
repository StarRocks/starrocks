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

#include <stdexcept>

#include "common/config_vector_index_fwd.h"
#include "common/status.h"
#include "common/statusor.h"
#include "fs/fs.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "storage/index/vector/tenann/tenann_index_utils.h"
#include "storage/index/vector/vector_index_file_reader.h"
#include "tenann/common/error.h"
#include "tenann/common/seq_view.h"
#include "tenann/factory/index_factory.h"
#include "tenann/index/index_cache.h"
#include "tenann/index/index_reader.h"
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

Status TenANNReader::init_searcher(const tenann::IndexMeta& meta, const std::string& index_path, FileSystem* fs) {
    auto* cache = tenann::GetGlobalIndexCache();
    if (cache == nullptr) {
        return Status::InternalError(
                "VectorIndexCache not injected. ExecEnv::init must call tenann::SetGlobalIndexCache.");
    }

    auto meta_copy = meta;
    apply_index_reader_cache_options(&meta_copy);

    // Loader runs under vector_index mem tracker and opens the remote file
    // lazily, so warm-path cache hits skip the OSS/S3 round-trip entirely.
    auto* tracker = GlobalEnv::GetInstance()->vector_index_mem_tracker();

    // Loader catches its own failures (incl. tenann::Error, which inherits
    // privately from std::exception and would otherwise escape GetOrCreate)
    // and surfaces the real Status to init_searcher's caller — preserves
    // NotFound for the brute-force fallback at vector_index_reader_factory.
    Status load_status;
    auto loader = [&meta_copy, &index_path, fs, cache, tracker, &load_status]() -> tenann::IndexRef {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(tracker);
        try {
            std::shared_ptr<VectorIndexFileReader> external_file_reader;
            if (fs != nullptr) {
                auto opened_or = VectorIndexFileReader::open(fs, index_path);
                if (!opened_or.ok()) {
                    load_status = opened_or.status();
                    return nullptr;
                }
                external_file_reader = std::shared_ptr<VectorIndexFileReader>(opened_or.value().release());
            }
            auto reader = tenann::IndexFactory::CreateReaderFromMeta(meta_copy);
            reader->SetIndexCache(cache);
            if (external_file_reader != nullptr) {
                reader->SetFileReader(external_file_reader);
            }
            return reader->ReadIndexFile(index_path);
        } catch (const tenann::Error& e) {
            load_status = tenann_error_to_status(e);
            return nullptr;
        } catch (const std::exception& e) {
            load_status = Status::InternalError(e.what());
            return nullptr;
        }
    };

    if (!cache->GetOrCreate(tenann::CacheKey(index_path), loader, &_cache_handle)) {
        return !load_status.ok() ? load_status : Status::InternalError("failed to load vector index: " + index_path);
    }

    try {
        _searcher = tenann::AnnSearcherFactory::CreateSearcherFromMeta(meta_copy);
        // AttachIndexRef skips the second cache lookup Searcher::ReadIndex
        // would otherwise do — we already hold the ref from GetOrCreate.
        _searcher->AttachIndexRef(_cache_handle.index_ref());

        // Hard-check in addition to tenann's internal DCHECK: a silent
        // AttachIndex failure would yield wrong search results downstream.
        if (!_searcher->is_index_loaded()) {
            return Status::InternalError("vector index searcher did not finish loading: " + index_path);
        }
    } catch (const tenann::Error& e) {
        return tenann_error_to_status(e);
    } catch (const std::exception& e) {
        return Status::InternalError(e.what());
    }
    return Status::OK();
}

Status TenANNReader::init_searcher(const tenann::IndexMeta& meta, const std::string& index_path, FileSystem* fs,
                                   size_t segment_num_rows, int query_k, bool user_set_ef) {
    auto adapted_meta = meta;
    apply_adaptive_ef_search(&adapted_meta, segment_num_rows, query_k, user_set_ef);
    return init_searcher(adapted_meta, index_path, fs);
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