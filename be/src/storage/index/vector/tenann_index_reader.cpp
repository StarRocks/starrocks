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

#include "tenann_index_reader.h"

#include "common/config.h"
#include "common/status.h"
#include "tenann/common/error.h"
#include "tenann/common/seq_view.h"
#include "tenann/searcher/id_filter.h"

namespace starrocks {

Status TenANNReader::init_searcher(const tenann::IndexMeta& meta, const std::string& index_path) {
    try {
        auto meta_copy = meta;
        if (meta.index_type() == tenann::IndexType::kFaissIvfPq) {
            if (config::enable_vector_index_block_cache) {
                // cache index blocks
                meta_copy.index_reader_options()[tenann::IndexReaderOptions::cache_index_file_key] = false;
                meta_copy.index_reader_options()[tenann::IndexReaderOptions::cache_index_block_key] = true;
            } else {
                // cache index file
                meta_copy.index_reader_options()[tenann::IndexReaderOptions::cache_index_file_key] = true;
                meta_copy.index_reader_options()[tenann::IndexReaderOptions::cache_index_block_key] = false;
            }
        } else {
            // cache index file
            meta_copy.index_reader_options()[tenann::IndexReaderOptions::cache_index_file_key] = true;
        }

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