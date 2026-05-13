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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/column_reader.h

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

#pragma once

#include "common/status.h"
#ifdef WITH_TENANN
#include "tenann/common/seq_view.h"
#include "tenann/searcher/id_filter.h"
#include "tenann/store/index_meta.h"
#endif

namespace starrocks {

class FileSystem;

class VectorIndexReader {
public:
    VectorIndexReader() = default;
    virtual ~VectorIndexReader() = default;

#ifdef WITH_TENANN
    virtual Status init_searcher(const tenann::IndexMeta& meta, const std::string& index_path) = 0;

    virtual Status init_searcher(const tenann::IndexMeta& meta, const std::string& index_path, FileSystem* fs) {
        return init_searcher(meta, index_path);
    }

    // Overload that carries context for per-segment adaptive ef_search:
    //   segment_num_rows: row count of this segment, used as the scaling signal
    //   query_k: effective k (already multiplied by k_factor) the caller plans
    //     to pass to search(); used as a floor for ef_base to match faiss's
    //     internal `ef = max(efSearch, k)` rule
    //   user_set_ef: true if the user explicitly specified efSearch (e.g. via
    //     query hint); when true, adaptive scaling is skipped
    // Default forwards to the row-count-unaware overload.
    virtual Status init_searcher(const tenann::IndexMeta& meta, const std::string& index_path, FileSystem* fs,
                                 size_t segment_num_rows, int query_k, bool user_set_ef) {
        return init_searcher(meta, index_path, fs);
    }

    virtual Status search(tenann::PrimitiveSeqView query_vector, int k, int64_t* result_ids, uint8_t* result_distances,
                          tenann::IdFilter* id_filter = nullptr) = 0;
    virtual Status range_search(tenann::PrimitiveSeqView query_vector, int k, std::vector<int64_t>* result_ids,
                                std::vector<float>* result_distances, tenann::IdFilter* id_filter, float range,
                                int order) = 0;
#endif
};

} // namespace starrocks
