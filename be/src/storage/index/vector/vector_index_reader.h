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

#include <cstdint>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/primitive/range.h"
#ifdef WITH_TENANN
#include "tenann/store/index_meta.h"
#endif

namespace starrocks {

class FileSystem;

class VectorIndexReader {
public:
    VectorIndexReader() = default;
    virtual ~VectorIndexReader() = default;

#ifdef WITH_TENANN
    virtual Status init_searcher(const tenann::IndexMeta& meta, const std::string& index_path,
                                 FileSystem* fs = nullptr) = 0;

    // Per-segment context for apply_adaptive_ef_search(). Default forwards
    // to the row-count-unaware form for readers without adaptive scaling.
    virtual Status init_searcher(const tenann::IndexMeta& meta, const std::string& index_path, FileSystem* fs,
                                 size_t segment_num_rows, int query_k, bool user_set_ef) {
        return init_searcher(meta, index_path, fs);
    }
#endif

    // Search the index for the k nearest neighbors of a float query vector. query_size is the
    // number of float elements (the vector dimension). scan_range carries the still-live row
    // ids; the implementation builds the per-query id filter from it. Result buffers are
    // caller-owned and must be sized to at least k.
    virtual Status search(const float* query_vector, size_t query_size, int k, int64_t* result_ids,
                          uint8_t* result_distances, const SparseRange<>& scan_range) = 0;
    virtual Status range_search(const float* query_vector, size_t query_size, int k, std::vector<int64_t>* result_ids,
                                std::vector<float>* result_distances, const SparseRange<>& scan_range, float range,
                                int order) = 0;
};

} // namespace starrocks
