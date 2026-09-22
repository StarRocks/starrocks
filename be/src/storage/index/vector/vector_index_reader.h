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

#include "common/status.h"
#include "common/statusor.h"
#include "fs/fs.h" // FileInfo
#ifdef WITH_TENANN
#include "tenann/common/seq_view.h"
#include "tenann/searcher/id_filter.h"
#include "tenann/store/index_meta.h"
#endif

namespace starrocks {

struct OlapReaderStatistics;

enum class VectorIndexReaderInitResult : uint8_t {
    kReady,
    kFallback,
};

class VectorIndexReader {
public:
    VectorIndexReader() = default;
    virtual ~VectorIndexReader() = default;

    // Whether the reader can restrict the ANN search to a candidate id set efficiently (filtered
    // search), which enables true pre-filtering. Readers that cannot should be queried via
    // post-filtering (oversample + filter the result). Conservative default: false.
    virtual bool supports_efficient_filtered_search() const { return false; }

#ifdef WITH_TENANN
    // `vi_file` describes the .vi file: path, the FileSystem that owns it, and its size once
    // resolved. A null `vi_file.fs` means read the path from the local filesystem. The
    // FileSystem is held by shared_ptr because the reader built from it is stored in the
    // tenann index cache and outlives the SegmentIterator that started the load.
    // Takes ownership of the query-specific meta so implementations can adjust it
    // without serializing and copying the full IndexMeta again.
    virtual StatusOr<VectorIndexReaderInitResult> init_searcher(tenann::IndexMeta meta, const FileInfo& vi_file,
                                                                OlapReaderStatistics& stats) = 0;

    virtual Status search(tenann::PrimitiveSeqView query_vector, int k, int64_t* result_ids, uint8_t* result_distances,
                          tenann::IdFilter* id_filter = nullptr) = 0;
    virtual Status range_search(tenann::PrimitiveSeqView query_vector, int k, std::vector<int64_t>* result_ids,
                                std::vector<float>* result_distances, tenann::IdFilter* id_filter, float range,
                                int order) = 0;
#endif
};

} // namespace starrocks
