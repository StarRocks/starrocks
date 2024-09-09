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

#include "storage/index/vector/vector_index_reader.h"

namespace starrocks {

class EmptyIndexReader final : public VectorIndexReader {
public:
    ~EmptyIndexReader() override = default;

    Status init_searcher(const tenann::IndexMeta& meta, const std::string& index_path) override {
        return Status::NotSupported("Not implement");
    }

    Status search(tenann::PrimitiveSeqView query_vector, int k, int64_t* result_ids, uint8_t* result_distances,
                  tenann::IdFilter* id_filter = nullptr) override {
        return Status::NotSupported("Not implement");
    }

    Status range_search(tenann::PrimitiveSeqView query_vector, int k, std::vector<int64_t>* result_ids,
                        std::vector<float>* result_distances, tenann::IdFilter* id_filter, float range,
                        int order) override {
        return Status::NotSupported("Not implement");
    }
};

} // namespace starrocks