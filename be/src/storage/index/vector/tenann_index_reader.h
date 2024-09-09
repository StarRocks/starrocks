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

#include "common/status.h"
#include "storage/index/vector/vector_index_reader.h"
#include "tenann/common/seq_view.h"
#include "tenann/common/type_traits.h"
#include "tenann/factory/ann_searcher_factory.h"
#include "tenann/factory/index_factory.h"
#include "tenann/searcher/ann_searcher.h"
#include "tenann/searcher/faiss_hnsw_ann_searcher.h"
#include "tenann/searcher/id_filter.h"
#include "tenann/store/index_meta.h"

namespace starrocks {

class TenANNReader final : public VectorIndexReader {
public:
    TenANNReader() = default;
    ~TenANNReader() override{};

    Status init_searcher(const tenann::IndexMeta& meta, const std::string& index_path) override;

    Status search(tenann::PrimitiveSeqView query_vector, int k, int64_t* result_ids, uint8_t* result_distances,
                  tenann::IdFilter* id_filter = nullptr) override;
    Status range_search(tenann::PrimitiveSeqView query_vector, int k, std::vector<int64_t>* result_ids,
                        std::vector<float>* result_distances, tenann::IdFilter* id_filter, float range,
                        int order) override;

private:
    std::shared_ptr<tenann::AnnSearcher> _searcher;
};

} // namespace starrocks
