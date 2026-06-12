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
#include <string>
#include <vector>

#include "common/global_types.h"

namespace starrocks {

struct VectorSearchOption {
public:
    int64_t k;

    std::vector<float> query_vector;

    std::string vector_distance_column_name;

    bool use_vector_index = false;

    int vector_column_id;

    SlotId vector_slot_id;

    std::map<std::string, std::string> query_params;

    double vector_range;

    int result_order;

    // When true, re-rank the ANN result by recomputing the exact distance on the full-precision
    // vectors (a quantized index returns lossy distances). Set by FE via enable_vector_index_refine.
    bool refine_distance = false;

    double pq_refine_factor;

    double k_factor;

    // ANN filter strategy override (AnnFilterStrategy: 0=AUTO, 1=PRE, 2=POST, 3=BRUTE). Set from the
    // ann_filter_strategy session variable via TQueryOptions.ann_filter_strategy; AUTO lets the BE
    // resolver decide per segment.
    int filter_strategy = 0;

    VectorSearchOption() = default;
};

} // namespace starrocks
