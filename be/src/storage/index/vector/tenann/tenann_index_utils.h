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

#ifdef WITH_TENANN

#pragma once

#include <boost/algorithm/string.hpp>
#include <iostream>
#include <memory>

#include "common/statusor.h"
#include "storage/tablet_index.h"
#include "tenann/store/index_meta.h"
#include "tenann/store/index_type.h"

namespace starrocks::index::vector {

constexpr size_t nb_ = 200;
static const std::string NLIST = "nlist";
static const std::string M = "M";
static const std::string NBITS = "nbits";
static const std::string EF_CONSTRUCTION = "efConstruction";
static const std::string EF_SEARCH = "efSearch";
static const std::string METRIC_TYPE = "metric_type";
static const std::string DIM = "dim";
static const std::string IS_VECTOR_NORMED = "is_vector_normed";
static const std::string RANGE_SEARCH_CONFIDENCE = "range_search_confidence";

}; // namespace starrocks::index::vector

namespace starrocks {
StatusOr<tenann::IndexMeta> get_vector_meta(const std::shared_ptr<TabletIndex>& tablet_index,
                                            const std::map<std::string, std::string>& query_params);

// Compute the effective ef_search for a single segment.
//
//   ef_base = max(user_ef, query_k)                      // faiss max(ef,k) floor
//   factor  = min(1 + alpha * log2(rows/baseline), cap)  // 1.0 when rows<=baseline
//   return    ef_base * factor
//
// `user_ef` and `query_k` must be positive; otherwise the returned value is clamped
// to a non-negative int. Caller is expected to check
// `config::enable_vector_adaptive_search` and user_set_ef overrides before calling.
int compute_adaptive_ef_search(int user_ef, int query_k, size_t segment_num_rows);

// Apply adaptive ef_search to meta.search_params["efSearch"] in place. Honors:
//   - `config::enable_vector_adaptive_search` (global enable)
//   - `user_set_ef` (user explicitly specified ef in query -> skip)
//   - Missing efSearch in meta (non-HNSW indexes -> skip)
void apply_adaptive_ef_search(tenann::IndexMeta* meta, size_t segment_num_rows, int query_k, bool user_set_ef);
} // namespace starrocks

#endif