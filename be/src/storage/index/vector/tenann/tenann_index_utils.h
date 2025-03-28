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
} // namespace starrocks

#endif