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

namespace starrocks {

static size_t nb_ = 200;

#define NLIST "nlist"
#define M "M"
#define NBITS "nbits"
#define EF_CONSTRUCTION "efConstruction"
#define EF_SEARCH "efSearch"
#define METRIC_TYPE "metric_type"
#define DIM "dim"
#define IS_VECTOR_NORMED "is_vector_normed"
#define RANGE_SEARCH_CONFIDENCE "range_search_confidence"

#define CHECK_AND_RETURN(STANDARD_STR, NAME, TYPE) \
    else if (STANDARD_STR == #NAME) {              \
        return TYPE;                               \
    }

#define CRITICAL_CHECK_AND_GET(INDEX, PROPERTY_NAME, KEY, PARAM_VALUE)                        \
    do {                                                                                      \
        auto it = INDEX->PROPERTY_NAME().find(#KEY);                                          \
        if (it == INDEX->PROPERTY_NAME().end()) {                                             \
            return Status::InternalError("Error when read critical common_properties " #KEY); \
        } else {                                                                              \
            PARAM_VALUE = it->second;                                                         \
        }                                                                                     \
    } while (0);

#define GET_OR_DEFAULT(INDEX, PROPERTY_NAME, KEY, PARAM_VALUE, DEFAULT_VALUE) \
    if (INDEX->PROPERTY_NAME().count(#KEY) <= 0) {                            \
        PARAM_VALUE = DEFAULT_VALUE;                                          \
    } else {                                                                  \
        PARAM_VALUE = INDEX->PROPERTY_NAME().at(#KEY);                        \
    }

static StatusOr<tenann::IndexType> convert_to_index_type(const std::string& type_string) {
    const std::string standard_type_string = boost::algorithm::to_lower_copy(type_string);
    if (false) {
    }
    CHECK_AND_RETURN(standard_type_string, hnsw, tenann::IndexType::kFaissHnsw)
    CHECK_AND_RETURN(standard_type_string, ivfflat, tenann::IndexType::kFaissIvfFlat)
    CHECK_AND_RETURN(standard_type_string, ivfpq, tenann::IndexType::kFaissIvfPq)
    else {
        return Status::InternalError("Do not support index type " + type_string);
    }
}

static StatusOr<tenann::MetricType> convert_to_metric_type(const std::string& type_string) {
    const std::string standard_type_string = boost::algorithm::to_lower_copy(type_string);
    if (false) {
    }
    CHECK_AND_RETURN(standard_type_string, cosine_distance, tenann::MetricType::kCosineDistance)
    CHECK_AND_RETURN(standard_type_string, cosine_similarity, tenann::MetricType::kCosineSimilarity)
    CHECK_AND_RETURN(standard_type_string, inner_product, tenann::MetricType::kInnerProduct)
    CHECK_AND_RETURN(standard_type_string, l2_distance, tenann::MetricType::kL2Distance)
    else {
        return Status::InternalError("Do not support metric type " + type_string);
    }
}

static StatusOr<tenann::IndexMeta> get_vector_meta(const std::shared_ptr<TabletIndex>& tablet_index,
                                                   const std::map<std::string, std::string>& query_params) {
    tenann::IndexMeta meta;
    meta.SetIndexFamily(tenann::IndexFamily::kVectorIndex);

    // critical index metadata
    std::string param_value;

    CRITICAL_CHECK_AND_GET(tablet_index, common_properties, index_type, param_value)
    ASSIGN_OR_RETURN(auto index_type, convert_to_index_type(param_value))
    meta.SetIndexType(index_type);

    if (meta.index_type() == tenann::IndexType::kFaissIvfPq) {
        meta.index_params()[NLIST] = int(4 * sqrt(nb_));

        CRITICAL_CHECK_AND_GET(tablet_index, index_properties, M, param_value)
        meta.index_params()[M] = std::atoi(param_value.c_str());

        CRITICAL_CHECK_AND_GET(tablet_index, index_properties, nbits, param_value)
        meta.index_params()[NBITS] = std::atoi(param_value.c_str());

    } else if (meta.index_type() == tenann::IndexType::kFaissHnsw) {
        CRITICAL_CHECK_AND_GET(tablet_index, index_properties, efconstruction, param_value)
        meta.index_params()[EF_CONSTRUCTION] = std::atoi(param_value.c_str());

        CRITICAL_CHECK_AND_GET(tablet_index, index_properties, M, param_value)
        meta.index_params()[M] = std::atoi(param_value.c_str());

        GET_OR_DEFAULT(tablet_index, search_properties, efsearch, param_value, "40")
        meta.search_params()[EF_SEARCH] = std::atoi(param_value.c_str());
    }

    CRITICAL_CHECK_AND_GET(tablet_index, common_properties, metric_type, param_value)
    ASSIGN_OR_RETURN(auto metrics_type, convert_to_metric_type(param_value))
    meta.common_params()[METRIC_TYPE] = metrics_type;

    CRITICAL_CHECK_AND_GET(tablet_index, common_properties, dim, param_value)
    meta.common_params()[DIM] = std::atoi(param_value.c_str());

    GET_OR_DEFAULT(tablet_index, common_properties, is_vector_normed, param_value, "false")
    meta.common_params()[IS_VECTOR_NORMED] = boost::algorithm::to_lower_copy(param_value) == "true";

    // support dynamic setting of query parameters
    for (const auto& entry : query_params) {
        if (entry.first == RANGE_SEARCH_CONFIDENCE) {
            meta.search_params()[entry.first] = std::stof(entry.second.c_str());
        } else {
            meta.search_params()[entry.first] = std::atoi(entry.second.c_str());
        }
    }

    return meta;
}

} // namespace starrocks

#endif