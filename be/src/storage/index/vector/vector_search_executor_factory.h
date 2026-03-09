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

#include <memory>
#include <string>

#include "common/statusor.h"
#include "storage/index/vector/vector_search_executor.h"

namespace starrocks {

enum class TableType {
    OLAP,
    PAIMON,
    ICEBERG,
    LANCE,
};

struct VectorSearchExecutorConfig {
    TableType table_type = TableType::OLAP;

    std::string index_path;
    VectorAnnIndex::IndexMeta index_meta;

    FilterStrategyType filter_strategy = FilterStrategyType::PRE_FILTER;
};

// Factory that creates VectorSearchExecutor instances configured for
// the given table type. The factory selects the appropriate
// VectorAnnIndex and ScalarIndexProvider implementations.
class VectorSearchExecutorFactory {
public:
    static StatusOr<std::unique_ptr<VectorSearchExecutor>> create(const VectorSearchExecutorConfig& config);
};

} // namespace starrocks
