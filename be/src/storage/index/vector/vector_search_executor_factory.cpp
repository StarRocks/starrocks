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

#include "storage/index/vector/vector_search_executor_factory.h"

#include "storage/index/vector/hnsw_ann_index.h"

namespace starrocks {

static StatusOr<std::unique_ptr<VectorAnnIndex>> create_ann_index(const VectorSearchExecutorConfig& config) {
    switch (config.table_type) {
    case TableType::PAIMON:
        // TODO: implement PaimonAnnIndex
        return Status::NotSupported("Paimon vector index not yet implemented");
    case TableType::OLAP:
    case TableType::ICEBERG: {
        std::unique_ptr<VectorAnnIndex> index;
        switch (config.index_meta.index_type) {
        case VectorIndexType::HNSW:
            index = std::make_unique<HnswAnnIndex>();
            break;
        default:
            return Status::NotSupported("unsupported vector index type");
        }
        RETURN_IF_ERROR(index->init(config.index_path, config.index_meta));
        return index;
    }
    default:
        return Status::NotSupported("unsupported table type for vector search");
    }
}

static std::unique_ptr<ScalarIndexProvider> create_scalar_provider(const VectorSearchExecutorConfig& config) {
    switch (config.table_type) {
    case TableType::PAIMON:
        // TODO: implement PaimonScalarProvider
        return nullptr;
    case TableType::OLAP:
    case TableType::ICEBERG:
    case TableType::LANCE:
        // TODO: implement StarRocksScalarProvider, IcebergScalarProvider, LanceScalarProvider
        return nullptr;
    default:
        return nullptr;
    }
}

StatusOr<std::unique_ptr<VectorSearchExecutor>> VectorSearchExecutorFactory::create(
        const VectorSearchExecutorConfig& config) {
    ASSIGN_OR_RETURN(auto ann_index, create_ann_index(config));
    auto scalar_provider = create_scalar_provider(config);
    auto strategy = FilterStrategy::create(config.filter_strategy);

    auto executor = std::make_unique<DefaultVectorSearchExecutor>(std::move(ann_index), std::move(scalar_provider),
                                                                  std::move(strategy));
    RETURN_IF_ERROR(executor->init());
    return executor;
}

} // namespace starrocks
