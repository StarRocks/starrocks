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

#include "common/status.h"
#include "storage/index/vector/filter_strategy.h"
#include "storage/index/vector/scalar_index_provider.h"
#include "storage/index/vector/vector_ann_index.h"

namespace starrocks {

struct VectorSearchOutput {
    VectorAnnResult ann_result;
    std::string profile;
};

// VectorSearchExecutor is the top-level entry point (Layer 3) that
// Scanners interact with. It composes VectorAnnIndex (Layer 1) +
// FilterStrategy/ScalarIndexProvider (Layer 2) into an end-to-end
// vector search pipeline.
//
// Lifecycle: create -> init() -> search() [repeatable] -> close()
class VectorSearchExecutor {
public:
    virtual ~VectorSearchExecutor() = default;

    virtual Status init() = 0;
    virtual Status search(const VectorQuery& query, const ScalarPredicate* predicate,
                          VectorSearchOutput* output) = 0;
    virtual Status close() = 0;
};

// DefaultVectorSearchExecutor composes injected components.
// Works for all table types — differences are in the injected
// VectorAnnIndex and ScalarIndexProvider implementations.
class DefaultVectorSearchExecutor final : public VectorSearchExecutor {
public:
    DefaultVectorSearchExecutor(std::unique_ptr<VectorAnnIndex> ann_index,
                                std::unique_ptr<ScalarIndexProvider> scalar_provider,
                                std::unique_ptr<FilterStrategy> strategy)
            : _ann_index(std::move(ann_index)),
              _scalar_provider(std::move(scalar_provider)),
              _strategy(std::move(strategy)) {}

    ~DefaultVectorSearchExecutor() override = default;

    Status init() override { return Status::OK(); }

    Status search(const VectorQuery& query, const ScalarPredicate* predicate, VectorSearchOutput* output) override;

    Status close() override;

    // Access the underlying ANN index.
    VectorAnnIndex* ann_index() { return _ann_index.get(); }

private:
    std::unique_ptr<VectorAnnIndex> _ann_index;
    std::unique_ptr<ScalarIndexProvider> _scalar_provider;
    std::unique_ptr<FilterStrategy> _strategy;
};

} // namespace starrocks
