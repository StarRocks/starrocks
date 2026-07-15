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
#include <memory>

#include "common/statusor.h"
#include "data_sink/pipeline/pipeline_sink_provider.h"

namespace starrocks::pipeline {

class PipelineSinkRegistry {
public:
    StatusOr<const PipelineSinkProvider*> find(TDataSinkType::type type) const;
    size_t size() const { return _providers.size(); }

private:
    friend class PipelineSinkRegistryBuilder;

    explicit PipelineSinkRegistry(std::map<TDataSinkType::type, PipelineSinkProvider> providers);

    const std::map<TDataSinkType::type, PipelineSinkProvider> _providers;
};

class PipelineSinkRegistryBuilder {
public:
    Status add(PipelineSinkProvider provider);
    StatusOr<std::shared_ptr<const PipelineSinkRegistry>> freeze();

private:
    std::map<TDataSinkType::type, PipelineSinkProvider> _providers;
    std::shared_ptr<const PipelineSinkRegistry> _frozen;
};

} // namespace starrocks::pipeline
