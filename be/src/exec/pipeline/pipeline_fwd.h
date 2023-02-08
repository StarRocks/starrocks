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

#include <future>
#include <memory>
#include <vector>

namespace starrocks::pipeline {
class QueryContext;
using QueryContextPtr = std::shared_ptr<QueryContext>;
class FragmentContext;
using FragmentContextPtr = std::shared_ptr<FragmentContext>;
class FragmentContextManager;
using FragmentContextManagerPtr = std::unique_ptr<FragmentContextManager>;
class FragmentExecutor;
using FragmentPromise = std::promise<void>;
using FragmentFuture = std::future<void>;
class Pipeline;
using PipelinePtr = std::shared_ptr<Pipeline>;
using Pipelines = std::vector<PipelinePtr>;
class PipelineDriver;
using DriverPtr = std::shared_ptr<PipelineDriver>;
using DriverRawPtr = PipelineDriver*;
using DriverConstRawPtr = const PipelineDriver*;
using Drivers = std::vector<DriverPtr>;
class OperatorFactory;
using OpFactoryPtr = std::shared_ptr<OperatorFactory>;
using OpFactories = std::vector<OpFactoryPtr>;
class SourceOperatorFactory;
class Operator;
using OperatorRawPtr = Operator*;
using OperatorPtr = std::shared_ptr<Operator>;
using Operators = std::vector<OperatorPtr>;
class DriverExecutor;
using DriverExecutorPtr = std::shared_ptr<DriverExecutor>;
class GlobalDriverExecutor;
class ExecStateReporter;
} // namespace starrocks::pipeline
