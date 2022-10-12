// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
using Drivers = std::vector<DriverPtr>;
class OperatorFactory;
using OpFactoryPtr = std::shared_ptr<OperatorFactory>;
using OpFactories = std::vector<OpFactoryPtr>;
class Operator;
using OperatorPtr = std::shared_ptr<Operator>;
using Operators = std::vector<OperatorPtr>;
class DriverExecutor;
using DriverExecutorPtr = std::shared_ptr<DriverExecutor>;
class GlobalDriverExecutor;
class ExecStateReporter;
} // namespace starrocks::pipeline
