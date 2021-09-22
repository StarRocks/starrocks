// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

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
class Pipeline;
using PipelinePtr = std::shared_ptr<Pipeline>;
using Pipelines = std::vector<PipelinePtr>;
class PipelineDriver;
using DriverPtr = std::shared_ptr<PipelineDriver>;
using DriverRawPtr = PipelineDriver*;
using Drivers = std::vector<DriverPtr>;
class DriverDispatcher;
using DriverDispatcherPtr = std::shared_ptr<DriverDispatcher>;
class GlobalDriverDispatcher;
class ExecStateReporter;
using QueryPromise = std::promise<void>;
using QueryFuture = std::future<void>;
using QueryPromisePtr = std::shared_ptr<QueryPromise>;

} // namespace starrocks::pipeline
