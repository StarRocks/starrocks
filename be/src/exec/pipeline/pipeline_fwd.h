// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <memory>
#include <vector>
namespace starrocks {
namespace pipeline {
class QueryContext;
using QueryContextPtr = std::shared_ptr<QueryContext>;
class FragmentContext;
using FragmentContextPtr = std::shared_ptr<FragmentContext>;
class FragmentExecutor;
class Pipeline;
using PipelinePtr = std::shared_ptr<Pipeline>;
using Pipelines = std::vector<PipelinePtr>;
class PipelineDriver;
using DriverPtr = std::shared_ptr<PipelineDriver>;
using Drivers = std::vector<DriverPtr>;
class DriverDispatcher;
using DriverDispatcherPtr = std::shared_ptr<DriverDispatcher>;
class GlobalDriverDispatcher;
class ExecStateReporter;
} // namespace pipeline
} // namespace starrocks
