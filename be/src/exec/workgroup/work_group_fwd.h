// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <memory>

namespace starrocks::pipeline {
class DriverQueue;
}

namespace starrocks::workgroup {

class WorkGroup;
using WorkGroupPtr = std::shared_ptr<WorkGroup>;

class ScanTaskQueue;

template <typename Q>
class WorkGroupSchedEntity;
using WorkGroupDriverSchedEntity = WorkGroupSchedEntity<pipeline::DriverQueue>;
using WorkGroupScanSchedEntity = WorkGroupSchedEntity<ScanTaskQueue>;

class WorkGroupManager;
class ScanExecutor;

struct RunningQueryToken;
using RunningQueryTokenPtr = std::unique_ptr<RunningQueryToken>;

} // namespace starrocks::workgroup
