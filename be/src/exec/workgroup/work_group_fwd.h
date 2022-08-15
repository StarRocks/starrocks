// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <memory>

namespace starrocks::workgroup {

class WorkGroup;
class WorkGroupManager;
class ScanExecutor;

using WorkGroupPtr = std::shared_ptr<WorkGroup>;

// Two types of scan executor: OlapScan and HdfsScan
enum ScanExecutorType {
    TypeOlapScanExecutor,
    TypeConnectorScanExecutor,
};

} // namespace starrocks::workgroup
