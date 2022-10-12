// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "agent/status.h"

namespace starrocks {

class TReportRequest;
class TMasterResult;

AgentStatus report_task(const TReportRequest& request, TMasterResult* result);

} // namespace starrocks
