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

#include <vector>

#include "compute_env/profile_report_task.h"

namespace starrocks::pipeline {

class QueryContextManager;

} // namespace starrocks::pipeline

namespace starrocks {

std::vector<PipeLineReportTaskKey> report_pipeline_fragments(
        pipeline::QueryContextManager* query_context_mgr,
        const std::vector<PipeLineReportTaskKey>& pipeline_need_report_query_fragment_ids);

} // namespace starrocks
