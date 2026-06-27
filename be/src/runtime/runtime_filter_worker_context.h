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
#include <utility>

#include "exec/pipeline/pipeline_fwd.h"
#include "gen_cpp/internal_service.pb.h"

namespace starrocks {

class MemTracker;
struct RuntimeServices;

std::pair<pipeline::QueryContextPtr, std::shared_ptr<MemTracker>> get_runtime_filter_mem_tracker(
        const RuntimeServices* runtime_services, const PUniqueId& query_id, bool is_pipeline);

} // namespace starrocks
