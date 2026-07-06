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

#include <cstdint>

namespace starrocks::pipeline {

struct OperatorExecStatsSnapshot {
    static OperatorExecStatsSnapshot ignored() {
        OperatorExecStatsSnapshot snapshot;
        snapshot.valid = false;
        return snapshot;
    }

    bool valid = true;
    bool require_registered_plan_node = false;
    bool update_push_rows = false;
    bool update_pull_rows = false;
    bool force_set_pull_rows = false;
    bool update_pred_filter_rows = false;
    bool update_rf_filter_rows = false;
    int32_t plan_node_id = -1;
    int64_t push_rows = 0;
    int64_t pull_rows = 0;
    int64_t pred_filter_rows = 0;
    int64_t rf_filter_rows = 0;
};

} // namespace starrocks::pipeline
