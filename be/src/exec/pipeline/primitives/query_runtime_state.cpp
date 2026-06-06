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

#include "exec/pipeline/primitives/query_runtime_state.h"

namespace starrocks::pipeline {

void QueryRuntimeState::update_operator_exec_stats(const OperatorExecStatsSnapshot& snapshot) {
    if (!snapshot.valid) {
        return;
    }
    if (snapshot.require_registered_plan_node && !need_record_exec_stats(snapshot.plan_node_id)) {
        return;
    }

    if (snapshot.update_push_rows) {
        update_push_rows_stats(snapshot.plan_node_id, snapshot.push_rows);
    }
    if (snapshot.update_pull_rows) {
        if (snapshot.force_set_pull_rows) {
            force_set_pull_rows_stats(snapshot.plan_node_id, snapshot.pull_rows);
        } else {
            update_pull_rows_stats(snapshot.plan_node_id, snapshot.pull_rows);
        }
    }
    if (snapshot.update_pred_filter_rows) {
        update_pred_filter_stats(snapshot.plan_node_id, snapshot.pred_filter_rows);
    }
    if (snapshot.update_rf_filter_rows) {
        update_rf_filter_stats(snapshot.plan_node_id, snapshot.rf_filter_rows);
    }
}

} // namespace starrocks::pipeline
