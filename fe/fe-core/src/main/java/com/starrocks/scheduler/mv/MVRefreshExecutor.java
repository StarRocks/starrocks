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
package com.starrocks.scheduler.mv;

import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.plan.ExecPlan;

/**
 * MVRefreshExecutor is an interface that defines the contract for executing MV refresh operations.
 */
public interface MVRefreshExecutor {
    /**
     * Refresh the materialized view based on the provided execution plan and insert statement.
     * @param execPlan the execution plan that contains the necessary information for refreshing the materialized view
     * @param insertStmt the insert statement that specifies how to insert data into the materialized view
     * @throws Exception if an error occurs during the refresh process
     */
    void executePlan(ExecPlan execPlan, InsertStmt insertStmt) throws Exception;
}
