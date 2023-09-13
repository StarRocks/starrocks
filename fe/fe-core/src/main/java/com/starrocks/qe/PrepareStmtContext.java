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

package com.starrocks.qe;

import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.plan.ExecPlan;

public class PrepareStmtContext {
    private final PrepareStmt stmt;
    private final ConnectContext connectContext;
    private final ExecPlan execPlan;

    public PrepareStmtContext(PrepareStmt stmt, ConnectContext connectContext, ExecPlan execPlan) {
        this.stmt = stmt;
        this.connectContext = connectContext;
        this.execPlan = execPlan;
    }

    public PrepareStmt getStmt() {
        return stmt;
    }

    public ConnectContext getConnectContext() {
        return connectContext;
    }

    public ExecPlan getExecPlan() {
        return execPlan;
    }
}
