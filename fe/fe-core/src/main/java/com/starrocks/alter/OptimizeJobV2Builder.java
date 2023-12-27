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


package com.starrocks.alter;

import com.starrocks.catalog.OlapTable;
import com.starrocks.common.UserException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.OptimizeClause;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class OptimizeJobV2Builder extends AlterJobV2Builder {
    private static final Logger LOG = LogManager.getLogger(OptimizeJobV2Builder.class);

    private final OlapTable table;

    private OptimizeClause optimizeClause;

    public OptimizeJobV2Builder(OlapTable table) {
        this.table = table;
    }

    public OptimizeJobV2Builder withOptimizeClause(OptimizeClause optimizeClause) {
        this.optimizeClause = optimizeClause;
        return this;
    }

    @Override
    public AlterJobV2 build() throws UserException {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        long tableId = table.getId();
        OptimizeJobV2 optimizeJob = new OptimizeJobV2(jobId, dbId, tableId, table.getName(), timeoutMs, optimizeClause);
        return optimizeJob;
    }
}
