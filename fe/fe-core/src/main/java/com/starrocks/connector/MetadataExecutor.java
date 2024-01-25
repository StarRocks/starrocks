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

package com.starrocks.connector;

import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TResultBatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

// TODO(stephen): refactor and encapsulate this component
public class MetadataExecutor {
    private static final Logger LOG = LogManager.getLogger(MetadataExecutor.class);

    public static Coordinator executeDQL(MetadataCollectJob job) {
        ConnectContext context = job.getContext();
        StatementBase parsedStmt = SqlParser.parseOneWithStarRocksDialect(job.getSql(), context.getSessionVariable());
        ExecPlan execPlan = StatementPlanner.plan(parsedStmt, context, job.getSinkType());
        StmtExecutor executor = new StmtExecutor(context, parsedStmt);
        context.setExecutor(executor);
        context.setQueryId(UUIDUtil.genUUID());
        context.getSessionVariable().setEnableMaterializedViewRewrite(false);
        executor.runMetadataStmt(context, execPlan);
        return executor.getCoordinator();
    }

}
