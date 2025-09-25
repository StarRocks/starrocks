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

package com.starrocks.sql;

import com.starrocks.common.FeConstants;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.sql.analyzer.PlannerMetaLocker;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TPartialUpdateMode;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StatementPlannerTest extends PlanTestBase {

    @Test
    public void testDeferLock() throws Exception {
        {
            FeConstants.runningUnitTest = true;
            String sql = "insert into t0 select * from t0";
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            PlannerMetaLocker locker = new PlannerMetaLocker(connectContext, stmt);
            assertTrue(StatementPlanner.analyzeStatement(stmt, connectContext, locker));
        }

        {
            FeConstants.runningUnitTest = false;
            String sql = "insert into t0 select * from t0";
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            PlannerMetaLocker locker = new PlannerMetaLocker(connectContext, stmt);
            assertFalse(StatementPlanner.analyzeStatement(stmt, connectContext, locker));
        }

        {
            FeConstants.runningUnitTest = true;
            String sql = "submit task as insert into t0 select * from t0";
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            PlannerMetaLocker locker = new PlannerMetaLocker(connectContext, stmt);
            assertTrue(StatementPlanner.analyzeStatement(stmt, connectContext, locker));
        }
    }

    @Test
    public void testInsertPartialUpdateMode() throws Exception {
        {
            FeConstants.runningUnitTest = true;
            connectContext.getSessionVariable().setPartialUpdateMode("column");
            String sql = "insert into tprimary_multi_cols (pk, v1) values (1, '1')";
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            InsertPlanner planner = new InsertPlanner();
            ExecPlan plan = planner.plan((InsertStmt) stmt, connectContext);
            assertEquals(TPartialUpdateMode.COLUMN_UPSERT_MODE, getPartialUpdateMode(plan));
        }

        {
            FeConstants.runningUnitTest = true;
            connectContext.getSessionVariable().setPartialUpdateMode("auto");
            String sql = "insert into tprimary_multi_cols (pk, v1) values (1, '1')";
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            InsertPlanner planner = new InsertPlanner();
            ExecPlan plan = planner.plan((InsertStmt) stmt, connectContext);
            assertEquals(TPartialUpdateMode.COLUMN_UPSERT_MODE, getPartialUpdateMode(plan));
        }

        {
            FeConstants.runningUnitTest = true;
            connectContext.getSessionVariable().setPartialUpdateMode("auto");
            String sql = "insert into tprimary_multi_cols (pk, v1, v2) values (1, '1', 1)";
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            InsertPlanner planner = new InsertPlanner();
            ExecPlan plan = planner.plan((InsertStmt) stmt, connectContext);
            assertEquals(TPartialUpdateMode.AUTO_MODE, getPartialUpdateMode(plan));
        }
    }

    private TPartialUpdateMode getPartialUpdateMode(ExecPlan plan) {
        PlanFragment fragment = plan.getFragments().get(0);
        OlapTableSink sink = (OlapTableSink) fragment.getSink();
        return sink.getPartialUpdateMode();
    }
}