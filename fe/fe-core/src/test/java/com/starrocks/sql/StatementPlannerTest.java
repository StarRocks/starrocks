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
import com.starrocks.sql.analyzer.PlannerMetaLocker;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StatementPlannerTest extends PlanTestBase {

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

}