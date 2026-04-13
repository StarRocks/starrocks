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

package com.starrocks.sql.optimizer.rule.mv;

import com.starrocks.common.AnalysisException;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MaterializedViewPlanTest extends PlanTestBase {

    @Test
    public void testCreateIncrementalMVRejected() {
        String sql = "create materialized view rtmv \n" +
                "distributed by hash(v1) " +
                "refresh incremental as " +
                "select v1, count(*) as cnt from t0 join t1 on t0.v1 = t1.v4 group by v1";

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(sql, connectContext));
        Assertions.assertTrue(exception.getMessage().contains(
                "Legacy REFRESH INCREMENTAL materialized views are no longer supported"));
    }

    @Test
    public void testSelectFromBinlogRejected() {
        connectContext.getSessionVariable().setMVPlanner(true);

        SemanticException exception = Assertions.assertThrows(SemanticException.class,
                () -> getFragmentPlan("select * from t0 [_BINLOG_]"));
        Assertions.assertTrue(exception.getMessage().contains("Legacy _BINLOG_ queries are no longer supported"));
    }
}
