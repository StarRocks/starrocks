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

package com.starrocks.sql.plan;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class AutoFillColumnStatisticsTest extends PlanTestBase {
    @Test
    public void testCountWithScanPredicateReuseDisabled() throws Exception {
        testCountWithSplitScanPredicate("select /*+ SET_VAR(enable_scan_predicate_expr_reuse=false) */ " +
                "count(*) from t0 where (v1 = 1 or v2 = 2) and v2 + v3 > 10");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "select count(*) from t0 where (v1 = 1 or v2 = 2) and v2 + v3 > 10",
            "select count(*) from tarray where (v1 = 1 or v2 = 2) and v3[1] + v2 > 10",
            "select count(*) from tarray where (v1 = 1 or v2 = 2) and array_length(v3) + v2 > 10"
    })
    public void testCountWithSplitScanPredicate(String sql) throws Exception {
        double selectRatioThreshold = connectContext.getSessionVariable().getSelectRatioThreshold();
        boolean predicateExprReuse = connectContext.getSessionVariable().isEnablePredicateExprReuse();
        boolean syncMaterializedViewRewrite = connectContext.getSessionVariable().isEnableSyncMaterializedViewRewrite();
        try {
            connectContext.getSessionVariable().setSelectRatioThreshold(-1);
            connectContext.getSessionVariable().setEnablePredicateExprReuse(true);
            connectContext.getSessionVariable().setEnableSyncMaterializedViewRewrite(false);
            String plan = getCostExplain(sql);
            assertContains(plan, "UNION", "auto_fill_col");
        } finally {
            connectContext.getSessionVariable().setSelectRatioThreshold(selectRatioThreshold);
            connectContext.getSessionVariable().setEnablePredicateExprReuse(predicateExprReuse);
            connectContext.getSessionVariable().setEnableSyncMaterializedViewRewrite(syncMaterializedViewRewrite);
        }
    }
}
