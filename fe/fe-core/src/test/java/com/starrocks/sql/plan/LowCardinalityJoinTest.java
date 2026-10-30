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

import com.starrocks.common.FeConstants;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

// Isolated from LowCardinalityTest2 on purpose: adding a test that plans dict queries mutates the
// mock-dict/session state shared across methods within a class, which perturbs that class's
// absolute-id plan assertions. Surefire runs each class in its own fork (reuseForks=false), so a
// separate class keeps this regression self-contained.
public class LowCardinalityJoinTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withTable("CREATE TABLE `low_card_t1` (\n" +
                "  `d_date` date ,\n" +
                "  `c_user` varchar(50) ,\n" +
                "  `c_dept` varchar(50) ,\n" +
                "  `c_par` varchar(50) ,\n" +
                "  `cpc` int(11) \n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`d_date`, `c_user`, `c_dept`, `c_par`)\n" +
                "DISTRIBUTED BY HASH(`d_date`, `c_user`, `c_dept`, `c_par`) BUCKETS 16 \n" +
                "PROPERTIES (\"replication_num\" = \"1\");");
        FeConstants.USE_MOCK_DICT_MANAGER = true;
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(true);
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeV2(true);
        connectContext.getSessionVariable().setCboCteReuse(false);
    }

    @AfterAll
    public static void afterClass() {
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(false);
        FeConstants.USE_MOCK_DICT_MANAGER = false;
    }

    @Test
    public void testNestLoopJoinOnPredicateNotEncoded() throws Exception {
        FeConstants.runningUnitTest = true;
        // A same-table equality in an outer-join ON clause is not a hash-join key, so it plans as a
        // NestLoopJoin with the equality kept in onPredicate. The LC collector must NOT keep the ON
        // columns dict-encoded for a NestLoopJoin, because DecodeRewriter only rewrites the ON
        // predicate of a PhysicalHashJoinOperator; otherwise the join would reference string refs the
        // dict-encoded scan no longer emits, yielding "Invalid plan: Input dependency cols check failed".
        String sql = "SELECT COUNT(distinct t1.c_user), COUNT(distinct t1.c_dept) FROM low_card_t1 t1 "
                + "LEFT JOIN low_card_t1 t2 ON t1.c_user = t1.c_dept";
        String plan = getVerboseExplain(sql);
        // Must plan without throwing, and the NestLoopJoin ON keeps the columns as VARCHAR (decoded),
        // not INT dict codes.
        assertContains(plan, "NESTLOOP JOIN");
        assertContains(plan, "other join predicates: [2: c_user, VARCHAR, true] = [3: c_dept, VARCHAR, true]");

        // The RIGHT-join / group-by variants must plan as well.
        getVerboseExplain("SELECT COUNT(distinct t2.c_user), COUNT(distinct t2.c_dept) FROM low_card_t1 t1 "
                + "RIGHT JOIN low_card_t1 t2 ON t2.c_user = t2.c_dept");
        getVerboseExplain("SELECT t1.c_user, t1.c_dept, count(1) FROM low_card_t1 t1 "
                + "LEFT JOIN low_card_t1 t2 ON t1.c_user = t1.c_dept GROUP BY t1.c_user, t1.c_dept");

        // A sort-merge join has the same exposure: DecodeRewriter only rewrites hash joins, so its ON
        // columns must stay decoded too.
        String saved = connectContext.getSessionVariable().getJoinImplementationMode();
        connectContext.getSessionVariable().setJoinImplementationMode("merge");
        try {
            String mergePlan = getVerboseExplain("SELECT COUNT(distinct t1.c_user) FROM low_card_t1 t1 "
                    + "JOIN low_card_t1 t2 ON t1.c_user = t2.c_user");
            assertContains(mergePlan, "MERGE JOIN");
            assertContains(mergePlan, "equal join conjunct: [2: c_user, VARCHAR, true] = [7: c_user, VARCHAR, true]");
        } finally {
            connectContext.getSessionVariable().setJoinImplementationMode(saved);
        }
    }
}
