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
import org.junit.BeforeClass;
import org.junit.Test;

public class RuntimeFilterTest extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        connectContext.getSessionVariable().setGlobalRuntimeFilterProbeMinSize(0);
    }

    @Test
    public void testDeterministicBroadcastJoinForColocateJoin() throws Exception {
        String sql = "select * from \n" +
                "  t0 vt1 join [bucket] t0 vt2 on vt1.v1 = vt2.v1\n" +
                "  join [broadcast] t1 vt3 on vt1.v1 = vt3.v4\n" +
                "  join [colocate] t0 vt4 on vt1.v1 = vt4.v1";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  6:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  equal join conjunct: [1: v1, BIGINT, true] = [7: v4, BIGINT, true]\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 1, build_expr = (7: v4), remote = true\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  |----5:EXCHANGE\n" +
                "  |       distribution type: BROADCAST\n" +
                "  |       cardinality: 1");

    }

    @Test
    public void testDeterministicBroadcastJoinForBroadcastJoin() throws Exception {
        String sql = "select * from \n" +
                "  t0 vt1 join [bucket] t0 vt2 on vt1.v1 = vt2.v1\n" +
                "  join [broadcast] t1 vt3 on vt1.v1 = vt3.v4\n" +
                "  join [broadcast] t0 vt4 on vt1.v1 = vt4.v1";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  |----5:EXCHANGE\n" +
                "  |       distribution type: BROADCAST\n" +
                "  |       cardinality: 1\n" +
                "  |       probe runtime filters:\n" +
                "  |       - filter_id = 2, probe_expr = (7: v4)");
    }
}
