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

import com.starrocks.sql.optimizer.base.ColumnIdentifier;
import com.starrocks.sql.optimizer.statistics.ColumnMinMaxMgr;
import com.starrocks.sql.optimizer.statistics.IMinMaxStatsMgr;
import com.starrocks.sql.optimizer.statistics.StatsVersion;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class AggregateMetaTest extends PlanTestBase {

    @Test
    public void testAggregateMinMaxMeta() throws Exception {
        new MockUp<ColumnMinMaxMgr>() {
            @Mock
            public Optional<IMinMaxStatsMgr.ColumnMinMax> getStats(ColumnIdentifier identifier, StatsVersion version) {
                return Optional.of(new IMinMaxStatsMgr.ColumnMinMax("1", "200"));
            }
        };

        connectContext.getSessionVariable().setEnableRewriteSimpleAggToMetaScan(true);
        String sql = "SELECT MAX(v2), MIN(v3) FROM t0";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "  1:Project\n"
                + "  |  <slot 4> : 200\n"
                + "  |  <slot 5> : 1\n"
                + "  |  \n"
                + "  0:UNION\n"
                + "     constant exprs: \n"
                + "         NULL");
    }
}
