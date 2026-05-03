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

public class IcebergTopNRuntimeFilterTest extends ConnectorPlanTestBase {
    @Test
    public void testTopNOnGroupKeyAggBuildsRuntimeFilter() throws Exception {
        String sql = "select id, sum(c1) from iceberg0.unpartitioned_db.t_numeric " +
                "group by id order by id limit 10";
        String plan = getVerboseExplain(sql);

        assertContains(plan, "IcebergScanNode");
        assertContains(plan, "streaming preaggregation mode: force_preaggregation");
        assertContains(plan, "probe runtime filters");
    }

    @Test
    public void testTopNOnGroupKeyAggThroughAliasProject() throws Exception {
        String sql = "select id as k, sum(c1) from iceberg0.unpartitioned_db.t_numeric " +
                "group by id order by k limit 10";
        String plan = getVerboseExplain(sql);

        assertContains(plan, "IcebergScanNode");
        assertContains(plan, "streaming preaggregation mode: force_preaggregation");
        assertContains(plan, "probe runtime filters");
    }

    @Test
    public void testTopNOnGroupKeyAggRequiresFullGroupKey() throws Exception {
        String sql = "select id, c1, sum(c2) from iceberg0.unpartitioned_db.t_numeric " +
                "group by id, c1 order by id limit 10";
        String plan = getVerboseExplain(sql);

        assertContains(plan, "IcebergScanNode");
        assertNotContains(plan, "streaming preaggregation mode: force_preaggregation");
    }

    @Test
    public void testTopNOnGroupKeyAggDoesNotMoveNonColumnRefProject() throws Exception {
        String sql = "select id as k, 1 / sum(c1) as ratio from iceberg0.unpartitioned_db.t_numeric " +
                "group by id order by k limit 10";
        String plan = getVerboseExplain(sql);

        assertContains(plan, "IcebergScanNode");
        assertNotContains(plan, "streaming preaggregation mode: force_preaggregation");
    }
}
