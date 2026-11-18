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

import com.starrocks.planner.PaimonScanNode;
import com.starrocks.planner.RuntimeFilterDescription;
import com.starrocks.planner.ScanNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

public class PaimonTopNRuntimeFilterTest extends ConnectorPlanTestBase {
    private static final String TABLE = "paimon0.pmn_db1.unpartitioned_table";

    @ParameterizedTest
    @ValueSource(strings = {"asc nulls first", "asc nulls last", "desc nulls first", "desc nulls last"})
    public void testFilteredTopN(String order) throws Exception {
        String sql = "select pk, d from " + TABLE + " where d like '%2' order by d " + order + ", pk limit 1, 3";
        assertFilter(sql, RuntimeFilterDescription.RuntimeFilterType.TOPN_FILTER);
        assertVerbosePlanContains(sql, "NON-PARTITION PREDICATES:", "TOP-N", "probe runtime filters:");
    }

    @Test
    public void testPartitionedTopN() throws Exception {
        assertFilter("select pk from paimon0.pmn_db1.partitioned_table " +
                        "where pt >= '2020-01-01' and d like '%2' order by pk desc limit 3",
                RuntimeFilterDescription.RuntimeFilterType.TOPN_FILTER);
    }

    @Test
    public void testDisableTopNFilter() throws Exception {
        assertNoFilter("select /*+ set_var(enable_topn_runtime_filter=false) */ pk from " + TABLE +
                " where d like '%2' order by pk limit 3");
    }

    @Test
    public void testUnorderedLimit() throws Exception {
        assertNoFilter("select pk from " + TABLE + " where d like '%2' limit 3");
    }

    @Test
    public void testSortWithoutLimit() throws Exception {
        assertNoFilter("select pk from " + TABLE + " order by pk");
    }

    @Test
    public void testGroupKeyTopN() throws Exception {
        assertFilter("select /*+ set_var(new_planner_agg_stage=2) */ d, pk, count(*) from " + TABLE +
                        " where d like '%2' group by d, pk order by pk desc limit 3",
                RuntimeFilterDescription.RuntimeFilterType.TOPN_FILTER);
    }

    @Test
    public void testAggregateResultCannotFilterInput() throws Exception {
        assertNoFilter("select /*+ set_var(new_planner_agg_stage=2) */ pk, count(*) as n from " + TABLE +
                " group by pk order by n desc limit 3");
    }

    @Test
    public void testAggregateInFilter() throws Exception {
        assertFilter("select /*+ set_var(new_planner_agg_stage=2, agg_in_filter_limit=1024) */ pk, count(*) from " +
                        TABLE + " where d like '%2' group by pk limit 3",
                RuntimeFilterDescription.RuntimeFilterType.AGG_IN_FILTER);
    }

    private void assertFilter(String sql, RuntimeFilterDescription.RuntimeFilterType type) throws Exception {
        List<ScanNode> scans = getExecPlan(sql).getScanNodes();
        Assertions.assertEquals(1, scans.size());
        Assertions.assertInstanceOf(PaimonScanNode.class, scans.get(0));
        Assertions.assertTrue(scans.get(0).getProbeRuntimeFilters().stream()
                .anyMatch(filter -> filter.runtimeFilterType() == type), sql);
        // The accepted filter must survive plan serialization to the backend.
        Assertions.assertTrue(scans.get(0).treeToThrift().nodes.get(0).isSetProbe_runtime_filters());
    }

    private void assertNoFilter(String sql) throws Exception {
        List<ScanNode> scans = getExecPlan(sql).getScanNodes();
        Assertions.assertEquals(1, scans.size());
        Assertions.assertTrue(scans.get(0).getProbeRuntimeFilters().isEmpty(), sql);
    }
}
