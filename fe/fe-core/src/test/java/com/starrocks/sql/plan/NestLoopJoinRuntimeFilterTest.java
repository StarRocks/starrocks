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
import com.starrocks.planner.NestLoopJoinNode;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.RuntimeFilterDescription;
import com.starrocks.planner.ScanNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class NestLoopJoinRuntimeFilterTest extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;

        starRocksAssert.withTable("CREATE TABLE nl_rf_probe (" +
                "c_int int, c_bigint bigint, c_date date, c_datetime datetime, s varchar(20)) " +
                "DUPLICATE KEY(c_int) DISTRIBUTED BY HASH(c_int) BUCKETS 10 " +
                "PROPERTIES (\"replication_num\" = \"1\")");
        starRocksAssert.withTable("CREATE TABLE nl_rf_build (" +
                "c_int int, c_bigint bigint, c_date date, c_datetime datetime, pattern varchar(20)) " +
                "DUPLICATE KEY(c_int) DISTRIBUTED BY HASH(c_int) BUCKETS 3 " +
                "PROPERTIES (\"replication_num\" = \"1\")");
        starRocksAssert.withTable("CREATE TABLE nl_rf_dim (" +
                "c_int int) " +
                "DUPLICATE KEY(c_int) DISTRIBUTED BY HASH(c_int) BUCKETS 3 " +
                "PROPERTIES (\"replication_num\" = \"1\")");

        connectContext.getSessionVariable().setGlobalRuntimeFilterProbeMinSize(0);
    }

    @BeforeEach
    public void before() {
        setTableStatistics(getOlapTable("nl_rf_probe"), 1000000);
        setTableStatistics(getOlapTable("nl_rf_build"), 10);
        setTableStatistics(getOlapTable("nl_rf_dim"), 100);
        connectContext.getSessionVariable().setJoinImplementationMode("auto");
        connectContext.getSessionVariable().setEnableGlobalRuntimeFilter(true);
        connectContext.getSessionVariable().setGlobalRuntimeFilterProbeMinSize(0);
    }

    @AfterEach
    public void after() {
        connectContext.getSessionVariable().setJoinImplementationMode("auto");
        connectContext.getSessionVariable().setEnableGlobalRuntimeFilter(true);
    }

    // 1. Multi-row build with all range predicates.
    @Test
    public void testMultiRowRangePredicatesGenerateGlobalRuntimeFilter() throws Exception {
        assertGlobalRuntimeFilter(query("p.c_int > b.c_int"));
        assertGlobalRuntimeFilter(query("p.c_int >= b.c_int"));
        assertGlobalRuntimeFilter(query("p.c_int < b.c_int"));
        assertGlobalRuntimeFilter(query("p.c_int <= b.c_int"));
    }

    // 2. Range predicates after join reorder.
    @Test
    public void testRangePredicateAfterJoinReorderGeneratesGlobalRuntimeFilter() throws Exception {
        assertGlobalRuntimeFilter(reorderedQuery("b.c_int < p.c_int"));
        assertGlobalRuntimeFilter(reorderedQuery("b.c_int <= p.c_int"));
        assertGlobalRuntimeFilter(reorderedQuery("b.c_int > p.c_int"));
        assertGlobalRuntimeFilter(reorderedQuery("b.c_int >= p.c_int"));
    }

    // 3. Single-row build with all range predicates.
    @Test
    public void testSingleRowBuildGeneratesGlobalRuntimeFilter() throws Exception {
        assertGlobalRuntimeFilter(singleRowQuery("p.c_int > b.c_int"));
        assertGlobalRuntimeFilter(singleRowQuery("p.c_int >= b.c_int"));
        assertGlobalRuntimeFilter(singleRowQuery("p.c_int < b.c_int"));
        assertGlobalRuntimeFilter(singleRowQuery("p.c_int <= b.c_int"));
    }

    // 4. Global runtime filter disabled.
    @Test
    public void testDisableGlobalRuntimeFilterKeepsRuntimeFilterLocal() throws Exception {
        connectContext.getSessionVariable().setEnableGlobalRuntimeFilter(false);
        Assertions.assertNull(findGlobalRuntimeFilter(getExecPlan(query("p.c_int > b.c_int"))));
    }

    // 5. Predicates unsupported by global runtime filters.
    @Test
    public void testUnsupportedPredicatesDoNotGenerateGlobalRuntimeFilter() throws Exception {
        Assertions.assertNull(findGlobalRuntimeFilter(getExecPlan(query("p.c_int != b.c_int"))));
        Assertions.assertNull(findGlobalRuntimeFilter(getExecPlan(query("p.c_int + 1 > b.c_int"))));
        Assertions.assertNull(findGlobalRuntimeFilter(getExecPlan(query("p.s like b.pattern"))));
        Assertions.assertNull(findGlobalRuntimeFilter(getExecPlan(query("p.s > b.pattern"))));
    }

    // 6. Supported range predicate data types.
    @Test
    public void testSupportedTypesGenerateGlobalRuntimeFilter() throws Exception {
        assertGlobalRuntimeFilter(query("p.c_int > b.c_int"));
        assertGlobalRuntimeFilter(query("p.c_bigint > b.c_bigint"));
        assertGlobalRuntimeFilter(query("p.c_date > b.c_date"));
        assertGlobalRuntimeFilter(query("p.c_datetime > b.c_datetime"));
    }

    private void assertGlobalRuntimeFilter(String sql) throws Exception {
        ExecPlan execPlan = getExecPlan(sql);
        RuntimeFilterDescription runtimeFilter = findGlobalRuntimeFilter(execPlan);
        Assertions.assertNotNull(runtimeFilter, sql);
        assertProbeScanHasRuntimeFilter(execPlan, runtimeFilter.getFilterId());
    }

    private RuntimeFilterDescription findGlobalRuntimeFilter(ExecPlan execPlan) {
        for (PlanFragment fragment : execPlan.getFragments()) {
            for (PlanNode node : fragment.collectNodes()) {
                if (!(node instanceof NestLoopJoinNode)) {
                    continue;
                }
                for (RuntimeFilterDescription runtimeFilter : ((NestLoopJoinNode) node).getBuildRuntimeFilters()) {
                    if (runtimeFilter.isHasRemoteTargets()) {
                        return runtimeFilter;
                    }
                }
            }
        }
        return null;
    }

    private void assertProbeScanHasRuntimeFilter(ExecPlan execPlan, int filterId) {
        for (ScanNode scanNode : execPlan.getScanNodes()) {
            if (!(scanNode instanceof OlapScanNode)) {
                continue;
            }
            OlapScanNode olapScanNode = (OlapScanNode) scanNode;
            if (!olapScanNode.getOlapTable().getName().equals("nl_rf_probe")) {
                continue;
            }
            for (RuntimeFilterDescription runtimeFilter : olapScanNode.getProbeRuntimeFilters()) {
                if (runtimeFilter.getFilterId() == filterId) {
                    return;
                }
            }
            throw new AssertionError("Probe scan does not have runtime filter " + filterId);
        }
        throw new AssertionError("No scan node for nl_rf_probe");
    }

    private String query(String predicate) {
        return "select count(*) from " + probeSubquery() + " p join nl_rf_build b on " + predicate;
    }

    private String reorderedQuery(String predicate) {
        return "select count(*) from nl_rf_build b join " + probeSubquery() + " p on " + predicate;
    }

    private String singleRowQuery(String predicate) {
        return "select count(*) from " + probeSubquery() + " p join " +
                "(select max(c_int) as c_int from nl_rf_build) b on " + predicate;
    }

    private String probeSubquery() {
        return "(select p.c_int, p.c_bigint, p.c_date, p.c_datetime, p.s " +
                "from nl_rf_probe p join [shuffle] nl_rf_dim d on p.c_int = d.c_int)";
    }
}
