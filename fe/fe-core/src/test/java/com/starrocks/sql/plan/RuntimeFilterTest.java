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

import com.google.common.collect.ImmutableSet;
import com.starrocks.common.FeConstants;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class RuntimeFilterTest extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        connectContext.getSessionVariable().setGlobalRuntimeFilterProbeMinSize(0);
        starRocksAssert.withTable("CREATE TABLE `rf_sort_t` (\n"
                + "  `d` date,\n"
                + "  `k1` varchar(50),\n"
                + "  `k2` varchar(50),\n"
                + "  `p` varchar(50),\n"
                + "  `v` int\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`d`, `k1`, `k2`, `p`)\n"
                + "DISTRIBUTED BY HASH(`d`, `k1`, `k2`, `p`) BUCKETS 16\n"
                + "PROPERTIES (\"replication_num\" = \"1\");");
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

    // A probe runtime filter parked on a node is only useful if that node's operator either
    // evaluates the global bloom filter (Operator::eval_runtime_bloom_filters) ...
    private static final Set<String> BLOOM_FILTER_EVALUATING_NODES = ImmutableSet.of(
            "EXCHANGE", "MERGING-EXCHANGE", "AGGREGATE", "ANALYTIC");

    // ... or consumes a local runtime in-filter (Operator::eval_conjuncts_and_in_filters). The
    // placement seeds localRfWaitingSet, which is what tells the operator whose in-filter to wait
    // for -- so it matters even when the global filter itself is cleared before delivery.
    private static final Set<String> LOCAL_IN_FILTER_CONSUMING_NODES = ImmutableSet.of(
            "HASH", "NESTLOOP", "MERGE", "SELECT", "REPEAT_NODE");

    private static boolean runtimeFilterIsUsableOn(String nodeLabel) {
        // "ScanNode" covers every scan flavour: OlapScanNode, HdfsScanNode, IcebergScanNode, ...
        return nodeLabel.endsWith("ScanNode")
                || BLOOM_FILTER_EVALUATING_NODES.contains(nodeLabel)
                || LOCAL_IN_FILTER_CONSUMING_NODES.contains(nodeLabel);
    }

    private static final Pattern NODE_HEADER = Pattern.compile("^\\s*\\|?-*\\s*\\d+:([A-Za-z][\\w -]*)");

    private static void assertNoUselessRuntimeFilterPlacement(String verbosePlan) {
        String currentNode = "<none>";
        for (String line : verbosePlan.split("\n")) {
            Matcher m = NODE_HEADER.matcher(line);
            if (m.find()) {
                currentNode = m.group(1).trim();
            }
            if (line.contains("probe runtime filters")) {
                String label = currentNode.split("[ (]")[0];
                if (!runtimeFilterIsUsableOn(label)) {
                    fail("runtime filter parked on '" + currentNode + "', whose operator neither "
                            + "evaluates a bloom filter nor consumes a local in-filter:\n" + verbosePlan);
                }
            }
        }
    }

    /**
     * Mismatched integer widths across chained join keys make the planner materialise the implicit
     * widening CAST in a ProjectNode. The runtime filter probing that column must still reach the
     * scan; it used to stop on the projection, which evaluates no runtime filters at all.
     */
    @Test
    public void testRuntimeFilterDescendsPastWideningCast() throws Exception {
        String sql = "select count(*) from\n"
                + "  (select a.t1b as k from test_all_type a join [shuffle] test_all_type b on a.t1b = b.t1c) x\n"
                + "  join [shuffle] test_all_type c on x.k = c.t1d";
        String plan = getVerboseExplain(sql);
        assertNoUselessRuntimeFilterPlacement(plan);
        assertTrue(plan.contains("probe_expr = (CAST("),
                "expected a widening-CAST probe expr to be pushed down, plan was:\n" + plan);
    }

    @Test
    public void testNoRuntimeFilterParkedOnNonEvaluatingNode() throws Exception {
        String[] queries = new String[] {
                "select * from t0 vt1 join [bucket] t0 vt2 on vt1.v1 = vt2.v1"
                        + " join [broadcast] t1 vt3 on vt1.v1 = vt3.v4"
                        + " join [colocate] t0 vt4 on vt1.v1 = vt4.v1",
                "select count(*) from (select a.t1b as k, a.t1c as c from test_all_type a"
                        + " join [shuffle] test_all_type b on a.t1b = b.t1c) x"
                        + " join [shuffle] test_all_type c on x.k = c.t1d",
                "select count(*) from (select v1 from t0 union all select v4 from t1) x"
                        + " join [shuffle] t0 c on x.v1 = c.v1",
                "select count(*) from (select v1, sum(v2) s from t0 group by v1) x"
                        + " join [shuffle] t1 c on x.v1 = c.v4",
        };
        for (String sql : queries) {
            assertNoUselessRuntimeFilterPlacement(getVerboseExplain(sql));
        }
    }

    /**
     * A window whose PARTITION BY matches the join keys lets the filter descend past the ANALYTIC,
     * but the SORT feeding it evaluates no runtime filter. When the exchange below refuses to take
     * the filter -- here because multi-column global runtime filters are switched off -- the filter
     * must climb back to the ANALYTIC rather than sit on the SORT, where it would never be applied.
     */
    @Test
    public void testNoRuntimeFilterParkedOnSort() throws Exception {
        String sql = "select count(*) from"
                + " (select * from (select k1, k2, first_value(k1) over"
                + "   (partition by k1, k2 order by k2) as rz from rf_sort_t) t where rz = 'x') a"
                + " join [shuffle] (select * from (select k1, k2, first_value(k1) over"
                + "   (partition by k1, k2 order by k2) as rz from rf_sort_t) t where rz = 'x') b"
                + " on a.k1 = b.k1 and a.k2 = b.k2";
        boolean prev = connectContext.getSessionVariable().isEnableMultiColumnsOnGlobbalRuntimeFilter();
        try {
            connectContext.getSessionVariable().setEnableMultiColumnsOnGlobbalRuntimeFilter(false);
            assertNoUselessRuntimeFilterPlacement(getVerboseExplain(sql));
        } finally {
            connectContext.getSessionVariable().setEnableMultiColumnsOnGlobbalRuntimeFilter(prev);
        }
    }
}
