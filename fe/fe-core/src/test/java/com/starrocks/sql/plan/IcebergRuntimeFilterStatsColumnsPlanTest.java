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

import com.starrocks.planner.ExchangeNode;
import com.starrocks.planner.HashJoinNode;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.ScanNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

/**
 * An Iceberg scan learns, while it is set up, which of its columns a runtime filter of an enclosing hash join or
 * top-n may probe, so it can ask for their file bounds before the filters are assigned.
 */
public class IcebergRuntimeFilterStatsColumnsPlanTest extends ConnectorPlanTestBase {

    private static List<IcebergScanNode> icebergScans(ExecPlan plan) {
        return plan.getScanNodes().stream()
                .filter(IcebergScanNode.class::isInstance).map(IcebergScanNode.class::cast).toList();
    }

    private static HashJoinNode findHashJoin(ExecPlan plan) {
        for (PlanFragment fragment : plan.getFragments()) {
            HashJoinNode join = findHashJoin(fragment.getPlanRoot());
            if (join != null) {
                return join;
            }
        }
        return null;
    }

    private static HashJoinNode findHashJoin(PlanNode node) {
        if (node instanceof HashJoinNode join) {
            return join;
        }
        for (PlanNode child : node.getChildren()) {
            HashJoinNode join = findHashJoin(child);
            if (join != null) {
                return join;
            }
        }
        return null;
    }

    private static ScanNode scanUnder(PlanNode node) {
        while (!(node instanceof ScanNode)) {
            Assertions.assertFalse(node instanceof ExchangeNode, "the probe scan is in the join's fragment");
            node = node.getChild(0);
        }
        return (ScanNode) node;
    }

    @Test
    public void testInnerJoinProbeScanGetsJoinKey() throws Exception {
        ExecPlan plan = getExecPlan("select a.data from iceberg0.unpartitioned_db.t0 a " +
                "join [broadcast] iceberg0.unpartitioned_db.t0 b on a.id = b.id where b.data = 'x'");
        ScanNode probe = scanUnder(findHashJoin(plan).getChild(0));
        List<IcebergScanNode> scans = icebergScans(plan);
        Assertions.assertEquals(2, scans.size());
        for (IcebergScanNode scan : scans) {
            Assertions.assertEquals(scan == probe ? Set.of("id") : Set.of(), scan.getRuntimeFilterProbeColumnNames());
        }
    }

    // Only the types the BE builds a file zone map for ask for statistics: a string join key does not.
    @Test
    public void testOnlyZoneMapTypesAreStatsCandidates() throws Exception {
        ExecPlan plan = getExecPlan("select a.id from iceberg0.unpartitioned_db.t0 a " +
                "join [broadcast] iceberg0.unpartitioned_db.t0 b on a.id = b.id and a.data = b.data where b.date = 'x'");
        IcebergScanNode probe = (IcebergScanNode) scanUnder(findHashJoin(plan).getChild(0));
        Assertions.assertEquals(Set.of("id", "data"), probe.getRuntimeFilterProbeColumnNames());
        Set<String> readSchema = Set.of("id", "data", "date");
        Assertions.assertEquals(Set.of("id"), probe.runtimeFilterProbeStatsCandidates(readSchema, Set.of()));
        // columns the min/max optimization already requests are not asked again
        Assertions.assertEquals(Set.of(), probe.runtimeFilterProbeStatsCandidates(readSchema, Set.of("id")));
    }

    // A timestamp without time zone prunes by its bounds; one with time zone does not (its bounds are converted to
    // the session time zone inexactly).
    @Test
    public void testTimestampWithAndWithoutZoneAreStatsCandidates() throws Exception {
        Set<String> readSchema = Set.of("id", "data", "ts");
        for (String table : List.of("t0_year", "t0_year_tz")) {
            ExecPlan plan = getExecPlan("select * from iceberg0.partitioned_transforms_db." + table +
                    " order by ts limit 10");
            IcebergScanNode scan = icebergScans(plan).get(0);
            Assertions.assertEquals(Set.of("ts"), scan.getRuntimeFilterProbeColumnNames(), table);
            Assertions.assertEquals(Set.of("ts"), scan.runtimeFilterProbeStatsCandidates(readSchema, Set.of()), table);
        }
    }

    @Test
    public void testLeftOuterJoinProbesNothing() throws Exception {
        ExecPlan plan = getExecPlan("select a.data from iceberg0.unpartitioned_db.t0 a " +
                "left join [broadcast] iceberg0.unpartitioned_db.t0 b on a.id = b.id");
        for (IcebergScanNode scan : icebergScans(plan)) {
            Assertions.assertEquals(Set.of(), scan.getRuntimeFilterProbeColumnNames());
        }
    }

    @Test
    public void testTopNProbesFirstOrderByColumn() throws Exception {
        ExecPlan plan = getExecPlan("select * from iceberg0.unpartitioned_db.t0 order by id, data limit 10");
        List<IcebergScanNode> scans = icebergScans(plan);
        Assertions.assertEquals(1, scans.size());
        Assertions.assertEquals(Set.of("id"), scans.get(0).getRuntimeFilterProbeColumnNames());

        plan = getExecPlan("select * from iceberg0.unpartitioned_db.t0 order by id");
        Assertions.assertEquals(Set.of(), icebergScans(plan).get(0).getRuntimeFilterProbeColumnNames());

        // no top-n runtime filter is built when it is disabled
        connectContext.getSessionVariable().setEnableTopNRuntimeFilter(false);
        try {
            plan = getExecPlan("select * from iceberg0.unpartitioned_db.t0 order by id, data limit 10");
        } finally {
            connectContext.getSessionVariable().setEnableTopNRuntimeFilter(true);
        }
        Assertions.assertEquals(Set.of(), icebergScans(plan).get(0).getRuntimeFilterProbeColumnNames());
    }
}
