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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.LanceTable;
import com.starrocks.planner.LanceScanNode;
import com.starrocks.qe.scheduler.dag.JobSpec;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalLanceScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLanceScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.thrift.THdfsFileFormat;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TQueryType;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.type.IntegerType;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class LancePhysicalPlanTest extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
    }

    private ExecPlan createPlan() {
        ColumnRefFactory factory = new ColumnRefFactory();
        ColumnRefOperator id = factory.create("id", IntegerType.INT, true);
        Column column = new Column("id", IntegerType.INT);
        LanceTable table = new LanceTable(20001, "vectors", List.of(column), "file:///tmp/vectors.lance");
        BinaryPredicateOperator predicate = new BinaryPredicateOperator(BinaryType.GT, id,
                ConstantOperator.createInt(10));
        LogicalLanceScanOperator logical = new LogicalLanceScanOperator(table, Map.of(id, column),
                Map.of(column, id), 3, predicate);
        // Populated side channels must not duplicate the complete predicate in the scan's Thrift.
        logical.getScanOperatorPredicates().getNonPartitionConjuncts().add(predicate);
        logical.getScanOperatorPredicates().getNoEvalPartitionConjuncts().add(predicate);
        logical.getScanOperatorPredicates().getMinMaxConjuncts().add(predicate);
        OptExpression physical = OptExpression.create(new PhysicalLanceScanOperator(logical));
        physical.setStatistics(Statistics.builder().setOutputRowCount(3)
                .addColumnStatistic(id, ColumnStatistic.unknown()).build());
        // Exercise fragment translation directly: this series layer intentionally has no SQL entry point yet.
        return PlanFragmentBuilder.createPhysicalPlan(physical, connectContext, List.of(id), factory,
                List.of("id"), TResultSinkType.MYSQL_PROTOCAL, false);
    }

    @Test
    public void testPhysicalScanPreservesProjectionPredicateLimitAndRange() {
        ExecPlan plan = createPlan();
        Assertions.assertEquals(1, plan.getFragments().size());
        Assertions.assertEquals(1, plan.getScanNodes().size());
        LanceScanNode scan = (LanceScanNode) plan.getScanNodes().get(0);
        Assertions.assertSame(scan, plan.getFragments().get(0).getPlanRoot());
        Assertions.assertEquals(1, scan.getDesc().getSlots().size());
        Assertions.assertEquals("id", scan.getDesc().getSlots().get(0).getColumn().getName());
        Assertions.assertTrue(scan.getDesc().getSlots().get(0).isMaterialized());
        TPlanNode thrift = scan.treeToThrift().getNodes().get(0);
        Assertions.assertEquals(3, thrift.getLimit());
        Assertions.assertEquals(1, thrift.getConjunctsSize());
        Assertions.assertFalse(thrift.getHdfs_scan_node().isSetPartition_conjuncts());
        Assertions.assertFalse(thrift.getHdfs_scan_node().isSetMin_max_conjuncts());
        Assertions.assertEquals("lance", thrift.getConnector_scan_node().getConnector_name());
        Assertions.assertEquals("vectors", thrift.getHdfs_scan_node().getTable_name());
        Assertions.assertTrue(thrift.getHdfs_scan_node().getSql_predicates().contains("id > 10"));
        Assertions.assertEquals(1, scan.getScanRangeLocations(0).size());
        Assertions.assertFalse(scan.getScanRangeLocations(0).get(0).getScan_range()
                .getHdfs_scan_range().isSetUse_lance_jni_reader());
        Assertions.assertEquals(THdfsFileFormat.LANCE, scan.getScanRangeLocations(0).get(0).getScan_range()
                .getHdfs_scan_range().getFile_format());
        Assertions.assertFalse(scan.getScanRangeLocations(0).get(0).getScan_range()
                .getHdfs_scan_range().isSetLance_split_info());
    }

    @Test
    public void testMissingWorkersIsReportedAsPlannerError() {
        new MockUp<LanceScanNode>() {
            @Mock
            public List<Long> getAllAvailableBackendOrComputeIds() {
                return List.of();
            }
        };
        StarRocksPlannerException error = Assertions.assertThrows(StarRocksPlannerException.class, this::createPlan);
        Assertions.assertTrue(error.getMessage().contains("No alive backend or compute node for Lance scan"));
    }
    @Test
    public void testConnectorScanDisablesSingleNodeParallelSchedule() {
        boolean original = connectContext.getSessionVariable().enableSingleNodeSchedule();
        try {
            connectContext.getSessionVariable().setEnableSingleNodeSchedule(true);
            ExecPlan plan = createPlan();
            JobSpec job = JobSpec.Factory.fromQuerySpec(connectContext, plan.getFragments(), plan.getScanNodes(),
                    plan.getDescTbl().toThrift(), TQueryType.SELECT, plan);
            Assertions.assertFalse(job.supportSingleNodeParallelSchedule());
        } finally {
            connectContext.getSessionVariable().setEnableSingleNodeSchedule(original);
        }
    }

}
