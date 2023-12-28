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

package com.starrocks.connector.odps;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.split.InputSplit;
import com.aliyun.odps.table.read.split.impl.IndexedInputSplit;
import com.aliyun.odps.table.read.split.impl.RowRangeInputSplit;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OdpsTable;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.logical.LogicalOdpsScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOdpsScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.rule.implementation.OdpsScanImplementationRule;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;
import com.starrocks.thrift.TResultSinkType;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;

public class OdpsScanTest extends MockedBase {

    static ColumnRefOperator intColumnOperator = new ColumnRefOperator(1, Type.INT, "id", true);
    static ColumnRefOperator strColumnOperator = new ColumnRefOperator(2, Type.STRING, "name", true);

    static Map<ColumnRefOperator, Column> scanColumnMap = new HashMap<>() {
        {
            put(intColumnOperator, new Column("id", Type.INT));
            put(strColumnOperator, new Column("name", Type.STRING));
        }
    };

    static OdpsTable odpsTable;
    static LogicalOdpsScanOperator logicalOdpsScanOperator;
    static PhysicalOdpsScanOperator physicalOdpsScanOperator;
    static OdpsScanImplementationRule odpsRule = new OdpsScanImplementationRule();

    @Mocked
    static OptimizerContext optimizerContext;

    @BeforeClass
    public static void setUp() throws OdpsException, IOException {
        initMock();
        odpsTable = new OdpsTable("catalog", table);
        logicalOdpsScanOperator = new LogicalOdpsScanOperator(odpsTable,
                scanColumnMap, Maps.newHashMap(), -1,
                new BinaryPredicateOperator(BinaryType.EQ,
                        new ColumnRefOperator(1, Type.INT, "id", true),
                        ConstantOperator.createInt(1)));
        OptExpression scan = new OptExpression(logicalOdpsScanOperator);
        List<OptExpression> transform = odpsRule.transform(scan, optimizerContext);
        physicalOdpsScanOperator = (PhysicalOdpsScanOperator) transform.get(0).getOp();
    }

    @Test
    public void testPhysicalOdpsScanOperator() {
        ScanOperatorPredicates scanOperatorPredicates = physicalOdpsScanOperator.getScanOperatorPredicates();
        Assert.assertNotNull(scanOperatorPredicates);
        ColumnRefSet usedColumns = physicalOdpsScanOperator.getUsedColumns();
        Assert.assertNotNull(usedColumns);
        Assert.assertEquals(2, usedColumns.size());
    }

    @Test
    public void testPlanFragmentBuilder(@Mocked com.starrocks.qe.ConnectContext connectContext,
                                        @Mocked ColumnRefFactory columnRefFactory) {
        when(odpsSplitsInfo.getSplits()).thenReturn(ImmutableList.of(new RowRangeInputSplit("sessionId", 0, 2)));
        when(odpsSplitsInfo.getSplitPolicy()).thenReturn(OdpsSplitsInfo.SplitPolicy.ROW_OFFSET);
        OptExpression phys = new OptExpression(physicalOdpsScanOperator);
        ExecPlan plan = PlanFragmentBuilder.createPhysicalPlan(phys, connectContext,
                physicalOdpsScanOperator.getOutputColumns(), columnRefFactory,
                ImmutableList.of("id"), TResultSinkType.FILE, true);
        Assert.assertNotNull(plan);
        Assert.assertEquals("id", plan.getColNames().get(0));
    }

    @Test
    public void testLogicalOdpsScanOperatorBuilder() {
        LogicalOdpsScanOperator.Builder builder = new LogicalOdpsScanOperator.Builder();
        LogicalOdpsScanOperator cloneOperator = builder.withOperator(logicalOdpsScanOperator).build();
        Assert.assertEquals(logicalOdpsScanOperator, cloneOperator);
    }

    @Test
    public void testOdpsSplitsInfo(@Mocked TableBatchReadSession session) {
        OdpsSplitsInfo splitsInfo =
                new OdpsSplitsInfo(ImmutableList.of(new IndexedInputSplit("sessionId", 1)), session,
                        OdpsSplitsInfo.SplitPolicy.SIZE, null);
        OdpsSplitsInfo.SplitPolicy splitPolicy = splitsInfo.getSplitPolicy();
        String serializeSession = splitsInfo.getSerializeSession();
        List<InputSplit> splits = splitsInfo.getSplits();
        Assert.assertEquals(OdpsSplitsInfo.SplitPolicy.SIZE, splitPolicy);
        Assert.assertEquals(1, splits.size());
        Assert.assertNotNull(serializeSession);
    }
}
