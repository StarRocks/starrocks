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


package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class MvUtilsTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        Config.alter_scheduler_interval_millisecond = 1;
        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        String dbName = "test";
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);

        connectContext.getSessionVariable().setMaxTransformReorderJoins(8);
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(30000);
        connectContext.getSessionVariable().setEnableReplicationJoin(false);
        starRocksAssert.withTable("CREATE TABLE `t0` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `t1` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
    }

    @Test
    public void testGetAllPredicate() {
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        ColumnRefOperator columnRef1 = columnRefFactory.create("col1", Type.INT, false);
        ColumnRefOperator columnRef2 = columnRefFactory.create("col2", Type.INT, false);
        ColumnRefOperator columnRef3 = columnRefFactory.create("col3", Type.INT, false);
        BinaryPredicateOperator binaryPredicate = new BinaryPredicateOperator(
                BinaryType.EQ, columnRef1, columnRef2);

        Database db = starRocksAssert.getCtx().getGlobalStateMgr().getDb("test");
        Table table1 = db.getTable("t0");
        LogicalScanOperator scanOperator1 = new LogicalOlapScanOperator(table1);
        BinaryPredicateOperator binaryPredicate2 = new BinaryPredicateOperator(
                BinaryType.GE, columnRef1, ConstantOperator.createInt(1));
        scanOperator1.setPredicate(binaryPredicate2);
        OptExpression scanExpr = OptExpression.create(scanOperator1);
        Table table2 = db.getTable("t1");
        LogicalScanOperator scanOperator2 = new LogicalOlapScanOperator(table2);
        BinaryPredicateOperator binaryPredicate3 = new BinaryPredicateOperator(
                BinaryType.GE, columnRef2, ConstantOperator.createInt(1));
        scanOperator2.setPredicate(binaryPredicate3);
        OptExpression scanExpr2 = OptExpression.create(scanOperator2);
        LogicalJoinOperator joinOperator = new LogicalJoinOperator(JoinOperator.INNER_JOIN, binaryPredicate);
        OptExpression joinExpr = OptExpression.create(joinOperator, scanExpr, scanExpr2);
        List<ScalarOperator> predicates = MvUtils.getAllPredicates(joinExpr);
        Assert.assertEquals(3, predicates.size());
        Assert.assertTrue(MvUtils.isAllEqualInnerOrCrossJoin(joinExpr));
        LogicalJoinOperator joinOperator2 = new LogicalJoinOperator(JoinOperator.LEFT_OUTER_JOIN, binaryPredicate);
        OptExpression joinExpr2 = OptExpression.create(joinOperator2, scanExpr, scanExpr2);
        Assert.assertFalse(MvUtils.isAllEqualInnerOrCrossJoin(joinExpr2));
        OptExpression joinExpr3 = OptExpression.create(joinOperator, scanExpr, joinExpr2);
        Assert.assertFalse(MvUtils.isAllEqualInnerOrCrossJoin(joinExpr3));

        LogicalJoinOperator joinOperator3 = new LogicalJoinOperator(JoinOperator.INNER_JOIN,
                Utils.compoundAnd(binaryPredicate, binaryPredicate2));
        OptExpression joinExpr4 = OptExpression.create(joinOperator3, scanExpr, scanExpr2);
        Assert.assertFalse(MvUtils.isAllEqualInnerOrCrossJoin(joinExpr4));

        BinaryPredicateOperator binaryPredicate4 = new BinaryPredicateOperator(
                BinaryType.EQ, columnRef1, columnRef3);
        LogicalJoinOperator joinOperator4 = new LogicalJoinOperator(JoinOperator.INNER_JOIN,
                Utils.compoundAnd(binaryPredicate, binaryPredicate4));
        OptExpression joinExpr5 = OptExpression.create(joinOperator4, scanExpr, scanExpr2);
        Assert.assertTrue(MvUtils.isAllEqualInnerOrCrossJoin(joinExpr5));

        LogicalJoinOperator joinOperator5 = new LogicalJoinOperator(JoinOperator.INNER_JOIN,
                Utils.compoundOr(binaryPredicate, binaryPredicate4));
        OptExpression joinExpr6 = OptExpression.create(joinOperator5, scanExpr, scanExpr2);
        Assert.assertFalse(MvUtils.isAllEqualInnerOrCrossJoin(joinExpr6));
    }

    @Test
    public void testGetCompensationPredicateForDisjunctive() {
        ConstantOperator alwaysTrue = ConstantOperator.TRUE;
        ConstantOperator alwaysFalse = ConstantOperator.createBoolean(false);
        CompoundPredicateOperator compound = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.OR, alwaysFalse, alwaysTrue);
        Assert.assertEquals(alwaysTrue, MvUtils.getCompensationPredicateForDisjunctive(alwaysTrue, compound));
        Assert.assertEquals(alwaysFalse, MvUtils.getCompensationPredicateForDisjunctive(alwaysFalse, compound));
        Assert.assertEquals(null, MvUtils.getCompensationPredicateForDisjunctive(compound, alwaysFalse));
        Assert.assertEquals(alwaysTrue, MvUtils.getCompensationPredicateForDisjunctive(compound, compound));
    }

    @Test
    public void testCanonizePredicate() {
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        ColumnRefOperator columnRef1 = columnRefFactory.create("col1", Type.INT, false);
        ColumnRefOperator columnRef2 = columnRefFactory.create("col2", Type.INT, false);
        BinaryPredicateOperator binaryPredicate = new BinaryPredicateOperator(
                BinaryType.GT, columnRef1, ConstantOperator.createInt(1));
        BinaryPredicateOperator binaryPredicate2 = new BinaryPredicateOperator(
                BinaryType.GE, columnRef1, ConstantOperator.createInt(2));
        ScalarOperator canonizedPredicate = MvUtils.canonizePredicateForRewrite(binaryPredicate);
        Assert.assertEquals(binaryPredicate2, canonizedPredicate);
        BinaryPredicateOperator binaryPredicate3 = new BinaryPredicateOperator(
                BinaryType.LT, columnRef2, ConstantOperator.createInt(1));
        ScalarOperator canonizedPredicate2 = MvUtils.canonizePredicateForRewrite(binaryPredicate3);
        BinaryPredicateOperator binaryPredicate4 = new BinaryPredicateOperator(
                BinaryType.LE, columnRef2, ConstantOperator.createInt(0));
        Assert.assertEquals(binaryPredicate4, canonizedPredicate2);

        CompoundPredicateOperator compound1 = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND, binaryPredicate, binaryPredicate3);
        CompoundPredicateOperator compound2 = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND, binaryPredicate2, binaryPredicate4);
        ScalarOperator canonizedPredicate3 = MvUtils.canonizePredicateForRewrite(compound1);
        Assert.assertEquals(compound2, canonizedPredicate3);

        CompoundPredicateOperator compound3 = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.OR, binaryPredicate, binaryPredicate3);
        CompoundPredicateOperator compound4 = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.OR, binaryPredicate2, binaryPredicate4);
        ScalarOperator canonizedPredicate4 = MvUtils.canonizePredicateForRewrite(compound3);
        Assert.assertEquals(compound4, canonizedPredicate4);

        CompoundPredicateOperator compound5 = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.NOT, binaryPredicate);
        ScalarOperator canonizedPredicate5 = MvUtils.canonizePredicateForRewrite(compound5);
        BinaryPredicateOperator binaryPredicate5 = new BinaryPredicateOperator(
                BinaryType.LE, columnRef1, ConstantOperator.createInt(1));
        Assert.assertEquals(binaryPredicate5, canonizedPredicate5);
    }
}
