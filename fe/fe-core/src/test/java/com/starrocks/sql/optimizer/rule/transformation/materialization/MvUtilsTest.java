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

import com.google.common.collect.Range;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Operator;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.Set;

import static com.starrocks.sql.optimizer.operator.OpRuleBit.OP_PARTITION_PRUNED;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.MvPartitionCompensator.convertToDateRange;

public class MvUtilsTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
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

        Database db = starRocksAssert.getCtx().getGlobalStateMgr().getLocalMetastore().getDb("test");
        Table table1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "t0");
        LogicalScanOperator scanOperator1 = new LogicalOlapScanOperator(table1);
        BinaryPredicateOperator binaryPredicate2 = new BinaryPredicateOperator(
                BinaryType.GE, columnRef1, ConstantOperator.createInt(1));
        scanOperator1.setPredicate(binaryPredicate2);
        OptExpression scanExpr = OptExpression.create(scanOperator1);
        Table table2 = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "t1");
        LogicalScanOperator scanOperator2 = new LogicalOlapScanOperator(table2);
        BinaryPredicateOperator binaryPredicate3 = new BinaryPredicateOperator(
                BinaryType.GE, columnRef2, ConstantOperator.createInt(1));
        scanOperator2.setPredicate(binaryPredicate3);
        OptExpression scanExpr2 = OptExpression.create(scanOperator2);
        LogicalJoinOperator joinOperator = new LogicalJoinOperator(JoinOperator.INNER_JOIN, binaryPredicate);
        OptExpression joinExpr = OptExpression.create(joinOperator, scanExpr, scanExpr2);
        Set<ScalarOperator> predicates = MvUtils.getAllValidPredicates(joinExpr);
        Assertions.assertEquals(3, predicates.size());
        Assertions.assertTrue(MvUtils.isAllEqualInnerOrCrossJoin(joinExpr));
        LogicalJoinOperator joinOperator2 = new LogicalJoinOperator(JoinOperator.LEFT_OUTER_JOIN, binaryPredicate);
        OptExpression joinExpr2 = OptExpression.create(joinOperator2, scanExpr, scanExpr2);
        Assertions.assertFalse(MvUtils.isAllEqualInnerOrCrossJoin(joinExpr2));
        OptExpression joinExpr3 = OptExpression.create(joinOperator, scanExpr, joinExpr2);
        Assertions.assertFalse(MvUtils.isAllEqualInnerOrCrossJoin(joinExpr3));

        LogicalJoinOperator joinOperator3 = new LogicalJoinOperator(JoinOperator.INNER_JOIN,
                Utils.compoundAnd(binaryPredicate, binaryPredicate2));
        OptExpression joinExpr4 = OptExpression.create(joinOperator3, scanExpr, scanExpr2);
        Assertions.assertFalse(MvUtils.isAllEqualInnerOrCrossJoin(joinExpr4));

        BinaryPredicateOperator binaryPredicate4 = new BinaryPredicateOperator(
                BinaryType.EQ, columnRef1, columnRef3);
        LogicalJoinOperator joinOperator4 = new LogicalJoinOperator(JoinOperator.INNER_JOIN,
                Utils.compoundAnd(binaryPredicate, binaryPredicate4));
        OptExpression joinExpr5 = OptExpression.create(joinOperator4, scanExpr, scanExpr2);
        Assertions.assertTrue(MvUtils.isAllEqualInnerOrCrossJoin(joinExpr5));

        LogicalJoinOperator joinOperator5 = new LogicalJoinOperator(JoinOperator.INNER_JOIN,
                Utils.compoundOr(binaryPredicate, binaryPredicate4));
        OptExpression joinExpr6 = OptExpression.create(joinOperator5, scanExpr, scanExpr2);
        Assertions.assertFalse(MvUtils.isAllEqualInnerOrCrossJoin(joinExpr6));
    }

    @Test
    public void testGetCompensationPredicateForDisjunctive() {
        ConstantOperator alwaysTrue = ConstantOperator.TRUE;
        ConstantOperator alwaysFalse = ConstantOperator.createBoolean(false);
        CompoundPredicateOperator compound = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.OR, alwaysFalse, alwaysTrue);
        Assertions.assertEquals(alwaysTrue, MvUtils.getCompensationPredicateForDisjunctive(alwaysTrue, compound));
        Assertions.assertEquals(alwaysFalse, MvUtils.getCompensationPredicateForDisjunctive(alwaysFalse, compound));
        Assertions.assertEquals(null, MvUtils.getCompensationPredicateForDisjunctive(compound, alwaysFalse));
        Assertions.assertEquals(alwaysTrue, MvUtils.getCompensationPredicateForDisjunctive(compound, compound));
    }

    @Test
    public void testConvertToDateRange() throws AnalysisException {
        {
            LocalDate date1 = LocalDate.of(2025, 10, 10);
            PartitionKey upper = PartitionKey.ofDate(date1);
            Range<PartitionKey> upRange = Range.atMost(upper);
            Range<PartitionKey> upResult = convertToDateRange(upRange);
            Assertions.assertTrue(upResult.hasUpperBound());
            Assertions.assertTrue(upResult.upperEndpoint().getTypes().get(0).isDateType());
            Assertions.assertTrue(upResult.upperEndpoint().getKeys().get(0) instanceof DateLiteral);
            DateLiteral date = (DateLiteral) upResult.upperEndpoint().getKeys().get(0);
            Assertions.assertEquals(2025, date.getYear());
            Assertions.assertEquals(10, date.getMonth());
            Assertions.assertEquals(10, date.getDay());
            Assertions.assertEquals(0, date.getHour());
        }
        {
            LocalDate date1 = LocalDate.of(2025, 10, 1);
            PartitionKey lower = PartitionKey.ofDate(date1);
            Range<PartitionKey> lowRange = Range.atLeast(lower);
            Range<PartitionKey> lowResult = convertToDateRange(lowRange);
            Assertions.assertTrue(lowResult.hasLowerBound());
            Assertions.assertTrue(lowResult.lowerEndpoint().getTypes().get(0).isDateType());
            Assertions.assertTrue(lowResult.lowerEndpoint().getKeys().get(0) instanceof DateLiteral);
            DateLiteral date = (DateLiteral) lowResult.lowerEndpoint().getKeys().get(0);
            Assertions.assertEquals(2025, date.getYear());
            Assertions.assertEquals(10, date.getMonth());
            Assertions.assertEquals(1, date.getDay());
            Assertions.assertEquals(0, date.getHour());
        }
        {
            LocalDate date1 = LocalDate.of(2025, 10, 1);
            PartitionKey lower = PartitionKey.ofDate(date1);
            LocalDate date2 = LocalDate.of(2025, 10, 10);
            PartitionKey upper = PartitionKey.ofDate(date2);
            Range<PartitionKey> range = Range.atLeast(lower);
            range = range.intersection(Range.atMost(upper));
            Range<PartitionKey> result = convertToDateRange(range);
            Assertions.assertTrue(result.hasLowerBound());
            Assertions.assertTrue(result.lowerEndpoint().getTypes().get(0).isDateType());
            Assertions.assertTrue(result.lowerEndpoint().getKeys().get(0) instanceof DateLiteral);
            DateLiteral date = (DateLiteral) result.lowerEndpoint().getKeys().get(0);
            Assertions.assertEquals(2025, date.getYear());
            Assertions.assertEquals(10, date.getMonth());
            Assertions.assertEquals(1, date.getDay());
            Assertions.assertEquals(0, date.getHour());

            Assertions.assertTrue(result.hasUpperBound());
            Assertions.assertTrue(result.upperEndpoint().getTypes().get(0).isDateType());
            Assertions.assertTrue(result.upperEndpoint().getKeys().get(0) instanceof DateLiteral);
            DateLiteral upperDate = (DateLiteral) result.upperEndpoint().getKeys().get(0);
            Assertions.assertEquals(2025, upperDate.getYear());
            Assertions.assertEquals(10, upperDate.getMonth());
            Assertions.assertEquals(10, upperDate.getDay());
            Assertions.assertEquals(0, upperDate.getHour());
        }
        {
            PartitionKey upper = PartitionKey.ofString("20231010");
            Range<PartitionKey> upRange = Range.atMost(upper);
            Range<PartitionKey> upResult = convertToDateRange(upRange);
            Assertions.assertTrue(upResult.hasUpperBound());
            Assertions.assertTrue(upResult.upperEndpoint().getTypes().get(0).isDateType());
            Assertions.assertTrue(upResult.upperEndpoint().getKeys().get(0) instanceof DateLiteral);
            DateLiteral date = (DateLiteral) upResult.upperEndpoint().getKeys().get(0);
            Assertions.assertEquals(2023, date.getYear());
            Assertions.assertEquals(10, date.getMonth());
            Assertions.assertEquals(10, date.getDay());
            Assertions.assertEquals(0, date.getHour());
        }
        {
            PartitionKey lower = PartitionKey.ofString("20231010");
            Range<PartitionKey> lowRange = Range.atLeast(lower);
            Range<PartitionKey> lowResult = convertToDateRange(lowRange);
            Assertions.assertTrue(lowResult.hasLowerBound());
            Assertions.assertTrue(lowResult.lowerEndpoint().getTypes().get(0).isDateType());
            Assertions.assertTrue(lowResult.lowerEndpoint().getKeys().get(0) instanceof DateLiteral);
            DateLiteral date = (DateLiteral) lowResult.lowerEndpoint().getKeys().get(0);
            Assertions.assertEquals(2023, date.getYear());
            Assertions.assertEquals(10, date.getMonth());
            Assertions.assertEquals(10, date.getDay());
            Assertions.assertEquals(0, date.getHour());
        }
        {
            PartitionKey lower = PartitionKey.ofString("20231010");
            Range<PartitionKey> range = Range.atLeast(lower);
            range = range.intersection(Range.atMost(PartitionKey.ofString("20231020")));
            Range<PartitionKey> result = convertToDateRange(range);
            Assertions.assertTrue(result.hasLowerBound());
            Assertions.assertTrue(result.lowerEndpoint().getTypes().get(0).isDateType());
            Assertions.assertTrue(result.lowerEndpoint().getKeys().get(0) instanceof DateLiteral);
            DateLiteral date = (DateLiteral) result.lowerEndpoint().getKeys().get(0);
            Assertions.assertEquals(2023, date.getYear());
            Assertions.assertEquals(10, date.getMonth());
            Assertions.assertEquals(10, date.getDay());
            Assertions.assertEquals(0, date.getHour());

            Assertions.assertTrue(result.hasUpperBound());
            Assertions.assertTrue(result.upperEndpoint().getTypes().get(0).isDateType());
            Assertions.assertTrue(result.upperEndpoint().getKeys().get(0) instanceof DateLiteral);
            DateLiteral upperDate = (DateLiteral) result.upperEndpoint().getKeys().get(0);
            Assertions.assertEquals(2023, upperDate.getYear());
            Assertions.assertEquals(10, upperDate.getMonth());
            Assertions.assertEquals(20, upperDate.getDay());
            Assertions.assertEquals(0, upperDate.getHour());
        }
    }

    @Test
    public void testResetOpAppliedRule() {
        LogicalScanOperator.Builder builder = new LogicalOlapScanOperator.Builder();
        Operator op = builder.build();
        Assertions.assertFalse(op.isOpRuleBitSet(OP_PARTITION_PRUNED));
        // set
        op.setOpRuleBit(OP_PARTITION_PRUNED);
        Assertions.assertTrue(op.isOpRuleBitSet(OP_PARTITION_PRUNED));
        // reset
        op.resetOpRuleBit(OP_PARTITION_PRUNED);
        Assertions.assertFalse(op.isOpRuleBitSet(OP_PARTITION_PRUNED));
    }
}
