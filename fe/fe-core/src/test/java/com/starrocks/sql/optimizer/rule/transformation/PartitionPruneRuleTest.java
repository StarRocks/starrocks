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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.optimizer.Memo;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PartitionPruneRuleTest {
    @Test
    public void transform1(@Mocked OlapTable olapTable, @Mocked RangePartitionInfo partitionInfo) {
        FeConstants.runningUnitTest = true;
        Partition part1 = new Partition(1, 11, "p1", null, null);
        Partition part2 = new Partition(2, 22, "p2", null, null);
        Partition part3 = new Partition(3, 33, "p3", null, null);
        Partition part4 = new Partition(4, 44, "p4", null, null);
        Partition part5 = new Partition(5, 55, "p5", null, null);

        List<Column> columns = Lists.newArrayList(
                new Column("dealDate", Type.DATE, false)
        );

        List<ColumnId> columnNames = Lists.newArrayList(ColumnId.create(columns.get(0).getName()));

        Map<Long, Range<PartitionKey>> keyRange = Maps.newHashMap();

        PartitionKey p1 = new PartitionKey();
        p1.pushColumn(new DateLiteral(2019, 11, 1), PrimitiveType.DATE);

        PartitionKey p2 = new PartitionKey();
        p2.pushColumn(new DateLiteral(2020, 2, 1), PrimitiveType.DATE);

        PartitionKey p3 = new PartitionKey();
        p3.pushColumn(new DateLiteral(2020, 5, 1), PrimitiveType.DATE);

        PartitionKey p4 = new PartitionKey();
        p4.pushColumn(new DateLiteral(2020, 8, 1), PrimitiveType.DATE);

        PartitionKey p5 = new PartitionKey();
        p5.pushColumn(new DateLiteral(2020, 11, 1), PrimitiveType.DATE);

        PartitionKey p6 = new PartitionKey();
        p6.pushColumn(new DateLiteral(2021, 2, 1), PrimitiveType.DATE);

        keyRange.put(1L, Range.closed(p1, p2));
        keyRange.put(2L, Range.closed(p2, p3));
        keyRange.put(3L, Range.closed(p3, p4));
        keyRange.put(4L, Range.closed(p4, p5));
        keyRange.put(5L, Range.closed(p5, p6));

        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        ColumnRefOperator column1 = columnRefFactory.create("dealDate", ScalarType.DATE, false);
        Map<ColumnRefOperator, Column> scanColumnMap = Maps.newHashMap();
        scanColumnMap.put(column1, new Column("dealDate", Type.DATE, false));
        Map<Column, ColumnRefOperator> scanMetaColMap = Maps.newHashMap();
        scanMetaColMap.put(new Column("dealDate", Type.DATE, false), column1);
        BinaryPredicateOperator binaryPredicateOperator1 =
                new BinaryPredicateOperator(BinaryType.GE, column1,
                        ConstantOperator.createDate(LocalDateTime.of(2020, 6, 1, 0, 0, 0)));
        BinaryPredicateOperator binaryPredicateOperator2 =
                new BinaryPredicateOperator(BinaryType.LE, column1,
                        ConstantOperator.createDate(LocalDateTime.of(2020, 12, 1, 0, 0, 0)));
        ScalarOperator predicate = Utils.compoundAnd(binaryPredicateOperator1, binaryPredicateOperator2);
        LogicalOlapScanOperator operator =
                new LogicalOlapScanOperator(olapTable, scanColumnMap, scanMetaColMap, null, -1, predicate);
        operator.setPredicate(null);

        new Expectations() {
            {
                olapTable.getPartitionInfo();
                result = partitionInfo;

                partitionInfo.isRangePartition();
                result = true;

                partitionInfo.getIdToRange(false);
                result = keyRange;

                partitionInfo.getPartitionColumns((Map<ColumnId, Column>) any);
                result = columns;

                olapTable.getPartitions();
                result = Lists.newArrayList(part1, part2, part3, part4, part5);
                minTimes = 0;

                olapTable.getPartition(1);
                result = part1;
                minTimes = 0;
                olapTable.getPartition(2);
                result = part2;
                minTimes = 0;
                olapTable.getPartition(3);
                result = part3;
                minTimes = 0;
                olapTable.getPartition(4);
                result = part4;
                minTimes = 0;
                olapTable.getPartition(5);
                result = part5;
                minTimes = 0;
            }
        };

        PartitionPruneRule rule = new PartitionPruneRule();

        assertNull(operator.getSelectedPartitionId());
        OptExpression optExpression =
                rule.transform(new OptExpression(operator), new OptimizerContext(new Memo(), columnRefFactory)).get(0);

        assertEquals(3, ((LogicalOlapScanOperator) optExpression.getOp()).getSelectedPartitionId().size());
    }

    @Test
    public void transform2(@Mocked OlapTable olapTable, @Mocked RangePartitionInfo partitionInfo) {
        FeConstants.runningUnitTest = true;
        Partition part1 = new Partition(1, 11, "p1", null, null);
        Partition part2 = new Partition(2, 22, "p2", null, null);
        Partition part3 = new Partition(3, 33, "p3", null, null);
        Partition part4 = new Partition(4, 44, "p4", null, null);
        Partition part5 = new Partition(5, 55, "p5", null, null);

        List<Column> columns = Lists.newArrayList(
                new Column("dealDate", Type.DATE, false),
                new Column("main_brand_id", Type.INT, false)
        );
        Map<Long, Range<PartitionKey>> keyRange = Maps.newHashMap();

        PartitionKey p1 = new PartitionKey();
        p1.pushColumn(new DateLiteral(2019, 11, 1), PrimitiveType.DATE);
        p1.pushColumn(new IntLiteral(100), PrimitiveType.INT);

        PartitionKey p2 = new PartitionKey();
        p2.pushColumn(new DateLiteral(2020, 2, 1), PrimitiveType.DATE);
        p2.pushColumn(new IntLiteral(200), PrimitiveType.INT);

        PartitionKey p3 = new PartitionKey();
        p3.pushColumn(new DateLiteral(2020, 5, 1), PrimitiveType.DATE);
        p3.pushColumn(new IntLiteral(300), PrimitiveType.INT);

        PartitionKey p4 = new PartitionKey();
        p4.pushColumn(new DateLiteral(2020, 8, 1), PrimitiveType.DATE);
        p4.pushColumn(new IntLiteral(400), PrimitiveType.INT);

        PartitionKey p5 = new PartitionKey();
        p5.pushColumn(new DateLiteral(2020, 11, 1), PrimitiveType.DATE);
        p5.pushColumn(new IntLiteral(500), PrimitiveType.INT);

        PartitionKey p6 = new PartitionKey();
        p6.pushColumn(new DateLiteral(2021, 2, 1), PrimitiveType.DATE);
        p6.pushColumn(new IntLiteral(600), PrimitiveType.INT);

        keyRange.put(1L, Range.closed(p1, p2));
        keyRange.put(2L, Range.closed(p2, p3));
        keyRange.put(3L, Range.closed(p3, p4));
        keyRange.put(4L, Range.closed(p4, p5));
        keyRange.put(5L, Range.closed(p5, p6));

        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        ColumnRefOperator column1 = columnRefFactory.create("dealDate", ScalarType.DATE, false);
        ColumnRefOperator column2 = columnRefFactory.create("main_brand_id", ScalarType.INT, false);
        Map<ColumnRefOperator, Column> scanColumnMap = Maps.newHashMap();
        scanColumnMap.put(column1, new Column("dealDate", Type.DATE, false));
        scanColumnMap.put(column2, new Column("main_brand_id", Type.INT, false));
        Map<Column, ColumnRefOperator> scanMetaColMap = Maps.newHashMap();
        scanMetaColMap.put(new Column("dealDate", Type.DATE, false), column1);
        scanMetaColMap.put(new Column("main_brand_id", Type.INT, false), column2);
        BinaryPredicateOperator binaryPredicateOperator1 =
                new BinaryPredicateOperator(BinaryType.GE, column1,
                        ConstantOperator.createDate(LocalDateTime.of(2020, 8, 1, 0, 0, 0)));
        BinaryPredicateOperator binaryPredicateOperator2 =
                new BinaryPredicateOperator(BinaryType.LE, column1,
                        ConstantOperator.createDate(LocalDateTime.of(2020, 12, 1, 0, 0, 0)));
        BinaryPredicateOperator binaryPredicateOperator3 =
                new BinaryPredicateOperator(BinaryType.GE, column2,
                        ConstantOperator.createInt(150));
        BinaryPredicateOperator binaryPredicateOperator4 =
                new BinaryPredicateOperator(BinaryType.LE, column2,
                        ConstantOperator.createInt(150));
        ScalarOperator predicate =
                Utils.compoundAnd(binaryPredicateOperator1, binaryPredicateOperator2, binaryPredicateOperator3,
                        binaryPredicateOperator4);
        LogicalOlapScanOperator operator =
                new LogicalOlapScanOperator(olapTable, scanColumnMap, scanMetaColMap, null, -1, predicate);

        new Expectations() {
            {
                olapTable.getPartitionInfo();
                result = partitionInfo;

                partitionInfo.isRangePartition();
                result = true;

                partitionInfo.getIdToRange(false);
                result = keyRange;

                partitionInfo.getPartitionColumns((Map<ColumnId, Column>) any);
                result = columns;

                olapTable.getPartitions();
                result = Lists.newArrayList(part1, part2, part3, part4, part5);
                minTimes = 0;

                olapTable.getPartition(1);
                result = part1;
                minTimes = 0;

                olapTable.getPartition(2);
                result = part2;
                minTimes = 0;

                olapTable.getPartition(3);
                result = part3;
                minTimes = 0;

                olapTable.getPartition(4);
                result = part4;
                minTimes = 0;

                olapTable.getPartition(5);
                result = part5;
                minTimes = 0;
            }
        };

        PartitionPruneRule rule = new PartitionPruneRule();

        assertNull(operator.getSelectedPartitionId());
        OptExpression optExpression =
                rule.transform(new OptExpression(operator), new OptimizerContext(new Memo(), columnRefFactory)).get(0);

        assertEquals(3, ((LogicalOlapScanOperator) optExpression.getOp()).getSelectedPartitionId().size());
    }

    @Test
    public void transformForSingleItemListPartition(@Mocked OlapTable olapTable,
                                                    @Mocked ListPartitionInfo partitionInfo)
            throws AnalysisException {
        FeConstants.runningUnitTest = true;
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        ColumnRefOperator column = columnRefFactory.create("province", ScalarType.STRING, false);
        Map<ColumnRefOperator, Column> scanColumnMap = Maps.newHashMap();
        scanColumnMap.put(column, new Column("province", Type.STRING, false));
        Map<Column, ColumnRefOperator> columnMetaToColRefMap = new HashMap<>();
        columnMetaToColRefMap.put(new Column(column.getName(), column.getType()),
                new ColumnRefOperator(1, column.getType(), column.getName(), false));

        BinaryPredicateOperator binaryPredicateOperator =
                new BinaryPredicateOperator(BinaryType.EQ, column,
                        ConstantOperator.createVarchar("guangdong"));
        ScalarOperator predicate = Utils.compoundAnd(binaryPredicateOperator);

        LogicalOlapScanOperator operator =
                new LogicalOlapScanOperator(olapTable, scanColumnMap, columnMetaToColRefMap, null, -1, predicate);

        Partition part1 = new Partition(10001L, 10003L, "p1", null, null);
        Partition part2 = new Partition(10002L, 10004L, "p2", null, null);

        List<LiteralExpr> p1 = Lists.newArrayList(
                new PartitionValue("guangdong").getValue(Type.STRING),
                new PartitionValue("shanghai").getValue(Type.STRING));

        List<LiteralExpr> p2 = Lists.newArrayList(
                new PartitionValue("beijing").getValue(Type.STRING),
                new PartitionValue("chongqing").getValue(Type.STRING));

        Map<Long, List<LiteralExpr>> literalExprValues = new HashMap<>();
        literalExprValues.put(10001L, p1);
        literalExprValues.put(10002L, p2);

        List<ColumnId> partitionColumns = Lists.newArrayList(ColumnId.create("province"));

        new Expectations() {
            {
                olapTable.getPartitionInfo();
                result = partitionInfo;

                partitionInfo.getType();
                result = PartitionType.LIST;

                partitionInfo.getLiteralExprValues();
                result = literalExprValues;

                olapTable.getPartitions();
                result = Lists.newArrayList(part1, part2);
                minTimes = 0;

                partitionInfo.getPartitionColumns((Map<ColumnId, Column>) any);
                result = Lists.newArrayList(new Column("province", Type.STRING, false));
                minTimes = 0;

                partitionInfo.getPartitionIds(false);
                result = Lists.newArrayList(10001L, 10002L);

                olapTable.getPartition(10001L);
                result = part1;
                minTimes = 0;

                olapTable.getPartition(10002L);
                result = part2;
                minTimes = 0;
            }
        };

        PartitionPruneRule rule = new PartitionPruneRule();
        assertNull(operator.getSelectedPartitionId());
        OptExpression optExpression =
                rule.transform(new OptExpression(operator), new OptimizerContext(new Memo(), columnRefFactory)).get(0);

        List<Long> selectPartitionIds = ((LogicalOlapScanOperator) optExpression.getOp()).getSelectedPartitionId();
        assertEquals(1, selectPartitionIds.size());
        long actual = selectPartitionIds.get(0);
        assertEquals(10001L, actual);
    }

    @Test
    public void transformForSingleItemListPartitionWithTemp(@Mocked OlapTable olapTable,
                                                            @Mocked ListPartitionInfo partitionInfo)
            throws AnalysisException {
        FeConstants.runningUnitTest = true;
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        ColumnRefOperator column = columnRefFactory.create("province", ScalarType.STRING, false);
        Map<ColumnRefOperator, Column> scanColumnMap = Maps.newHashMap();
        scanColumnMap.put(column, new Column("province", Type.STRING, false));
        Map<Column, ColumnRefOperator> columnMetaToColRefMap = new HashMap<>();
        columnMetaToColRefMap.put(new Column(column.getName(), column.getType()),
                new ColumnRefOperator(1, column.getType(), column.getName(), false));

        PartitionNames partitionNames = new PartitionNames(true, Lists.newArrayList("p1", "p2"));
        LogicalOlapScanOperator operator =
                new LogicalOlapScanOperator(olapTable, scanColumnMap, columnMetaToColRefMap,
                        null, -1, null, olapTable.getBaseIndexId(),
                        null, partitionNames, false, Lists.newArrayList(), Lists.newArrayList(), null, false);

        Partition part1 = new Partition(10001L, 10003L, "p1", null, null);
        Partition part2 = new Partition(10002L, 10004L, "p2", null, null);

        List<LiteralExpr> p1 = Lists.newArrayList(
                new PartitionValue("guangdong").getValue(Type.STRING),
                new PartitionValue("shanghai").getValue(Type.STRING));

        List<LiteralExpr> p2 = Lists.newArrayList(
                new PartitionValue("beijing").getValue(Type.STRING),
                new PartitionValue("chongqing").getValue(Type.STRING));

        Map<Long, List<LiteralExpr>> literalExprValues = new HashMap<>();
        literalExprValues.put(10001L, p1);
        literalExprValues.put(10002L, p2);

        List<ColumnId> partitionColumns = Lists.newArrayList(ColumnId.create("province"));

        new Expectations() {
            {
                olapTable.getPartitionInfo();
                result = partitionInfo;

                partitionInfo.getType();
                result = PartitionType.LIST;

                partitionInfo.getLiteralExprValues();
                result = literalExprValues;

                olapTable.getPartitions();
                result = Lists.newArrayList(part1, part2);
                minTimes = 0;

                partitionInfo.getPartitionColumns((Map<ColumnId, Column>) any);
                result = Lists.newArrayList(new Column("province", Type.STRING, false));
                minTimes = 0;

                partitionInfo.getPartitionIds(true);
                result = Lists.newArrayList(10001L);
                minTimes = 0;

                olapTable.getPartition(10001L);
                result = part1;
                minTimes = 0;

                olapTable.getPartition(10002L);
                result = part2;
                minTimes = 0;

                olapTable.getPartition("p1", true);
                result = part1;
                minTimes = 0;

                olapTable.getPartition("p2", true);
                result = null;
                minTimes = 0;
            }
        };

        PartitionPruneRule rule = new PartitionPruneRule();
        assertNull(operator.getSelectedPartitionId());
        OptExpression optExpression =
                rule.transform(new OptExpression(operator), new OptimizerContext(new Memo(), columnRefFactory)).get(0);

        List<Long> selectPartitionIds = ((LogicalOlapScanOperator) optExpression.getOp()).getSelectedPartitionId();
        assertEquals(1, selectPartitionIds.size());
        long actual = selectPartitionIds.get(0);
        assertEquals(10001L, actual);
    }

    @Test
    public void testOlapPartitionScanLimit() throws Exception {
        PseudoCluster.getOrCreateWithRandomPort(true, 1);
        Connection connection = PseudoCluster.getInstance().getQueryConnection();
        Statement stmt = connection.createStatement();
        try {
            stmt.execute("create database olap_partition_scan_limit_test_db");
            stmt.execute("use olap_partition_scan_limit_test_db");
            stmt.execute("CREATE TABLE olap_partition_scan_limit_test_table " +
                    "(`a` varchar(65533),`b` varchar(65533),`ds` date) ENGINE=OLAP " +
                    "DUPLICATE KEY(`a`) PARTITION BY RANGE(`ds`)" +
                    "(START (\"2024-09-20\") END (\"2024-09-27\") EVERY (INTERVAL 1 DAY))" +
                    "DISTRIBUTED BY HASH(`a`)" +
                    "PROPERTIES (\"replication_num\" = \"1\")");
            stmt.execute("insert into olap_partition_scan_limit_test_table(a,b,ds) " +
                    "values('1','a','2024-09-20'),('2','a','2024-09-21')," +
                    "('3','a','2024-09-22'),('4','a','2024-09-23'),('5','a','2024-09-24')," +
                    "('6','a','2024-09-25'),('7','a','2024-09-26')");
            //check default value 0
            stmt.execute("select count(*) from olap_partition_scan_limit_test_table where ds>='2024-09-22';");
            if (stmt.getResultSet().next()) {
                int count = stmt.getResultSet().getInt(1);
                Assert.assertEquals(count, 5);
            }
            //check set value -1
            stmt.execute("set scan_olap_partition_num_limit=-1;");
            stmt.execute("select count(*) from olap_partition_scan_limit_test_table where ds>='2024-09-22';");
            if (stmt.getResultSet().next()) {
                int count = stmt.getResultSet().getInt(1);
                Assert.assertEquals(count, 5);
            }
            //check set value 3
            stmt.execute("set scan_olap_partition_num_limit=3;");
            try {
                stmt.execute("select count(*) from olap_partition_scan_limit_test_table where ds>='2024-09-22';");
            } catch (Exception e) {
                String exp = "Exceeded the limit of number of olap table partitions to be scanned. Number of partitions " +
                        "allowed: 3, number of partitions to be scanned: 5. Please adjust the SQL or " +
                        "change the limit by set variable scan_olap_partition_num_limit.";
                Assert.assertTrue(e.getMessage().contains(exp));
            }
            //check set invalid value abc
            try {
                stmt.execute("set scan_olap_partition_num_limit=abc;");
            } catch (Exception e) {
                String exp = "Incorrect argument type to variable 'scan_olap_partition_num_limit'";
                Assert.assertTrue(e.getMessage().contains(exp));
            }
        } finally {
            stmt.close();
            connection.close();
            PseudoCluster.getInstance().shutdown(true);
        }
    }

}
