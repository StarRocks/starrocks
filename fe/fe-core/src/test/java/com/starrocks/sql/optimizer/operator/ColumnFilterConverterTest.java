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


package com.starrocks.sql.optimizer.operator;

import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.ExpressionRangePartitionInfoV2;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.planner.PartitionColumnFilter;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ColumnFilterConverterTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.dynamic_partition_enable = false;
        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE `bill_detail` (\n" +
                        "  `bill_code` varchar(200) NOT NULL DEFAULT \"\" COMMENT \"\"\n" +
                        ") ENGINE=OLAP \n" +
                        "PRIMARY KEY(`bill_code`)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "PARTITION BY RANGE(CAST(substring(bill_code, 3, 13) AS BIGINT))\n" +
                        "(PARTITION p1 VALUES [(\"0\"), (\"5000000\")),\n" +
                        "PARTITION p2 VALUES [(\"5000000\"), (\"10000000\")),\n" +
                        "PARTITION p3 VALUES [(\"10000000\"), (\"15000000\")),\n" +
                        "PARTITION p4 VALUES [(\"15000000\"), (\"20000000\")))\n" +
                        "DISTRIBUTED BY HASH(`bill_code`) BUCKETS 10 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");");
    }

    @Test
    public void convertColumnFilterNormal() {
        ScalarOperator root1 = new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(1, Type.INT, "age", true),
                ConstantOperator.createInt(1));

        ScalarOperator root2 = new InPredicateOperator(new ColumnRefOperator(2, Type.INT, "name", true),
                ConstantOperator.createVarchar("1"),
                ConstantOperator.createVarchar("2"),
                ConstantOperator.createVarchar("3"),
                ConstantOperator.createVarchar("4"));

        ScalarOperator root3 = new IsNullPredicateOperator(new ColumnRefOperator(3, Type.BOOLEAN, "sex", true));

        ScalarOperator root4 = ConstantOperator.createBoolean(true);

        ScalarOperator root5 = new BinaryPredicateOperator(BinaryType.EQ,
                ConstantOperator.createInt(2),
                ConstantOperator.createInt(1));

        ScalarOperator root6 = new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(4, Type.INT, "value1", true),
                new ColumnRefOperator(5, Type.INT, "value2", true));

        List<ScalarOperator> list = Lists.newArrayList(root1, root2, root3, root4, root5, root6);

        Map<String, PartitionColumnFilter> result = ColumnFilterConverter.convertColumnFilter(list);

        assertEquals(3, result.size());

        assertTrue(result.containsKey("age"));
        assertTrue(result.containsKey("name"));
        assertTrue(result.containsKey("sex"));

        assertEquals(new IntLiteral(1), result.get("age").getLowerBound());
        assertEquals(new IntLiteral(1), result.get("age").getUpperBound());

        assertEquals(4, result.get("name").getInPredicateLiterals().size());
        assertEquals(new StringLiteral("1"), result.get("name").getInPredicateLiterals().get(0));
        assertEquals(new StringLiteral("2"), result.get("name").getInPredicateLiterals().get(1));
        assertEquals(new StringLiteral("3"), result.get("name").getInPredicateLiterals().get(2));
        assertEquals(new StringLiteral("4"), result.get("name").getInPredicateLiterals().get(3));

        assertEquals(new NullLiteral(), result.get("sex").getLowerBound());
        assertEquals(new NullLiteral(), result.get("sex").getUpperBound());
    }

    @Test
    public void testIsNullOnCastColumn() {
        {
            // cast(c0 as smallint) is null.
            IsNullPredicateOperator isNullPredicate = new IsNullPredicateOperator(false,
                    new CastOperator(Type.SMALLINT, new ColumnRefOperator(1, Type.INT, "c0", true)));
            List<ScalarOperator> list = Lists.newArrayList(isNullPredicate);
            Map<String, PartitionColumnFilter> result = ColumnFilterConverter.convertColumnFilter(list);
            assertEquals(result.size(), 0);
        }
        {
            // c0 is null.
            IsNullPredicateOperator isNullPredicate =
                    new IsNullPredicateOperator(false, new ColumnRefOperator(1, Type.INT, "c0", true));
            List<ScalarOperator> list = Lists.newArrayList(isNullPredicate);
            Map<String, PartitionColumnFilter> result = ColumnFilterConverter.convertColumnFilter(list);
            assertEquals(result.size(), 1);
            PartitionColumnFilter filter = result.get("c0");
            assertEquals(filter.getLowerBound(), new NullLiteral());
            assertEquals(filter.getUpperBound(), new NullLiteral());
        }
    }

    @Test
    public void convertColumnFilterExpr() {
        List<ScalarOperator> list = buildOperator("day", BinaryType.EQ);
        Map<String, PartitionColumnFilter> result = ColumnFilterConverter.convertColumnFilter(list);
        assertEquals(0, result.size());

        OlapTable olapTable = buildOlapTable("day");
        Map<String, PartitionColumnFilter> result1 = ColumnFilterConverter.convertColumnFilter(list, olapTable);
        assertEquals(1, result1.size());
    }

    @Test
    public void convertColumnFilterExprBinaryType() {
        List<ScalarOperator> listEq = buildOperator("day", BinaryType.EQ);
        OlapTable olapTable = buildOlapTable("day");
        Map<String, PartitionColumnFilter> resultEq = ColumnFilterConverter.convertColumnFilter(listEq, olapTable);
        assertEquals(1, resultEq.size());

        List<ScalarOperator> listGe = buildOperator("day", BinaryType.GE);
        Map<String, PartitionColumnFilter> resultGe = ColumnFilterConverter.convertColumnFilter(listGe, olapTable);
        assertEquals(1, resultGe.size());

        List<ScalarOperator> listGt = buildOperator("day", BinaryType.GT);
        Map<String, PartitionColumnFilter> resultGt = ColumnFilterConverter.convertColumnFilter(listGt, olapTable);
        assertEquals(1, resultGt.size());

        List<ScalarOperator> listLe = buildOperator("day", BinaryType.LE);
        Map<String, PartitionColumnFilter> resultLe = ColumnFilterConverter.convertColumnFilter(listLe, olapTable);
        assertEquals(1, resultLe.size());

        List<ScalarOperator> listLt = buildOperator("day", BinaryType.LT);
        Map<String, PartitionColumnFilter> resultLt = ColumnFilterConverter.convertColumnFilter(listLt, olapTable);
        assertEquals(1, resultLt.size());
    }

    @Test
    public void convertColumnFilterExprDateTruncContains() {
        OlapTable olapTable = buildOlapTable("month");

        List<ScalarOperator> listDay = buildOperator("day", BinaryType.EQ);
        Map<String, PartitionColumnFilter> resultDay = ColumnFilterConverter.convertColumnFilter(listDay, olapTable);
        assertEquals(1, resultDay.size());

        List<ScalarOperator> listWeek = buildOperator("week", BinaryType.EQ);
        Map<String, PartitionColumnFilter> resultWeek = ColumnFilterConverter.convertColumnFilter(listWeek, olapTable);
        assertEquals(0, resultWeek.size());

        List<ScalarOperator> listMonth = buildOperator("month", BinaryType.EQ);
        Map<String, PartitionColumnFilter> resultMonth =
                ColumnFilterConverter.convertColumnFilter(listMonth, olapTable);
        assertEquals(1, resultMonth.size());

        List<ScalarOperator> listYear = buildOperator("year", BinaryType.EQ);
        Map<String, PartitionColumnFilter> resultYear = ColumnFilterConverter.convertColumnFilter(listYear, olapTable);
        assertEquals(0, resultYear.size());
    }

    private List<ScalarOperator> buildOperator(String timeKey, BinaryType binaryType) {
        List<ScalarOperator> arguments = new ArrayList<>(2);
        arguments.add(ConstantOperator.createVarchar(timeKey));
        arguments.add(new ColumnRefOperator(2, Type.INT, "date_col", true));
        ScalarOperator callOperator = new CallOperator("date_trunc", Type.DATE, arguments);

        ScalarOperator root1 = new BinaryPredicateOperator(binaryType,
                callOperator,
                ConstantOperator.createDate(LocalDateTime.of(2022, 12, 23, 0, 0, 0)));

        return Lists.newArrayList(root1);
    }

    private OlapTable buildOlapTable(String timeKey) {
        List<Expr> exprList = new ArrayList<>();
        List<Expr> params = new ArrayList<>();
        List<Column> columns = new ArrayList<>();
        StringLiteral stringLiteral = new StringLiteral(timeKey);
        params.add(stringLiteral);
        TableName tableName = new TableName("testdb", "testtbl");
        SlotRef slotRefDate = new SlotRef(tableName, "date_col");
        slotRefDate.setType(Type.DATE);
        params.add(slotRefDate);
        FunctionCallExpr zdtestCallExpr = new FunctionCallExpr(FunctionSet.DATE_TRUNC,
                params);
        exprList.add(zdtestCallExpr);
        columns.add(new Column("date_col", ScalarType.DATE));
        ExpressionRangePartitionInfo expressionRangePartitionInfo = new ExpressionRangePartitionInfo(exprList, columns,
                PartitionType.RANGE);

        return new OlapTable(1L, "table1", new ArrayList<>(), KeysType.AGG_KEYS, expressionRangePartitionInfo,
                new RandomDistributionInfo(10));
    }

    @Test
    public void testConvertPredicate() {
        List<ScalarOperator> argument = Lists.newArrayList();
        ColumnRefOperator columnRefOperator = new ColumnRefOperator(1, Type.VARCHAR, "bill_code", false);
        ConstantOperator constantOperator = new ConstantOperator("JT2921712368984", Type.VARCHAR);
        argument.add(columnRefOperator);
        argument.add(constantOperator);
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.EQ, argument);

        Table table = GlobalStateMgr.getCurrentState().getDb("test").getTable("bill_detail");
        ExpressionRangePartitionInfoV2 partitionInfo = (ExpressionRangePartitionInfoV2) ((OlapTable) table).getPartitionInfo();
        ScalarOperator afterConvert = ColumnFilterConverter.convertPredicate(predicate, partitionInfo);
        Assert.assertEquals(2921712368984L, ((ConstantOperator) afterConvert.getChild(1)).getValue());
    }

}