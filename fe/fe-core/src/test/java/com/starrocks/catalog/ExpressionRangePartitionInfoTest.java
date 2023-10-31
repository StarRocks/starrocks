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

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.PartitionKeyDesc;
import com.starrocks.sql.ast.PartitionKeyDesc.PartitionRangeType;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ExpressionRangePartitionInfoTest {

    private List<Expr> partitionExprs;
    private RangePartitionInfo partitionInfo;

    private List<SingleRangePartitionDesc> singleRangePartitionDescs;

    private TableName tableName;
    private Column k2;
    private FunctionCallExpr functionCallExpr;

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;


    @Before
    public void setUp() throws Exception {
        partitionExprs = Lists.newArrayList();
        singleRangePartitionDescs = Lists.newArrayList();
        tableName = new TableName("test", "tbl1");
        k2 = new Column("k2", new ScalarType(PrimitiveType.DATETIME), true, null, "", "");
        SlotRef slotRef2 = new SlotRef(tableName, "k2");
        List<Expr> fnChildren = Lists.newArrayList();
        fnChildren.add(new StringLiteral("month"));
        fnChildren.add(slotRef2);
        functionCallExpr = new FunctionCallExpr("date_trunc", fnChildren);
        functionCallExpr.setFn(Expr.getBuiltinFunction(
                "date_trunc", new Type[] {Type.VARCHAR, Type.DATETIME}, Function.CompareMode.IS_IDENTICAL));

        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("test").useDatabase("test");
    }

    @Test
    public void testInitUseSlotRef() {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.DATETIME), true, null, "", "");
        SlotRef slotRef = new SlotRef(tableName, "k1");
        partitionExprs.add(slotRef);
        ExpressionRangePartitionInfo expressionRangePartitionInfo = new ExpressionRangePartitionInfo(partitionExprs,
                Arrays.asList(k1), PartitionType.RANGE);
        List<Column> partitionColumns = expressionRangePartitionInfo.getPartitionColumns();
        Assert.assertEquals(partitionColumns.size(), 1);
        Assert.assertEquals(partitionColumns.get(0), k1);
    }

    @Test
    public void testInitUseFunction() {
        partitionExprs.add(functionCallExpr);
        ExpressionRangePartitionInfo expressionRangePartitionInfo =
                new ExpressionRangePartitionInfo(partitionExprs, Arrays.asList(k2), PartitionType.RANGE);
        List<Column> partitionColumns = expressionRangePartitionInfo.getPartitionColumns();
        Assert.assertEquals(partitionColumns.size(), 1);
        Assert.assertEquals(partitionColumns.get(0), k2);
    }

    @Test
    public void testInitHybrid() {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.DATETIME), true, null, "", "");
        SlotRef slotRef = new SlotRef(tableName, "k1");
        partitionExprs.add(slotRef);
        partitionExprs.add(functionCallExpr);
        ExpressionRangePartitionInfo expressionRangePartitionInfo =
                new ExpressionRangePartitionInfo(partitionExprs, Arrays.asList(k1, k2), PartitionType.RANGE);
        List<Column> partitionColumns = expressionRangePartitionInfo.getPartitionColumns();
        Assert.assertEquals(partitionColumns.size(), 2);
        Assert.assertEquals(partitionColumns.get(0), k1);
        Assert.assertEquals(partitionColumns.get(1), k2);
    }

    // cover RangePartitionInfo test cases

    @Test(expected = DdlException.class)
    public void testTinyInt() throws DdlException, AnalysisException {

        Column k1 = new Column("k1", new ScalarType(PrimitiveType.TINYINT), true, null, "", "");
        SlotRef slotRef = new SlotRef(tableName, "k1");
        partitionExprs.add(slotRef);

        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p1",
                new PartitionKeyDesc(Lists.newArrayList(new PartitionValue("-128"))),
                null));

        partitionInfo = new ExpressionRangePartitionInfo(partitionExprs, Arrays.asList(k1), PartitionType.RANGE);
        for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
            singleRangePartitionDesc.analyze(1, null);
            partitionInfo.handleNewSinglePartitionDesc(singleRangePartitionDesc, 20000L, false);
        }
    }

    @Test(expected = DdlException.class)
    public void testSmallInt() throws DdlException, AnalysisException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.SMALLINT), true, null, "", "");
        SlotRef slotRef = new SlotRef(tableName, "k1");
        partitionExprs.add(slotRef);

        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p1",
                new PartitionKeyDesc(Lists.newArrayList(new PartitionValue("-32768"))),
                null));

        partitionInfo = new ExpressionRangePartitionInfo(partitionExprs, Arrays.asList(k1), PartitionType.RANGE);
        for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
            singleRangePartitionDesc.analyze(1, null);
            partitionInfo.handleNewSinglePartitionDesc(singleRangePartitionDesc, 20000L, false);
        }
    }

    @Test(expected = DdlException.class)
    public void testInt() throws DdlException, AnalysisException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.INT), true, null, "", "");
        SlotRef slotRef = new SlotRef(tableName, "k1");
        partitionExprs.add(slotRef);

        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p1",
                new PartitionKeyDesc(Lists.newArrayList(new PartitionValue("-2147483648"))),
                null));

        partitionInfo = new ExpressionRangePartitionInfo(partitionExprs, Arrays.asList(k1), PartitionType.RANGE);
        for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
            singleRangePartitionDesc.analyze(1, null);
            partitionInfo.handleNewSinglePartitionDesc(singleRangePartitionDesc, 20000L, false);
        }
    }

    @Test(expected = DdlException.class)
    public void testBigInt() throws DdlException, AnalysisException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.BIGINT), true, null, "", "");
        SlotRef slotRef = new SlotRef(tableName, "k1");
        partitionExprs.add(slotRef);

        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p1", new PartitionKeyDesc(Lists
                .newArrayList(new PartitionValue("-9223372036854775808"))), null));
        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p2", new PartitionKeyDesc(Lists
                .newArrayList(new PartitionValue("-9223372036854775806"))), null));
        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p3", new PartitionKeyDesc(Lists
                .newArrayList(new PartitionValue("0"))), null));
        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p4", new PartitionKeyDesc(Lists
                .newArrayList(new PartitionValue("9223372036854775806"))), null));

        partitionInfo = new ExpressionRangePartitionInfo(partitionExprs, Arrays.asList(k1), PartitionType.RANGE);
        ;

        for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
            singleRangePartitionDesc.analyze(1, null);
            partitionInfo.handleNewSinglePartitionDesc(singleRangePartitionDesc, 20000L, false);
        }
    }

    @Test
    public void testBigIntNormal() throws DdlException, AnalysisException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.BIGINT), true, null, "", "");
        SlotRef slotRef = new SlotRef(tableName, "k1");
        partitionExprs.add(slotRef);

        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p1", new PartitionKeyDesc(Lists
                .newArrayList(new PartitionValue("-9223372036854775806"))), null));
        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p2", new PartitionKeyDesc(Lists
                .newArrayList(new PartitionValue("-9223372036854775805"))), null));
        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p3", new PartitionKeyDesc(Lists
                .newArrayList(new PartitionValue("0"))), null));
        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p4", new PartitionKeyDesc(Lists
                .newArrayList(new PartitionValue("9223372036854775806"))), null));

        partitionInfo = new ExpressionRangePartitionInfo(partitionExprs, Arrays.asList(k1), PartitionType.RANGE);
        ;

        for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
            singleRangePartitionDesc.analyze(1, null);
            partitionInfo.handleNewSinglePartitionDesc(singleRangePartitionDesc, 20000L, false);
        }
    }

    /**
     * PARTITION BY RANGE(`k1`, `k2`) (
     * PARTITION p0 VALUES  [("20190101", "100"),("20190101", "200")),
     * PARTITION p1 VALUES  [("20190105", "10"),("20190107", "10")),
     * PARTITION p2 VALUES  [("20181231", "10"),("20190101", "100")),
     * PARTITION p3 VALUES  [("20190105", "100"),("20190120", MAXVALUE))
     * )
     */
    @Test
    public void testFixedRange() throws DdlException, AnalysisException {
        //add columns
        int columns = 2;
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.INT), true, null, "", "");
        SlotRef slotRef1 = new SlotRef(tableName, "k1");
        partitionExprs.add(slotRef1);
        Column k2 = new Column("k2", new ScalarType(PrimitiveType.BIGINT), true, null, "", "");
        SlotRef slotRef2 = new SlotRef(tableName, "k2");
        partitionExprs.add(slotRef2);

        //add RangePartitionDescs
        PartitionKeyDesc p1 = new PartitionKeyDesc(
                Lists.newArrayList(new PartitionValue("20190101"), new PartitionValue("100")),
                Lists.newArrayList(new PartitionValue("20190101"), new PartitionValue("200")));
        PartitionKeyDesc p2 = new PartitionKeyDesc(
                Lists.newArrayList(new PartitionValue("20190105"), new PartitionValue("10")),
                Lists.newArrayList(new PartitionValue("20190107"), new PartitionValue("10")));
        PartitionKeyDesc p3 = new PartitionKeyDesc(
                Lists.newArrayList(new PartitionValue("20181231"), new PartitionValue("10")),
                Lists.newArrayList(new PartitionValue("20190101"), new PartitionValue("100")));
        PartitionKeyDesc p4 = new PartitionKeyDesc(
                Lists.newArrayList(new PartitionValue("20190105"), new PartitionValue("100")),
                Lists.newArrayList(new PartitionValue("20190120"), new PartitionValue("10000000000")));

        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p1", p1, null));
        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p2", p2, null));
        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p3", p3, null));
        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p4", p4, null));

        partitionInfo = new ExpressionRangePartitionInfo(partitionExprs, Arrays.asList(k1, k2), PartitionType.RANGE);

        for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
            singleRangePartitionDesc.analyze(columns, null);
            partitionInfo.handleNewSinglePartitionDesc(singleRangePartitionDesc, 20000L, false);
        }
    }

    /**
     * failed cases less than && fixed
     * partition by range(k1,k2,k3) (
     * partition p1 values less than("2019-02-01", "100", "200"),
     * partition p2 values [("2020-02-01", "100", "200"), (MAXVALUE)),
     * partition p3 values less than("2021-02-01")
     * )
     */
    @Test(expected = AnalysisException.class)
    public void testFixedRange1() throws DdlException, AnalysisException {
        //add columns
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.DATE), true, null, "", "");
        SlotRef slotRef1 = new SlotRef(tableName, "k1");
        partitionExprs.add(slotRef1);
        Column k2 = new Column("k2", new ScalarType(PrimitiveType.INT), true, null, "", "");
        SlotRef slotRef2 = new SlotRef(tableName, "k2");
        partitionExprs.add(slotRef2);
        Column k3 = new Column("k3", new ScalarType(PrimitiveType.INT), true, null, "", "");
        SlotRef slotRef3 = new SlotRef(tableName, "k3");
        partitionExprs.add(slotRef3);

        //add RangePartitionDescs
        PartitionKeyDesc p1 = new PartitionKeyDesc(
                Lists.newArrayList(new PartitionValue("2019-02-01"), new PartitionValue("100"),
                        new PartitionValue("200")));
        PartitionKeyDesc p2 = new PartitionKeyDesc(
                Lists.newArrayList(new PartitionValue("2020-02-01"), new PartitionValue("100"),
                        new PartitionValue("200")),
                Lists.newArrayList(new PartitionValue("10000000000")));
        PartitionKeyDesc p3 = new PartitionKeyDesc(
                Lists.newArrayList(new PartitionValue("2021-02-01")));

        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p1", p1, null));
        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p2", p2, null));
        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p3", p3, null));
        partitionInfo = new ExpressionRangePartitionInfo(partitionExprs, Arrays.asList(k1, k2, k3), PartitionType.RANGE);
        ;
        PartitionRangeType partitionType = PartitionRangeType.INVALID;
        for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
            // check partitionType
            if (partitionType == PartitionRangeType.INVALID) {
                partitionType = singleRangePartitionDesc.getPartitionKeyDesc().getPartitionType();
            } else if (partitionType != singleRangePartitionDesc.getPartitionKeyDesc().getPartitionType()) {
                throw new AnalysisException("You can only use one of these methods to create partitions");
            }
            singleRangePartitionDesc.analyze(partitionExprs.size(), null);
            partitionInfo.handleNewSinglePartitionDesc(singleRangePartitionDesc, 20000L, false);
        }
    }

    /**
     * PARTITION BY RANGE(`k1`, `k2`) (
     * PARTITION p1 VALUES  [(), ("20190301", "400"))
     * )
     */
    @Test
    public void testFixedRange2() throws DdlException, AnalysisException {
        //add columns
        int columns = 2;
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.INT), true, null, "", "");
        SlotRef slotRef1 = new SlotRef(tableName, "k1");
        partitionExprs.add(slotRef1);
        Column k2 = new Column("k2", new ScalarType(PrimitiveType.BIGINT), true, null, "", "");
        SlotRef slotRef2 = new SlotRef(tableName, "k2");
        partitionExprs.add(slotRef2);

        //add RangePartitionDescs
        PartitionKeyDesc p1 = new PartitionKeyDesc(new ArrayList<>(),
                Lists.newArrayList(new PartitionValue("20190101"), new PartitionValue("200")));

        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p1", p1, null));

        partitionInfo = new ExpressionRangePartitionInfo(partitionExprs, Arrays.asList(k1, k2), PartitionType.RANGE);

        for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
            singleRangePartitionDesc.analyze(columns, null);
            partitionInfo.handleNewSinglePartitionDesc(singleRangePartitionDesc, 20000L, false);
        }
    }

    /**
     * failed cases
     * PARTITION BY RANGE(`k1`, `k2`) (
     * PARTITION p1 VALUES  [("20190301", "400"), ())
     * )
     */
    @Test(expected = AnalysisException.class)
    public void testFixedRange3() throws DdlException, AnalysisException {
        //add columns
        int columns = 2;
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.INT), true, null, "", "");
        SlotRef slotRef1 = new SlotRef(tableName, "k1");
        partitionExprs.add(slotRef1);
        Column k2 = new Column("k2", new ScalarType(PrimitiveType.BIGINT), true, null, "", "");
        SlotRef slotRef2 = new SlotRef(tableName, "k2");
        partitionExprs.add(slotRef2);

        //add RangePartitionDescs
        PartitionKeyDesc p1 = new PartitionKeyDesc(
                Lists.newArrayList(new PartitionValue("20190101"), new PartitionValue("200")),
                new ArrayList<>());

        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p1", p1, null));

        partitionInfo = new ExpressionRangePartitionInfo(partitionExprs, Arrays.asList(k1, k2), PartitionType.RANGE);

        for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
            singleRangePartitionDesc.analyze(columns, null);
            partitionInfo.handleNewSinglePartitionDesc(singleRangePartitionDesc, 20000L, false);
        }
    }

    /**
     * PARTITION BY RANGE(`k1`, `k2`) (
     * PARTITION p0 VALUES  [("20190101", "100"),("20190201"))
     * )
     */
    @Test
    public void testFixedRange4() throws DdlException, AnalysisException {
        //add columns
        int columns = 2;
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.INT), true, null, "", "");
        SlotRef slotRef1 = new SlotRef(tableName, "k1");
        partitionExprs.add(slotRef1);
        Column k2 = new Column("k2", new ScalarType(PrimitiveType.BIGINT), true, null, "", "");
        SlotRef slotRef2 = new SlotRef(tableName, "k2");
        partitionExprs.add(slotRef2);

        //add RangePartitionDescs
        PartitionKeyDesc p1 = new PartitionKeyDesc(
                Lists.newArrayList(new PartitionValue("20190101"), new PartitionValue("100")),
                Lists.newArrayList(new PartitionValue("20190201")));

        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p1", p1, null));

        partitionInfo = new ExpressionRangePartitionInfo(partitionExprs, Arrays.asList(k1, k2), PartitionType.RANGE);

        for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
            singleRangePartitionDesc.analyze(columns, null);
            partitionInfo.handleNewSinglePartitionDesc(singleRangePartitionDesc, 20000L, false);
        }
    }

    /**
     * failed cases
     * PARTITION BY RANGE(`k1`, `k2`) (
     * PARTITION p0 VALUES  [("20190101", "100"),("20190101", "100"))
     * )
     */
    @Test(expected = DdlException.class)
    public void testFixedRange5() throws DdlException, AnalysisException {
        //add columns
        int columns = 2;
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.INT), true, null, "", "");
        SlotRef slotRef1 = new SlotRef(tableName, "k1");
        partitionExprs.add(slotRef1);
        Column k2 = new Column("k2", new ScalarType(PrimitiveType.BIGINT), true, null, "", "");
        SlotRef slotRef2 = new SlotRef(tableName, "k2");
        partitionExprs.add(slotRef2);

        //add RangePartitionDescs
        PartitionKeyDesc p1 = new PartitionKeyDesc(
                Lists.newArrayList(new PartitionValue("20190101"), new PartitionValue("100")),
                Lists.newArrayList(new PartitionValue("20190101"), new PartitionValue("100")));

        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p1", p1, null));

        partitionInfo = new ExpressionRangePartitionInfo(partitionExprs, Arrays.asList(k1, k2), PartitionType.RANGE);

        for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
            singleRangePartitionDesc.analyze(columns, null);
            partitionInfo.handleNewSinglePartitionDesc(singleRangePartitionDesc, 20000L, false);
        }
    }

    @Test
    public void testExpressionRangePartitionInfoSerialized() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createSQL = "CREATE TABLE table_hitcount (\n" +
                "databaseName varchar(200) NULL COMMENT \"\",\n" +
                "tableName varchar(200) NULL COMMENT \"\",\n" +
                "queryTime varchar(50) NULL COMMENT \"\",\n" +
                "queryId varchar(50) NULL COMMENT \"\",\n" +
                "partitionHitSum int(11) NULL COMMENT \"\",\n" +
                "partitionSum int(11) NULL COMMENT \"\",\n" +
                "tabletHitNum int(11) NULL COMMENT \"\",\n" +
                "tabletSum int(11) NULL COMMENT \"\",\n" +
                "startHitPartition varchar(20) NULL COMMENT \"\",\n" +
                "dt date NULL COMMENT \"\",\n" +
                "clusterAddress varchar(50) NULL COMMENT \"\",\n" +
                "costTime int(11) NULL COMMENT \"\",\n" +
                "tableQueryCount int(11) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(databaseName, tableName)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY date_trunc('day', dt)\n" +
                "DISTRIBUTED BY HASH(databaseName) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"enable_persistent_index\" = \"false\",\n" +
                "\"replicated_storage\" = \"true\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        GlobalStateMgr.getCurrentState().createTable(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        Table table = db.getTable("table_hitcount");
        // serialize
        String json = GsonUtils.GSON.toJson(table);
        // deserialize
        OlapTable readTable = GsonUtils.GSON.fromJson(json, OlapTable.class);
        ExpressionRangePartitionInfo expressionRangePartitionInfo = (ExpressionRangePartitionInfo) readTable.getPartitionInfo();
        List<Expr> readPartitionExprs = expressionRangePartitionInfo.getPartitionExprs();
        Function fn = readPartitionExprs.get(0).getFn();
        Assert.assertNotNull(fn);
    }

<<<<<<< HEAD
=======
    @Test
    public void testExpressionRangePartitionInfoSerialized_SlotRef() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createSQL = "CREATE TABLE test_table (\n" +
                "databaseName varchar(200) NULL COMMENT \"\",\n" +
                "tableName varchar(200) NULL COMMENT \"\",\n" +
                "queryTime varchar(50) NULL COMMENT \"\",\n" +
                "queryId varchar(50) NULL COMMENT \"\",\n" +
                "dt date NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(databaseName, tableName)\n" +
                "PARTITION BY RANGE (`dt`)\n" +
                "(\n" +
                "    PARTITION p1 values less than('2021-02-01'),\n" +
                "    PARTITION p2 values less than('2021-03-01')\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(databaseName) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        GlobalStateMgr.getCurrentState().createTable(createTableStmt);

        starRocksAssert.withMaterializedView("create materialized view test_mv1 " +
                " DISTRIBUTED BY HASH(dt, queryId) BUCKETS 4\n" +
                " PARTITION BY dt\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ") as select dt, queryId, count(1) from test_table group by dt, queryId"
        );
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        Table table = db.getTable("test_mv1");
        // serialize
        String json = GsonUtils.GSON.toJson(table);
        // deserialize
        OlapTable readTable = GsonUtils.GSON.fromJson(json, OlapTable.class);
        ExpressionRangePartitionInfo expressionRangePartitionInfo = (ExpressionRangePartitionInfo) readTable.getPartitionInfo();
        List<Expr> readPartitionExprs = expressionRangePartitionInfo.getPartitionExprs();
        Assert.assertTrue(readPartitionExprs.get(0) instanceof SlotRef);
        SlotRef slotRef = (SlotRef)  readPartitionExprs.get(0);
        Assert.assertTrue(!slotRef.getType().isInvalid());
        Assert.assertTrue(slotRef.getType().isDateType());
        starRocksAssert.dropMaterializedView("test_mv1");
        starRocksAssert.dropTable("test_table");
    }

    @Test
    public void testExpressionRangePartitionInfoSerializedWrongNotFailed() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createSQL = "CREATE TABLE `game_log` (\n" +
                "  `cloud_id` varchar(65533) NULL COMMENT \"\",\n" +
                "  `user_id` varchar(65533) NULL COMMENT \"\",\n" +
                "  `day` date NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`cloud_id`, `user_id`)\n" +
                "PARTITION BY date_trunc('day', day)\n" +
                "DISTRIBUTED BY HASH(`cloud_id`, `user_id`) BUCKETS 1 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"enable_persistent_index\" = \"false\",\n" +
                "\"replicated_storage\" = \"true\",\n" +
                "\"compression\" = \"ZSTD\"\n" +
                ");";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        GlobalStateMgr.getCurrentState().createTable(createTableStmt);

        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable olapTable = (OlapTable) db.getTable("game_log");
        ExpressionRangePartitionInfo expressionRangePartitionInfo =
                (ExpressionRangePartitionInfo) olapTable.getPartitionInfo();
        List<Column> partitionColumns = expressionRangePartitionInfo.getPartitionColumns();
        partitionColumns.get(0).setType(Type.VARCHAR);
        // serialize
        String json = GsonUtils.GSON.toJson(olapTable);
        // deserialize
        OlapTable readTable = GsonUtils.GSON.fromJson(json, OlapTable.class);
    }

>>>>>>> a916db1536 ([Enhancement] Add exception handling to avoid FE startup failure (#33999))
}
