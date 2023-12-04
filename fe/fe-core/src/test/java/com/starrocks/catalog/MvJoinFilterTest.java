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
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class MvJoinFilterTest {
    private static List<Map<Expr, SlotRef>> mapper = new ArrayList<>();
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        // create connect context
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test");

        starRocksAssert.withTable("CREATE TABLE test.tbl1\n" +
                "(\n" +
                "    k1 date,\n" +
                "    k2 int,\n" +
                "    v1 int sum\n" +
                ")\n" +
                "PARTITION BY RANGE(k1)\n" +
                "(\n" +
                "    PARTITION p1 values less than('2020-02-01'),\n" +
                "    PARTITION p2 values less than('2020-03-01')\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2)\n" +
                "PROPERTIES(\"replication_num\" = \"1\");");

        starRocksAssert.withTable("CREATE TABLE test.tbl2\n" +
                "(\n" +
                "    k1 date,\n" +
                "    k2 int,\n" +
                "    v1 int sum\n" +
                ")\n" +
                "PARTITION BY RANGE(k1)\n" +
                "(\n" +
                "    PARTITION p1 values less than('2020-02-01'),\n" +
                "    PARTITION p2 values less than('2020-03-01')\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2)\n" +
                "PROPERTIES(\"replication_num\" = \"1\");");
    }

    @Test
    public void testInsertIntoMapper() {
        // Set up test data
        SlotRef slotRefA = new SlotRef(new TableName("db", "table"), "columnA");
        Expr exprA = new SlotRef(new TableName("db", "table"), "columnA");
        Pair<Expr, SlotRef> pairA = new Pair<>(exprA, slotRefA);

        SlotRef slotRefB = new SlotRef(new TableName("db", "table"), "columnB");
        Expr exprB = new SlotRef(new TableName("db", "table"), "columnB");
        Pair<Expr, SlotRef> pairB = new Pair<>(exprB, slotRefB);

        // Execute the method under test
        MvJoinFilter.insertIntoMapper(mapper, pairA, pairB);

        // Assertions
        Assert.assertEquals(1, mapper.size());
        Assert.assertTrue(mapper.get(0).containsKey(exprA));
        Assert.assertTrue(mapper.get(0).containsKey(exprB));
    }

    @Test
    public void testGetSupportMvPartitionExpr() {
        // Set up test data for SlotRef
        SlotRef slotRef = new SlotRef(new TableName("db", "table"), "column");

        // Execute the method under test
        Pair<Expr, SlotRef> result = MvJoinFilter.getSupportMvPartitionExpr(slotRef);

        // Assertions
        Assert.assertNotNull(result);
        Assert.assertEquals(slotRef, result.first);
        Assert.assertEquals(slotRef, result.second);

        // Set up test data for FunctionCallExpr
        slotRef = new SlotRef(new TableName("db", "table"), "column");
        StringLiteral day = new StringLiteral("day");
        FunctionCallExpr functionCallExpr = new FunctionCallExpr("date_trunc", Lists.newArrayList(day, slotRef));

        // Execute the method under test
        result = MvJoinFilter.getSupportMvPartitionExpr(functionCallExpr);

        // Assertions
        Assert.assertNotNull(result);
        Assert.assertEquals(functionCallExpr, result.first);
        Assert.assertEquals(slotRef, result.second);
    }

    @Test
    public void testGetPartitionJoinMap() throws Exception {
        SlotRef slot1 = new SlotRef(new TableName("test", "tbl1"), "k1", "k1");
        slot1.getTblNameWithoutAnalyzed().normalization(connectContext);
        slot1.setType(Type.DATE);

        SlotRef slot2 = new SlotRef(new TableName("test", "tbl2"), "k1", "k1");
        slot2.getTblNameWithoutAnalyzed().normalization(connectContext);
        slot2.setType(Type.DATE);

        String sql1 = "select t1.k1, t2.k2 from tbl1 t1 join tbl2 t2 on t1.k1 = t2.k1 and t1.v1 = t2.v1;";
        QueryStatement query = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(sql1, connectContext);
        Map<Expr, SlotRef> result = MvJoinFilter.getPartitionJoinMap(slot1, query);
        Assert.assertEquals(2, result.size());
        Assert.assertTrue(result.containsKey(slot1));
        Assert.assertTrue(result.containsKey(slot2));

        StringLiteral day = new StringLiteral("day");
        FunctionCallExpr functionCallExpr = new FunctionCallExpr("date_trunc", Lists.newArrayList(day, slot2));
        String sql2 = "select t1.k1, t2.k2 from tbl1 t1 join tbl2 t2 on t1.k1 = date_trunc('day', t2.k1);";
        query = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(sql2, connectContext);
        result = MvJoinFilter.getPartitionJoinMap(functionCallExpr, query);
        Assert.assertEquals(2, result.size());

        String sql3 = "select k1, k2 from tbl1 union select k1, k2 from tbl2";
        query = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(sql3, connectContext);
        result = MvJoinFilter.getPartitionJoinMap(slot1, query);
        Assert.assertEquals(2, result.size());
    }
}