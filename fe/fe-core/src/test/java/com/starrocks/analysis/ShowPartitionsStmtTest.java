// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/ShowPartitionsStmtTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ShowPartitionsStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class ShowPartitionsStmtTest {
    private ConnectContext ctx;
    private static StarRocksAssert starRocksAssert;

    private Analyzer analyzer;
    @Before
    public void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("testDb").useDatabase("testTbl");
        starRocksAssert.withTable("create table testDb.testTable(" +
                "id int, name string) DISTRIBUTED BY HASH(id) BUCKETS 10 " +
                "PROPERTIES(\"replication_num\" = \"1\")");
    }

    @Test
    public void testNormal() {
        ShowPartitionsStmt stmt = new ShowPartitionsStmt(new TableName("testDb", "testTable"), null, null, null, false);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("SHOW PARTITIONS FROM `testDb`.`testTable`", stmt.toString());
    }

    @Test
    public void testShowPartitionsStmtWithBinaryPredicate() {
        SlotRef slotRef = new SlotRef(null, "LastConsistencyCheckTime");
        StringLiteral stringLiteral = new StringLiteral("2019-12-22 10:22:11");
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.GT, slotRef, stringLiteral);
        ShowPartitionsStmt stmt =
                new ShowPartitionsStmt(new TableName("testDb", "testTable"), binaryPredicate, null, null, false);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals(
                "SHOW PARTITIONS FROM `testDb`.`testTable` WHERE `LastConsistencyCheckTime` > '2019-12-22 10:22:11'",
                stmt.toString());
    }

    @Test
    public void testShowPartitionsStmtWithLikePredicate() {
        SlotRef slotRef = new SlotRef(null, "PartitionName");
        StringLiteral stringLiteral = new StringLiteral("%p2019%");
        LikePredicate likePredicate = new LikePredicate(LikePredicate.Operator.LIKE, slotRef, stringLiteral);
        ShowPartitionsStmt stmt =
                new ShowPartitionsStmt(new TableName("testDb", "testTable"), likePredicate, null, null, false);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals(
                "SHOW PARTITIONS FROM `testDb`.`testTable` WHERE `PartitionName` LIKE '%p2019%'",
                stmt.toString());
    }

    @Test
    public void testShowPartitionsStmtWithEqualPredicate() {
        SlotRef slotRef = new SlotRef(null, "PartitionName");
        StringLiteral stringLiteral = new StringLiteral("p1");
        BinaryPredicate equalPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, slotRef, stringLiteral);
        ShowPartitionsStmt stmt =
                new ShowPartitionsStmt(new TableName("testDb", "testTable"), equalPredicate, null, null, false);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals(
                "SHOW PARTITIONS FROM `testDb`.`testTable` WHERE `PartitionName` = 'p1'",
                stmt.toString());
    }

    @Test
    public void testShowParitionsStmtOrderByAndLimit() {
        SlotRef slotRef = new SlotRef(null, "PartitionId");
        OrderByElement orderByElement = new OrderByElement(slotRef, true, true);
        LimitElement limitElement = new LimitElement(10);
        ShowPartitionsStmt stmt =
                new ShowPartitionsStmt(new TableName("testDb", "testTable"), null, Arrays.asList(orderByElement),
                        limitElement, false);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("SHOW PARTITIONS FROM `testDb`.`testTable` ORDER BY `PartitionId` ASC LIMIT 10",
                stmt.toString());
    }

    @Test(expected = SemanticException.class)
    public void testUnsupportFilter() {
        SlotRef slotRef = new SlotRef(null, "DataSize");
        StringLiteral stringLiteral = new StringLiteral("3.2 GB");
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, slotRef, stringLiteral);
        ShowPartitionsStmt stmt =
                new ShowPartitionsStmt(new TableName("testDb", "testTable"), binaryPredicate, null, null, false);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.fail("Only the columns of PartitionId/PartitionName/" +
                "State/Buckets/ReplicationNum/LastConsistencyCheckTime are supported.");
    }

}
