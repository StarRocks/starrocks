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

import com.starrocks.catalog.FakeGlobalStateMgr;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.PartitionType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.catalog.ListPartitionInfo;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

public class ShowPartitionsStmtTest {
    @Mocked
    private Analyzer analyzer;
    private GlobalStateMgr globalStateMgr;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Before
    public void setUp() {
        globalStateMgr = AccessTestUtil.fetchAdminCatalog();
        FakeGlobalStateMgr.setGlobalStateMgr(globalStateMgr);
        Auth auth = AccessTestUtil.fetchAdminAccess();

        ListPartitionInfo partitionInfo = new ListPartitionInfo();
        new Expectations(partitionInfo) {
            {
                partitionInfo.getType();
                minTimes = 0;
                result = PartitionType.LIST;
            }
        };

        OlapTable table = new OlapTable();
        new Expectations(table) {
            {
                table.getPartitionInfo();
                minTimes = 0;
                result = partitionInfo;
            }
        };

        Database db = new Database();
        new Expectations(db) {
            {
                db.getTable(anyString);
                minTimes = 0;
                result = table;

                db.getTable(0);
                minTimes = 0;
                result = table;
            }
        };

        new Expectations() {
            {
                analyzer.getDefaultDb();
                minTimes = 0;
                result = "testDb";

                analyzer.getCatalog();
                minTimes = 0;
                result = globalStateMgr;

                analyzer.getClusterName();
                minTimes = 0;
                result = "default_cluster";

                globalStateMgr.getAuth();
                minTimes = 0;
                result = auth;

                globalStateMgr.getDb(anyString);
                minTimes = 0;
                result = db;

                globalStateMgr.getDb(0);
                minTimes = 0;
                result = db;
            }
        };

    }

    @Test
    public void testNormal() throws UserException {
        ShowPartitionsStmt stmt = new ShowPartitionsStmt(new TableName("testDb", "testTable"), null, null, null, false);
        stmt.analyzeImpl(analyzer);
        Assert.assertEquals("SHOW PARTITIONS FROM `default_cluster:testDb`.`testTable`", stmt.toString());
    }

    @Test
    public void testShowPartitionsStmtWithBinaryPredicate() throws UserException {
        SlotRef slotRef = new SlotRef(null, "LastConsistencyCheckTime");
        StringLiteral stringLiteral = new StringLiteral("2019-12-22 10:22:11");
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.GT, slotRef, stringLiteral);
        ShowPartitionsStmt stmt =
                new ShowPartitionsStmt(new TableName("testDb", "testTable"), binaryPredicate, null, null, false);
        stmt.analyzeImpl(analyzer);
        Assert.assertEquals(
                "SHOW PARTITIONS FROM `default_cluster:testDb`.`testTable` WHERE `LastConsistencyCheckTime` > '2019-12-22 10:22:11'",
                stmt.toString());
    }

    @Test
    public void testShowPartitionsStmtWithLikePredicate() throws UserException {
        SlotRef slotRef = new SlotRef(null, "PartitionName");
        StringLiteral stringLiteral = new StringLiteral("%p2019%");
        LikePredicate likePredicate = new LikePredicate(LikePredicate.Operator.LIKE, slotRef, stringLiteral);
        ShowPartitionsStmt stmt =
                new ShowPartitionsStmt(new TableName("testDb", "testTable"), likePredicate, null, null, false);
        stmt.analyzeImpl(analyzer);
        Assert.assertEquals(
                "SHOW PARTITIONS FROM `default_cluster:testDb`.`testTable` WHERE `PartitionName` LIKE '%p2019%'",
                stmt.toString());
    }

    @Test
    public void testShowParitionsStmtOrderByAndLimit() throws UserException {
        SlotRef slotRef = new SlotRef(null, "PartitionId");
        OrderByElement orderByElement = new OrderByElement(slotRef, true, true);
        LimitElement limitElement = new LimitElement(10);
        ShowPartitionsStmt stmt =
                new ShowPartitionsStmt(new TableName("testDb", "testTable"), null, Arrays.asList(orderByElement),
                        limitElement, false);
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW PARTITIONS FROM `default_cluster:testDb`.`testTable` ORDER BY `PartitionId` ASC LIMIT 10",
                stmt.toString());
    }

    @Test
    public void testUnsupportFilter() throws UserException {
        SlotRef slotRef = new SlotRef(null, "DataSize");
        StringLiteral stringLiteral = new StringLiteral("3.2 GB");
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, slotRef, stringLiteral);
        ShowPartitionsStmt stmt =
                new ShowPartitionsStmt(new TableName("testDb", "testTable"), binaryPredicate, null, null, false);
        expectedEx.expect(AnalysisException.class);
        expectedEx.expectMessage("Only the columns of PartitionId/PartitionName/" +
                "State/Buckets/ReplicationNum/LastConsistencyCheckTime are supported.");
        stmt.analyzeImpl(analyzer);
    }

}
