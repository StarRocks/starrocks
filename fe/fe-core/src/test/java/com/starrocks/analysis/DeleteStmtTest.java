// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/DeleteStmtTest.java

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

import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryPredicate.Operator;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.MockedAuth;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.PartitionNames;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class DeleteStmtTest {

    Analyzer analyzer;

    @Mocked
    private Auth auth;
    @Mocked
    private ConnectContext ctx;

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer();
        MockedAuth.mockedAuth(auth);
        MockedAuth.mockedConnectContext(ctx, "root", "192.168.1.1");
    }

    @Test
    public void getMethodTest() {
        BinaryPredicate wherePredicate = new BinaryPredicate(Operator.EQ, new SlotRef(null, "k1"),
                new StringLiteral("abc"));
        DeleteStmt deleteStmt = new DeleteStmt(new TableName("testDb", "testTbl"),
                new PartitionNames(false, Lists.newArrayList("partition")), wherePredicate);

        Assert.assertEquals("testDb", deleteStmt.getTableName().getDb());
        Assert.assertEquals("testTbl", deleteStmt.getTableName().getTbl());
        Assert.assertEquals(Lists.newArrayList("partition"), deleteStmt.getPartitionNames());
        Assert.assertEquals("DELETE FROM `testDb`.`testTbl` PARTITION (partition) WHERE `k1` = 'abc'",
                deleteStmt.toSql());
    }

    @Test
    public void testAnalyze() {
        // case 1
        LikePredicate likePredicate = new LikePredicate(com.starrocks.analysis.LikePredicate.Operator.LIKE,
                new SlotRef(null, "k1"),
                new StringLiteral("abc"));
        DeleteStmt deleteStmt = new DeleteStmt(new TableName("testDb", "testTbl"),
                new PartitionNames(false, Lists.newArrayList("partition")), likePredicate);
        try {
            deleteStmt.analyze(analyzer);
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("Where clause only supports compound predicate, " +
                    "binary predicate, is_null predicate and in predicate"));
        }

        // case 2
        BinaryPredicate binaryPredicate = new BinaryPredicate(Operator.EQ, new SlotRef(null, "k1"),
                new StringLiteral("abc"));
        CompoundPredicate compoundPredicate =
                new CompoundPredicate(com.starrocks.analysis.CompoundPredicate.Operator.OR, binaryPredicate,
                        binaryPredicate);

        deleteStmt = new DeleteStmt(new TableName("testDb", "testTbl"),
                new PartitionNames(false, Lists.newArrayList("partition")), compoundPredicate);

        try {
            deleteStmt.analyze(analyzer);
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("should be AND"));
        }

        // case 3
        compoundPredicate = new CompoundPredicate(com.starrocks.analysis.CompoundPredicate.Operator.AND,
                binaryPredicate,
                likePredicate);

        deleteStmt = new DeleteStmt(new TableName("testDb", "testTbl"),
                new PartitionNames(false, Lists.newArrayList("partition")), compoundPredicate);
        try {
            deleteStmt.analyze(analyzer);
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("Where clause only supports compound predicate, " +
                    "binary predicate, is_null predicate and in predicate"));
        }

        // case 4
        binaryPredicate = new BinaryPredicate(Operator.EQ, new SlotRef(null, "k1"),
                new SlotRef(null, "k1"));
        compoundPredicate = new CompoundPredicate(com.starrocks.analysis.CompoundPredicate.Operator.AND,
                binaryPredicate,
                binaryPredicate);

        deleteStmt = new DeleteStmt(new TableName("testDb", "testTbl"),
                new PartitionNames(false, Lists.newArrayList("partition")), compoundPredicate);
        try {
            deleteStmt.analyze(analyzer);
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("Right expr of binary predicate should be value"));
        }

        // case 5
        binaryPredicate = new BinaryPredicate(Operator.EQ, new StringLiteral("abc"),
                new SlotRef(null, "k1"));
        compoundPredicate = new CompoundPredicate(com.starrocks.analysis.CompoundPredicate.Operator.AND,
                binaryPredicate,
                binaryPredicate);

        deleteStmt = new DeleteStmt(new TableName("testDb", "testTbl"),
                new PartitionNames(false, Lists.newArrayList("partition")), compoundPredicate);
        try {
            deleteStmt.analyze(analyzer);
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("Left expr of binary predicate should be column name"));
        }

        // case 6 partition is null
        binaryPredicate = new BinaryPredicate(Operator.EQ, new SlotRef(null, "k1"), new StringLiteral("abc"));
        compoundPredicate = new CompoundPredicate(com.starrocks.analysis.CompoundPredicate.Operator.AND,
                binaryPredicate,
                binaryPredicate);

        deleteStmt = new DeleteStmt(new TableName("testDb", "testTbl"), null, compoundPredicate);
        try {
            deleteStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.assertTrue(e.getMessage().contains("Partition is not set"));
        }

        // normal
        binaryPredicate = new BinaryPredicate(Operator.EQ, new SlotRef(null, "k1"),
                new StringLiteral("abc"));
        List<Expr> inList = Lists.newArrayList();
        inList.add(new StringLiteral("1234"));
        inList.add(new StringLiteral("5678"));
        inList.add(new StringLiteral("1314"));
        inList.add(new StringLiteral("8972"));
        InPredicate inPredicate = new InPredicate(new SlotRef(null, "k2"), inList, true);
        CompoundPredicate compoundPredicate2 =
                new CompoundPredicate(com.starrocks.analysis.CompoundPredicate.Operator.AND,
                        binaryPredicate,
                        inPredicate);
        compoundPredicate = new CompoundPredicate(com.starrocks.analysis.CompoundPredicate.Operator.AND,
                binaryPredicate,
                compoundPredicate2);

        deleteStmt = new DeleteStmt(new TableName("testDb", "testTbl"),
                new PartitionNames(false, Lists.newArrayList("partition")), compoundPredicate);
        try {
            deleteStmt.analyze(analyzer);
        } catch (UserException e) {
            Assert.fail();
        }

        // multi partition
        deleteStmt = new DeleteStmt(new TableName("testDb", "testTbl"),
                new PartitionNames(false, Lists.newArrayList("partition1", "partition2")), compoundPredicate);
        try {
            deleteStmt.analyze(analyzer);
            Assert.assertEquals(Lists.newArrayList("partition1", "partition2"), deleteStmt.getPartitionNames());
        } catch (UserException e) {
            Assert.fail();
        }

        // no partition
        deleteStmt = new DeleteStmt(new TableName("testDb", "testTbl"), null, compoundPredicate);
        try {
            deleteStmt.analyze(analyzer);
            Assert.assertEquals(Lists.newArrayList(), deleteStmt.getPartitionNames());
        } catch (UserException e) {
            Assert.fail();
        }
    }

}
