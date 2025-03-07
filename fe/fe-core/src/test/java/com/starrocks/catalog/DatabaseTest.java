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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/DatabaseTest.java

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

package com.starrocks.catalog;

import com.starrocks.analysis.FunctionName;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.persist.CreateTableInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.transaction.GtidGenerator;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public class DatabaseTest {

    private Database db;
    private long dbId = 10000;

    @Mocked
    private GlobalStateMgr globalStateMgr;
    @Mocked
    private EditLog editLog;

    @Mocked
    NodeMgr nodeMgr;

    @Before
    public void setup() {
        db = new Database(dbId, "dbTest");
        new Expectations() {
            {
                editLog.logCreateTable((CreateTableInfo) any);
                minTimes = 0;

                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };

        new Expectations(globalStateMgr) {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getNodeMgr();
                minTimes = 0;
                result = nodeMgr;

                globalStateMgr.getLockManager();
                minTimes = 0;
                result = new LockManager();

                globalStateMgr.getNextId();
                minTimes = 0;
                result = 1L;

                globalStateMgr.getGtidGenerator();
                minTimes = 0;
                result = new GtidGenerator();
            }
        };
    }

    @Test
    public void createAndDropPartitionTest() {
        Assert.assertEquals("dbTest", db.getOriginName());
        Assert.assertEquals(dbId, db.getId());

        MaterializedIndex baseIndex = new MaterializedIndex(10001, IndexState.NORMAL);
        Partition partition = new Partition(20000L, 20001L,
                "baseTable", baseIndex, new RandomDistributionInfo(10));
        List<Column> baseSchema = new LinkedList<Column>();
        OlapTable table = new OlapTable(2000, "baseTable", baseSchema, KeysType.AGG_KEYS,
                new SinglePartitionInfo(), new RandomDistributionInfo(10));
        table.addPartition(partition);

        // create
        Assert.assertTrue(db.registerTableUnlocked(table));
        // duplicate
        Assert.assertFalse(db.registerTableUnlocked(table));

        Assert.assertEquals(table, db.getTable(table.getId()));
        Assert.assertEquals(table, db.getTable(table.getName()));

        Assert.assertEquals(1, db.getTables().size());
        Assert.assertEquals(table, db.getTables().get(0));

        Assert.assertEquals(1, db.getTableNamesViewWithLock().size());
        for (String tableFamilyGroupName : db.getTableNamesViewWithLock()) {
            Assert.assertEquals(table.getName(), tableFamilyGroupName);
        }

        // drop
        // drop not exist tableFamily
        db.dropTable("invalid");
        Assert.assertEquals(1, db.getTables().size());

        db.registerTableUnlocked(table);
        db.dropTable(table.getName());
        Assert.assertEquals(0, db.getTables().size());
    }

    @Test
    public void testGetUUID() {
        // Internal database
        Database db1 = new Database();
        Assert.assertEquals("0", db1.getUUID());

        Database db2 = new Database(101, "db2");
        Assert.assertEquals("101", db2.getUUID());

        // External database
        Database db3 = new Database(101, "db3");
        db3.setCatalogName("hive");
        Assert.assertEquals("hive.db3", db3.getUUID());
    }

    @Test
    public void testAddFunction() throws StarRocksException {
        // Add addIntInt function to database
        FunctionName name = new FunctionName(null, "addIntInt");
        name.setDb(db.getCatalogName());
        final Type[] argTypes = {Type.INT, Type.INT};
        Function f = new Function(name, argTypes, Type.INT, false);
        db.addFunction(f);

        // Add addDoubleDouble function to database
        FunctionName name2 = new FunctionName(null, "addDoubleDouble");
        name2.setDb(db.getCatalogName());
        final Type[] argTypes2 = {Type.DOUBLE, Type.DOUBLE};
        Function f2 = new Function(name2, argTypes2, Type.DOUBLE, false);
        db.addFunction(f2);
    }

    @Test
    public void testAddFunctionGivenFunctionAlreadyExists() throws StarRocksException {
        FunctionName name = new FunctionName(null, "addIntInt");
        name.setDb(db.getCatalogName());
        final Type[] argTypes = {Type.INT, Type.INT};
        Function f = new Function(name, argTypes, Type.INT, false);

        // Add the UDF for the first time
        db.addFunction(f);

        // Attempt to add the same UDF again, expecting an exception
        Assert.assertThrows(StarRocksException.class, () -> db.addFunction(f));
    }

    @Test
    public void testAddFunctionGivenFunctionAlreadyExistsAndAllowExisting() throws StarRocksException {
        FunctionName name = new FunctionName(null, "addIntInt");
        name.setDb(db.getCatalogName());
        final Type[] argTypes = {Type.INT, Type.INT};
        Function f = new Function(name, argTypes, Type.INT, false);

        // Add the UDF for the first time
        db.addFunction(f, true, false);
        // Attempt to add the same UDF again
        db.addFunction(f, true, false);

        List<Function> functions = db.getFunctions();
        Assert.assertEquals(functions.size(), 1);
        Assert.assertTrue(functions.get(0).compare(f, Function.CompareMode.IS_IDENTICAL));
    }

    @Test
    public void testAddAndDropFunctionForRestore() {
        Function f1 = new Function(new FunctionName(db.getFullName(), "test_function"),
                                   new Type[] {Type.INT}, new String[] {"argName"}, Type.INT, false);
        try {
            db.addFunction(f1);
        } catch (Exception e) {
        }
        db.dropFunctionForRestore(f1);
    }
}
