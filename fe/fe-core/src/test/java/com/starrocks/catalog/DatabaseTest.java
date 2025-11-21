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

import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.common.StarRocksException;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.List;

public class DatabaseTest {

    private Database db;
    private long dbId = 10000;

    @BeforeEach
    public void setup() {
        db = new Database(dbId, "dbTest");
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void createAndDropPartitionTest() {
        Assertions.assertEquals("dbTest", db.getOriginName());
        Assertions.assertEquals(dbId, db.getId());

        MaterializedIndex baseIndex = new MaterializedIndex(10001, IndexState.NORMAL);
        Partition partition = new Partition(20000L, 20001L,
                "baseTable", baseIndex, new RandomDistributionInfo(10));
        List<Column> baseSchema = new LinkedList<Column>();
        OlapTable table = new OlapTable(2000, "baseTable", baseSchema, KeysType.AGG_KEYS,
                new SinglePartitionInfo(), new RandomDistributionInfo(10));
        table.addPartition(partition);

        // create
        Assertions.assertTrue(db.registerTableUnlocked(table));
        // duplicate
        Assertions.assertFalse(db.registerTableUnlocked(table));

        Assertions.assertEquals(table, db.getTable(table.getId()));
        Assertions.assertEquals(table, db.getTable(table.getName()));

        Assertions.assertEquals(1, db.getTables().size());
        Assertions.assertEquals(table, db.getTables().get(0));

        Assertions.assertEquals(1, db.getTableNamesViewWithLock().size());
        for (String tableFamilyGroupName : db.getTableNamesViewWithLock()) {
            Assertions.assertEquals(table.getName(), tableFamilyGroupName);
        }

        // drop
        // drop not exist tableFamily
        db.dropTable("invalid");
        Assertions.assertEquals(1, db.getTables().size());

        db.registerTableUnlocked(table);
        db.dropTable(table.getName());
        Assertions.assertEquals(0, db.getTables().size());
    }

    @Test
    public void testGetUUID() {
        // Internal database
        Database db1 = new Database();
        Assertions.assertEquals("0", db1.getUUID());

        Database db2 = new Database(101, "db2");
        Assertions.assertEquals("101", db2.getUUID());

        // External database
        Database db3 = new Database(101, "db3");
        db3.setCatalogName("hive");
        Assertions.assertEquals("hive.db3", db3.getUUID());
    }

    @Test
    public void testAddFunction() throws StarRocksException {
        // Add addIntInt function to database
        FunctionName name = new FunctionName(null, "addIntInt");
        name.setDb(db.getCatalogName());
        final Type[] argTypes = {IntegerType.INT, IntegerType.INT};
        Function f = new ScalarFunction(name, argTypes, IntegerType.INT, false);
        f.setBinaryType(TFunctionBinaryType.SRJAR);
        f.setUserVisible(true);
        db.addFunction(f);

        // Add addDoubleDouble function to database
        FunctionName name2 = new FunctionName(null, "addDoubleDouble");
        name2.setDb(db.getCatalogName());
        final Type[] argTypes2 = {FloatType.DOUBLE, FloatType.DOUBLE};
        Function f2 = new ScalarFunction(name2, argTypes2, FloatType.DOUBLE, false);
        f2.setBinaryType(TFunctionBinaryType.SRJAR);
        f2.setUserVisible(true);
        db.addFunction(f2);
    }

    @Test
    public void testAddFunctionGivenFunctionAlreadyExists() throws StarRocksException {
        FunctionName name = new FunctionName(null, "addIntInt");
        name.setDb(db.getCatalogName());
        final Type[] argTypes = {IntegerType.INT, IntegerType.INT};
        Function f = new ScalarFunction(name, argTypes, IntegerType.INT, false);
        f.setBinaryType(TFunctionBinaryType.SRJAR);
        f.setUserVisible(true);

        // Add the UDF for the first time
        db.addFunction(f);

        // Attempt to add the same UDF again, expecting an exception
        Assertions.assertThrows(StarRocksException.class, () -> db.addFunction(f));
    }

    @Test
    public void testAddFunctionGivenFunctionAlreadyExistsAndAllowExisting() throws StarRocksException {
        FunctionName name = new FunctionName(null, "addIntInt");
        name.setDb(db.getCatalogName());
        final Type[] argTypes = {IntegerType.INT, IntegerType.INT};
        Function f = new ScalarFunction(name, argTypes, IntegerType.INT, false);
        f.setBinaryType(TFunctionBinaryType.SRJAR);
        f.setUserVisible(true);

        // Add the UDF for the first time
        db.addFunction(f, true, false);
        // Attempt to add the same UDF again
        db.addFunction(f, true, false);

        List<Function> functions = db.getFunctions();
        Assertions.assertEquals(functions.size(), 1);
        Assertions.assertTrue(functions.get(0).compare(f, Function.CompareMode.IS_IDENTICAL));
    }

    @Test
    public void testAddAndDropFunctionForRestore() {
        Function f1 = new Function(new FunctionName(db.getFullName(), "test_function"),
                                   new Type[] {IntegerType.INT}, new String[] {"argName"}, IntegerType.INT, false);
        try {
            db.addFunction(f1);
        } catch (Exception e) {
        }
        db.dropFunctionForRestore(f1);
    }
}
