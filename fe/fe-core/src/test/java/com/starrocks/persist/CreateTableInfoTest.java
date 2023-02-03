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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/persist/CreateTableInfoTest.java

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

package com.starrocks.persist;

import com.google.common.collect.Lists;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FakeGlobalStateMgr;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.common.FeConstants;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStorageType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;

public class CreateTableInfoTest {
    private GlobalStateMgr globalStateMgr;

    private FakeGlobalStateMgr fakeGlobalStateMgr;

    @Before
    public void setUp() {
        fakeGlobalStateMgr = new FakeGlobalStateMgr();
        globalStateMgr = Deencapsulation.newInstance(GlobalStateMgr.class);

        FakeGlobalStateMgr.setGlobalStateMgr(globalStateMgr);
        FakeGlobalStateMgr.setMetaVersion(FeConstants.META_VERSION);
    }

    @Test
    public void testSerialization() throws Exception {
        // 1. Write objects to file
        File file = new File("./createTableInfo");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        List<Column> columns = new ArrayList<Column>();
        Column column2 = new Column("column2",
                ScalarType.createType(PrimitiveType.TINYINT), false, AggregateType.MIN, "", "");
        columns.add(column2);
        columns.add(new Column("column3",
                ScalarType.createType(PrimitiveType.SMALLINT), false, AggregateType.SUM, "", ""));
        columns.add(new Column("column4",
                ScalarType.createType(PrimitiveType.INT), false, AggregateType.REPLACE, "", ""));
        columns.add(new Column("column5",
                ScalarType.createType(PrimitiveType.BIGINT), false, AggregateType.REPLACE, "", ""));
        columns.add(new Column("column6",
                ScalarType.createType(PrimitiveType.FLOAT), false, AggregateType.REPLACE, "", ""));
        columns.add(new Column("column7",
                ScalarType.createType(PrimitiveType.DOUBLE), false, AggregateType.REPLACE, "", ""));
        columns.add(new Column("column8", ScalarType.createCharType(10), true, null, "", ""));
        columns.add(new Column("column9", ScalarType.createVarchar(10), true, null, "", ""));
        columns.add(new Column("column10", ScalarType.createType(PrimitiveType.DATE), true, null, "", ""));
        columns.add(new Column("column11", ScalarType.createType(PrimitiveType.DATETIME), true, null, "", ""));

        MaterializedIndex index = new MaterializedIndex(1, IndexState.NORMAL);
        RandomDistributionInfo distributionInfo = new RandomDistributionInfo(10);
        Partition partition = new Partition(20000L, "table", index, distributionInfo);
        OlapTable table = new OlapTable(1000L, "table", columns, KeysType.AGG_KEYS,
                new SinglePartitionInfo(), distributionInfo);
        short shortKeyColumnCount = 1;
        table.setIndexMeta(1000, "group1", columns, 1, 1, shortKeyColumnCount, TStorageType.COLUMN, KeysType.AGG_KEYS);

        List<Column> column = Lists.newArrayList();
        column.add(column2);
        table.setIndexMeta(1, "test", column, 1, 1, shortKeyColumnCount,
                TStorageType.COLUMN, KeysType.AGG_KEYS);
        Deencapsulation.setField(table, "baseIndexId", 1000);
        table.addPartition(partition);
        CreateTableInfo info = new CreateTableInfo("db1", table);
        info.write(dos);

        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));

        CreateTableInfo rInfo1 = CreateTableInfo.read(dis);
        Assert.assertTrue(rInfo1.getTable().equals(table));
        Assert.assertTrue(rInfo1.equals(info));
        Assert.assertEquals(rInfo1.getDbName(), "db1");

        // 3. delete files
        dis.close();
        file.delete();
    }
}
