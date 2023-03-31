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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/TableTest.java

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

import com.google.common.collect.Lists;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.common.FeConstants;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.persist.gson.GsonUtils;
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

public class TableTest {
    private FakeGlobalStateMgr fakeGlobalStateMgr;

    private GlobalStateMgr globalStateMgr;

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
        File file = new File("./tableFamilyGroup");
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

        OlapTable table1 = new OlapTable(1000L, "group1", columns, KeysType.AGG_KEYS,
                new SinglePartitionInfo(), new RandomDistributionInfo(10));
        short shortKeyColumnCount = 1;
        table1.setIndexMeta(1000, "group1", columns, 1, 1, shortKeyColumnCount, TStorageType.COLUMN, KeysType.AGG_KEYS);
        List<Column> column = Lists.newArrayList();
        column.add(column2);

        table1.setIndexMeta(2, "test", column, 1, 1, shortKeyColumnCount, TStorageType.COLUMN,
                KeysType.AGG_KEYS);
        Deencapsulation.setField(table1, "baseIndexId", 1000);
        table1.write(dos);
        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));

        Table rFamily1 = Table.read(dis);
        Assert.assertTrue(table1.equals(rFamily1));
        Assert.assertEquals(table1.getCreateTime(), rFamily1.getCreateTime());
        Assert.assertEquals(table1.getIndexMetaByIndexId(2).getKeysType(), KeysType.AGG_KEYS);

        // 3. delete files
        dis.close();
        file.delete();
    }

    @Test
    public void testGetMysqlType() {
        Assert.assertEquals("BASE TABLE", new Table(TableType.OLAP).getMysqlType());
        Assert.assertEquals("BASE TABLE", new Table(TableType.OLAP_EXTERNAL).getMysqlType());
        Assert.assertEquals("BASE TABLE", new Table(TableType.CLOUD_NATIVE).getMysqlType());

        Assert.assertEquals("BASE TABLE", new Table(TableType.MYSQL).getMysqlType());
        Assert.assertEquals("BASE TABLE", new Table(TableType.BROKER).getMysqlType());
        Assert.assertEquals("BASE TABLE", new Table(TableType.ELASTICSEARCH).getMysqlType());
        Assert.assertEquals("BASE TABLE", new Table(TableType.HIVE).getMysqlType());
        Assert.assertEquals("BASE TABLE", new Table(TableType.ICEBERG).getMysqlType());
        Assert.assertEquals("BASE TABLE", new Table(TableType.HUDI).getMysqlType());
        Assert.assertEquals("BASE TABLE", new Table(TableType.JDBC).getMysqlType());
        Assert.assertEquals("BASE TABLE", new Table(TableType.DELTALAKE).getMysqlType());
        Assert.assertEquals("BASE TABLE", new Table(TableType.FILE).getMysqlType());

        Assert.assertEquals("VIEW", new Table(TableType.INLINE_VIEW).getMysqlType());
        Assert.assertEquals("VIEW", new Table(TableType.VIEW).getMysqlType());
        Assert.assertEquals("VIEW", new Table(TableType.MATERIALIZED_VIEW).getMysqlType());
        Assert.assertEquals("VIEW", new Table(TableType.CLOUD_NATIVE_MATERIALIZED_VIEW).getMysqlType());

        Assert.assertEquals("SYSTEM VIEW", new Table(TableType.SCHEMA).getMysqlType());
    }

    @Test
    public void testTableTypeSerialization() {
        Assert.assertEquals("\"OLAP\"", GsonUtils.GSON.toJson(TableType.OLAP));
        Assert.assertEquals("\"LAKE\"", GsonUtils.GSON.toJson(TableType.CLOUD_NATIVE));
        Assert.assertEquals("\"LAKE_MATERIALIZED_VIEW\"", GsonUtils.GSON.toJson(TableType.CLOUD_NATIVE_MATERIALIZED_VIEW));
        Assert.assertEquals("\"MYSQL\"", GsonUtils.GSON.toJson(TableType.MYSQL));
        Assert.assertEquals("\"OLAP_EXTERNAL\"", GsonUtils.GSON.toJson(TableType.OLAP_EXTERNAL));
        Assert.assertEquals("\"SCHEMA\"", GsonUtils.GSON.toJson(TableType.SCHEMA));
        Assert.assertEquals("\"INLINE_VIEW\"", GsonUtils.GSON.toJson(TableType.INLINE_VIEW));
        Assert.assertEquals("\"VIEW\"", GsonUtils.GSON.toJson(TableType.VIEW));
        Assert.assertEquals("\"BROKER\"", GsonUtils.GSON.toJson(TableType.BROKER));
        Assert.assertEquals("\"ELASTICSEARCH\"", GsonUtils.GSON.toJson(TableType.ELASTICSEARCH));
        Assert.assertEquals("\"HIVE\"", GsonUtils.GSON.toJson(TableType.HIVE));
        Assert.assertEquals("\"ICEBERG\"", GsonUtils.GSON.toJson(TableType.ICEBERG));
        Assert.assertEquals("\"HUDI\"", GsonUtils.GSON.toJson(TableType.HUDI));
        Assert.assertEquals("\"JDBC\"", GsonUtils.GSON.toJson(TableType.JDBC));
        Assert.assertEquals("\"MATERIALIZED_VIEW\"", GsonUtils.GSON.toJson(TableType.MATERIALIZED_VIEW));
        Assert.assertEquals("\"DELTALAKE\"", GsonUtils.GSON.toJson(TableType.DELTALAKE));
        Assert.assertEquals("\"FILE\"", GsonUtils.GSON.toJson(TableType.FILE));
    }
}
