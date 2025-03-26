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

import com.starrocks.catalog.Table.TableType;
import com.starrocks.common.FeConstants;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.server.GlobalStateMgr;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
    public void testGetMysqlType_Version5() {
        GlobalVariable.version = "5.7.0";
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
    public void testGetMysqlType_Version8() {
        GlobalVariable.version = "8.0.33";
        Assert.assertEquals("TABLE", new Table(TableType.OLAP).getMysqlType());
        Assert.assertEquals("TABLE", new Table(TableType.OLAP_EXTERNAL).getMysqlType());
        Assert.assertEquals("TABLE", new Table(TableType.CLOUD_NATIVE).getMysqlType());

        Assert.assertEquals("TABLE", new Table(TableType.MYSQL).getMysqlType());
        Assert.assertEquals("TABLE", new Table(TableType.BROKER).getMysqlType());
        Assert.assertEquals("TABLE", new Table(TableType.ELASTICSEARCH).getMysqlType());
        Assert.assertEquals("TABLE", new Table(TableType.HIVE).getMysqlType());
        Assert.assertEquals("TABLE", new Table(TableType.ICEBERG).getMysqlType());
        Assert.assertEquals("TABLE", new Table(TableType.HUDI).getMysqlType());
        Assert.assertEquals("TABLE", new Table(TableType.JDBC).getMysqlType());
        Assert.assertEquals("TABLE", new Table(TableType.DELTALAKE).getMysqlType());
        Assert.assertEquals("TABLE", new Table(TableType.FILE).getMysqlType());

        Assert.assertEquals("VIEW", new Table(TableType.INLINE_VIEW).getMysqlType());
        Assert.assertEquals("VIEW", new Table(TableType.VIEW).getMysqlType());
        Assert.assertEquals("VIEW", new Table(TableType.MATERIALIZED_VIEW).getMysqlType());
        Assert.assertEquals("VIEW", new Table(TableType.CLOUD_NATIVE_MATERIALIZED_VIEW).getMysqlType());

        Assert.assertEquals("SYSTEM VIEW", new Table(TableType.SCHEMA).getMysqlType());
    }

    @Test
    public void testGetMysqlType_InvalidVersion() {
        GlobalVariable.version = "invalid.version";
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
