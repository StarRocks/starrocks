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
import com.starrocks.server.GlobalStateMgr;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TableTest {
    private FakeGlobalStateMgr fakeGlobalStateMgr;

    private GlobalStateMgr globalStateMgr;

    @BeforeEach
    public void setUp() {
        fakeGlobalStateMgr = new FakeGlobalStateMgr();
        globalStateMgr = Deencapsulation.newInstance(GlobalStateMgr.class);

        FakeGlobalStateMgr.setGlobalStateMgr(globalStateMgr);
        FakeGlobalStateMgr.setMetaVersion(FeConstants.META_VERSION);
    }

    @Test
    public void testGetMysqlType() {
        Assertions.assertEquals("BASE TABLE", new Table(TableType.OLAP).getMysqlType());
        Assertions.assertEquals("BASE TABLE", new Table(TableType.OLAP_EXTERNAL).getMysqlType());
        Assertions.assertEquals("BASE TABLE", new Table(TableType.CLOUD_NATIVE).getMysqlType());

        Assertions.assertEquals("BASE TABLE", new Table(TableType.MYSQL).getMysqlType());
        Assertions.assertEquals("BASE TABLE", new Table(TableType.BROKER).getMysqlType());
        Assertions.assertEquals("BASE TABLE", new Table(TableType.ELASTICSEARCH).getMysqlType());
        Assertions.assertEquals("BASE TABLE", new Table(TableType.HIVE).getMysqlType());
        Assertions.assertEquals("BASE TABLE", new Table(TableType.ICEBERG).getMysqlType());
        Assertions.assertEquals("BASE TABLE", new Table(TableType.HUDI).getMysqlType());
        Assertions.assertEquals("BASE TABLE", new Table(TableType.JDBC).getMysqlType());
        Assertions.assertEquals("BASE TABLE", new Table(TableType.DELTALAKE).getMysqlType());
        Assertions.assertEquals("BASE TABLE", new Table(TableType.FILE).getMysqlType());

        Assertions.assertEquals("VIEW", new Table(TableType.INLINE_VIEW).getMysqlType());
        Assertions.assertEquals("VIEW", new Table(TableType.VIEW).getMysqlType());
        Assertions.assertEquals("VIEW", new Table(TableType.MATERIALIZED_VIEW).getMysqlType());
        Assertions.assertEquals("VIEW", new Table(TableType.CLOUD_NATIVE_MATERIALIZED_VIEW).getMysqlType());

        Assertions.assertEquals("SYSTEM VIEW", new Table(TableType.SCHEMA).getMysqlType());
    }

    @Test
    public void testTableTypeSerialization() {
        Assertions.assertEquals("\"OLAP\"", GsonUtils.GSON.toJson(TableType.OLAP));
        Assertions.assertEquals("\"LAKE\"", GsonUtils.GSON.toJson(TableType.CLOUD_NATIVE));
        Assertions.assertEquals("\"LAKE_MATERIALIZED_VIEW\"", GsonUtils.GSON.toJson(TableType.CLOUD_NATIVE_MATERIALIZED_VIEW));
        Assertions.assertEquals("\"MYSQL\"", GsonUtils.GSON.toJson(TableType.MYSQL));
        Assertions.assertEquals("\"OLAP_EXTERNAL\"", GsonUtils.GSON.toJson(TableType.OLAP_EXTERNAL));
        Assertions.assertEquals("\"SCHEMA\"", GsonUtils.GSON.toJson(TableType.SCHEMA));
        Assertions.assertEquals("\"INLINE_VIEW\"", GsonUtils.GSON.toJson(TableType.INLINE_VIEW));
        Assertions.assertEquals("\"VIEW\"", GsonUtils.GSON.toJson(TableType.VIEW));
        Assertions.assertEquals("\"BROKER\"", GsonUtils.GSON.toJson(TableType.BROKER));
        Assertions.assertEquals("\"ELASTICSEARCH\"", GsonUtils.GSON.toJson(TableType.ELASTICSEARCH));
        Assertions.assertEquals("\"HIVE\"", GsonUtils.GSON.toJson(TableType.HIVE));
        Assertions.assertEquals("\"ICEBERG\"", GsonUtils.GSON.toJson(TableType.ICEBERG));
        Assertions.assertEquals("\"HUDI\"", GsonUtils.GSON.toJson(TableType.HUDI));
        Assertions.assertEquals("\"JDBC\"", GsonUtils.GSON.toJson(TableType.JDBC));
        Assertions.assertEquals("\"MATERIALIZED_VIEW\"", GsonUtils.GSON.toJson(TableType.MATERIALIZED_VIEW));
        Assertions.assertEquals("\"DELTALAKE\"", GsonUtils.GSON.toJson(TableType.DELTALAKE));
        Assertions.assertEquals("\"FILE\"", GsonUtils.GSON.toJson(TableType.FILE));
    }
}
