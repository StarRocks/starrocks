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
import com.google.common.collect.Maps;
import com.starrocks.common.DdlException;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.iceberg.IcebergCatalog;
import com.starrocks.connector.iceberg.IcebergCatalogType;
import com.starrocks.connector.iceberg.IcebergCustomCatalogTest;
import com.starrocks.connector.iceberg.IcebergUtil;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IcebergTableTest {
    private String db;
    private String tableName;
    String resourceName;
    private List<Column> columns;
    private Map<String, String> properties;

    @Before
    public void setUp() {
        db = "db0";
        tableName = "table0";
        resourceName = "iceberg0";

        columns = Lists.newArrayList();
        Column column = new Column("col1", Type.BIGINT, true);
        columns.add(column);

        properties = Maps.newHashMap();
        properties.put("database", db);
        properties.put("table", tableName);
        properties.put("resource", resourceName);
    }

    @Test
    public void testWithResourceName(@Mocked GlobalStateMgr globalStateMgr,
                                     @Mocked ResourceMgr resourceMgr,
                                     @Mocked IcebergCatalog icebergCatalog,
                                     @Mocked Table iTable) throws DdlException {
        Resource icebergResource = new IcebergResource(resourceName);
        Map<String, String> resourceProperties = Maps.newHashMap();
        resourceProperties.put("iceberg.catalog.hive.metastore.uris", "thrift://127.0.0.1:9083");
        resourceProperties.put("iceberg.catalog.type", "hive");
        icebergResource.setProperties(resourceProperties);

        List<Types.NestedField> fields = new ArrayList<>();
        fields.add(Types.NestedField.of(1, false, "col1", new Types.LongType()));
        Schema schema = new Schema(fields);

        new MockUp<IcebergUtil>() {
            @Mock
            public IcebergCatalog getIcebergHiveCatalog(String uris, Map<String, String> icebergProperties,
                                                        HdfsEnvironment hdfsEnvironment) {
                return icebergCatalog;
            }
        };

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;

                globalStateMgr.getResourceMgr();
                result = resourceMgr;

                resourceMgr.getResource("iceberg0");
                result = icebergResource;

                icebergCatalog.loadTable((TableIdentifier) any);
                result = iTable;

                iTable.schema();
                result = schema;
            }
        };

        properties.put("resource", resourceName);
        IcebergTable table = new IcebergTable(1000, "iceberg_table", columns, properties);
        Assert.assertEquals(tableName, table.getRemoteTableName());
        Assert.assertEquals(db, table.getRemoteDbName());
    }

    @Test
    public void testCustomWithResourceName(@Mocked GlobalStateMgr globalStateMgr,
                                           @Mocked ResourceMgr resourceMgr,
                                           @Mocked IcebergCatalog icebergCatalog,
                                           @Mocked Table iTable) throws DdlException {
        Resource icebergResource = new IcebergResource(resourceName);
        Map<String, String> resourceProperties = Maps.newHashMap();
        resourceProperties.put("iceberg.catalog.type", "custom");
        resourceProperties.put("iceberg.catalog-impl",
                IcebergCustomCatalogTest.IcebergCustomTestingCatalog.class.getName());
        icebergResource.setProperties(resourceProperties);

        List<Types.NestedField> fields = new ArrayList<>();
        fields.add(Types.NestedField.of(1, false, "col1", new Types.LongType()));
        Schema schema = new Schema(fields);

        new MockUp<IcebergUtil>() {
            @Mock
            public IcebergCatalog getIcebergCustomCatalog(String catalogImpl, Map<String, String> icebergProperties,
                                                          HdfsEnvironment hdfsEnvironment) {
                return icebergCatalog;
            }
        };

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;
                minTimes = 0;

                globalStateMgr.getResourceMgr();
                result = resourceMgr;

                resourceMgr.getResource("iceberg0");
                result = icebergResource;

                icebergCatalog.loadTable((TableIdentifier) any);
                result = iTable;

                iTable.schema();
                result = schema;
            }
        };

        properties.put("resource", resourceName);
        IcebergTable table = new IcebergTable(1000, "iceberg_table", columns, properties);
        Assert.assertEquals(tableName, table.getRemoteTableName());
        Assert.assertEquals(db, table.getRemoteDbName());
    }

    @Test
    public void testWithGlueMetaStore() throws DdlException {
        this.properties.put("iceberg.catalog.type", IcebergCatalogType.GLUE_CATALOG.name());
        IcebergTable table = new IcebergTable(1000, "iceberg_table", columns, properties);
        Assert.assertEquals(tableName, table.getRemoteTableName());
        Assert.assertEquals(db, table.getRemoteDbName());
    }

    @Test(expected = DdlException.class)
    public void testNoDb() throws DdlException {
        properties.remove("database");
        new IcebergTable(1000, "iceberg_table", columns, properties);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testNoTbl() throws DdlException {
        properties.remove("table");
        new IcebergTable(1000, "iceberg_table", columns, properties);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testNonNullAbleColumn() throws DdlException {
        List<Column> columns1 = Lists.newArrayList();
        Column column = new Column("col1", Type.BIGINT, false);
        columns1.add(column);
        properties.remove("table");
        new IcebergTable(1000, "iceberg_table", columns1, properties);
        Assert.fail("No exception throws.");
    }
}
