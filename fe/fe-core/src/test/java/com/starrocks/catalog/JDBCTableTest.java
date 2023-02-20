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

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TJDBCTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class JDBCTableTest {
    private String table;
    private String resourceName;
    private List<Column> columns;
    private Map<String, String> properties;

    @Before
    public void setUp() {
        table = "table0";
        resourceName = "jdbc0";

        columns = Lists.newArrayList();
        Column column = new Column("col1", Type.BIGINT, true);
        columns.add(column);

        properties = Maps.newHashMap();
        properties.put("table", table);
        properties.put("resource", resourceName);
    }

    private Resource getMockedJDBCResource(String name) throws Exception {
        FeConstants.runningUnitTest = true;
        Resource jdbcResource = new JDBCResource(name);
        Map<String, String> resourceProperties = Maps.newHashMap();
        resourceProperties.put("jdbc_uri", "jdbc_uri");
        resourceProperties.put("user", "user0");
        resourceProperties.put("password", "password0");
        resourceProperties.put("driver_url", "driver_url");
        resourceProperties.put("driver_class", "driver_class");
        jdbcResource.setProperties(resourceProperties);
        FeConstants.runningUnitTest = false;
        return jdbcResource;
    }

    @Test
    public void testWithProperties(@Mocked GlobalStateMgr globalStateMgr,
                                   @Mocked ResourceMgr resourceMgr) throws Exception {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                globalStateMgr.getResourceMgr();
                result = resourceMgr;

                resourceMgr.getResource("jdbc0");
                result = getMockedJDBCResource(resourceName);
            }
        };
        JDBCTable table = new JDBCTable(1000, "jdbc_table", columns, properties);
        Assert.assertEquals(this.resourceName, table.getResourceName());
        Assert.assertEquals(this.table, table.getJdbcTable());
    }

    @Test
    public void testToThrift(@Mocked GlobalStateMgr globalStateMgr,
                             @Mocked ResourceMgr resourceMgr) throws Exception {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                globalStateMgr.getResourceMgr();
                result = resourceMgr;

                resourceMgr.getResource("jdbc0");
                result = getMockedJDBCResource(resourceName);
            }
        };
        JDBCTable table = new JDBCTable(1000, "jdbc_table", columns, properties);
        TTableDescriptor tableDescriptor = table.toThrift(null);

        // build expected table descriptor
        JDBCResource resource = (JDBCResource) getMockedJDBCResource("jdbc0");
        TTableDescriptor expectedDesc =
                new TTableDescriptor(1000, TTableType.JDBC_TABLE, columns.size(), 0, "jdbc_table", "");
        TJDBCTable expectedTable = new TJDBCTable();
        // we will not compute checksum in ut, so we can skip to setJdbc_driver_checksum
        expectedTable.setJdbc_driver_name(resource.getName());
        expectedTable.setJdbc_driver_url(resource.getProperty(JDBCResource.DRIVER_URL));
        expectedTable.setJdbc_driver_class(resource.getProperty(JDBCResource.DRIVER_CLASS));
        expectedTable.setJdbc_url(resource.getProperty(JDBCResource.URI));
        expectedTable.setJdbc_table(this.table);
        expectedTable.setJdbc_user(resource.getProperty(JDBCResource.USER));
        expectedTable.setJdbc_passwd(resource.getProperty(JDBCResource.PASSWORD));
        expectedDesc.setJdbcTable(expectedTable);

        Assert.assertEquals(tableDescriptor, expectedDesc);
    }

    @Test(expected = DdlException.class)
    public void testWithIlegalResourceName(@Mocked GlobalStateMgr globalStateMgr,
                                           @Mocked ResourceMgr resourceMgr) throws Exception {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                globalStateMgr.getResourceMgr();
                result = resourceMgr;

                resourceMgr.getResource("jdbc0");
                result = null;
            }
        };
        new JDBCTable(1000, "jdbc_table", columns, properties);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testWithIlegalResourceType(@Mocked GlobalStateMgr globalStateMgr,
                                           @Mocked ResourceMgr resourceMgr) throws Exception {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                globalStateMgr.getResourceMgr();
                result = resourceMgr;

                resourceMgr.getResource("jdbc0");
                result = new SparkResource("jdbc0");
            }
        };
        new JDBCTable(1000, "jdbc_table", columns, properties);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testNoResource() throws Exception {
        properties.remove("resource");
        new JDBCTable(1000, "jdbc_table", columns, properties);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testNoTable() throws Exception {
        properties.remove("table");
        new JDBCTable(1000, "jdbc_table", columns, properties);
        Assert.fail("No exception throws.");
    }
}
