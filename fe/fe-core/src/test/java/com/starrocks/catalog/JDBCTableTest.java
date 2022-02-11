// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.DdlException;
import com.sun.org.apache.bcel.internal.generic.ATHROW;
import mockit.Expectations;
import mockit.Mock;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.internal.runners.statements.ExpectException;

import java.io.*;
import java.util.List;
import java.util.Map;

public class JDBCTableTest {
    private String database;
    private String table;
    private String resourceName;
    private List<Column> columns;
    private Map<String, String> properties;

    @Before
    public void setUp() {
        database = "db0";
        table = "table0";
        resourceName = "jdbc0";

        columns = Lists.newArrayList();
        Column column = new Column("col1", Type.BIGINT, true);
        columns.add(column);

        properties = Maps.newHashMap();
        properties.put("database", database);
        properties.put("table", table);
        properties.put("resource", resourceName);
    }

    private Resource getMockedJDBCResource(String name) throws Exception {
        Resource jdbcResource = new JDBCResource(name);
        Map<String, String> resourceProperties = Maps.newHashMap();
        resourceProperties.put("hosts", "host0,host1");
        resourceProperties.put("user", "user0");
        resourceProperties.put("password", "password0");
        resourceProperties.put("driver", "driver0");
        resourceProperties.put("jdbc_type", "jdbc_type0");
        jdbcResource.setProperties(resourceProperties);
        return jdbcResource;
    }

    @Test
    public void testWithProperties(@Mocked Catalog catalog,
                                   @Mocked ResourceMgr resourceMgr) throws Exception {
        new Expectations() {
            {
                Catalog.getCurrentCatalog();
                result = catalog;

                catalog.getResourceMgr();
                result = resourceMgr;

                resourceMgr.getResource("jdbc0");
                result = getMockedJDBCResource(resourceName);
            }
        };
        // put jdbc properties
        Map<String, String> jdbcProperties = Maps.newHashMap();
        jdbcProperties.put("socketTimeout", "1000");
        jdbcProperties.put("connectionTimeout", "1000");
        properties.putAll(jdbcProperties);

        JDBCTable table = new JDBCTable(1000, "jdbc_table", columns, properties);
        Assert.assertEquals(this.resourceName, table.getResourceName());
        Assert.assertEquals(this.database, table.getJdbcDatabase());
        Assert.assertEquals(this.table, table.getJdbcTable());
        Assert.assertEquals(jdbcProperties, table.getJdbcProperties());
    }


    @Test(expected = DdlException.class)
    public void testWithIlegalResourceName(@Mocked Catalog catalog,
                                           @Mocked ResourceMgr resourceMgr) throws Exception {
        new Expectations() {
            {
                Catalog.getCurrentCatalog();
                result = catalog;

                catalog.getResourceMgr();
                result = resourceMgr;

                resourceMgr.getResource("jdbc0");
                result = null;
            }
        };
        new JDBCTable(1000, "jdbc_table", columns, properties);
        Assert.fail("No exception throws.");
    }

    @Test(expected = DdlException.class)
    public void testWithIlegalResourceType(@Mocked Catalog catalog,
                                           @Mocked ResourceMgr resourceMgr) throws Exception {
        new Expectations() {
            {
                Catalog.getCurrentCatalog();
                result = catalog;

                catalog.getResourceMgr();
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
    public void testNoDatabase() throws Exception {
        properties.remove("database");
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
