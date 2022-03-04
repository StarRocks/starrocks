// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import java.util.List;
import java.util.Map;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.DdlException;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
        Resource jdbcResource = new JDBCResource(name);
        Map<String, String> resourceProperties = Maps.newHashMap();
        resourceProperties.put("jdbc_uri", "jdbc_uri");
        resourceProperties.put("user", "user0");
        resourceProperties.put("password", "password0");
        resourceProperties.put("driver", "driver0");
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
        JDBCTable table = new JDBCTable(1000, "jdbc_table", columns, properties);
        Assert.assertEquals(this.resourceName, table.getResourceName());
        Assert.assertEquals(this.table, table.getJdbcTable());
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
    public void testNoTable() throws Exception {
        properties.remove("table");
        new JDBCTable(1000, "jdbc_table", columns, properties);
        Assert.fail("No exception throws.");
    }
}
