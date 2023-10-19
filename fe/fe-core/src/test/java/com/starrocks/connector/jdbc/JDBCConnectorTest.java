// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.jdbc;

import com.starrocks.catalog.JDBCResource;
import com.starrocks.common.FeConstants;
import com.starrocks.connector.ConnectorContext;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class JDBCConnectorTest {
    @Test
    public void testProperties() {
        FeConstants.runningUnitTest = true;
        Map<String, String> properties = new HashMap<>();
        properties.put(JDBCResource.DRIVER_CLASS, "com.mysql.cj.jdbc.Driver");
        properties.put(JDBCResource.URI, "jdbc:mysql://127.0.0.1:3306");
        properties.put(JDBCResource.USER, "root");
        properties.put(JDBCResource.PASSWORD, "123456");
        ConnectorContext context = new ConnectorContext("jdbcmysql", "jdbc", properties);
        Assert.assertThrows(IllegalArgumentException.class, () -> new JDBCConnector(context));
        properties.put(JDBCResource.DRIVER_URL, "xxxx");
        try {
            new JDBCConnector(context);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }
}
