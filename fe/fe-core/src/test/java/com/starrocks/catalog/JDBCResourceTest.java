// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.common.io.FileBackedOutputStream;
import com.google.gson.Gson;
import com.starrocks.analysis.AccessTestUtil;
import com.starrocks.analysis.Analyzer;
import com.starrocks.persist.gson.GsonUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class JDBCResourceTest {
    private Analyzer analyzer;

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
    }

    @Test
    public void testSerialization() throws Exception {
        Map<String, String> configs = new HashMap<>();
        configs.put("user", "user");
        configs.put("password", "password");
        configs.put("driver", "driver");
        configs.put("jdbc_uri", "jdbc:postgresql://host1:port1,host2:port2/");
        JDBCResource resource0 = new JDBCResource("jdbc_resource_test", configs);

        String json = GsonUtils.GSON.toJson(resource0);
        Resource resource1 = GsonUtils.GSON.fromJson(json, Resource.class);

        Assert.assertTrue(resource1 instanceof JDBCResource);
        Assert.assertEquals(resource0.getName(), resource1.getName());
        Assert.assertEquals(resource0.getProperty(JDBCResource.DRIVER), ((JDBCResource) resource1).getProperty(JDBCResource.DRIVER));
        Assert.assertEquals(resource0.getProperty(JDBCResource.URI), ((JDBCResource) resource1).getProperty(JDBCResource.URI));
        Assert.assertEquals(resource0.getProperty(JDBCResource.USER), ((JDBCResource) resource1).getProperty(JDBCResource.USER));
        Assert.assertEquals(resource0.getProperty(JDBCResource.PASSWORD), ((JDBCResource) resource1).getProperty(JDBCResource.PASSWORD));
    }
}
