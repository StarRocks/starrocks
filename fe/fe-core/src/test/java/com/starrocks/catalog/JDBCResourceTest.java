// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.common.io.FileBackedOutputStream;
import com.starrocks.analysis.AccessTestUtil;
import com.starrocks.analysis.Analyzer;
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
        // 1. Write objects to file
        File file = new File("./jdbcResource");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

        Map<String, String> configs = new HashMap<>();
        configs.put("user", "user");
        configs.put("password", "password");
        configs.put("driver", "driver");
        configs.put("jdbc_uri", "jdbc:postgresql://host1:port1,host2:port2/");
        configs.put("socketTimeout", "1000");
        JDBCResource jdbcResourceWrite = new JDBCResource("jdbc_resource_test", configs);
        jdbcResourceWrite.write(dos);

        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));

        JDBCResource jdbcResourceRead = (JDBCResource) JDBCResource.read(dis);

        Assert.assertEquals("jdbc_resource_test", jdbcResourceRead.getName());
        Assert.assertEquals(jdbcResourceRead.getProperty("user"), "user");
        Assert.assertEquals(jdbcResourceRead.getProperty("password"), "password");
        Assert.assertEquals(jdbcResourceRead.getProperty("driver"), "driver");
        Assert.assertEquals(jdbcResourceRead.getProperty("jdbc_uri"), "jdbc:postgresql://host1:port1,host2:port2/");
        Assert.assertEquals(jdbcResourceRead.getProperty("socketTimeout"), "1000");

        dis.close();
        file.delete();
    }
}
