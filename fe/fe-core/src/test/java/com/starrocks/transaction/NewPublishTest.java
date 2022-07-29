// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.transaction;

import com.starrocks.common.Config;
import com.starrocks.pseudocluster.PseudoCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Random;

public class NewPublishTest {
    @BeforeClass
    public static void setUp() throws Exception {
        int fePort = new Random().nextInt(10000) + 50000;
        PseudoCluster.getOrCreate("pseudo_cluster", fePort, 3);
        Config.enable_new_publish_mechanism = true;
    }

    @AfterClass
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().shutdown();
    }

    @Test
    public void testInsertUsingNewPublish() throws Exception {
        Connection connection = PseudoCluster.getInstance().getQueryConnection();
        Statement stmt = connection.createStatement();
        try {
            stmt.execute("create database test");
            stmt.execute("use test");
            stmt.execute(
                    "create table test ( pk bigint NOT NULL, v0 string not null, v1 int not null ) primary KEY (pk) DISTRIBUTED BY HASH(pk) BUCKETS 3 PROPERTIES(\"replication_num\" = \"3\", \"storage_medium\" = \"SSD\");");
            Assert.assertFalse(stmt.execute("insert into test values (1,\"1\", 1), (2,\"2\",2), (3,\"3\",3);"));
            System.out.printf("updated %d rows\n", stmt.getUpdateCount());
        } finally {
            stmt.close();
            connection.close();
        }
    }
}
