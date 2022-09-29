// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.transaction;

import com.starrocks.common.Config;
import com.starrocks.pseudocluster.PseudoCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;

public class ReplicaWriteFailTest {
    @BeforeClass
    public static void setUp() throws Exception {
        Config.enable_new_publish_mechanism = true;
        PseudoCluster.getOrCreateWithRandomPort(true, 3);
        PseudoCluster cluster = PseudoCluster.getInstance();
        cluster.runSql(null, "create database test");
        cluster.runSql("test",
                "create table test ( pk bigint NOT NULL, v0 string not null, v1 int not null ) primary KEY (pk) DISTRIBUTED BY HASH(pk) BUCKETS 3 PROPERTIES(\"replication_num\" = \"3\", \"storage_medium\" = \"SSD\");");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().shutdown(true);
    }

    @Test
    public void test3Replica0FailInsertSuccess() throws Exception {
        PseudoCluster cluster = PseudoCluster.getInstance();
        int numLoad = 10;
        long startTs = System.nanoTime();
        for (int i = 0; i < numLoad; i++) {
            cluster.runSql("test", "insert into test values (1,\"1\", 1), (2,\"2\",2), (3,\"3\",3);");
        }
        double t = (System.nanoTime() - startTs) / 1e9;
        System.out.printf("numLoad:%d Time: %.2fs, %.2f tps\n", numLoad, t, numLoad / t);
    }

    @Test
    public void test3Replica1FailInsertSuccess() throws Exception {
        PseudoCluster cluster = PseudoCluster.getInstance();
        cluster.getBackend(10001).setWriteFailureRate(1.0f);
        try {
            int numLoad = 10;
            long startTs = System.nanoTime();
            for (int i = 0; i < numLoad; i++) {
                cluster.runSql("test", "insert into test values (1,\"1\", 1), (2,\"2\",2), (3,\"3\",3);");
            }
            double t = (System.nanoTime() - startTs) / 1e9;
            System.out.printf("numLoad:%d Time: %.2fs, %.2f tps\n", numLoad, t, numLoad / t);
        } finally {
            cluster.getBackend(10001).setWriteFailureRate(0.0f);
        }
    }

    @Test(expected = SQLException.class)
    public void test3Replica2FailInsertFail() throws Exception {
        PseudoCluster cluster = PseudoCluster.getInstance();
        cluster.getBackend(10001).setWriteFailureRate(1.0f);
        cluster.getBackend(10002).setWriteFailureRate(1.0f);
        try {
            cluster.runSql("test", "insert into test values (1,\"1\", 1), (2,\"2\",2), (3,\"3\",3);");
        } finally {
            cluster.getBackend(10001).setWriteFailureRate(0.0f);
            cluster.getBackend(10002).setWriteFailureRate(0.0f);
        }
    }

}
