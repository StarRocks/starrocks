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

package com.starrocks.transaction;

import com.starrocks.common.Config;
import com.starrocks.pseudocluster.PseudoCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;

public class WriteQuorumTest {
    @BeforeClass
    public static void setUp() throws Exception {
        Config.enable_new_publish_mechanism = true;
        PseudoCluster.getOrCreateWithRandomPort(true, 3);
        PseudoCluster cluster = PseudoCluster.getInstance();
        cluster.runSql(null, "create database test");
        cluster.runSql("test",
                "create table test_default ( pk bigint NOT NULL, v0 string not null, v1 int not null ) " +
                        "primary KEY (pk) DISTRIBUTED BY HASH(pk) BUCKETS 3 " +
                        "PROPERTIES(\"replication_num\" = \"3\", \"storage_medium\" = \"SSD\");");
        cluster.runSql("test",
                "create table test_all ( pk bigint NOT NULL, v0 string not null, v1 int not null ) " +
                        "primary KEY (pk) DISTRIBUTED BY HASH(pk) BUCKETS 3 " +
                        "PROPERTIES(\"write_quorum\" = \"all\", \"replication_num\" = \"3\", \"storage_medium\" = \"SSD\");");
        cluster.runSql("test",
                "create table test_one ( pk bigint NOT NULL, v0 string not null, v1 int not null ) " +
                        "primary KEY (pk) DISTRIBUTED BY HASH(pk) BUCKETS 3 " +
                        "PROPERTIES(\"write_quorum\" = \"one\", \"replication_num\" = \"3\", \"storage_medium\" = \"SSD\");");
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
            cluster.runSql("test", "insert into test_default values (1,\"1\", 1), (2,\"2\",2), (3,\"3\",3);");
            cluster.runSql("test", "insert into test_all values (1,\"1\", 1), (2,\"2\",2), (3,\"3\",3);");
            cluster.runSql("test", "insert into test_one values (1,\"1\", 1), (2,\"2\",2), (3,\"3\",3);");
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
                cluster.runSql("test", "insert into test_default values (1,\"1\", 1), (2,\"2\",2), (3,\"3\",3);");
                cluster.runSql("test", "insert into test_one values (1,\"1\", 1), (2,\"2\",2), (3,\"3\",3);");
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
            cluster.runSql("test", "insert into test_default values (1,\"1\", 1), (2,\"2\",2), (3,\"3\",3);");
        } finally {
            cluster.getBackend(10001).setWriteFailureRate(0.0f);
            cluster.getBackend(10002).setWriteFailureRate(0.0f);
        }
    }

    @Test(expected = SQLException.class)
    public void test3Replica1FailInsertAllFail() throws Exception {
        PseudoCluster cluster = PseudoCluster.getInstance();
        cluster.getBackend(10001).setWriteFailureRate(1.0f);
        try {
            cluster.runSql("test", "insert into test_all values (1,\"1\", 1), (2,\"2\",2), (3,\"3\",3);");
        } finally {
            cluster.getBackend(10001).setWriteFailureRate(0.0f);
        }
    }

    @Test(expected = SQLException.class)
    public void test3Replica3FailInsertFail() throws Exception {
        PseudoCluster cluster = PseudoCluster.getInstance();
        cluster.getBackend(10001).setWriteFailureRate(1.0f);
        cluster.getBackend(10002).setWriteFailureRate(1.0f);
        cluster.getBackend(10003).setWriteFailureRate(1.0f);
        try {
            cluster.runSql("test", "insert into test_one values (1,\"1\", 1), (2,\"2\",2), (3,\"3\",3);");
        } finally {
            cluster.getBackend(10001).setWriteFailureRate(0.0f);
            cluster.getBackend(10002).setWriteFailureRate(0.0f);
            cluster.getBackend(10003).setWriteFailureRate(0.0f);
        }
    }

}
