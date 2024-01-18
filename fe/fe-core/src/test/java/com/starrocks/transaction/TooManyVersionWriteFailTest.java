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

import com.starrocks.common.conf.Config;
import com.starrocks.pseudocluster.PseudoBackend;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.pseudocluster.Tablet;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TooManyVersionWriteFailTest {
    @BeforeClass
    public static void setUp() throws Exception {
        Config.enable_new_publish_mechanism = true;
        PseudoCluster.getOrCreateWithRandomPort(true, 3);
        PseudoCluster cluster = PseudoCluster.getInstance();
        cluster.runSql(null, "create database test");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().runSql(null, "drop database test force");
        PseudoCluster.getInstance().shutdown(false);
    }

    @Test
    public void testTooManyVersionFail() throws Exception {
        PseudoCluster cluster = PseudoCluster.getInstance();
        final String tableName = "test";
        final String createSql = PseudoCluster.newCreateTableSqlBuilder()
                .setTableName(tableName)
                .setBuckets(3)
                .setReplication(3)
                .build();
        final String insertSql = PseudoCluster.buildInsertSql("test", tableName);
        cluster.runSqls("test", createSql, insertSql, insertSql, insertSql);
        Tablet.maxVersions = 4;
        try {
            for (int i = 0; i < 5; i++) {
                System.out.println("insert version " + (i + 5));
                cluster.runSql("test", insertSql);
            }
            Assert.fail("should fail with too many versions");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Too many versions"));
        }
        Tablet.compactionIntervalMs = 500;
        PseudoBackend.tabletCheckIntervalMs = 1000;
        while (Tablet.getTotalCompaction() < 9) {
            System.out.printf("sleep to wait compaction %d/9\n", Tablet.getTotalCompaction());
            Thread.sleep(1000);
        }
        // should be able to insert after compaction
        for (int i = 0; i < 3; i++) {
            cluster.runSql("test", insertSql, true);
        }
    }
}
