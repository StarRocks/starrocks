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

package com.starrocks.pseudocluster;

import com.starrocks.common.Config;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;

public class PseudoClusterTest {
    @BeforeClass
    public static void setUp() throws Exception {
        PseudoCluster.getOrCreateWithRandomPort(true, 3);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().shutdown(true);
    }

    @Test
    public void testCreateTabletAndInsert() throws Exception {
        Connection connection = PseudoCluster.getInstance().getQueryConnection();
        Statement stmt = connection.createStatement();
        try {
            stmt.execute("create database test");
            stmt.execute("use test");
            stmt.execute("create table test ( pk bigint NOT NULL, v0 string not null, v1 int not null ) " + 
                    "primary KEY (pk) DISTRIBUTED BY HASH(pk) BUCKETS 7 " +
                    "PROPERTIES(\"replication_num\" = \"3\", \"storage_medium\" = \"SSD\")");
            Assert.assertFalse(stmt.execute("insert into test values (1,\"1\", 1), (2,\"2\",2), (3,\"3\",3)"));
            System.out.printf("updated %d rows\n", stmt.getUpdateCount());
            stmt.execute("select * from test");
        } finally {
            stmt.close();
            connection.close();
        }
    }

    @Test
    public void testCreateLakeTable() throws Exception {
        Config.use_staros = true;
        Connection connection = PseudoCluster.getInstance().getQueryConnection();
        Statement stmt = connection.createStatement();
        try {
            stmt.execute("create database db_test_lake");
            stmt.execute("use db_test_lake");
            stmt.execute("create table test (k0 bigint NOT NULL, v0 string not null, v1 int not null ) " +
                    "ENGINE=STARROCKS duplicate KEY(k0) DISTRIBUTED BY HASH(k0) BUCKETS 7");
        } finally {
            stmt.close();
            connection.close();
        }
        Config.use_staros = false;
    }
}
