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

package com.starrocks.alter;

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class AlterTableAutoIncrementTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.alter_scheduler_interval_millisecond = 100;
        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test");
    }

    @Test
    public void testAlterTableAutoIncrement() throws Exception {
        // Create a table with auto_increment column
        starRocksAssert.withTable("CREATE TABLE test_auto_increment (" +
                "id BIGINT NOT NULL AUTO_INCREMENT," +
                "name VARCHAR(100)" +
                ") " +
                "PRIMARY KEY (`id`) " +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 1 " +
                "PROPERTIES(\"replication_num\" = \"1\");");

        // Insert some data
        String jdbcUrl = "jdbc:mysql://127.0.0.1:" + connectContext.getStarRocksPort() + "/test";
        try (Connection conn = DriverManager.getConnection(jdbcUrl, "root", "")) {
            Statement stmt = conn.createStatement();

            // Insert rows with predetermined auto-increment values
            stmt.execute("INSERT INTO test_auto_increment (id, name) VALUES (1, 'test1')");
            stmt.execute("INSERT INTO test_auto_increment (id, name) VALUES (2, 'test2')");

            // Verify current auto-increment value (should be 1, 2)
            ResultSet rs = stmt.executeQuery("SELECT * FROM test_auto_increment ORDER BY id");
            int lastId = 0;
            while (rs.next()) {
                lastId = rs.getInt("id");
                String name = rs.getString("name");
                System.out.println("ID: " + lastId + ", Name: " + name);
            }
            Assert.assertEquals(2, lastId);

            // Alter auto_increment to 3
            stmt.execute("ALTER TABLE test_auto_increment AUTO_INCREMENT = 3");

            // Get the table and verify auto_increment value
            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test").getTable("test_auto_increment");
            OlapTable olapTable = (OlapTable) table;
            long autoIncrementValue = olapTable.getAutoIncrementValue();
            Assert.assertEquals(3, autoIncrementValue);

            // Insert another row without specifying id
            stmt.execute("INSERT INTO test_auto_increment (name) VALUES ('test3')");

            // Verify that the new row has id=3
            rs = stmt.executeQuery("SELECT * FROM test_auto_increment WHERE name = 'test3'");
            if (rs.next()) {
                int id = rs.getInt("id");
                Assert.assertEquals(3, id);
            } else {
                Assert.fail("Row with name='test3' not found");
            }

            // Insert one more row to check auto-increment continues from 3
            stmt.execute("INSERT INTO test_auto_increment (name) VALUES ('test4')");
            rs = stmt.executeQuery("SELECT * FROM test_auto_increment WHERE name = 'test4'");
            if (rs.next()) {
                int id = rs.getInt("id");
                Assert.assertEquals(4, id);
            } else {
                Assert.fail("Row with name='test4' not found");
            }
        }
    }
}
