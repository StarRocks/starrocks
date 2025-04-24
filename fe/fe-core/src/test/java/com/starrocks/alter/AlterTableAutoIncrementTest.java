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

import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import java.util.List;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

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

        // Insert rows with predetermined auto-increment values
        connectContext.executeSql("INSERT INTO test_auto_increment (id, name) VALUES (1, 'test1')");
        connectContext.executeSql("INSERT INTO test_auto_increment (id, name) VALUES (2, 'test2')");

        // Verify current auto-increment values (should be 1, 2)
        List<List<String>> resultRows = executeQuery("SELECT id FROM test_auto_increment ORDER BY id");
        Assert.assertEquals(2, resultRows.size());
        Assert.assertEquals("1", resultRows.get(0).get(0));
        Assert.assertEquals("2", resultRows.get(1).get(0));

        // Alter table to set auto-increment to 3
        connectContext.executeSql("ALTER TABLE test_auto_increment AUTO_INCREMENT = 3");

        // Insert another row without specifying id
        connectContext.executeSql("INSERT INTO test_auto_increment (name) VALUES ('test3')");

        // Verify that the new row has id=3
        resultRows = executeQuery("SELECT id FROM test_auto_increment WHERE name = 'test3'");
        Assert.assertEquals(1, resultRows.size());
        Assert.assertEquals("3", resultRows.get(0).get(0));

        // Insert one more row to check auto-increment continues from 3
        connectContext.executeSql("INSERT INTO test_auto_increment (name) VALUES ('test4')");
        resultRows = executeQuery("SELECT id FROM test_auto_increment WHERE name = 'test4'");
        Assert.assertEquals(1, resultRows.size());
        Assert.assertEquals("4", resultRows.get(0).get(0));

        // Verify we now have 4 rows in total
        resultRows = executeQuery("SELECT COUNT(*) FROM test_auto_increment");
        Assert.assertEquals("4", resultRows.get(0).get(0));
      }

    /**
     * Helper method to execute a query and return the result rows.
     */
    private List<List<String>> executeQuery(String sql) throws Exception {
        StatementBase stmt = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        StmtExecutor executor = new StmtExecutor(connectContext, stmt);
        executor.execute();
        ShowResultSet resultSet = executor.getShowResultSet();
        Assert.assertNotNull("Failed to get result set for query: " + sql, resultSet);
        return resultSet.getResultRows();
    }
}
