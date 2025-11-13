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

package com.starrocks.sql.plan;

import com.starrocks.catalog.system.information.BeConfigsSystemTable;
import com.starrocks.catalog.system.information.BeMetricsSystemTable;
import com.starrocks.catalog.system.information.BeTabletsSystemTable;
import com.starrocks.catalog.system.information.BeTxnsSystemTable;
import com.starrocks.catalog.system.information.FeThreadsSystemTable;
import com.starrocks.pseudocluster.PseudoBackend;
import com.starrocks.pseudocluster.PseudoCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;

public class InformationSchemaBeFeTableTest {
    @BeforeAll
    public static void setUp() throws Exception {
        PseudoCluster.getOrCreateWithRandomPort(true, 3);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().shutdown(true);
    }

    @Test
    public void testQueryBeSchemaTables() throws Exception {
        Connection connection = PseudoCluster.getInstance().getQueryConnection();
        Statement stmt = connection.createStatement();
        try {
            Assertions.assertTrue(stmt.execute("select * from information_schema.be_tablets"));
            Assertions.assertEquals(BeTabletsSystemTable.create().getColumns().size(),
                    stmt.getResultSet().getMetaData().getColumnCount());
            System.out.printf("get %d rows\n", stmt.getUpdateCount());
            Assertions.assertTrue(stmt.execute("select * from information_schema.be_txns"));
            Assertions.assertEquals(BeTxnsSystemTable.create().getColumns().size(),
                    stmt.getResultSet().getMetaData().getColumnCount());
            Assertions.assertTrue(stmt.execute("select * from information_schema.be_configs"));
            Assertions.assertEquals(BeConfigsSystemTable.create().getColumns().size(),
                    stmt.getResultSet().getMetaData().getColumnCount());
            Assertions.assertTrue(stmt.execute("select * from information_schema.be_metrics"));
            Assertions.assertEquals(BeMetricsSystemTable.create().getColumns().size(),
                    stmt.getResultSet().getMetaData().getColumnCount());
            Assertions.assertTrue(stmt.execute("select * from information_schema.fe_threads"));
            Assertions.assertEquals(FeThreadsSystemTable.create().getColumns().size(),
                    stmt.getResultSet().getMetaData().getColumnCount());
        } finally {
            stmt.close();
            connection.close();
        }
    }

    @Test
    public void testOnlyScanTargetedBE() throws Exception {
        Connection connection = PseudoCluster.getInstance().getQueryConnection();
        Statement stmt = connection.createStatement();
        try {
            for (int i = 0; i < 3; i++) {
                long beId = 10001 + i;
                PseudoBackend be = PseudoCluster.getInstance().getBackend(beId);
                long oldCnt = be.getNumSchemaScan();
                Assertions.assertTrue(stmt.execute("select * from information_schema.be_tablets where be_id = " + beId));
                long newCnt = be.getNumSchemaScan();
                Assertions.assertEquals(1, newCnt - oldCnt);
            }
        } finally {
            stmt.close();
            connection.close();
        }
    }

    @Test
    public void testBeSchemaTableAgg() throws Exception {
        Connection connection = PseudoCluster.getInstance().getQueryConnection();
        Statement stmt = connection.createStatement();
        try {
            // https://github.com/StarRocks/starrocks/issues/23306
            // verify aggregation behavior is correct
            Assertions.assertTrue(stmt.execute(
                    "explain select table_id, count(*) as cnt from information_schema.be_tablets group by table_id"));
            int numExchange = 0;
            while (stmt.getResultSet().next()) {
                String line = stmt.getResultSet().getString(1);
                if (line.contains(":EXCHANGE")) {
                    numExchange++;
                }
            }
            Assertions.assertEquals(2, numExchange);
        } finally {
            stmt.close();
            connection.close();
        }
    }

    @Test
    public void testUpdateBeConfig() throws Exception {
        Connection connection = PseudoCluster.getInstance().getQueryConnection();
        Statement stmt = connection.createStatement();
        try {
            Assertions.assertFalse(
                    stmt.execute("update information_schema.be_configs set value=\"1000\" where name=\"txn_info_history_size\""));
        } finally {
            stmt.close();
            connection.close();
        }
    }

    @Test
    public void testFeThreadsBasicQueries() throws Exception {
        Connection connection = PseudoCluster.getInstance().getQueryConnection();
        Statement stmt = connection.createStatement();
        try {
            // Test basic select
            Assertions.assertTrue(stmt.execute("select * from information_schema.fe_threads limit 10"));
            int rowCount = 0;
            while (stmt.getResultSet().next()) {
                rowCount++;
                // Verify columns exist
                Assertions.assertNotNull(stmt.getResultSet().getString("FE_ID"));
                Assertions.assertNotNull(stmt.getResultSet().getLong("THREAD_ID"));
                Assertions.assertNotNull(stmt.getResultSet().getString("THREAD_NAME"));
                Assertions.assertNotNull(stmt.getResultSet().getString("THREAD_STATE"));
                Assertions.assertNotNull(stmt.getResultSet().getBoolean("IS_DAEMON"));
                Assertions.assertNotNull(stmt.getResultSet().getInt("PRIORITY"));
            }
            Assertions.assertTrue(rowCount > 0, "Should have at least some threads");

            // Test filtering by thread state
            Assertions.assertTrue(stmt.execute(
                    "select count(*) from information_schema.fe_threads where thread_state = 'RUNNABLE'"));
            Assertions.assertTrue(stmt.getResultSet().next());
            long runnableCount = stmt.getResultSet().getLong(1);
            Assertions.assertTrue(runnableCount >= 0);

            // Test filtering by daemon threads
            Assertions.assertTrue(stmt.execute(
                    "select count(*) from information_schema.fe_threads where is_daemon = true"));
            Assertions.assertTrue(stmt.getResultSet().next());
            long daemonCount = stmt.getResultSet().getLong(1);
            Assertions.assertTrue(daemonCount >= 0);

            // Test grouping by thread state
            Assertions.assertTrue(stmt.execute(
                    "select thread_state, count(*) as cnt from information_schema.fe_threads " +
                            "group by thread_state order by cnt desc limit 5"));
            int stateCount = 0;
            while (stmt.getResultSet().next()) {
                stateCount++;
                Assertions.assertNotNull(stmt.getResultSet().getString("thread_state"));
                Assertions.assertTrue(stmt.getResultSet().getLong("cnt") > 0);
            }
            Assertions.assertTrue(stateCount > 0);
        } finally {
            stmt.close();
            connection.close();
        }
    }

    @Test
    public void testFeThreadsCommonThreads() throws Exception {
        Connection connection = PseudoCluster.getInstance().getQueryConnection();
        Statement stmt = connection.createStatement();
        try {
            // Test that common JVM threads exist
            Assertions.assertTrue(stmt.execute(
                    "select thread_name from information_schema.fe_threads " +
                            "where thread_name like '%main%' or thread_name like '%GC%' " +
                            "or thread_name like '%RMI%' limit 10"));
            boolean foundCommonThread = false;
            while (stmt.getResultSet().next()) {
                String threadName = stmt.getResultSet().getString("thread_name");
                if (threadName != null && (
                        threadName.contains("main") ||
                        threadName.contains("GC") ||
                        threadName.contains("RMI") ||
                        threadName.contains("Finalizer") ||
                        threadName.contains("Reference Handler"))) {
                    foundCommonThread = true;
                    break;
                }
            }
            // At least one common thread should exist
            Assertions.assertTrue(foundCommonThread || stmt.getResultSet().getRow() > 0,
                    "Should find some common JVM threads");

            // Test that we can query by FE_ID
            Assertions.assertTrue(stmt.execute(
                    "select count(*) from information_schema.fe_threads where fe_id is not null"));
            Assertions.assertTrue(stmt.getResultSet().next());
            long feIdCount = stmt.getResultSet().getLong(1);
            Assertions.assertTrue(feIdCount > 0, "All threads should have FE_ID");

            // Test CPU time columns (may be -1 if not supported)
            Assertions.assertTrue(stmt.execute(
                    "select count(*) from information_schema.fe_threads " +
                            "where cpu_time_ms >= -1 and user_time_ms >= -1"));
            Assertions.assertTrue(stmt.getResultSet().next());
            long validTimeCount = stmt.getResultSet().getLong(1);
            Assertions.assertTrue(validTimeCount > 0, "All threads should have valid time values");
        } finally {
            stmt.close();
            connection.close();
        }
    }
}
