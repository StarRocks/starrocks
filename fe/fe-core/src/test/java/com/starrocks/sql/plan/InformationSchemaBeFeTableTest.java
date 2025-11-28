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
                Assertions.assertTrue(
                        stmt.execute("select * from information_schema.be_tablets where be_id = " + beId));
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
                    stmt.execute(
                            "update information_schema.be_configs set value=\"1000\" where " +
                                    "name=\"txn_info_history_size\""));
        } finally {
            stmt.close();
            connection.close();
        }
    }

}
