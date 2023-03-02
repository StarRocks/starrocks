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

import com.starrocks.catalog.SchemaTable;
import com.starrocks.pseudocluster.PseudoCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;

public class InformationSchemaBeFeTableTest {
    @BeforeClass
    public static void setUp() throws Exception {
        PseudoCluster.getOrCreateWithRandomPort(true, 3);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().shutdown(true);
    }

    @Test
    public void testQueryBeSchemaTables() throws Exception {
        Connection connection = PseudoCluster.getInstance().getQueryConnection();
        Statement stmt = connection.createStatement();
        try {
            Assert.assertTrue(stmt.execute("select * from information_schema.be_tablets"));
            Assert.assertEquals(SchemaTable.TABLE_MAP.get("be_tablets").getColumns().size(),
                    stmt.getResultSet().getMetaData().getColumnCount());
            System.out.printf("get %d rows\n", stmt.getUpdateCount());
            Assert.assertTrue(stmt.execute("select * from information_schema.be_txns"));
            Assert.assertEquals(SchemaTable.TABLE_MAP.get("be_txns").getColumns().size(),
                    stmt.getResultSet().getMetaData().getColumnCount());
            Assert.assertTrue(stmt.execute("select * from information_schema.be_configs"));
            Assert.assertEquals(SchemaTable.TABLE_MAP.get("be_configs").getColumns().size(),
                    stmt.getResultSet().getMetaData().getColumnCount());
            Assert.assertTrue(stmt.execute("select * from information_schema.be_metrics"));
            Assert.assertEquals(SchemaTable.TABLE_MAP.get("be_metrics").getColumns().size(),
                    stmt.getResultSet().getMetaData().getColumnCount());
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
            Assert.assertFalse(
                    stmt.execute("update information_schema.be_configs set value=\"1000\" where name=\"txn_info_history_size\""));
        } finally {
            stmt.close();
            connection.close();
        }
    }
}
