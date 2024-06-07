// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.starrocks.common.FeConstants;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.ShowCreateTableStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class ShowCreateExternalOlapTableStmtTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        starRocksAssert = new StarRocksAssert(AnalyzeTestUtil.getConnectContext());
        starRocksAssert.withDatabase("test_db").useDatabase("test_db");
    }


    @Test
    public void testShowExternalOlapTable() throws Exception {
        DDLStmtExecutor.execute(analyzeSuccess(
                "CREATE TABLE `test_db`.`test_table` (\n" +
                        "  `k1` date NULL COMMENT \"\",\n" +
                        "  `k2` int(11) NULL COMMENT \"\",\n" +
                        "  `k3` smallint(6) NULL COMMENT \"\",\n" +
                        "  `k4` varchar(2048) NULL COMMENT \"\"\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                        "PARTITION BY RANGE(`k1`)(\n" +
                        "  PARTITION p20240606 VALUES [(\"2024-06-06\"), (\"2024-06-07\"))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(`k1`)\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");"), starRocksAssert.getCtx());

        DDLStmtExecutor.execute(analyzeSuccess(
                "CREATE EXTERNAL TABLE `test_db`.`test_table_external` (\n" +
                        "  `k1` date NULL COMMENT \"\",\n" +
                        "  `k2` int(11) NULL COMMENT \"\",\n" +
                        "  `k3` smallint(6) NULL COMMENT \"\",\n" +
                        "  `k4` varchar(2048) NULL COMMENT \"\"\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                        "PARTITION BY RANGE(`k1`)(\n" +
                        "  PARTITION p20240606 VALUES [(\"2024-06-06\"), (\"2024-06-07\"))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(`k1`)\n" +
                        "properties (" +
                        "\"host\" = \"127.0.0.2\",\n" +
                        "\"port\" = \"9020\",\n" +
                        "\"user\" = \"root\",\n" +
                        "\"password\" = \"\",\n" +
                        "\"database\" = \"test_db\",\n" +
                        "\"table\" = \"test_table\"\n" +
                        ");"), starRocksAssert.getCtx());


        String sql = "show create table test_db.test_table_external;";
        ShowCreateTableStmt showCreateTableStmt = (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        ShowResultSet resultSet = ShowExecutor.execute(showCreateTableStmt, starRocksAssert.getCtx());
        String res = resultSet.getResultRows().get(0).get(1);
        Assert.assertTrue(res.contains("host"));
        Assert.assertTrue(res.contains("\"host\" = \"127.0.0.2\""));
    }
}