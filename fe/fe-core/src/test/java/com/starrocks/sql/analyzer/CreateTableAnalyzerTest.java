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

package com.starrocks.sql.analyzer;

import com.starrocks.common.FeConstants;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class CreateTableAnalyzerTest {

    private static ConnectContext connectContext;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();


    @BeforeClass
    public static void beforeClass() throws Exception {

        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test_create_table_db");


    }

    @Test
    public void testAnalyze() throws Exception {

        String sql = "CREATE TABLE test_create_table_db.starrocks_test_table\n" +
                "(\n" +
                "    `tag_id` string,\n" +
                "    `tag_name` string\n" +
                ") ENGINE = OLAP PRIMARY KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`)\n" +
                "ORDER BY(`id`)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"enable_persistent_index\" = \"true\",\n" +
                "\"replicated_storage\" = \"true\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ")\n";

        expectedEx.expect(AnalysisException.class);
        expectedEx.expectMessage("Getting analyzing error. Detail message: Unknown column 'id' does not exist.");
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);

        CreateTableAnalyzer.analyze(createTableStmt, connectContext);
    }

}
