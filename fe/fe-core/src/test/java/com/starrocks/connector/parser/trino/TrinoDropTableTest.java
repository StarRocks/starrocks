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

package com.starrocks.connector.parser.trino;

import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.parser.SqlParser;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TrinoDropTableTest extends TrinoTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        TrinoTestBase.beforeClass();
        String dbName = "test";
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);
        starRocksAssert.withTable("CREATE TABLE `t4` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
    }

    @Test
    public void testDropTableTrinoDialect() {
        String dropTableSql = "drop table if exists test.t4";
        connectContext.getSessionVariable().setSqlDialect("trino");
        DropTableStmt dropTableStmt =
                (DropTableStmt) SqlParser.parse(dropTableSql, connectContext.getSessionVariable()).get(0);
        Assert.assertTrue(dropTableStmt.isForceDrop());

        connectContext.getSessionVariable().setSqlDialect("starrocks");
        dropTableStmt =
                (DropTableStmt) SqlParser.parse(dropTableSql, connectContext.getSessionVariable()).get(0);
        Assert.assertFalse(dropTableStmt.isForceDrop());
    }
}
