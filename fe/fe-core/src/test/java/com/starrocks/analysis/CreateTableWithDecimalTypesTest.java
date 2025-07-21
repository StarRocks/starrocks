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


package com.starrocks.analysis;

import com.starrocks.common.Config;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class CreateTableWithDecimalTypesTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        AnalyzeTestUtil.init();
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("db1").useDatabase("db1");
    }

    public void createTable(boolean enableDecimalV3, String columnType) throws Exception {
        if (enableDecimalV3) {
            Config.enable_decimal_v3 = true;
        } else {
            Config.enable_decimal_v3 = false;
        }
        String createTableSql = "" +
                "CREATE TABLE if not exists db1.decimalv3_table\n" +
                "(\n" +
                "key0 INT NOT NULL,\n" +
                "col_decimal " + columnType + " NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`key0`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`key0`) BUCKETS 1\n" +
                "PROPERTIES(\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");";
        starRocksAssert.withTable(createTableSql);
    }

    @Test
    public void createTableWithDecimalV3p39s12() throws Exception {
        createTable(true, "DECIMAL(39, 12)");
    }

    @Test
    public void createTableWithDecimalV3p9s10() {
        assertThrows(Exception.class, () -> {
            createTable(true, "DECIMAL(9, 10)");
            Assertions.fail("should throw an exception");
        });
    }

    @Test
    public void createTableWithDecimalV3p0s1() {
        assertThrows(Exception.class, () -> {
            createTable(true, "DECIMAL(0, 1)");
            Assertions.fail("should throw an exception");
        });
    }

    @Test
    public void createTableWithDecimalV3ScaleAbsent() throws Exception {
        createTable(true, "DECIMAL(9)");
    }

    @Test
    public void createTableWithDecimalV2p28s9() {
        assertThrows(Exception.class, () -> {
            createTable(false, "DECIMAL(28, 9)");
            Assertions.fail("should throw an exception");
        });
    }

    @Test
    public void createTableWithDecimalV2p27s10() {
        assertThrows(Exception.class, () -> {
            createTable(false, "DECIMAL(27, 10)");
            Assertions.fail("should throw an exception");
        });
    }

    @Test
    public void createTableWithDecimalV2p9s10() {
        assertThrows(Exception.class, () -> {
            createTable(false, "DECIMAL(9, 10)");
            Assertions.fail("should throw an exception");
        });
    }

    @Test
    public void createTableWithDecimalV2p0s1() {
        assertThrows(Exception.class, () -> {
            createTable(false, "DECIMAL(0, 1)");
            Assertions.fail("should throw an exception");
        });
    }

    @Test
    public void createTableWithDecimalV2ScaleAbsent() throws Exception {
        createTable(false, "DECIMAL(9)");
    }

    @Test
    public void testCreateTableWithDecimal256Column() throws Exception {
        starRocksAssert.dropTable("decimalv3_table");
        createTable(true, "DECIMAL(50)");
        String createTableSql = starRocksAssert.showCreateTable("show create table decimalv3_table;");
        Assertions.assertTrue(createTableSql.contains("`col_decimal` decimal(50, 0)"));
        starRocksAssert.dropTable("decimalv3_table");

        createTable(true, "DECIMAL(76, 75)");
        createTableSql = starRocksAssert.showCreateTable("show create table decimalv3_table;");
        Assertions.assertTrue(createTableSql.contains("`col_decimal` decimal(76, 75)"));
        starRocksAssert.dropTable("decimalv3_table");
        ExceptionChecker.expectThrowsWithMsg(IllegalArgumentException.class,
                "DECIMAL(P[,S]) type P must be greater than or equal to the value of S",
                () -> starRocksAssert.withTable("create table test_decimal (col_decimal decimal(57, 58))"));

        ExceptionChecker.expectThrowsWithMsg(IllegalArgumentException.class,
                "DECIMAL's precision should range from 1 to 76",
                () -> starRocksAssert.withTable("create table test_decimal (col_decimal decimal(77, 58))"));
    }
}

