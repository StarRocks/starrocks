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
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class CreateTableWithDecimalTypesTest {
    private static StarRocksAssert starRocksAssert;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
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

    @Test(expected = Exception.class)
    public void createTableWithDecimalV3p39s12() throws Exception {
        createTable(true, "DECIMAL(39, 12)");
        Assert.fail("should throw an exception");
    }

    @Test(expected = Exception.class)
    public void createTableWithDecimalV3p9s10() throws Exception {
        createTable(true, "DECIMAL(9, 10)");
        Assert.fail("should throw an exception");
    }

    @Test(expected = Exception.class)
    public void createTableWithDecimalV3p0s1() throws Exception {
        createTable(true, "DECIMAL(0, 1)");
        Assert.fail("should throw an exception");
    }

    @Test
    public void createTableWithDecimalV3ScaleAbsent() throws Exception {
        createTable(true, "DECIMAL(9)");
    }

    @Test(expected = Exception.class)
    public void createTableWithDecimalV2p28s9() throws Exception {
        createTable(false, "DECIMAL(28, 9)");
        Assert.fail("should throw an exception");
    }

    @Test(expected = Exception.class)
    public void createTableWithDecimalV2p27s10() throws Exception {
        createTable(false, "DECIMAL(27, 10)");
        Assert.fail("should throw an exception");
    }

    @Test(expected = Exception.class)
    public void createTableWithDecimalV2p9s10() throws Exception {
        createTable(false, "DECIMAL(9, 10)");
        Assert.fail("should throw an exception");
    }

    @Test(expected = Exception.class)
    public void createTableWithDecimalV2p0s1() throws Exception {
        createTable(false, "DECIMAL(0, 1)");
        Assert.fail("should throw an exception");
    }

    @Test
    public void createTableWithDecimalV2ScaleAbsent() throws Exception {
        createTable(false, "DECIMAL(9)");
    }
}

