// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/CreateMaterializedViewStmtTest.java

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

import com.google.common.collect.Lists;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.CatalogUtils;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.UserException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class CreateMaterializedViewTest {

    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        // create connect context
        ConnectContext connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.tbl1\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('2020-02-01'),\n" +
                        "    PARTITION p2 values less than('2020-03-01')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE test.tbl2\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('2021-02-01'),\n" +
                        "    PARTITION p2 values less than('2021-03-01')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE `t1` (\n" +
                        "  `c_1_0` decimal128(30, 4) NOT NULL COMMENT \"\",\n" +
                        "  `c_1_1` boolean NOT NULL COMMENT \"\",\n" +
                        "  `c_1_2` date NULL COMMENT \"\",\n" +
                        "  `c_1_3` date NOT NULL COMMENT \"\",\n" +
                        "  `c_1_4` double NULL COMMENT \"\",\n" +
                        "  `c_1_5` double NULL COMMENT \"\",\n" +
                        "  `c_1_6` datetime NULL COMMENT \"\",\n" +
                        "  `c_1_7` ARRAY<int(11)> NULL COMMENT \"\",\n" +
                        "  `c_1_8` smallint(6) NULL COMMENT \"\",\n" +
                        "  `c_1_9` bigint(20) NOT NULL COMMENT \"\",\n" +
                        "  `c_1_10` varchar(31) NOT NULL COMMENT \"\",\n" +
                        "  `c_1_11` decimal128(22, 18) NULL COMMENT \"\",\n" +
                        "  `c_1_12` boolean NULL COMMENT \"\"\n" +
                        ") ENGINE=OLAP \n" +
                        "DUPLICATE KEY(`c_1_0`, `c_1_1`, `c_1_2`, `c_1_3`)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "PARTITION BY RANGE(`c_1_3`)\n" +
                        "(PARTITION p20000101 VALUES [('2000-01-01'), ('2010-12-31')),\n" +
                        "PARTITION p20101231 VALUES [('2010-12-31'), ('2021-12-30')),\n" +
                        "PARTITION p20211230 VALUES [('2021-12-30'), ('2032-12-29')))\n" +
                        "DISTRIBUTED BY HASH(`c_1_3`, `c_1_2`, `c_1_0`) BUCKETS 10 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"in_memory\" = \"false\",\n" +
                        "\"storage_format\" = \"DEFAULT\"\n" +
                        ");");
    }


    @Test
    public void testFullDesc() throws Exception{
        ConnectContext ctx = starRocksAssert.getCtx();
        String sql = "create materialized view mv1 " +
                "partition by tbl1.k1 " +
                "distributed by hash(ss) " +
                "refresh async START('2016-12-31') EVERY(INTERVAL 1 HOUR) " +
                "as select tbl1.k1 ss from tbl1 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testSelectHasStar() throws Exception{

    }


    @Test
    public void testPartitionByColumn() throws Exception {

    }

    @Test
    public void testPartitionByFunction() throws Exception {
    }

    @Test
    public void testPartitionByFunctionNoSlotRef() throws Exception {
    }

    @Test
    public void testPartitionByFunctionNoSupports() throws Exception {
    }

    @Test
    public void testPartitionByFunctionNoColumn() throws Exception {

    }

    @Test
    public void testPartitionByInSelectList() throws Exception {

    }

    @Test
    public void testDistributeBy() throws Exception {

    }

    @Test
    public void testDistributeByIsNull() throws Exception {

    }


    @Test
    public void testRefreshAsyncHasAllArgs() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String sql = "create materialized view star_view " +
                "partition by tbl1.k1 " +
                "distributed by hash(k2) " +
                "refresh async ('2016-12-31',INTERVAL 1 HOURS)" +
                "as select * from tbl1;";
        UtFrameUtils.parseStmtWithNewParser(sql, ctx);
    }

    @Test
    public void testRefreshAsyncNoArgs() {

    }

    @Test
    public void testRefreshAsyncOnlyStart() {

    }

    @Test
    public void testRefreshAsyncOnlyEvery() {

    }

    @Test
    public void testRefreshManual() {

    }

    @Test
    public void testQueryStatementNoSelectRelation() {

    }

    @Test
    public void testTableNotInOneDatabase() {

    }

    @Test
    public void testTableNoOlapTable() {

    }

    @Test
    public void testSelectItemNoAlias() {

    }


}

