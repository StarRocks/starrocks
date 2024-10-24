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

package com.starrocks.common;

import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.common.SqlDigestBuilder;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SqlDigestBuilderTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        SessionVariable sessionVariable = new SessionVariable();
        connectContext.setSessionVariable(sessionVariable);
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("db1").useDatabase("db1")
                .withTable("CREATE TABLE db1.tbl1\n" +
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
                        "PROPERTIES('replication_num' = '1');");
    }

    @Test
    void testWhereAndLimit() throws Exception {
        String sql1 = "select k1 from db1.tbl1 where k1 > 1";
        String sql2 = "select k1 from db1.tbl1 where k1 > 5";
        String digest1 = SqlDigestBuilder.build(com.starrocks.sql.parser.SqlParser.parse(sql1, 32L).get(0));
        String digest2 = SqlDigestBuilder.build(com.starrocks.sql.parser.SqlParser.parse(sql2, 32L).get(0));
        Assertions.assertEquals(digest1, digest2);
    }

    @Test
    void testLimit() throws Exception {
        String sql1 = "select k1 from db1.tbl1 where k1 > 1 limit 10";
        String sql2 = "select k1 from db1.tbl1 where k1 > 5 limit 999";
        String digest1 = SqlDigestBuilder.build(com.starrocks.sql.parser.SqlParser.parse(sql1, 32L).get(0));
        String digest2 = SqlDigestBuilder.build(com.starrocks.sql.parser.SqlParser.parse(sql2, 32L).get(0));
        Assertions.assertEquals(digest1, digest2);
    }

    @Test
    void testInPredicate() throws Exception {
        String sql1 = "select k1 from db1.tbl1 where k1 in (1, 2, 3)";
        String sql2 = "select k1 from db1.tbl1 where k1 in (4, 5, 6)";
        String digest1 = SqlDigestBuilder.build(com.starrocks.sql.parser.SqlParser.parse(sql1, 32L).get(0));
        String digest2 = SqlDigestBuilder.build(com.starrocks.sql.parser.SqlParser.parse(sql2, 32L).get(0));
        Assertions.assertEquals(digest1, digest2);

        String sql3 = "select k1 from db1.tbl1 where k1 in (1, 2, 3, 4, 5, 6)";
        String sql4 = "select k1 from db1.tbl1 where k1 in (4, 5, 6)";
        String digest3 = SqlDigestBuilder.build(com.starrocks.sql.parser.SqlParser.parse(sql3, 32L).get(0));
        String digest4 = SqlDigestBuilder.build(com.starrocks.sql.parser.SqlParser.parse(sql4, 32L).get(0));
        Assertions.assertEquals(digest3, digest4);

        String sql5 = "select k1 from db1.tbl1 where k1 in ( select k1 from db1.tbl1 limit 1 )";
        String sql6 = "select k1 from db1.tbl1 where k1 in ( select k1 from db1.tbl1 limit 100 )";
        String digest5 = SqlDigestBuilder.build(com.starrocks.sql.parser.SqlParser.parse(sql5, 32L).get(0));
        String digest6 = SqlDigestBuilder.build(com.starrocks.sql.parser.SqlParser.parse(sql6, 32L).get(0));
        Assertions.assertEquals(digest5, digest6);

        String sql7 = "select k1 from db1.tbl1 where k1 in ( select k1 + 1 from db1.tbl1 )";
        String sql8 = "select k1 from db1.tbl1 where k1 in ( select k1 from db1.tbl1 limit 100 )";
        String digest7 = SqlDigestBuilder.build(com.starrocks.sql.parser.SqlParser.parse(sql7, 32L).get(0));
        String digest8 = SqlDigestBuilder.build(com.starrocks.sql.parser.SqlParser.parse(sql8, 32L).get(0));
        Assertions.assertNotEquals(digest7, digest8);

        String sql9 = "select k1 from db1.tbl1 where k1 in (abs(1), greatest(1, 2, 3))";
        String sql10 = "select k1 from db1.tbl1 where k1 in (abs(2), greatest(4, 5, 6))";
        String digest9 = SqlDigestBuilder.build(com.starrocks.sql.parser.SqlParser.parse(sql9, 32L).get(0));
        String digest10 = SqlDigestBuilder.build(com.starrocks.sql.parser.SqlParser.parse(sql10, 32L).get(0));
        Assertions.assertEquals(digest9, digest10);

        String sql11 = "select k1 from db1.tbl1 where k1 in (abs(1), greatest(1, 2, 3))";
        String sql12 = "select k1 from db1.tbl1 where k1 in (abs(2))";
        String digest11 = SqlDigestBuilder.build(com.starrocks.sql.parser.SqlParser.parse(sql11, 32L).get(0));
        String digest12 = SqlDigestBuilder.build(com.starrocks.sql.parser.SqlParser.parse(sql12, 32L).get(0));
        Assertions.assertNotEquals(digest11, digest12);
    }

    @Test
    void testNotInPredicate() throws Exception {
        String sql1 = "select k1 from db1.tbl1 where k1 in (1, 2, 3)";
        String sql2 = "select k1 from db1.tbl1 where k1 not in (1, 2, 3)";
        String digest1 = SqlDigestBuilder.build(com.starrocks.sql.parser.SqlParser.parse(sql1, 32L).get(0));
        String digest2 = SqlDigestBuilder.build(com.starrocks.sql.parser.SqlParser.parse(sql2, 32L).get(0));
        Assertions.assertNotEquals(digest1, digest2);

        String sql3 = "select k1 from db1.tbl1 where k1 not in (1, 2, 3)";
        String sql4 = "select k1 from db1.tbl1 where k1 not in (5, 6, 7, 8)";
        String digest3 = SqlDigestBuilder.build(com.starrocks.sql.parser.SqlParser.parse(sql3, 32L).get(0));
        String digest4 = SqlDigestBuilder.build(com.starrocks.sql.parser.SqlParser.parse(sql4, 32L).get(0));
        Assertions.assertEquals(digest3, digest4);
    }

    @Test
    void testFunction() throws Exception {
        String sql1 = "select least(k1, 1) from db1.tbl1";
        String sql2 = "select least(k1, 9) from db1.tbl1";
        String digest1 = SqlDigestBuilder.build(com.starrocks.sql.parser.SqlParser.parse(sql1, 32L).get(0));
        String digest2 = SqlDigestBuilder.build(com.starrocks.sql.parser.SqlParser.parse(sql2, 32L).get(0));
        Assertions.assertEquals(digest1, digest2);
    }

    @Test
    void testArithmetic() throws Exception {
        String sql1 = "select k1 + 1 from db1.tbl1";
        String sql2 = "select k1 + 9 from db1.tbl1";

        String digest1 = SqlDigestBuilder.build(com.starrocks.sql.parser.SqlParser.parse(sql1, 32L).get(0));
        String digest2 = SqlDigestBuilder.build(com.starrocks.sql.parser.SqlParser.parse(sql2, 32L).get(0));
        Assertions.assertEquals(digest1, digest2);
    }

    @Test
    void testCaseWhen() throws Exception {
        String sql1 = "select k1+20, case k2 when k3 then 1 else 0 end from db1.tbl1";
        String sql2 = "select k1+20, case k2 when k3 then 666 else 999 end from db1.tbl1";
        String digest1 = SqlDigestBuilder.build(com.starrocks.sql.parser.SqlParser.parse(sql1, 32L).get(0));
        String digest2 = SqlDigestBuilder.build(com.starrocks.sql.parser.SqlParser.parse(sql2, 32L).get(0));
        Assertions.assertEquals(digest1, digest2);
    }
}
