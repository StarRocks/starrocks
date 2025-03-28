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

import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.RunMode;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.Test;

public class DictQueryFunctionTest {

    private static final String TEST_DICT_DATABASE = "dict";

    public static ConnectContext connectContext;
    public static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);


        starRocksAssert.withDatabase(TEST_DICT_DATABASE);
        starRocksAssert.useDatabase(TEST_DICT_DATABASE);


        String dictTable = "" +
                "CREATE TABLE dict_table(    \n" +
                "   key_varchar VARCHAR NOT NULL,\n" +
                "   key_dt DATETIME NOT NULL,\n" +
                "   value BIGINT(20) NOT NULL AUTO_INCREMENT    \n" +
                ") ENGINE=OLAP \n" +
                "PRIMARY KEY(`key_varchar`, `key_dt`)\n" +
                "PARTITION BY RANGE(`key_dt`)\n" +
                "(PARTITION p20230506 VALUES [(\"2023-05-06 00:00:00\"), (\"2023-05-07 00:00:00\")))\n" +
                "DISTRIBUTED BY HASH(`key_varchar`) BUCKETS 12\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\",\n" +
                "    \"enable_persistent_index\" = \"true\",\n" +
                "    \"replicated_storage\" = \"true\"\n" +
                ");";

        String dictTableDup = "" +
                "CREATE TABLE dict_table_dup(    \n" +
                "   key_varchar VARCHAR NOT NULL,\n" +
                "   key_dt DATETIME NOT NULL,\n" +
                "   value BIGINT(20) NOT NULL     \n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`key_varchar`, `key_dt`)\n" +
                "PARTITION BY RANGE(`key_dt`)\n" +
                "(PARTITION p20230506 VALUES [(\"2023-05-06 00:00:00\"), (\"2023-05-07 00:00:00\")))\n" +
                "DISTRIBUTED BY HASH(`key_varchar`) BUCKETS 12\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\",\n" +
                "    \"enable_persistent_index\" = \"true\",\n" +
                "    \"replicated_storage\" = \"true\"\n" +
                ");";

        String dictTableNoAutoInc = "" +
                "CREATE TABLE dict_table_without_inc(    \n" +
                "   key_varchar VARCHAR NOT NULL,\n" +
                "   key_dt DATETIME NOT NULL,\n" +
                "   value STRING NOT NULL    \n" +
                ") ENGINE=OLAP \n" +
                "PRIMARY KEY(`key_varchar`, `key_dt`)\n" +
                "PARTITION BY RANGE(`key_dt`)\n" +
                "(PARTITION p20230506 VALUES [(\"2023-05-06 00:00:00\"), (\"2023-05-07 00:00:00\")))\n" +
                "DISTRIBUTED BY HASH(`key_varchar`) BUCKETS 12\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\",\n" +
                "    \"enable_persistent_index\" = \"true\",\n" +
                "    \"replicated_storage\" = \"true\"\n" +
                ");";
        starRocksAssert.withTable(dictTable);
        starRocksAssert.withTable(dictTableDup);
        starRocksAssert.withTable(dictTableNoAutoInc);
    }

    @Test
    public void testFunction() throws Exception {
        new ExceptionChecker("SELECT dict_mapping('dict.dict_table', 'key', CAST('2023-05-06' AS DATETIME))")
                .ok();

        new ExceptionChecker("SELECT dict_mapping('dict_table', 'key', CAST('2023-05-06' AS DATETIME))")
                .ok();

        new ExceptionChecker("SELECT dict_mapping('dict.dict_table', 'key', CAST('2023-05-06' AS DATETIME), 'value')")
                .ok();

        new ExceptionChecker("SELECT dict_mapping('dict.dict_table', 'key', CAST('2023-05-06' AS DATETIME), 'value', true)")
                .ok();

    }

    @Test
    public void testFunctionWithException() throws Exception {
        testDictMappingFunction(
                "SELECT dict_mapping('catalog.db.tbl', 'k');",
                "dict_mapping function first param table_name should be 'db.tbl' or 'tbl' format.");

        testDictMappingFunction(
                "SELECT dict_mapping('not_exist.dict_table', 'key', '2023-05-06');",
                "Database not_exist is not found.");

        testDictMappingFunction(
                "SELECT dict_mapping('dict.dict_table_not_exist', 'key', '2023-05-06');",
                "dict table dict_table_not_exist is not found.");

        testDictMappingFunction(
                "SELECT dict_mapping('dict.dict_table_dup', 'key', '2023-05-06', 'value');",
                "dict table dict.dict_table_dup should be primary key table.");

        testDictMappingFunction(
                "SELECT dict_mapping('dict.dict_table', 'key', '2023-05-06', 'value', 'extra');",
                "dict_mapping function null_if_not_found param should be bool constant.");

        testDictMappingFunction(
                "SELECT dict_mapping('dict.dict_table', 'key', CAST('2023-05-06' AS datetime), 'v');",
                "dict_mapping function can't find value column v in dict table.");

        testDictMappingFunction(
                "SELECT dict_mapping('dict.dict_table_without_inc', 'key', CAST('2023-05-06' AS datetime));",
                "dict_mapping function can't find auto increment column in dict table.");

        testDictMappingFunction(
                "SELECT dict_mapping('dict.dict_table', 'key', '2023-05-06');",
                "dict_mapping function params not match expected");

        testDictMappingFunction(
                "SELECT dict_mapping('dict.dict_table', 'key', '2023-05-06', 'value', 'extra', true);",
                "dict_mapping function param size should be 3 - 5.");

        testDictMappingFunction(
                "SELECT dict_mapping('dict.dict_table', 'key', null, 'value', true);",
                "dict_mapping function params not match expected");

        Config.run_mode = "shared_data";
        RunMode.detectRunMode();
        testDictMappingFunction(
                "SELECT dict_mapping('dict.dict_table', 'key', null, 'value', true);",
                "dict_mapping function do not support shared data mode");
        Config.run_mode = "shared_nothing";
        RunMode.detectRunMode();
    }

    private void testDictMappingFunction(String sql, String expectException) {
        new ExceptionChecker(sql)
                .withException(SemanticException.class)
                .containMessage(expectException)
                .ok();
    }

    static class ExceptionChecker {
        private final String stmt;
        private Class<? extends Exception> expectException = null;
        private String expectMsg = null;


        ExceptionChecker(String stmt) {
            this.stmt = stmt;
        }

        public ExceptionChecker withException(Class<? extends Exception> expectException) {
            this.expectException = expectException;
            return this;
        }

        public ExceptionChecker containMessage(String expectMsg) {
            this.expectMsg = expectMsg;
            return this;
        }

        public void ok() {
            try {
                starRocksAssert.query(stmt).explainQuery();
            } catch (Exception e) {
                if (expectException != null) {
                    if (!e.getClass().isAssignableFrom(expectException)) {
                        throw new RuntimeException(
                            String.format("expect exception: %s, actual: %s", expectException.getName(), e.getClass().getName()),
                            e);
                    }
                    if (expectMsg != null) {
                        if (!e.getMessage().contains(expectMsg)) {
                            throw new RuntimeException(
                                String.format("expect exception message: %s, actual: %s", expectMsg, e.getMessage()),
                                e);
                        } else {
                            return;
                        }
                    }
                }
                throw new RuntimeException("expect no exception, actual: " + e.getMessage(), e);
            }
        }

    }


}
