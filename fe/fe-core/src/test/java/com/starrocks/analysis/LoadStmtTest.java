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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/LoadStmtTest.java

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

import com.starrocks.common.UserException;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.DataDescription;
import com.starrocks.sql.ast.LoadStmt;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class LoadStmtTest {

    @Before
    public void setUp() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testNormal() throws UserException {
        LoadStmt stmt = (LoadStmt) analyzeSuccess(
                "LOAD LABEL test.testLabel " +
                        "(DATA INFILE(\"hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/file\") INTO TABLE `t0`)");
        DataDescription dataDescription = stmt.getDataDescriptions().get(0);
        Assert.assertEquals("test", stmt.getLabel().getDbName());
        Assert.assertEquals("testLabel", stmt.getLabel().getLabelName());
        Assert.assertFalse(dataDescription.isLoadFromTable());
        Assert.assertTrue(dataDescription.isHadoopLoad());
        Assert.assertNull(stmt.getProperties());
        Assert.assertEquals(
                "[DATA INFILE ('hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/file') INTO TABLE t0]",
                stmt.getDataDescriptions().toString());
    }

    @Test
    public void testNoData() {
        analyzeFail("LOAD LABEL test.testLabel", "No data file in load statement.");
    }

    @Test
    public void testNoDb() {
        AnalyzeTestUtil.getStarRocksAssert().useDatabase(null);
        analyzeFail("LOAD LABEL testLabel", "No database selected");
    }

    @Test
    public void testMultiTable() {
        analyzeFail(
                "LOAD LABEL testLabel (DATA FROM TABLE t0 INTO TABLE t1, DATA FROM TABLE t2 INTO TABLE t1)",
                "Only support one olap table load from one external table");
    }

    @Test
    public void testNoSparkLoad() {
        analyzeFail(
                "LOAD LABEL testLabel (DATA FROM TABLE t0 INTO TABLE t1)",
                "Load from table should use Spark Load");
    }

    @Test
    public void testBrokerLoad() {
        analyzeSuccess("LOAD LABEL test.testLabel (DATA INFILE(\"hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/file\") INTO TABLE `t0`) WITH BROKER hdfs_broker PROPERTIES (\"strict_mode\"=\"true\")");
        analyzeSuccess("LOAD LABEL test.testLabel (DATA INFILE(\"hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/file\") INTO TABLE `t0`) WITH BROKER PROPERTIES (\"strict_mode\"=\"true\")");
        analyzeSuccess("LOAD LABEL test.testLabel (DATA INFILE(\"hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/file\") INTO TABLE `t0`) WITH BROKER hdfs_broker (\"username\"=\"sr\") PROPERTIES (\"strict_mode\"=\"true\")");
        analyzeSuccess("LOAD LABEL test.testLabel (DATA INFILE(\"hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/file\") INTO TABLE `t0`) WITH BROKER (\"username\"=\"sr\") PROPERTIES (\"strict_mode\"=\"true\")");
        analyzeSuccess("LOAD LABEL test.testLabel (DATA INFILE(\"hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/file\") INTO TABLE `t0`) WITH BROKER (\"username\"=\"sr\")");
        analyzeSuccess("LOAD LABEL test.testLabel (DATA INFILE(\"hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/file\") INTO TABLE `t0` FORMAT AS CSV (SKIP_HEADER = 2 TRIM_SPACE = TRUE ENCLOSE = \"'\" ESCAPE = \"|\")) WITH BROKER (\"username\"=\"sr\")");
    }

    @Test
    public void testToString() {
        LoadStmt stmt = (LoadStmt) analyzeSuccess("LOAD  LABEL test.testLabel (DATA INFILE(\"hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/file\") INTO TABLE `t0`) WITH BROKER hdfs_broker (\"username\"=\"sr\", \"password\"=\"PASSWORDDDD\") PROPERTIES (\"strict_mode\"=\"true\")");
        Assert.assertEquals("LOAD LABEL `test`.`testLabel` (DATA INFILE ('hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/file') INTO TABLE t0) WITH BROKER hdfs_broker (\"password\"  =  \"***\", \"username\"  =  \"sr\") PROPERTIES (\"strict_mode\" = \"true\")", AstToStringBuilder.toString(stmt));

        stmt = (LoadStmt) analyzeSuccess("LOAD  LABEL test.testLabel " +
                "(DATA INFILE(\"hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/file\") INTO TABLE `t0`(v1,v2,v3))" +
                " WITH BROKER hdfs_broker (\"username\"=\"sr\", \"password\"=\"PASSWORDDDD\")" +
                " PROPERTIES (\"strict_mode\"=\"true\")");
        Assert.assertEquals("LOAD LABEL `test`.`testLabel` " +
                "(DATA INFILE ('hdfs://hdfs_host:hdfs_port/user/starRocks/data/input/file') " +
                "INTO TABLE t0 (`v1`, `v2`, `v3`)) WITH BROKER hdfs_broker " +
                "(\"password\"  =  \"***\", \"username\"  =  \"sr\") PROPERTIES (\"strict_mode\" = \"true\")",
                AstToStringBuilder.toString(stmt));

        stmt = (LoadStmt) analyzeSuccess("LOAD  LABEL test.testLabel (DATA INFILE(\"wasbs://aa@bb.blob.core.windows.net/*\") INTO TABLE `t0`) WITH BROKER " +
                "(\"gcp.gcs.service_account_email\" = \"someone@gmail.com\",\n" +
                "\"gcp.gcs.service_account_private_key_id\" = \"some_private_key_id\",\n" +
                "\"gcp.gcs.service_account_private_key\" = \"some_private_key\")");
        Assert.assertEquals("LOAD LABEL `test`.`testLabel` (DATA INFILE ('wasbs://aa@bb.blob.core.windows.net/*') INTO TABLE t0) WITH BROKER  " +
                "(\"gcp.gcs.service_account_email\"  =  \"someone@gmail.com\", \"gcp.gcs.service_account_private_key_id\"  =  \"***\", \"gcp.gcs.service_account_private_key\"  =  \"***\")", AstToStringBuilder.toString(stmt));

        stmt = (LoadStmt) analyzeSuccess("LOAD  LABEL test.testLabel (DATA INFILE(\"s3a://us-west-benchmark-data/tpch_100g/parquet/part/000004_0\") INTO TABLE `t0`) WITH BROKER " +
                "(\"aws.s3.use_instance_profile\" = \"false\",\"aws.s3.access_key\" = \"some_key\", \"aws.s3.secret_key\" = \"some_secret\", \"aws.s3.region\" = \"us-west-2\")" +
                " PROPERTIES (\"strict_mode\"=\"true\")");
        Assert.assertEquals("LOAD LABEL `test`.`testLabel` (DATA INFILE ('s3a://us-west-benchmark-data/tpch_100g/parquet/part/000004_0') INTO TABLE t0) WITH BROKER  " +
                "(\"aws.s3.access_key\"  =  \"***\", \"aws.s3.secret_key\"  =  \"***\", \"aws.s3.use_instance_profile\"  =  \"false\", \"aws.s3.region\"  =  \"us-west-2\") PROPERTIES (\"strict_mode\" = \"true\")", AstToStringBuilder.toString(stmt));

        stmt = (LoadStmt) analyzeSuccess("LOAD  LABEL test.testLabel (DATA INFILE(\"wasbs://aa@bb.blob.core.windows.net/*\") INTO TABLE `t0`) WITH BROKER " +
                "(\"azure.blob.storage_account\" = \"some_account\",\n" +
                "    \"azure.blob.shared_key\" = \"some_shared_key\")");
        Assert.assertEquals("LOAD LABEL `test`.`testLabel` (DATA INFILE ('wasbs://aa@bb.blob.core.windows.net/*') INTO TABLE t0) WITH BROKER  " +
                "(\"azure.blob.storage_account\"  =  \"some_account\", \"azure.blob.shared_key\"  =  \"***\")", AstToStringBuilder.toString(stmt));

        stmt = (LoadStmt) analyzeSuccess("LOAD  LABEL test.testLabel (DATA INFILE(\"wasbs://aa@bb.blob.core.windows.net/*\") INTO TABLE `t0`) WITH BROKER " +
                "(\"azure.blob.account_name\" = \"some_account\",\n" +
                "\"azure.blob.container_name\" = \"some_container\",\n" +
                "\"azure.blob.sas_token\" = \"some_token\")");
        Assert.assertEquals("LOAD LABEL `test`.`testLabel` (DATA INFILE ('wasbs://aa@bb.blob.core.windows.net/*') INTO TABLE t0) WITH BROKER  " +
                "(\"azure.blob.account_name\"  =  \"some_account\", \"azure.blob.container_name\"  =  \"some_container\", \"azure.blob.sas_token\"  =  \"***\")", AstToStringBuilder.toString(stmt));

        stmt = (LoadStmt) analyzeSuccess("LOAD  LABEL test.testLabel (DATA INFILE(\"wasbs://aa@bb.blob.core.windows.net/*\") INTO TABLE `t0`) WITH BROKER " +
                "(\"azure.adls1.oauth2_client_id\" = \"some_client_id\",\n" +
                "\"azure.adls1.oauth2_credential\" = \"some_credential\",\n" +
                "\"azure.adls1.oauth2_endpoint\" = \"some_endpoint\")");
        Assert.assertEquals("LOAD LABEL `test`.`testLabel` (DATA INFILE ('wasbs://aa@bb.blob.core.windows.net/*') INTO TABLE t0) WITH BROKER  " +
                "(\"azure.adls1.oauth2_credential\"  =  \"***\", \"azure.adls1.oauth2_endpoint\"  =  \"some_endpoint\", \"azure.adls1.oauth2_client_id\"  =  \"some_client_id\")", AstToStringBuilder.toString(stmt));

        stmt = (LoadStmt) analyzeSuccess("LOAD  LABEL test.testLabel (DATA INFILE(\"wasbs://aa@bb.blob.core.windows.net/*\") INTO TABLE `t0`) WITH BROKER " +
                "(\"azure.adls2.storage_account\" = \"some_account\",\n" +
                "\"azure.adls2.shared_key\" = \"some_key\")");
        Assert.assertEquals("LOAD LABEL `test`.`testLabel` (DATA INFILE ('wasbs://aa@bb.blob.core.windows.net/*') INTO TABLE t0) WITH BROKER  " +
                "(\"azure.adls2.shared_key\"  =  \"***\", \"azure.adls2.storage_account\"  =  \"some_account\")", AstToStringBuilder.toString(stmt));

        stmt = (LoadStmt) analyzeSuccess("LOAD  LABEL test.testLabel (DATA INFILE(\"wasbs://aa@bb.blob.core.windows.net/*\") INTO TABLE `t0`) WITH BROKER " +
                "(\"azure.adls2.oauth2_client_id\" = \"some_client_id\",\n" +
                "\"azure.adls2.oauth2_client_secret\" = \"some_secret\",\n" +
                "\"azure.adls2.oauth2_client_endpoint\" = \"some_endpoint\")");
        Assert.assertEquals("LOAD LABEL `test`.`testLabel` (DATA INFILE ('wasbs://aa@bb.blob.core.windows.net/*') INTO TABLE t0) WITH BROKER  " +
                "(\"azure.adls2.oauth2_client_id\"  =  \"some_client_id\", \"azure.adls2.oauth2_client_endpoint\"  =  \"some_endpoint\", \"azure.adls2.oauth2_client_secret\"  =  \"***\")", AstToStringBuilder.toString(stmt));
    }
}