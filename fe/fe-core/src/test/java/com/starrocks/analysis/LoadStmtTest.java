// This file is made available under Elastic License 2.0.
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
    }
}