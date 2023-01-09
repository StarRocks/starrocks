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

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.ast.CreateResourceStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CreateResourceStmtTest {
    private static StarRocksAssert starRocksAssert;
    private static ConnectContext ctx;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("db1").useDatabase("tbl1");
        ctx = new ConnectContext(null);
        ctx.setGlobalStateMgr(AccessTestUtil.fetchAdminCatalog());
    }


    @Test
    public void testToString() {
        String sql = "CREATE EXTERNAL RESOURCE \"spark0\"\n" +
                "PROPERTIES (\n" +
                "    \"type\" = \"spark\",\n" +
                "    \"spark.master\" = \"yarn\",\n" +
                "    \"spark.submit.deployMode\" = \"cluster\",\n" +
                "    \"spark.executor.memory\" = \"1g\",\n" +
                "    \"spark.yarn.queue\" = \"queue0\",\n" +
                "    \"spark.hadoop.yarn.resourcemanager.address\" = \"resourcemanager_host:8032\",\n" +
                "    \"spark.hadoop.fs.defaultFS\" = \"hdfs://namenode_host:9000\",\n" +
                "    \"working_dir\" = \"hdfs://namenode_host:9000/tmp/starrocks\",\n" +
                "    \"broker\" = \"broker0\",\n" +
                "    \"broker.username\" = \"user0\",\n" +
                "    \"broker.password\" = \"password0\"\n" +
                ")";
        ConnectContext ctx = starRocksAssert.getCtx();
        CreateResourceStmt stmt = (CreateResourceStmt) com.starrocks.sql.parser.SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
        Assert.assertEquals("CREATE EXTERNAL RESOURCE spark0 PROPERTIES (\"spark.executor.memory\" = \"1g\"," +
                " \"spark.master\" = \"yarn\", \"working_dir\" = \"hdfs://namenode_host:9000/tmp/starrocks\", \"broker.password\" = \"***\"," +
                " \"spark.hadoop.yarn.resourcemanager.address\" = \"resourcemanager_host:8032\", \"spark.submit.deployMode\" = \"cluster\"," +
                " \"type\" = \"spark\", \"spark.hadoop.fs.defaultFS\" = \"hdfs://namenode_host:9000\", \"broker\" = \"broker0\", " +
                "\"spark.yarn.queue\" = \"queue0\", \"broker.username\" = \"user0\")", AstToStringBuilder.toString(stmt));
    }
}
