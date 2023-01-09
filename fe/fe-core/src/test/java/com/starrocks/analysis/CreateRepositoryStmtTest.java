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
import com.starrocks.sql.ast.CreateRepositoryStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CreateRepositoryStmtTest {
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
        String sql = "CREATE REPOSITORY oss_repo\n" +
                "WITH BROKER oss_broker\n" +
                "ON LOCATION \"oss://starRocks_backup\"\n" +
                "PROPERTIES\n" +
                "(\n" +
                "    \"fs.oss.accessKeyId\" = \"access_key_id\",\n" +
                "    \"fs.oss.accessKeySecret\" = \"access_key_secret\",\n" +
                "    \"fs.oss.endpoint\" = \"oss-cn-beijing.aliyuncs.com\"\n" +
                ")";
        ConnectContext ctx = starRocksAssert.getCtx();
        CreateRepositoryStmt stmt = (CreateRepositoryStmt) com.starrocks.sql.parser.SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
        Assert.assertEquals("CREATE REPOSITORY oss_repo WITH BROKER oss_broker ON LOCATION \"oss://starRocks_backup\" " +
                "PROPERTIES (\"fs.oss.accessKeyId\" = \"***\", \"fs.oss.accessKeySecret\" = \"***\", \"fs.oss.endpoint\" = \"oss-cn-beijing.aliyuncs.com\" )", AstToStringBuilder.toString(stmt));
    }
}
