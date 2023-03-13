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

import com.google.common.collect.Sets;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.privilege.PrivilegeBuiltinConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.CatalogMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class UseCatalogStmtTest {
    private static ConnectContext ctx;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        StarRocksAssert starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("db1").useDatabase("tbl1");
        String createCatalog = "create external catalog hive_catalog properties(" +
                "\"type\" = \"hive\", \"hive.metastore.uris\" = \"thrift://127.0.0.1:3333\")";
        starRocksAssert.withCatalog(createCatalog);
        ctx = new ConnectContext(null);
        ctx.setGlobalStateMgr(AccessTestUtil.fetchAdminCatalog());
    }

    @Test
    public void testParserAndAnalyzer() {
        String sql = "USE 'catalog hive_catalog'";
        AnalyzeTestUtil.analyzeSuccess(sql);

        String sql_2 = "USE 'catalog default_catalog'";
        AnalyzeTestUtil.analyzeSuccess(sql_2);

        String sql_3 = "USE 'xxxx default_catalog'";
        AnalyzeTestUtil.analyzeFail(sql_3);
    }

    @Test
    public void testUse(@Mocked CatalogMgr catalogMgr) throws Exception {
        new Expectations() {
            {
                catalogMgr.catalogExists("hive_catalog");
                result = true;
                minTimes = 0;

                catalogMgr.catalogExists("default_catalog");
                result = true;
                minTimes = 0;
            }
        };

        ctx.setQueryId(UUIDUtil.genUUID());
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
        StmtExecutor executor = new StmtExecutor(ctx, "use 'catalog hive_catalog'");
        executor.execute();

        Assert.assertEquals("hive_catalog", ctx.getCurrentCatalog());

        executor = new StmtExecutor(ctx, "use 'catalog default_catalog'");
        executor.execute();

        Assert.assertEquals("default_catalog", ctx.getCurrentCatalog());

        executor = new StmtExecutor(ctx, "use 'xxx default_catalog'");
        executor.execute();
        Assert.assertSame(ctx.getState().getStateType(), QueryState.MysqlStateType.ERR);

        executor = new StmtExecutor(ctx, "use 'catalog default_catalog xxx'");
        executor.execute();
        Assert.assertSame(ctx.getState().getStateType(), QueryState.MysqlStateType.ERR);
    }
}
