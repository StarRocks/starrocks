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

package com.starrocks.connector.jdbc;


import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.sql.ast.AdminSetConfigStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class JDBCConfigSetTest {

    private static ConnectContext connectContext;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void testSetJDBCCacheEnable() {
        try {
            String stmt = "admin set frontend config(\"jdbc_meta_cache_enable\" = \"true\");";
            AdminSetConfigStmt adminSetConfigStmt =
                    (AdminSetConfigStmt) UtFrameUtils.parseStmtWithNewParser(stmt, connectContext);
            DDLStmtExecutor.execute(adminSetConfigStmt, connectContext);
            Assert.assertTrue(Config.jdbc_meta_cache_enable);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testSetJDBCCacheExpireSec() {
        try {
            String stmt = "admin set frontend config(\"jdbc_meta_cache_expire_sec\" = \"6000\");";
            AdminSetConfigStmt adminSetConfigStmt =
                    (AdminSetConfigStmt) UtFrameUtils.parseStmtWithNewParser(stmt, connectContext);
            DDLStmtExecutor.execute(adminSetConfigStmt, connectContext);
            Assert.assertEquals(6000, Config.jdbc_meta_cache_expire_sec);
        } catch (Exception e) {
            Assert.fail();
        }
    }

}
