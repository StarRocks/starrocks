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
            enableJDBCMetaCache();
            Assert.assertTrue(Config.jdbc_meta_cache_enable);
            disEnableJDBCMetaCache();
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testJDBCCacheGetWithOutEnable() {
        try {
            JDBCAsyncCache<String, Integer> cache = new JDBCAsyncCache<>();
            int step1 = cache.get("testKey", k -> 20231111);
            int step2 = cache.get("testKey", k -> 20231212);
            Assert.assertNotEquals(step1, step2);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testJDBCCacheGetWithEnable() {
        try {
            enableJDBCMetaCache();
            Assert.assertTrue(Config.jdbc_meta_cache_enable);
            JDBCAsyncCache<String, Integer> cache = new JDBCAsyncCache<>();
            int step1 = cache.get("testKey", k -> 20231111);
            int step2 = cache.get("testKey", k -> 20231212);
            Assert.assertEquals(step1, step2);
            disEnableJDBCMetaCache();
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testSetJDBCCacheExpireSec() {
        try {
            enableJDBCMetaCache();
            JDBCAsyncCache<String, Integer> cache = new JDBCAsyncCache<>();
            int changeBefore = cache.get("testKey", k -> 20231111);
            Assert.assertEquals(changeBefore, 20231111);
            changeJDBCCacheExpireSec();
            int changeAfter = cache.get("testKey", k -> 20231212);
            Assert.assertEquals(changeAfter, 20231212);
            disEnableJDBCMetaCache();
        } catch (Exception e) {
            Assert.fail();
        }
    }

    private static void enableJDBCMetaCache() throws Exception {
        String stmt = "admin set frontend config(\"jdbc_meta_cache_enable\" = \"true\");";
        AdminSetConfigStmt adminSetConfigStmt =
                (AdminSetConfigStmt) UtFrameUtils.parseStmtWithNewParser(stmt, connectContext);
        DDLStmtExecutor.execute(adminSetConfigStmt, connectContext);
    }

    public static void disEnableJDBCMetaCache() throws Exception {
        String stmt = "admin set frontend config(\"jdbc_meta_cache_enable\" = \"false\");";
        AdminSetConfigStmt adminSetConfigStmt =
                (AdminSetConfigStmt) UtFrameUtils.parseStmtWithNewParser(stmt, connectContext);
        DDLStmtExecutor.execute(adminSetConfigStmt, connectContext);
    }

    private static void changeJDBCCacheExpireSec() throws Exception {
        String setStmt = "admin set frontend config(\"jdbc_meta_cache_expire_sec\" = \"6000\");";
        AdminSetConfigStmt adminSetConfigStmt =
                (AdminSetConfigStmt) UtFrameUtils.parseStmtWithNewParser(setStmt, connectContext);
        DDLStmtExecutor.execute(adminSetConfigStmt, connectContext);
        Assert.assertEquals(6000, Config.jdbc_meta_cache_expire_sec);
    }

}
