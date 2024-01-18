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


import com.starrocks.common.conf.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.sql.ast.AdminSetConfigStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;

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
    public void testSetJDBCCacheConfig() {
        try {
            Map<String, String> properties = new HashMap<>();
            JDBCMetaCache<String, Integer> cacheWithDefaultClose = new JDBCMetaCache<>(properties, false);
            Assert.assertFalse(cacheWithDefaultClose.isEnableCache());
            JDBCCacheTestUtil.openCacheEnable(connectContext);
            Assert.assertTrue(Config.jdbc_meta_default_cache_enable);
            JDBCMetaCache<String, Integer> cacheWithDefaultOpen = new JDBCMetaCache<>(properties, false);
            Assert.assertTrue(cacheWithDefaultOpen.isEnableCache());
            Assert.assertEquals(cacheWithDefaultOpen.getCurrentExpireSec(), Config.jdbc_meta_default_cache_expire_sec);
            properties.put("jdbc_meta_cache_enable", "true");
            properties.put("jdbc_meta_cache_expire_sec", "60");
            JDBCMetaCache<String, Integer> cacheOpen = new JDBCMetaCache<>(properties, false);
            Assert.assertTrue(cacheOpen.isEnableCache());
            Assert.assertNotEquals(cacheOpen.getCurrentExpireSec(), Config.jdbc_meta_default_cache_expire_sec);
            properties.put("jdbc_meta_cache_enable", "false");
            JDBCMetaCache<String, Integer> cacheClose = new JDBCMetaCache<>(properties, false);
            Assert.assertFalse(cacheClose.isEnableCache());
            JDBCCacheTestUtil.closeCacheEnable(connectContext);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testJDBCCacheGetWithOutEnable() {
        try {
            Map<String, String> properties = new HashMap<>();
            JDBCMetaCache<String, Integer> cache = new JDBCMetaCache<>(properties, false);
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
            JDBCCacheTestUtil.openCacheEnable(connectContext);
            Assert.assertTrue(Config.jdbc_meta_default_cache_enable);
            Map<String, String> properties = new HashMap<>();
            properties.put("jdbc_meta_cache_enable", "true");
            JDBCMetaCache<String, Integer> cache = new JDBCMetaCache<>(properties, false);
            Assert.assertTrue(cache.isEnableCache());
            int step1 = cache.get("testKey", k -> 20231111);
            int step2 = cache.get("testKey", k -> 20231212);
            Assert.assertEquals(step1, step2);
            JDBCCacheTestUtil.closeCacheEnable(connectContext);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testSetJDBCCacheExpireSec() {
        try {
            JDBCCacheTestUtil.openCacheEnable(connectContext);
            Map<String, String> properties = new HashMap<>();
            properties.put("jdbc_meta_cache_enable", "true");
            JDBCMetaCache<String, Integer> cache = new JDBCMetaCache<>(properties, false);
            int invalidateBefore = cache.get("testKey", k -> 20231111);
            Assert.assertEquals(invalidateBefore, 20231111);
            cache.invalidate("testKey");
            int invalidateAfter = cache.get("testKey", k -> 20231212);
            Assert.assertEquals(invalidateAfter, 20231212);
            JDBCCacheTestUtil.closeCacheEnable(connectContext);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testRefreshJDBCCache() {
        try {
            JDBCCacheTestUtil.openCacheEnable(connectContext);
            Map<String, String> properties = new HashMap<>();
            properties.put("jdbc_meta_cache_enable", "true");
            JDBCMetaCache<String, Integer> cache = new JDBCMetaCache<>(properties, false);
            int changeBefore = cache.get("testKey", k -> 20231111);
            Assert.assertEquals(changeBefore, 20231111);
            cache.invalidate("testKey");
            int changeAfter = cache.get("testKey", k -> 20231212);
            Assert.assertEquals(changeAfter, 20231212);
            JDBCCacheTestUtil.closeCacheEnable(connectContext);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    private static void changeJDBCCacheExpireSec() throws Exception {
        String setStmt = "admin set frontend config(\"jdbc_meta_default_cache_expire_sec\" = \"6000\");";
        AdminSetConfigStmt adminSetConfigStmt =
                (AdminSetConfigStmt) UtFrameUtils.parseStmtWithNewParser(setStmt, connectContext);
        DDLStmtExecutor.execute(adminSetConfigStmt, connectContext);
        Assert.assertEquals(6000, Config.jdbc_meta_default_cache_expire_sec);
    }
}
