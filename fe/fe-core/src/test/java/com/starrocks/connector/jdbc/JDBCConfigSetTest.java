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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class JDBCConfigSetTest {

    private static ConnectContext connectContext;

    @BeforeAll
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
            Assertions.assertFalse(cacheWithDefaultClose.isEnableCache());
            JDBCCacheTestUtil.openCacheEnable(connectContext);
            Assertions.assertTrue(Config.jdbc_meta_default_cache_enable);
            JDBCMetaCache<String, Integer> cacheWithDefaultOpen = new JDBCMetaCache<>(properties, false);
            Assertions.assertTrue(cacheWithDefaultOpen.isEnableCache());
            Assertions.assertEquals(cacheWithDefaultOpen.getCurrentExpireSec(), Config.jdbc_meta_default_cache_expire_sec);
            properties.put("jdbc_meta_cache_enable", "true");
            properties.put("jdbc_meta_cache_expire_sec", "60");
            JDBCMetaCache<String, Integer> cacheOpen = new JDBCMetaCache<>(properties, false);
            Assertions.assertTrue(cacheOpen.isEnableCache());
            Assertions.assertNotEquals(cacheOpen.getCurrentExpireSec(), Config.jdbc_meta_default_cache_expire_sec);
            properties.put("jdbc_meta_cache_enable", "false");
            JDBCMetaCache<String, Integer> cacheClose = new JDBCMetaCache<>(properties, false);
            Assertions.assertFalse(cacheClose.isEnableCache());
            JDBCCacheTestUtil.closeCacheEnable(connectContext);
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testJDBCCacheGetWithOutEnable() {
        try {
            Map<String, String> properties = new HashMap<>();
            JDBCMetaCache<String, Integer> cache = new JDBCMetaCache<>(properties, false);
            int step1 = cache.get("testKey", k -> 20231111);
            int step2 = cache.get("testKey", k -> 20231212);
            Assertions.assertNotEquals(step1, step2);
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testJDBCCacheGetWithEnable() {
        try {
            JDBCCacheTestUtil.openCacheEnable(connectContext);
            Assertions.assertTrue(Config.jdbc_meta_default_cache_enable);
            Map<String, String> properties = new HashMap<>();
            properties.put("jdbc_meta_cache_enable", "true");
            JDBCMetaCache<String, Integer> cache = new JDBCMetaCache<>(properties, false);
            Assertions.assertTrue(cache.isEnableCache());
            int step1 = cache.get("testKey", k -> 20231111);
            int step2 = cache.get("testKey", k -> 20231212);
            Assertions.assertEquals(step1, step2);
            JDBCCacheTestUtil.closeCacheEnable(connectContext);
        } catch (Exception e) {
            Assertions.fail();
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
            Assertions.assertEquals(invalidateBefore, 20231111);
            cache.invalidate("testKey");
            int invalidateAfter = cache.get("testKey", k -> 20231212);
            Assertions.assertEquals(invalidateAfter, 20231212);
            JDBCCacheTestUtil.closeCacheEnable(connectContext);
        } catch (Exception e) {
            Assertions.fail();
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
            Assertions.assertEquals(changeBefore, 20231111);
            cache.invalidate("testKey");
            int changeAfter = cache.get("testKey", k -> 20231212);
            Assertions.assertEquals(changeAfter, 20231212);
            JDBCCacheTestUtil.closeCacheEnable(connectContext);
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    private static void changeJDBCCacheExpireSec() throws Exception {
        String setStmt = "admin set frontend config(\"jdbc_meta_default_cache_expire_sec\" = \"6000\");";
        AdminSetConfigStmt adminSetConfigStmt =
                (AdminSetConfigStmt) UtFrameUtils.parseStmtWithNewParser(setStmt, connectContext);
        DDLStmtExecutor.execute(adminSetConfigStmt, connectContext);
        Assertions.assertEquals(6000, Config.jdbc_meta_default_cache_expire_sec);
    }
}
