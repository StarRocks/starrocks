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

package com.starrocks.transaction;

import com.starrocks.common.Config;
import com.starrocks.common.ConfigRefreshDaemon;
import com.starrocks.server.GlobalStateMgr;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ThreadPoolExecutor;

public class PublishVersionDaemonTest {
    public int oldValue;

    @Before
    public void setUp() {
        oldValue = Config.lake_publish_version_max_threads;
    }

    @After
    public void tearDown() {
        Config.lake_publish_version_max_threads = oldValue;
    }

    @Test
    public void testUpdateLakeExecutorThreads()
            throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        PublishVersionDaemon daemon = new PublishVersionDaemon();

        ThreadPoolExecutor executor = (ThreadPoolExecutor) MethodUtils.invokeMethod(daemon, true, "getLakeTaskExecutor");
        Assert.assertNotNull(executor);
        Assert.assertEquals(Config.lake_publish_version_max_threads, executor.getCorePoolSize());

        ConfigRefreshDaemon configDaemon = GlobalStateMgr.getCurrentState().getConfigRefreshDaemon();

        // scale out
        Config.lake_publish_version_max_threads += 10;
        MethodUtils.invokeMethod(configDaemon, true, "runAfterCatalogReady");
        Assert.assertEquals(Config.lake_publish_version_max_threads, executor.getCorePoolSize());


        // scale in
        Config.lake_publish_version_max_threads -= 5;
        MethodUtils.invokeMethod(configDaemon, true, "runAfterCatalogReady");
        Assert.assertEquals(Config.lake_publish_version_max_threads, executor.getCorePoolSize());

        int oldNumber = executor.getCorePoolSize();

        // config set to < 0
        Config.lake_publish_version_max_threads = -1;
        MethodUtils.invokeMethod(configDaemon, true, "runAfterCatalogReady");
        Assert.assertEquals(oldNumber, executor.getCorePoolSize());

        // config set to > HARD_LIMIT_LAKE_PUBLISH_THREAD_POOL_SIZE
        Config.lake_publish_version_max_threads = PublishVersionDaemon.HARD_LIMIT_LAKE_PUBLISH_THREAD_POOL_SIZE + 1;
        MethodUtils.invokeMethod(configDaemon, true, "runAfterCatalogReady");
        Assert.assertEquals(oldNumber, executor.getCorePoolSize());

        // config set to HARD_LIMIT_LAKE_PUBLISH_THREAD_POOL_SIZE / 2 - 1
        Config.lake_publish_version_max_threads = PublishVersionDaemon.HARD_LIMIT_LAKE_PUBLISH_THREAD_POOL_SIZE / 2 - 1;
        MethodUtils.invokeMethod(configDaemon, true, "runAfterCatalogReady");
        Assert.assertEquals(Config.lake_publish_version_max_threads, executor.getCorePoolSize());
        Assert.assertEquals(Config.lake_publish_version_max_threads * 2, executor.getMaximumPoolSize());

        // config set to HARD_LIMIT_LAKE_PUBLISH_THREAD_POOL_SIZE / 2 + 1, maxPoolSize exceeds hard limit
        Config.lake_publish_version_max_threads = PublishVersionDaemon.HARD_LIMIT_LAKE_PUBLISH_THREAD_POOL_SIZE / 2 + 1;
        MethodUtils.invokeMethod(configDaemon, true, "runAfterCatalogReady");
        Assert.assertEquals(Config.lake_publish_version_max_threads, executor.getCorePoolSize());
        Assert.assertEquals(PublishVersionDaemon.HARD_LIMIT_LAKE_PUBLISH_THREAD_POOL_SIZE, executor.getMaximumPoolSize());
    }

    @Test
    public void testInvalidInitConfiguration()
            throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        int hardCodeDefaultThreads = (int) FieldUtils.readDeclaredStaticField(PublishVersionDaemon.class,
                "DEFAULT_LAKE_PUBLISH_THREAD_POOL_SIZE", true);

        // <= 0
        int initValue = 0;
        Config.lake_publish_version_max_threads = initValue;
        {
            PublishVersionDaemon daemon = new PublishVersionDaemon();
            ThreadPoolExecutor executor =
                    (ThreadPoolExecutor) MethodUtils.invokeMethod(daemon, true, "getLakeTaskExecutor");

            Assert.assertNotNull(executor);
            Assert.assertNotEquals(initValue, executor.getCorePoolSize());
            Assert.assertEquals(hardCodeDefaultThreads, executor.getCorePoolSize());
            Assert.assertEquals(hardCodeDefaultThreads * 2, executor.getMaximumPoolSize());
            // configVar set to default value.
            Assert.assertEquals(hardCodeDefaultThreads, Config.lake_publish_version_max_threads);
        }

        // > HARD_LIMIT_LAKE_PUBLISH_THREAD_POOL_SIZE
        initValue = PublishVersionDaemon.HARD_LIMIT_LAKE_PUBLISH_THREAD_POOL_SIZE + 1;
        Config.lake_publish_version_max_threads = initValue;
        {
            PublishVersionDaemon daemon = new PublishVersionDaemon();
            ThreadPoolExecutor executor =
                    (ThreadPoolExecutor) MethodUtils.invokeMethod(daemon, true, "getLakeTaskExecutor");
            Assert.assertNotNull(executor);
            Assert.assertNotEquals(initValue, executor.getCorePoolSize());
            Assert.assertEquals(hardCodeDefaultThreads, executor.getCorePoolSize());
            Assert.assertEquals(hardCodeDefaultThreads * 2, executor.getMaximumPoolSize());
            // configVar set to default value.
            Assert.assertEquals(hardCodeDefaultThreads, Config.lake_publish_version_max_threads);
        }
    }
}
