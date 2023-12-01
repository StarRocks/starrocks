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
        Assert.assertEquals(Config.lake_publish_version_max_threads, executor.getMaximumPoolSize());

        Config.lake_publish_version_max_threads += 10;
        // manual trigger the configVar listener refresh

        ConfigRefreshDaemon configDaemon = GlobalStateMgr.getCurrentState().getConfigRefreshDaemon();
        // force run one cycle to update config listeners
        MethodUtils.invokeMethod(configDaemon, true, "runAfterCatalogReady");
        // after configVar refresh, the threadPool size is changed.
        Assert.assertEquals(Config.lake_publish_version_max_threads, executor.getMaximumPoolSize());
    }
}
