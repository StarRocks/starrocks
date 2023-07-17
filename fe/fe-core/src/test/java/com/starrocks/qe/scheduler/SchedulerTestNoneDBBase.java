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

package com.starrocks.qe.scheduler;

import com.starrocks.catalog.CatalogIdGenerator;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.PBackendService;
import com.starrocks.sql.plan.PlanTestNoneDBBase;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SchedulerTestNoneDBBase extends PlanTestNoneDBBase {
    protected static final long BACKEND1_ID = 10001L;
    protected static Backend backend2 = null;
    protected static Backend backend3 = null;

    private static final String FILE_UNIT_TEST_ROOT_PATH =
            Objects.requireNonNull(ClassLoader.getSystemClassLoader().getResource("sql")).getPath();

    public static List<String> listTestFileNames(String directory) {
        File folder = new File(FILE_UNIT_TEST_ROOT_PATH + "/" + directory);
        return Arrays.stream(Objects.requireNonNull(folder.listFiles()))
                .filter(file -> file.isFile() && file.getName().endsWith(".sql"))
                .map(file -> directory + file.getName().replace(".sql", ""))
                .collect(Collectors.toList());
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestNoneDBBase.beforeClass(); // Added mockBackend(10001).
        backend2 = UtFrameUtils.addMockBackend(10002, "127.0.0.2", 9060);
        backend3 = UtFrameUtils.addMockBackend(10003, "127.0.0.3", 9060);

        makeCreateTabletStable();

        Config.tablet_sched_disable_colocate_overall_balance = true;
        connectContext.getSessionVariable().setPipelineDop(16);
    }

    @AfterClass
    public static void afterClass() {
        try {
            UtFrameUtils.dropMockBackend(10002);
            UtFrameUtils.dropMockBackend(10003);
        } catch (DdlException e) {
            e.printStackTrace();
        }

        Config.tablet_sched_disable_colocate_overall_balance = false;
        connectContext.getSessionVariable().setPipelineDop(0);
    }

    @Before
    public void makeQueryRandomStableBeforeTestCase() {
        makeQueryRandomStable();
    }

    public String getSchedulerPlan(String sql) throws Exception {
        return UtFrameUtils.getPlanAndStartScheduling(connectContext, sql).first;
    }

    public DefaultScheduler startScheduling(String sql) throws Exception {
        return UtFrameUtils.startScheduling(connectContext, sql, true).second;
    }

    public DefaultScheduler getScheduler(String sql) throws Exception {
        return UtFrameUtils.getScheduler(connectContext, sql);
    }

    public static void makeQueryRandomStable() {
        resetNextBackendIndex();
        resetRandomNextInt();
    }

    public static void makeCreateTabletStable() {
        resetCatalogIdGenerator();
        resetChooseBackendIds();
    }

    public static void setBackendService(PBackendService backendService) {
        new MockUp<BrpcProxy>() {
            @Mock
            private synchronized PBackendService getBackendService(TNetworkAddress address) {
                return backendService;
            }
        };
    }

    public static void setBackendService(Function<TNetworkAddress, PBackendService> supplier) {
        new MockUp<BrpcProxy>() {
            @Mock
            private synchronized PBackendService getBackendService(TNetworkAddress address) {
                return supplier.apply(address);
            }
        };
    }

    private static void resetNextBackendIndex() {
        Thread currentThread = Thread.currentThread();
        AtomicInteger currentThreadIndex = new AtomicInteger(0);
        AtomicInteger otherThreadIndex = new AtomicInteger(0);
        new MockUp<DefaultWorkerProvider>() {
            @Mock
            int getNextBackendIndex() {
                if (currentThread == Thread.currentThread()) {
                    return currentThreadIndex.getAndIncrement();
                } else {
                    return otherThreadIndex.getAndIncrement();
                }
            }
        };
    }

    private static void resetRandomNextInt() {
        Thread currentThread = Thread.currentThread();
        AtomicInteger currentThreadIndex = new AtomicInteger(0);
        AtomicInteger otherThreadIndex = new AtomicInteger(0);
        new MockUp<Random>() {
            @Mock
            public int nextInt(int bound) {
                if (currentThread == Thread.currentThread()) {
                    return currentThreadIndex.getAndIncrement() % bound;
                } else {
                    return otherThreadIndex.getAndIncrement() % bound;
                }
            }
        };
    }

    private static void resetCatalogIdGenerator() {
        AtomicLong nextId = new AtomicLong(1000L);
        new MockUp<CatalogIdGenerator>() {
            @Mock
            public synchronized long getNextId() {
                return nextId.getAndIncrement();
            }
        };
    }

    private static void resetChooseBackendIds() {
        AtomicInteger nextBackendIndex = new AtomicInteger(0);
        new MockUp<SystemInfoService>() {
            @Mock
            public synchronized List<Long> seqChooseBackendIds(int backendNum, boolean needAvailable, boolean isCreate,
                                                               final List<Backend> srcBackends) {
                List<Long> backendIds = new ArrayList<>(backendNum);
                for (int i = 0; i < backendNum; i++) {
                    int index = nextBackendIndex.getAndIncrement();
                    long id = srcBackends.get(index % srcBackends.size()).getId();
                    backendIds.add(id);
                }
                return backendIds;
            }
        };
    }

}
