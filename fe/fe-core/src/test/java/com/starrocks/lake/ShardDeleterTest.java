// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake;

import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Before;

import java.util.HashSet;
import java.util.Set;

public class ShardDeleterTest {

    private ShardDeleter shardDeleter = new ShardDeleter();

    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Mocked
    private SystemInfoService systemInfoService;

    @Mocked
    private StarOSAgent starOSAgent;

    @Mocked
    private LakeService lakeService;

    @Mocked
    private Backend be;

    private Set<Long> ids = new HashSet<>();

    @Before
    public void setUp() throws Exception {
        ids.add(1001L);
        ids.add(1002L);

        be = new Backend(100, "127.0.0.1", 8090);
        new MockUp<GlobalStateMgr>() {
            @Mock
            public SystemInfoService getCurrentSystemInfo() {
                return systemInfoService;
            }

            @Mock
            public StarOSAgent getStarOSAgent() {
                return starOSAgent;
            }
        };

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) {
                return lakeService;
            }
            @Mock
            public LakeService getLakeService(String host, int port) {
                return lakeService;
            }
        };

        new Expectations() {
            {
                starOSAgent.getPrimaryBackendIdByShard(anyLong);
                minTimes = 0;
                result = 1;

                systemInfoService.getBackend(1);
                minTimes = 0;
                result = be;
            }
        };

    }
}
