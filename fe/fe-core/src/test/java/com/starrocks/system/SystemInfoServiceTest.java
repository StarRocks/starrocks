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


package com.starrocks.system;

import com.starrocks.cluster.Cluster;
import com.starrocks.common.DdlException;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.ast.ModifyBackendAddressClause;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.stream.Collectors;

public class SystemInfoServiceTest {

    SystemInfoService service;

    @Mocked
    GlobalStateMgr globalStateMgr;

    @Mocked
    EditLog editLog;

    @Before
    public void setUp() throws NoSuchFieldException,
            SecurityException,
            IllegalArgumentException,
            IllegalAccessException {
        service = new SystemInfoService();
        Field field = FrontendOptions.class.getDeclaredField("useFqdn");
        field.setAccessible(true);
        field.set(null, true);
    }

    private void mockFunc() {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return globalStateMgr;
            }
        };
        new Expectations() {
            {
                globalStateMgr.getEditLog();
                result = editLog;
            }
        };
        new MockUp<EditLog>() {
            @Mock
            public void logBackendStateChange(DataNode be) {
            }
        };
    }

    @Test
    public void testUpdateBackendHostWithOneBe() throws Exception {
        mockFunc();
        DataNode be = new DataNode(100, "127.0.0.1", 1000);
        service.addBackend(be);
        ModifyBackendAddressClause clause = new ModifyBackendAddressClause("127.0.0.1", "sandbox");
        service.modifyDataNodeHost(clause);
        DataNode backend = service.getDataNodeWithHeartbeatPort("sandbox", 1000);
        Assert.assertNotNull(backend);
    }

    @Test
    public void testUpdateBackendHostWithMoreBe() throws Exception {
        mockFunc();
        DataNode be1 = new DataNode(100, "127.0.0.1", 1000);
        DataNode be2 = new DataNode(101, "127.0.0.1", 1001);
        service.addBackend(be1);
        service.addBackend(be2);
        ModifyBackendAddressClause clause = new ModifyBackendAddressClause("127.0.0.1", "sandbox");
        service.modifyDataNodeHost(clause);
        DataNode backend = service.getDataNodeWithHeartbeatPort("sandbox", 1000);
        Assert.assertNotNull(backend);
    }

    @Test(expected = DdlException.class)
    public void testUpdateBackendAddressNotFoundBe() throws Exception {
        DataNode be = new DataNode(100, "originalHost", 1000);
        service.addBackend(be);
        ModifyBackendAddressClause clause = new ModifyBackendAddressClause("originalHost-test", "sandbox");
        // This case will occur backend [%s] not found exception
        service.modifyDataNodeHost(clause);
    }

    @Test
    public void testUpdateBackend() throws Exception {
        DataNode be = new DataNode(10001, "newHost", 1000);
        service.addBackend(be);
        service.updateBackendState(be);
        DataNode newBe = service.getBackend(10001);
        Assert.assertTrue(newBe.getHost().equals("newHost"));
    }

    @Test
    public void testGetBackendOrComputeNode() {
        DataNode be = new DataNode(10001, "host1", 1000);
        service.addBackend(be);
        ComputeNode cn = new ComputeNode(10002, "host2", 1000);
        service.addComputeNode(cn);

        Assert.assertEquals(be, service.getBackendOrComputeNode(be.getId()));
        Assert.assertEquals(cn, service.getBackendOrComputeNode(cn.getId()));
        Assert.assertNull(service.getBackendOrComputeNode(/* Not Exist */ 100));

        List<ComputeNode> nodes = service.backendAndComputeNodeStream().collect(Collectors.toList());
        Assert.assertEquals(2, nodes.size());
        Assert.assertEquals(be, nodes.get(0));
        Assert.assertEquals(cn, nodes.get(1));
    }

    @Test
    public void testDropBackend() throws Exception {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        DataNode be = new DataNode(10001, "newHost", 1000);
        service.addBackend(be);

        new Expectations() {
            {
                service.getDataNodeWithHeartbeatPort("newHost", 1000);
                minTimes = 0;
                result = be;

                globalStateMgr.getCluster();
                minTimes = 0;
                result = new Cluster("cluster", 1);
            }
        };
        
        service.addBackend(be);
        be.setStarletPort(1001);
        service.dropDataNode("newHost", 1000, false);
        DataNode beIP = service.getDataNodeWithHeartbeatPort("newHost", 1000);
        Assert.assertTrue(beIP == null);
    }

    @Test
    public void testReplayDropBackend() throws Exception {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        DataNode be = new DataNode(10001, "newHost", 1000);
        be.setStarletPort(1001);

        new Expectations() {
            {
                service.getDataNodeWithHeartbeatPort("newHost", 1000);
                minTimes = 0;
                result = be;

                globalStateMgr.getCluster();
                minTimes = 0;
                result = new Cluster("cluster", 1);
            }
        };

        service.addBackend(be);
        service.replayDropBackend(be);
        DataNode beIP = service.getDataNodeWithHeartbeatPort("newHost", 1000);
        Assert.assertTrue(beIP == null);
    }


    @Mocked
    InetAddress addr;

    private void mockNet() {
        new MockUp<InetAddress>() {
            @Mock
            public InetAddress getByName(String host) throws UnknownHostException {
                return addr;
            }
        };
        new Expectations() {
            {
                addr.getHostAddress();
                result = "127.0.0.1";
            }
        };
    }

    @Test
    public void testGetBackendWithBePort() throws Exception {

        mockNet();

        DataNode be1 = new DataNode(10001, "127.0.0.1", 1000);
        be1.setBePort(1001);
        service.addBackend(be1);
        DataNode beIP1 = service.getBackendWithBePort("127.0.0.1", 1001);

        service.dropAllBackend();

        DataNode be2 = new DataNode(10001, "newHost-1", 1000);
        be2.setBePort(1001);
        service.addBackend(be2);
        DataNode beFqdn = service.getBackendWithBePort("127.0.0.1", 1001);

        Assert.assertTrue(beFqdn != null && beIP1 != null);

        service.dropAllBackend();

        DataNode be3 = new DataNode(10001, "127.0.0.1", 1000);
        be3.setBePort(1001);
        service.addBackend(be3);
        DataNode beIP3 = service.getBackendWithBePort("127.0.0.2", 1001);
        Assert.assertTrue(beIP3 == null);
    }

    @Test
    public void testGetBackendOnlyWithHost() throws Exception {

        DataNode be = new DataNode(10001, "newHost", 1000);
        be.setBePort(1001);
        service.addBackend(be);
        List<DataNode> bes = service.getBackendOnlyWithHost("newHost");
        Assert.assertTrue(bes.size() == 1);
    }

    @Test
    public void testGetBackendIdWithStarletPort() throws Exception {
        DataNode be = new DataNode(10001, "newHost", 1000);
        be.setStarletPort(10001);
        service.addBackend(be);
        long backendId = service.getBackendIdWithStarletPort("newHost", 10001);
        Assert.assertEquals(be.getId(), backendId);
    }
}
