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

import com.google.api.client.util.Maps;
import com.starrocks.cluster.Cluster;
import com.starrocks.common.DdlException;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.RunMode;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.analyzer.AlterSystemStmtAnalyzer;
import com.starrocks.sql.ast.ModifyBackendClause;
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
import java.util.Map;
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
            public void logBackendStateChange(Backend be) {
            }
        };
    }

    @Test
    public void testUpdateBackendHostWithOneBe() throws Exception {
        mockFunc();
        Backend be = new Backend(100, "127.0.0.1", 1000);
        service.addBackend(be);
        ModifyBackendClause clause = new ModifyBackendClause("127.0.0.1", "sandbox");
        service.modifyBackendHost(clause);
        Backend backend = service.getBackendWithHeartbeatPort("sandbox", 1000);
        Assert.assertNotNull(backend);
    }

    @Test
    public void testUpdateBackendHostWithMoreBe() throws Exception {
        mockFunc();
        Backend be1 = new Backend(100, "127.0.0.1", 1000);
        Backend be2 = new Backend(101, "127.0.0.1", 1001);
        service.addBackend(be1);
        service.addBackend(be2);
        ModifyBackendClause clause = new ModifyBackendClause("127.0.0.1", "sandbox");
        service.modifyBackendHost(clause);
        Backend backend = service.getBackendWithHeartbeatPort("sandbox", 1000);
        Assert.assertNotNull(backend);
    }

    @Test(expected = DdlException.class)
    public void testUpdateBackendAddressNotFoundBe() throws Exception {
        Backend be = new Backend(100, "originalHost", 1000);
        service.addBackend(be);
        ModifyBackendClause clause = new ModifyBackendClause("originalHost-test", "sandbox");
        // This case will occur backend [%s] not found exception
        service.modifyBackendHost(clause);
    }

    /**
     * Test method for {@link SystemInfoService#modifyBackendProperty(ModifyBackendClause)}.
     */
    @Test
    public void testModifyBackendProperty() throws DdlException {
        Backend be = new Backend(100, "originalHost", 1000);
        service.addBackend(be);
        Map<String, String> properties = Maps.newHashMap();
        String location = "rack:rack1";
        properties.put(AlterSystemStmtAnalyzer.PROP_KEY_LOCATION, location);
        ModifyBackendClause clause = new ModifyBackendClause("originalHost:1000", properties);
        service.modifyBackendProperty(clause);
        Backend backend = service.getBackendWithHeartbeatPort("originalHost", 1000);
        Assert.assertNotNull(backend);
        Assert.assertEquals("{rack=rack1}", backend.getLocation().toString());
    }

    @Test
    public void testUpdateBackend() throws Exception {
        Backend be = new Backend(10001, "newHost", 1000);
        service.addBackend(be);
        service.updateBackendState(be);
        Backend newBe = service.getBackend(10001);
        Assert.assertTrue(newBe.getHost().equals("newHost"));
    }

    @Test
    public void testGetBackendOrComputeNode() {
        Backend be = new Backend(10001, "host1", 1000);
        service.addBackend(be);
        ComputeNode cn = new ComputeNode(10002, "host2", 1000);
        cn.setBePort(1001);
        service.addComputeNode(cn);

        Assert.assertEquals(be, service.getBackendOrComputeNode(be.getId()));
        Assert.assertEquals(cn, service.getBackendOrComputeNode(cn.getId()));
        Assert.assertNull(service.getBackendOrComputeNode(/* Not Exist */ 100));

        Assert.assertEquals(cn, service.getBackendOrComputeNodeWithBePort("host2", 1001));
        Assert.assertFalse(service.checkNodeAvailable(cn));
        Assert.assertFalse(service.checkNodeAvailable(be));

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

        Backend be = new Backend(10001, "newHost", 1000);
        service.addBackend(be);

        LocalMetastore localMetastore = new LocalMetastore(globalStateMgr, null, null);

        new Expectations() {
            {
                service.getBackendWithHeartbeatPort("newHost", 1000);
                minTimes = 0;
                result = be;

                globalStateMgr.getLocalMetastore();
                minTimes = 0;
                result = localMetastore;
            }
        };

        new Expectations(localMetastore) {
            {
                localMetastore.getCluster();
                minTimes = 0;
                result = new Cluster("cluster", 1);
            }
        };

        service.addBackend(be);
        be.setStarletPort(1001);
        service.dropBackend("newHost", 1000, false);
        Backend beIP = service.getBackendWithHeartbeatPort("newHost", 1000);
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

        Backend be = new Backend(10001, "newHost", 1000);
        be.setStarletPort(1001);

        LocalMetastore localMetastore = new LocalMetastore(globalStateMgr, null, null);
        new Expectations() {
            {
                service.getBackendWithHeartbeatPort("newHost", 1000);
                minTimes = 0;
                result = be;

                globalStateMgr.getLocalMetastore();
                minTimes = 0;
                result = localMetastore;
            }
        };

        new Expectations(localMetastore) {
            {
                localMetastore.getCluster();
                minTimes = 0;
                result = new Cluster("cluster", 1);
            }
        };

        service.addBackend(be);
        service.replayDropBackend(be);
        Backend beIP = service.getBackendWithHeartbeatPort("newHost", 1000);
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

        Backend be1 = new Backend(10001, "127.0.0.1", 1000);
        be1.setBePort(1001);
        service.addBackend(be1);
        Backend beIP1 = service.getBackendWithBePort("127.0.0.1", 1001);

        service.dropAllBackend();

        Backend be2 = new Backend(10001, "newHost-1", 1000);
        be2.setBePort(1001);
        service.addBackend(be2);
        Backend beFqdn = service.getBackendWithBePort("127.0.0.1", 1001);

        Assert.assertTrue(beFqdn != null && beIP1 != null);

        service.dropAllBackend();

        Backend be3 = new Backend(10001, "127.0.0.1", 1000);
        be3.setBePort(1001);
        service.addBackend(be3);
        Backend beIP3 = service.getBackendWithBePort("127.0.0.2", 1001);
        Assert.assertTrue(beIP3 == null);
    }

    @Test
    public void testGetBackendOnlyWithHost() throws Exception {

        Backend be = new Backend(10001, "newHost", 1000);
        be.setBePort(1001);
        service.addBackend(be);
        List<Backend> bes = service.getBackendOnlyWithHost("newHost");
        Assert.assertTrue(bes.size() == 1);
    }

    @Test
    public void testGetBackendIdWithStarletPort() throws Exception {
        Backend be = new Backend(10001, "newHost", 1000);
        be.setStarletPort(10001);
        service.addBackend(be);
        long backendId = service.getBackendIdWithStarletPort("newHost", 10001);
        Assert.assertEquals(be.getId(), backendId);
    }
}
