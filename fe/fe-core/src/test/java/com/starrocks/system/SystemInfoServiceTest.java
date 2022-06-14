// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.system;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import com.starrocks.analysis.ModifyBackendAddressClause;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;

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
        new Expectations(){
            {
                globalStateMgr.getEditLog();
                result = editLog;
            }
        };
        new MockUp<EditLog>() {
            @Mock
            public void logBackendStateChange(Backend be) {}
        };
    }
    
    @Test
    public void testUpdateBackendHostWithOneBe() throws Exception {
        mockFunc();
        Backend be = new Backend(100, "127.0.0.1", 1000);
        service.addBackend(be);
        ModifyBackendAddressClause clause = new ModifyBackendAddressClause("127.0.0.1", "sandbox");
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
        ModifyBackendAddressClause clause = new ModifyBackendAddressClause("127.0.0.1", "sandbox");
        service.modifyBackendHost(clause);    
        Backend backend = service.getBackendWithHeartbeatPort("sandbox", 1000);
        Assert.assertNotNull(backend);
    }

    @Test(expected = DdlException.class)
    public void testUpdateBackendAddressNotFoundBe() throws Exception {
        Backend be = new Backend(100, "originalHost", 1000);
        service.addBackend(be);
        ModifyBackendAddressClause clause = new ModifyBackendAddressClause(
            "originalHost-test", "sandbox"
        );
        // This case will occur backend [%s] not found exception
        service.modifyBackendHost(clause);
    }

    @Test
    public void testUpdateBackend() throws Exception {
        Backend be = new Backend(10001, "newHost", 1000);
        service.addBackend(be);
        service.updateBackendState(be);
        Backend newBe = service.getBackend(10001);
        Assert.assertTrue(newBe.getHost().equals("newHost"));
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
        new Expectations(){
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
}
