// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.system;

import com.starrocks.analysis.ModifyBackendAddressClause;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class SystemInfoServiceTest {
    
    @BeforeClass
    public static void setUp() {
        UtFrameUtils.createMinStarRocksCluster();
        Config.enable_fqdn = true;
    }

    @Test
    public void testUpdateBackendAddress() throws Exception {
        Backend be = new Backend(100, "originalHost", 1000);
        GlobalStateMgr.getCurrentSystemInfo().addBackend(be);

        ModifyBackendAddressClause clause = new ModifyBackendAddressClause(
            new Pair<String, Integer>("originalHost", 1000), 
            new Pair<String, Integer>("newHost", 1000)
        );
        GlobalStateMgr.getCurrentSystemInfo().updateBackendAddress(clause);    
        Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackendWithHeartbeatPort("newHost", 1000);
        Assert.assertNotNull(backend);
    }

    @Test(expected = DdlException.class)
    public void testUpdateBackendAddressNotFoundBe() throws Exception {
        Backend be = new Backend(100, "originalHost", 1000);
        GlobalStateMgr.getCurrentSystemInfo().addBackend(be);

        ModifyBackendAddressClause clause = new ModifyBackendAddressClause(
            new Pair<String, Integer>("originalHost-test", 1000), 
            new Pair<String, Integer>("newHost", 1000)
        );
        GlobalStateMgr.getCurrentSystemInfo().updateBackendAddress(clause);    
    }

    @Test
    public void testUpdateBackend() throws Exception {
        Backend be = new Backend(10001, "newHost", 1000);
        GlobalStateMgr.getCurrentSystemInfo().updateBackendState(be);
        Backend newBe = GlobalStateMgr.getCurrentSystemInfo().getBackend(10001);
        Assert.assertTrue(newBe.getHost().equals("newHost"));
    }
}
