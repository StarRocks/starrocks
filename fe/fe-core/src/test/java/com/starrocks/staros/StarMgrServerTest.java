// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.staros;

import com.staros.manager.StarManager;
import com.staros.manager.StarManagerServer;
import com.starrocks.common.Config;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class StarMgrServerTest {
    @Mocked
    private StarManager manager;
    @Mocked
    private StarManagerServer server;
    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Before
    public void setUp() {
        Config.integrate_starmgr = true;
        Config.starmgr_s3_bucket = "abc";
    }

    @After
    public void tearDown() {
        Config.integrate_starmgr = false;
        Config.starmgr_s3_bucket = "";
    }

    @Test
    public void testStarMgrServer() throws Exception {
        new MockUp<StarManagerServer>() {
            @Mock
            public void start(int port) throws IOException {
            }
            @Mock
            public StarManager getStarManager() {
                return manager;
            }
        };
        new MockUp<StarManager>() {
            @Mock
            public void start() {
            }
            @Mock
            public void stop() {
            }
            @Mock
            public void dumpMeta(DataOutputStream out) throws IOException {
            }
            @Mock
            public void loadMeta(DataInputStream in) throws IOException {
            }
        };
        new MockUp<GlobalStateMgr>() {
            @Mock
            public void start(int port) throws IOException {
            }
            @Mock
            public StarManager getStarManager() {
                return manager;
            }
        };
        StarMgrServer starMgrServer = new StarMgrServer(server);

        Assert.assertEquals(manager, starMgrServer.getStarMgr());

        try {
            starMgrServer.start(null, null);
        } catch (IOException e) {
        }

        new Expectations() {
            {
                starMgrServer.loadMeta(null);
                minTimes = 0;
                result = null;

                starMgrServer.dumpMeta(null);
                minTimes = 0;
                result = null;
            }
        };

        Assert.assertEquals(manager, starMgrServer.getStarMgr());

        try {
            globalStateMgr.saveStarMgrMeta(null, 0);
            globalStateMgr.loadStarMgrMeta(null, 0);
        } catch (IOException e) {
        }
    }
}
