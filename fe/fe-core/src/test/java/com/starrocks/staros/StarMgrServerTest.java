// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.staros;

import com.staros.manager.StarManager;
import com.staros.manager.StarManagerServer;
import com.starrocks.common.Config;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class StarMgrServerTest {
    @Mocked
    private StarManagerServer server;

    @Test
    public void testStarMgrServer() {
        new MockUp<StarManagerServer>() {
            @Mock
            public void start(int port) throws IOException {
            }
            @Mock
            public StarManager getStarManager() {
                return null;
            }
        };
        StarMgrServer starMgrServer = new StarMgrServer(server);

        Assert.assertEquals(null, starMgrServer.getStarMgr());

        Config.starmgr_s3_bucket = "abc";
        try {
            starMgrServer.start(null, null);
        } catch (IOException e) {
        }
    }
}
