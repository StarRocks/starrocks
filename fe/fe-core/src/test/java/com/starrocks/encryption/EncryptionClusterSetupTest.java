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
package com.starrocks.encryption;

import com.starrocks.common.Config;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.server.GlobalStateMgr;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class EncryptionClusterSetupTest {
    @BeforeClass
    public static void setUp() throws Exception {
        Config.default_master_key = "plain:aes_128:enwSdCUAiCLLx2Bs9E/neQ==";
        PseudoCluster.getOrCreateWithRandomPort(true, 3);
        PseudoCluster cluster = PseudoCluster.getInstance();
        Thread.sleep(1000);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().shutdown(true);
        Config.default_master_key = "";
    }

    @Test
    public void testNormalSetup() throws Exception {
        Assert.assertTrue(KeyMgr.isEncrypted());
        KeyMgr keyMgr = GlobalStateMgr.getCurrentState().getKeyMgr();
        Assert.assertEquals(2, keyMgr.numKeys());
        Assert.assertEquals(2, keyMgr.getCurrentKEK().getId());
        Assert.assertEquals(2, keyMgr.getKeyById(2).getId());
    }
}
