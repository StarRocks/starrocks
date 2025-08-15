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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class EncryptionClusterSetupTest {
    @BeforeAll
    public static void setUp() throws Exception {
        Config.default_master_key = "plain:aes_128:enwSdCUAiCLLx2Bs9E/neQ==";
        PseudoCluster.getOrCreateWithRandomPort(true, 3);
        PseudoCluster cluster = PseudoCluster.getInstance();
        Thread.sleep(1000);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().shutdown(true);
        Config.default_master_key = "";
    }

    @Test
    public void testNormalSetup() throws Exception {
        Assertions.assertTrue(KeyMgr.isEncrypted());
        KeyMgr keyMgr = GlobalStateMgr.getCurrentState().getKeyMgr();
        Assertions.assertEquals(2, keyMgr.numKeys());
        Assertions.assertEquals(2, keyMgr.getCurrentKEK().getId());
        Assertions.assertEquals(2, keyMgr.getKeyById(2).getId());
    }
}
