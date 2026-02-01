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

package com.staros.provisioner;

import com.staros.common.HijackConfig;
import com.staros.proto.NodeInfo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

public class StarProvisionServerTest {
    private HijackConfig configDir;

    @Before
    public void setUp() throws IOException {
        File tmpFileDir = Files.createTempDirectory("StarProvisionServerTest").toFile();
        tmpFileDir.deleteOnExit();
        configDir = new HijackConfig("BUILTIN_PROVISION_SERVER_DATA_DIR", tmpFileDir.getAbsolutePath());
    }

    @After
    public void tearDown() {
        configDir.reset();
    }

    @Test
    public void testStarProvisionerServerPersistData() {
        StarProvisionServer serverA = new StarProvisionServer();

        String host = "xyz";
        serverA.processAddNodeRequest(host);
        Assert.assertEquals(1L, serverA.getFreeNodeCount());
        Assert.assertEquals(0L, serverA.getAssignedPoolsSize());

        { // serverB loads the data from persist data
            StarProvisionServer serverB = new StarProvisionServer();
            Assert.assertEquals(1L, serverB.getFreeNodeCount());
            Assert.assertEquals(0L, serverB.getAssignedPoolsSize());
        }

        String groupX = "groupX";
        List<NodeInfo> nodesFromA = serverA.processProvisionResourceRequest(groupX, 1);
        Assert.assertEquals(1, nodesFromA.size());
        Assert.assertEquals(host, nodesFromA.get(0).getHost());
        Assert.assertEquals(0L, serverA.getFreeNodeCount());
        Assert.assertEquals(1L, serverA.getAssignedPoolsSize());

        { // serverB loads the data from persist data
            StarProvisionServer serverB = new StarProvisionServer();
            Assert.assertEquals(0L, serverB.getFreeNodeCount());
            Assert.assertEquals(1L, serverB.getAssignedPoolsSize());
            List<NodeInfo> nodesFromB = serverB.processGetResourceRequest(groupX);
            Assert.assertEquals(nodesFromA, nodesFromB);
        }
    }
}
