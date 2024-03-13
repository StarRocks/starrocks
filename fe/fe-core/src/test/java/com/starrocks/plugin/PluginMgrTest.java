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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/plugin/PluginMgrTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.plugin;

import com.starrocks.common.Config;
import com.starrocks.common.UserException;
import com.starrocks.common.io.DataOutputBuffer;
import com.starrocks.common.util.DigitalVersion;
import com.starrocks.plugin.PluginInfo.PluginType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PluginMgrTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
    }

    @Before
    public void setUp() throws IOException {
        FileUtils.deleteQuietly(PluginTestUtil.getTestFile("target"));
        assertFalse(Files.exists(PluginTestUtil.getTestPath("target")));
        Files.createDirectory(PluginTestUtil.getTestPath("target"));
        assertTrue(Files.exists(PluginTestUtil.getTestPath("target")));
        Config.plugin_dir = PluginTestUtil.getTestPathString("target");
    }

    @Test
    public void testLoadPluginFail() {
        try {

            PluginMgr pluginMgr = GlobalStateMgr.getCurrentState().getPluginMgr();
            PluginInfo info = new PluginInfo();
            info.name = "plugin-name";
            info.type = PluginType.AUDIT;
            info.description = "plugin description";
            info.version = DigitalVersion.CURRENT_STARROCKS_VERSION;
            info.javaVersion = DigitalVersion.JDK_1_8_0;
            info.className = "hello.jar";
            info.soName = "hello.so";
            info.source = "test";
            info.properties.put("md5sum", "cf0c536b8f2a0a0690b44d783d019e90");
            pluginMgr.replayLoadDynamicPlugin(info);

        } catch (IOException | UserException e) {
            e.printStackTrace();
            assert false;
        }
    }

    private void testSerializeBuiltinPlugin(PluginMgr mgr) {
        try {
            DataOutputBuffer dob = new DataOutputBuffer();
            DataOutputStream dos = new DataOutputStream(dob);
            mgr.write(dos);

            PluginMgr test = new PluginMgr();

            test.readFields(new DataInputStream(new ByteArrayInputStream(dob.getData())));
            assertEquals(1, test.getAllDynamicPluginInfo().size());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
