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

import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.util.DigitalVersion;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.UninstallPluginLog;
import com.starrocks.persist.WALApplier;
import com.starrocks.plugin.PluginInfo.PluginType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.InstallPluginStmt;
import com.starrocks.sql.ast.UninstallPluginStmt;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PluginMgrTest {
    private static final String TARGET_DIR = "target_plugin_mgr";

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
    }

    @BeforeEach
    public void setUp() throws IOException {
        FileUtils.deleteQuietly(PluginTestUtil.getTestFile(TARGET_DIR));
        assertFalse(Files.exists(PluginTestUtil.getTestPath(TARGET_DIR)));
        Files.createDirectory(PluginTestUtil.getTestPath(TARGET_DIR));
        assertTrue(Files.exists(PluginTestUtil.getTestPath(TARGET_DIR)));
        Config.plugin_dir = PluginTestUtil.getTestPathString(TARGET_DIR);
    }

    /**
     * The plugin dir is created by another process -- a parallel FE unit test starting its own frontend -- after this
     * one found it missing: mkdir fails, as the directory exists. It does exist, so that is no failure.
     */
    @Test
    public void testCreatePluginDirCreatedConcurrently() throws Exception {
        File dir = PluginTestUtil.getTestFile(TARGET_DIR + "/created_concurrently");
        File checkedBeforeCreated = new File(dir.getPath()) {
            @Override
            public boolean mkdir() {
                // Missing when checked; another process creates it right before this one's mkdir.
                assertTrue(dir.mkdir());
                return super.mkdir();
            }
        };
        assertDoesNotThrow(() -> PluginMgr.createPluginDir(checkedBeforeCreated));
        assertTrue(dir.isDirectory());
    }

    @Test
    public void testCreatePluginDir() throws Exception {
        File dir = PluginTestUtil.getTestFile(TARGET_DIR + "/missing");
        PluginMgr.createPluginDir(dir);
        assertTrue(dir.isDirectory());
        // Already there.
        PluginMgr.createPluginDir(dir);
        assertTrue(dir.isDirectory());

        File notADirectory = PluginTestUtil.getTestFile(TARGET_DIR + "/a_file");
        assertTrue(notADirectory.createNewFile());
        PluginException e = assertThrows(PluginException.class, () -> PluginMgr.createPluginDir(notADirectory));
        assertTrue(e.getMessage().contains("is not a directory"), e.getMessage());

        File parentMissing = PluginTestUtil.getTestFile(TARGET_DIR + "/no_such_parent/plugins");
        e = assertThrows(PluginException.class, () -> PluginMgr.createPluginDir(parentMissing));
        assertTrue(e.getMessage().contains("failed to create FE plugin dir"), e.getMessage());
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

        } catch (IOException e) {
            e.printStackTrace();
            assert false;
        }
    }

    @Test
    public void testInstallPluginIfNotExistsIdempotent() throws Exception {
        String pluginName = "idempotent_install_plugin";
        PluginInfo pluginInfo = new PluginInfo(pluginName, PluginType.AUDIT, "test");

        PluginMgr pluginMgr = new PluginMgr();
        pluginMgr.replayLoadDynamicPlugin(pluginInfo);

        new MockUp<DynamicPluginLoader>() {
            @Mock
            public PluginInfo getPluginInfo() throws IOException {
                return pluginInfo;
            }
        };

        Map<String, String> props = Maps.newHashMap();
        InstallPluginStmt stmt = new InstallPluginStmt("http://dummy/test.zip", props, true, NodePosition.ZERO);
        PluginInfo result = pluginMgr.installPlugin(stmt);
        assertSame(pluginInfo, result);
    }

    @Test
    public void testUninstallPluginIfExistsIdempotent() {
        PluginMgr pluginMgr = new PluginMgr();
        UninstallPluginStmt stmt = new UninstallPluginStmt("nonexistent_plugin", true, NodePosition.ZERO);
        assertDoesNotThrow(() -> pluginMgr.uninstallPluginFromStmt(stmt));
    }

    @Test
    public void testUninstallPluginFromStmtNormal() throws Exception {
        String pluginName = "normal_uninstall_plugin";
        PluginInfo pluginInfo = new PluginInfo(pluginName, PluginType.AUDIT, "test");

        PluginMgr pluginMgr = new PluginMgr();
        pluginMgr.replayLoadDynamicPlugin(pluginInfo);

        new MockUp<EditLog>() {
            @Mock
            public void logUninstallPlugin(UninstallPluginLog log, WALApplier walApplier) {
                walApplier.apply(log);
            }
        };

        UninstallPluginStmt stmt = new UninstallPluginStmt(pluginName);
        pluginMgr.uninstallPluginFromStmt(stmt);
        assertFalse(pluginMgr.getAllDynamicPluginInfo()
                .stream().anyMatch(p -> p.getName().equals(pluginName)));
    }
}
