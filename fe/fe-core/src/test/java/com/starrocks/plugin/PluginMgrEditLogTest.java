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

package com.starrocks.plugin;

import com.starrocks.common.Config;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.UninstallPluginLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class PluginMgrEditLogTest {
    private PluginMgr masterPluginMgr;
    private static final String TEST_PLUGIN_NAME = "test_plugin";
    private File tempPluginDir;
    private String originalPluginDir;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        GlobalStateMgr.getCurrentState().getWarehouseMgr().initDefaultWarehouse();

        // Create temporary plugin directory
        tempPluginDir = Files.createTempDirectory("plugin_test_").toFile();
        originalPluginDir = Config.plugin_dir;
        Config.plugin_dir = tempPluginDir.getAbsolutePath();

        // Create PluginMgr instance
        masterPluginMgr = GlobalStateMgr.getCurrentState().getPluginMgr();
    }

    @AfterEach
    public void tearDown() throws IOException {
        // Restore original plugin_dir
        if (originalPluginDir != null) {
            Config.plugin_dir = originalPluginDir;
        }
        
        // Clean up temporary directory
        if (tempPluginDir != null && tempPluginDir.exists()) {
            FileUtils.deleteQuietly(tempPluginDir);
        }
        
        UtFrameUtils.tearDownForPersisTest();
    }

    // ==================== Install Plugin Tests ====================

    @Test
    public void testInstallPluginNormalCase() throws Exception {
        // 1. Prepare test data
        PluginInfo pluginInfo = createMockPluginInfo();

        // 2. Verify initial state
        List<PluginInfo> initialPlugins = masterPluginMgr.getAllDynamicPluginInfo();
        Assertions.assertFalse(initialPlugins.stream().anyMatch(p -> p.getName().equals(TEST_PLUGIN_NAME)));

        // 3. Test follower replay functionality by directly logging and replaying
        GlobalStateMgr.getCurrentState().getEditLog().logInstallPlugin(pluginInfo, wal -> {
            try {
                masterPluginMgr.replayLoadDynamicPlugin(pluginInfo);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        // 4. Verify master state - check if plugin is in plugins map
        Assertions.assertTrue(pluginExists(masterPluginMgr, TEST_PLUGIN_NAME, PluginInfo.PluginType.AUDIT));

        // 5. Test follower replay functionality
        PluginMgr followerPluginMgr = new PluginMgr();

        PluginInfo replayPluginInfo = (PluginInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_INSTALL_PLUGIN);

        followerPluginMgr.replayLoadDynamicPlugin(replayPluginInfo);

        // 6. Verify follower state is consistent with master
        Assertions.assertTrue(pluginExists(followerPluginMgr, TEST_PLUGIN_NAME, PluginInfo.PluginType.AUDIT));
    }

    @Test
    public void testInstallPluginEditLogException() throws Exception {
        // 1. Prepare test data
        PluginInfo pluginInfo = createMockPluginInfo();

        // 2. Create a separate PluginMgr for exception testing
        PluginMgr exceptionPluginMgr = new PluginMgr();
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());

        // 3. Mock EditLog.logInstallPlugin to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logInstallPlugin(any(PluginInfo.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Verify initial state
        List<PluginInfo> initialPlugins = exceptionPluginMgr.getAllDynamicPluginInfo();
        Assertions.assertFalse(initialPlugins.stream().anyMatch(p -> p.getName().equals(TEST_PLUGIN_NAME)));

        // 4. Execute logInstallPlugin operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            GlobalStateMgr.getCurrentState().getEditLog().logInstallPlugin(pluginInfo, wal -> {});
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        List<PluginInfo> pluginsAfterException = exceptionPluginMgr.getAllDynamicPluginInfo();
        Assertions.assertFalse(pluginsAfterException.stream().anyMatch(p -> p.getName().equals(TEST_PLUGIN_NAME)));
    }

    // ==================== Uninstall Plugin From Stmt Tests ====================

    @Test
    public void testUninstallPluginFromStmtNormalCase() throws Exception {
        // 1. Prepare test data and install plugin first
        PluginInfo pluginInfo = createMockPluginInfo();
        masterPluginMgr.replayLoadDynamicPlugin(pluginInfo);
        Assertions.assertTrue(pluginExists(masterPluginMgr, TEST_PLUGIN_NAME, PluginInfo.PluginType.AUDIT));

        // 2. Execute uninstallPluginFromStmt operation by directly testing the editlog part
        UninstallPluginLog uninstallLog = new UninstallPluginLog(TEST_PLUGIN_NAME);
        GlobalStateMgr.getCurrentState().getEditLog().logUninstallPlugin(uninstallLog, wal -> {
            masterPluginMgr.replayUninstallPlugin(uninstallLog);
        });

        // 3. Verify master state
        Assertions.assertFalse(pluginExists(masterPluginMgr, TEST_PLUGIN_NAME, PluginInfo.PluginType.AUDIT));
    }

    @Test
    public void testUninstallPluginFromStmtEditLogException() throws Exception {
        // 1. Prepare test data and install plugin first
        PluginInfo pluginInfo = createMockPluginInfo();
        PluginMgr exceptionPluginMgr = new PluginMgr();
        exceptionPluginMgr.replayLoadDynamicPlugin(pluginInfo);
        Assertions.assertTrue(pluginExists(exceptionPluginMgr, TEST_PLUGIN_NAME, PluginInfo.PluginType.AUDIT));

        // 2. Mock EditLog.logUninstallPlugin to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logUninstallPlugin(any(UninstallPluginLog.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute logUninstallPlugin operation and expect exception
        UninstallPluginLog uninstallLog = new UninstallPluginLog(TEST_PLUGIN_NAME);
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            GlobalStateMgr.getCurrentState().getEditLog().logUninstallPlugin(uninstallLog, wal -> {});
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify leader memory state remains unchanged after exception
        Assertions.assertTrue(pluginExists(exceptionPluginMgr, TEST_PLUGIN_NAME, PluginInfo.PluginType.AUDIT));
    }

    private PluginInfo createMockPluginInfo() {
        return new PluginInfo(TEST_PLUGIN_NAME, PluginInfo.PluginType.AUDIT, "Test plugin");
    }

    // Helper method to check if plugin exists in plugins map
    private boolean pluginExists(PluginMgr pluginMgr, String pluginName, PluginInfo.PluginType type) {
        try {
            Field pluginsField = PluginMgr.class.getDeclaredField("plugins");
            pluginsField.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<String, PluginLoader>[] plugins = (Map<String, PluginLoader>[]) pluginsField.get(pluginMgr);
            return plugins[type.ordinal()].containsKey(pluginName);
        } catch (Exception e) {
            // Fallback to getAllDynamicPluginInfo
            List<PluginInfo> plugins = pluginMgr.getAllDynamicPluginInfo();
            return plugins.stream().anyMatch(p -> p.getName().equals(pluginName));
        }
    }
}

