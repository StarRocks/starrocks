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

import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.UninstallPluginLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.InstallPluginStmt;
import com.starrocks.sql.ast.UninstallPluginStmt;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
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

    // ==================== Install Plugin From Stmt Tests ====================

    @Test
    public void testInstallPluginFromStmtNormalCase() throws Exception {
        // 1. Prepare test data - use a unique plugin name to avoid conflicts
        String uniquePluginName = "test_plugin_install_" + System.currentTimeMillis();
        Map<String, String> properties = Maps.newHashMap();
        properties.put("md5sum", "test_md5sum");
        InstallPluginStmt stmt = new InstallPluginStmt("http://test/test.zip", properties);

        // Mock DynamicPluginLoader to avoid actual file operations
        PluginInfo mockPluginInfo = new PluginInfo(uniquePluginName, PluginInfo.PluginType.AUDIT, "Test plugin");
        
        new MockUp<DynamicPluginLoader>() {
            @Mock
            public PluginInfo getPluginInfo() throws IOException {
                return mockPluginInfo;
            }

            @Mock
            public void install() throws StarRocksException, IOException {
                // Mock uninstall to do nothing
            }

            @Mock
            public void uninstall() throws IOException, StarRocksException {
                // Mock uninstall to do nothing
            }
        };

        // 2. Verify initial state
        Assertions.assertFalse(pluginExists(masterPluginMgr, uniquePluginName, PluginInfo.PluginType.AUDIT));

        // 3. Execute installPlugin operation
        PluginInfo installedInfo = masterPluginMgr.installPlugin(stmt);

        // 4. Verify master state
        Assertions.assertNotNull(installedInfo);
        Assertions.assertEquals(uniquePluginName, installedInfo.getName());
        Assertions.assertTrue(pluginExists(masterPluginMgr, uniquePluginName, PluginInfo.PluginType.AUDIT));

        // 5. Test follower replay functionality
        PluginMgr followerPluginMgr = new PluginMgr();
        PluginInfo replayPluginInfo = (PluginInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_INSTALL_PLUGIN);

        followerPluginMgr.replayLoadDynamicPlugin(replayPluginInfo);

        // 6. Verify follower state is consistent with master
        Assertions.assertTrue(pluginExists(followerPluginMgr, uniquePluginName, PluginInfo.PluginType.AUDIT));
    }

    @Test
    public void testInstallPluginFromStmtEditLogException() throws Exception {
        // 1. Prepare test data - use a unique plugin name to avoid conflicts
        String uniquePluginName = "test_plugin_editlog_exception_" + System.currentTimeMillis();
        Map<String, String> properties = Maps.newHashMap();
        properties.put("md5sum", "test_md5sum");
        InstallPluginStmt stmt = new InstallPluginStmt("http://test/test.zip", properties);

        PluginInfo mockPluginInfo = new PluginInfo(uniquePluginName, PluginInfo.PluginType.AUDIT, "Test plugin");
        
        // Mock DynamicPluginLoader
        new MockUp<DynamicPluginLoader>() {
            @Mock
            public PluginInfo getPluginInfo() throws IOException {
                return mockPluginInfo;
            }

            @Mock
            public void install() throws StarRocksException, IOException {
                // Mock install to do nothing
            }

            @Mock
            public void uninstall() throws IOException, StarRocksException {
                // Mock uninstall to do nothing
            }
        };

        // 2. Mock EditLog.logInstallPlugin to throw exception
        EditLog spyEditLog = spy(GlobalStateMgr.getCurrentState().getEditLog());
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logInstallPlugin(any(PluginInfo.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute installPlugin operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            masterPluginMgr.installPlugin(stmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify plugin is not installed after exception
        Assertions.assertFalse(pluginExists(masterPluginMgr, uniquePluginName, PluginInfo.PluginType.AUDIT));
    }

    @Test
    public void testInstallPluginFromStmtPluginAlreadyExists() throws Exception {
        // 1. Prepare test data - install plugin first
        PluginInfo pluginInfo = createMockPluginInfo();
        masterPluginMgr.replayLoadDynamicPlugin(pluginInfo);
        Assertions.assertTrue(pluginExists(masterPluginMgr, TEST_PLUGIN_NAME, PluginInfo.PluginType.AUDIT));

        // 2. Try to install the same plugin again
        Map<String, String> properties = Maps.newHashMap();
        properties.put("md5sum", "test_md5sum");
        InstallPluginStmt stmt = new InstallPluginStmt("http://test/test.zip", properties);

        new MockUp<DynamicPluginLoader>() {
            @Mock
            public PluginInfo getPluginInfo() throws IOException {
                return pluginInfo;
            }
        };

        // 3. Execute installPlugin operation and expect exception
        StarRocksException exception = Assertions.assertThrows(StarRocksException.class, () -> {
            masterPluginMgr.installPlugin(stmt);
        });
        Assertions.assertTrue(exception.getMessage().contains("has already been installed"));
    }

    // ==================== Uninstall Plugin From Stmt Method Tests ====================

    @Test
    public void testUninstallPluginFromStmtMethodNormalCase() throws Exception {
        // 1. Prepare test data and install plugin first
        PluginInfo pluginInfo = createMockPluginInfo();
        masterPluginMgr.replayLoadDynamicPlugin(pluginInfo);
        Assertions.assertTrue(pluginExists(masterPluginMgr, TEST_PLUGIN_NAME, PluginInfo.PluginType.AUDIT));

        // 2. Execute uninstallPluginFromStmt operation
        UninstallPluginStmt stmt = new UninstallPluginStmt(TEST_PLUGIN_NAME);
        masterPluginMgr.uninstallPluginFromStmt(stmt);

        // 3. Verify master state
        Assertions.assertFalse(pluginExists(masterPluginMgr, TEST_PLUGIN_NAME, PluginInfo.PluginType.AUDIT));

        // 4. Test follower replay functionality
        PluginMgr followerPluginMgr = new PluginMgr();
        followerPluginMgr.replayLoadDynamicPlugin(pluginInfo);
        Assertions.assertTrue(pluginExists(followerPluginMgr, TEST_PLUGIN_NAME, PluginInfo.PluginType.AUDIT));

        UninstallPluginLog replayLog = (UninstallPluginLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_UNINSTALL_PLUGIN);

        followerPluginMgr.replayUninstallPlugin(replayLog);

        // 5. Verify follower state is consistent with master
        Assertions.assertFalse(pluginExists(followerPluginMgr, TEST_PLUGIN_NAME, PluginInfo.PluginType.AUDIT));
    }

    @Test
    public void testUninstallPluginFromStmtMethodEditLogException() throws Exception {
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

        // 3. Execute uninstallPluginFromStmt operation and expect exception
        UninstallPluginStmt stmt = new UninstallPluginStmt(TEST_PLUGIN_NAME);
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionPluginMgr.uninstallPluginFromStmt(stmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 4. Verify leader memory state remains unchanged after exception
        Assertions.assertTrue(pluginExists(exceptionPluginMgr, TEST_PLUGIN_NAME, PluginInfo.PluginType.AUDIT));
    }

    @Test
    public void testUninstallPluginFromStmtMethodPluginNotExists() throws Exception {
        // 1. Verify plugin does not exist - use a unique name that definitely doesn't exist
        String nonExistentPluginName = "non_existent_plugin_" + System.currentTimeMillis();
        Assertions.assertFalse(pluginExists(masterPluginMgr, nonExistentPluginName, PluginInfo.PluginType.AUDIT));

        // 2. Try to uninstall non-existent plugin
        UninstallPluginStmt stmt = new UninstallPluginStmt(nonExistentPluginName);

        // 3. Execute uninstallPluginFromStmt operation and expect exception
        StarRocksException exception = Assertions.assertThrows(StarRocksException.class, () -> {
            masterPluginMgr.uninstallPluginFromStmt(stmt);
        });
        Assertions.assertTrue(exception.getMessage().contains("does not exist"));
    }
}

