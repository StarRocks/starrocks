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

package com.starrocks.server;

import com.starrocks.catalog.Catalog;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.persist.AlterCatalogLog;
import com.starrocks.persist.DropCatalogLog;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class CatalogMgrEditLogTest {
    private CatalogMgr masterCatalogMgr;
    private ConnectorMgr connectorMgr;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        // Get CatalogMgr and ConnectorMgr instances from GlobalStateMgr
        masterCatalogMgr = GlobalStateMgr.getCurrentState().getCatalogMgr();
        connectorMgr = GlobalStateMgr.getCurrentState().getConnectorMgr();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    private Map<String, String> createTestCatalogProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "hive");
        properties.put("hive.metastore.uris", "thrift://127.0.0.1:9083");
        return properties;
    }

    @Test
    public void testCreateCatalogNormalCase() throws Exception {
        // 1. Prepare test data
        String catalogName = "test_catalog";
        String type = "hive";
        String comment = "test catalog";
        Map<String, String> properties = createTestCatalogProperties();

        // 2. Verify initial state
        Assertions.assertFalse(masterCatalogMgr.catalogExists(catalogName));

        // 3. Execute createCatalog operation (master side)
        masterCatalogMgr.createCatalog(type, catalogName, comment, properties);

        // 4. Verify master state
        Assertions.assertTrue(masterCatalogMgr.catalogExists(catalogName));
        Catalog catalog = masterCatalogMgr.getCatalogByName(catalogName);
        Assertions.assertNotNull(catalog);
        Assertions.assertEquals(catalogName, catalog.getName());
        Assertions.assertEquals(type, catalog.getType());
        Assertions.assertEquals(comment, catalog.getComment());

        // 5. Test follower replay functionality
        // Create a new ConnectorMgr for follower to avoid connector conflicts
        ConnectorMgr followerConnectorMgr = new ConnectorMgr();
        CatalogMgr followerCatalogMgr = new CatalogMgr(followerConnectorMgr);

        Catalog replayCatalog = (Catalog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_CATALOG);

        // Execute follower replay
        followerCatalogMgr.replayCreateCatalog(replayCatalog);

        // 6. Verify follower state is consistent with master
        Assertions.assertTrue(followerCatalogMgr.catalogExists(catalogName));
        Catalog followerCatalog = followerCatalogMgr.getCatalogByName(catalogName);
        Assertions.assertNotNull(followerCatalog);
        Assertions.assertEquals(catalogName, followerCatalog.getName());
        Assertions.assertEquals(type, followerCatalog.getType());
    }

    @Test
    public void testCreateCatalogEditLogException() throws Exception {
        // 1. Prepare test data
        String catalogName = "exception_catalog";
        String type = "hive";
        String comment = "test catalog";
        Map<String, String> properties = createTestCatalogProperties();

        // 2. Create a separate CatalogMgr for exception testing
        CatalogMgr exceptionCatalogMgr = GlobalStateMgr.getCurrentState().getCatalogMgr();

        EditLog spyEditLog = spy(new EditLog(null));

        // 3. Mock EditLog.logCreateCatalog to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logCreateCatalog(any(Catalog.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Verify initial state
        Assertions.assertFalse(exceptionCatalogMgr.catalogExists(catalogName));

        // 4. Execute createCatalog operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionCatalogMgr.createCatalog(type, catalogName, comment, properties);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertFalse(exceptionCatalogMgr.catalogExists(catalogName));
        Catalog catalog = exceptionCatalogMgr.getCatalogByName(catalogName);
        Assertions.assertNull(catalog);
    }

    @Test
    public void testCreateCatalogDuplicate() throws Exception {
        // 1. Create a catalog first
        String catalogName = "duplicate_catalog";
        String type = "hive";
        Map<String, String> properties = createTestCatalogProperties();
        masterCatalogMgr.createCatalog(type, catalogName, "comment", properties);

        // 2. Verify initial state
        Assertions.assertTrue(masterCatalogMgr.catalogExists(catalogName));

        // 3. Try to create duplicate catalog and expect IllegalStateException
        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class, () -> {
            masterCatalogMgr.createCatalog(type, catalogName, "comment2", properties);
        });
        Assertions.assertTrue(exception.getMessage().contains("Catalog '" + catalogName + "' already exists"));
    }

    @Test
    public void testDropCatalogNormalCase() throws Exception {
        // 1. Create a catalog first
        String catalogName = "test_drop_catalog";
        String type = "hive";
        Map<String, String> properties = createTestCatalogProperties();
        masterCatalogMgr.createCatalog(type, catalogName, "comment", properties);

        // 2. Verify initial state
        Assertions.assertTrue(masterCatalogMgr.catalogExists(catalogName));

        // 3. Execute dropCatalog operation (master side)
        masterCatalogMgr.dropCatalog(new com.starrocks.sql.ast.DropCatalogStmt(catalogName));

        // 4. Verify master state
        Assertions.assertFalse(masterCatalogMgr.catalogExists(catalogName));
        Catalog catalog = masterCatalogMgr.getCatalogByName(catalogName);
        Assertions.assertNull(catalog);

        // 5. Test follower replay functionality
        // Create a new ConnectorMgr for follower to avoid connector conflicts
        ConnectorMgr followerConnectorMgr = new ConnectorMgr();
        CatalogMgr followerCatalogMgr = new CatalogMgr(followerConnectorMgr);
        followerCatalogMgr.createCatalog(type, catalogName, "comment", properties);

        DropCatalogLog replayLog = (DropCatalogLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_DROP_CATALOG);

        // Execute follower replay
        followerCatalogMgr.replayDropCatalog(replayLog);

        // 6. Verify follower state is consistent with master
        Assertions.assertFalse(followerCatalogMgr.catalogExists(catalogName));
        Catalog followerCatalog = followerCatalogMgr.getCatalogByName(catalogName);
        Assertions.assertNull(followerCatalog);
    }

    @Test
    public void testDropCatalogEditLogException() throws Exception {
        // 1. Create a catalog first
        String catalogName = "exception_drop_catalog";
        String type = "hive";
        Map<String, String> properties = createTestCatalogProperties();
        masterCatalogMgr.createCatalog(type, catalogName, "comment", properties);

        // 2. Create a separate CatalogMgr for exception testing
        CatalogMgr exceptionCatalogMgr = GlobalStateMgr.getCurrentState().getCatalogMgr();
        
        // Ensure catalog exists for exception testing
        if (!exceptionCatalogMgr.catalogExists(catalogName)) {
            exceptionCatalogMgr.createCatalog(type, catalogName, "comment", properties);
        }

        EditLog spyEditLog = spy(new EditLog(null));

        // 3. Mock EditLog.logDropCatalog to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logDropCatalog(any(DropCatalogLog.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Verify initial state
        Assertions.assertTrue(exceptionCatalogMgr.catalogExists(catalogName));

        // 4. Execute dropCatalog operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionCatalogMgr.dropCatalog(new com.starrocks.sql.ast.DropCatalogStmt(catalogName));
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertTrue(exceptionCatalogMgr.catalogExists(catalogName));
        Catalog catalog = exceptionCatalogMgr.getCatalogByName(catalogName);
        Assertions.assertNotNull(catalog);
    }

    @Test
    public void testDropCatalogNonExistent() throws Exception {
        // 1. Test dropping non-existent catalog
        String nonExistentCatalogName = "non_existent_catalog";

        // 2. Verify initial state
        Assertions.assertFalse(masterCatalogMgr.catalogExists(nonExistentCatalogName));

        // 3. Execute dropCatalog operation and expect IllegalStateException
        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class, () -> {
            masterCatalogMgr.dropCatalog(new com.starrocks.sql.ast.DropCatalogStmt(nonExistentCatalogName));
        });
        Assertions.assertTrue(exception.getMessage().contains("Catalog '" + nonExistentCatalogName + "' doesn't exist"));
    }

    @Test
    public void testAlterCatalogNormalCase() throws Exception {
        // 1. Create a catalog first
        String catalogName = "test_alter_catalog";
        String type = "hive";
        Map<String, String> properties = createTestCatalogProperties();
        masterCatalogMgr.createCatalog(type, catalogName, "comment", properties);

        // 2. Prepare alter properties
        Map<String, String> alterProperties = new HashMap<>();
        alterProperties.put("hive.metastore.uris", "thrift://127.0.0.1:9084");

        // 3. Create AlterCatalogStmt
        com.starrocks.sql.ast.AlterCatalogStmt alterStmt = new com.starrocks.sql.ast.AlterCatalogStmt(
                catalogName, new com.starrocks.sql.ast.ModifyTablePropertiesClause(alterProperties), null);

        // 4. Execute alterCatalog operation (master side)
        masterCatalogMgr.alterCatalog(alterStmt);

        // 5. Verify master state
        Catalog catalog = masterCatalogMgr.getCatalogByName(catalogName);
        Assertions.assertNotNull(catalog);
        Assertions.assertEquals("thrift://127.0.0.1:9084", catalog.getConfig().get("hive.metastore.uris"));

        // 6. Test follower replay functionality
        // Create a new ConnectorMgr for follower to avoid connector conflicts
        ConnectorMgr followerConnectorMgr = new ConnectorMgr();
        CatalogMgr followerCatalogMgr = new CatalogMgr(followerConnectorMgr);
        
        // First replay the create catalog operation to set up the initial state
        Catalog createReplayCatalog = (Catalog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_CATALOG);
        followerCatalogMgr.replayCreateCatalog(createReplayCatalog);
        Assertions.assertTrue(followerCatalogMgr.catalogExists(catalogName));

        AlterCatalogLog replayLog = (AlterCatalogLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_CATALOG);

        // Execute follower replay
        followerCatalogMgr.replayAlterCatalog(replayLog);

        // 7. Verify follower state is consistent with master
        Catalog followerCatalog = followerCatalogMgr.getCatalogByName(catalogName);
        Assertions.assertNotNull(followerCatalog);
        Assertions.assertEquals("thrift://127.0.0.1:9084", followerCatalog.getConfig().get("hive.metastore.uris"));
    }

    @Test
    public void testAlterCatalogEditLogException() throws Exception {
        // 1. Create a catalog first
        String catalogName = "exception_alter_catalog";
        String type = "hive";
        Map<String, String> properties = createTestCatalogProperties();
        masterCatalogMgr.createCatalog(type, catalogName, "comment", properties);

        // 2. Create a separate CatalogMgr for exception testing
        CatalogMgr exceptionCatalogMgr = GlobalStateMgr.getCurrentState().getCatalogMgr();
        
        // Ensure catalog exists for exception testing
        if (!exceptionCatalogMgr.catalogExists(catalogName)) {
            exceptionCatalogMgr.createCatalog(type, catalogName, "comment", properties);
        }

        EditLog spyEditLog = spy(new EditLog(null));

        // 3. Mock EditLog.logAlterCatalog to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logAlterCatalog(any(AlterCatalogLog.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Prepare alter properties
        Map<String, String> alterProperties = new HashMap<>();
        alterProperties.put("hive.metastore.uris", "thrift://127.0.0.1:9084");
        com.starrocks.sql.ast.AlterCatalogStmt alterStmt = new com.starrocks.sql.ast.AlterCatalogStmt(
                catalogName, new com.starrocks.sql.ast.ModifyTablePropertiesClause(alterProperties), null);

        // Save initial state
        Catalog initialCatalog = exceptionCatalogMgr.getCatalogByName(catalogName);
        String initialUri = initialCatalog.getConfig().get("hive.metastore.uris");

        // 4. Execute alterCatalog operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionCatalogMgr.alterCatalog(alterStmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Catalog currentCatalog = exceptionCatalogMgr.getCatalogByName(catalogName);
        Assertions.assertEquals(initialUri, currentCatalog.getConfig().get("hive.metastore.uris"));
    }
}

