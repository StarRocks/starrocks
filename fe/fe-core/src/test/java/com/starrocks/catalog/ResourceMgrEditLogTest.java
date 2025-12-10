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

package com.starrocks.catalog;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Catalog;
import com.starrocks.persist.AlterResourceInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterResourceStmt;
import com.starrocks.sql.ast.CreateResourceStmt;
import com.starrocks.sql.ast.DropResourceStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class ResourceMgrEditLogTest {
    private ResourceMgr masterResourceMgr;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        // Create ResourceMgr instance
        masterResourceMgr = new ResourceMgr();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    // Helper method to create Hive resource
    private CreateResourceStmt createHiveResourceStmt(String name, String metastoreUris) {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("type", "hive");
        properties.put("hive.metastore.uris", metastoreUris);
        CreateResourceStmt stmt = new CreateResourceStmt(true, name, properties);
        stmt.setResourceType(Resource.ResourceType.HIVE);
        return stmt;
    }

    // Helper method to create Hudi resource
    private CreateResourceStmt createHudiResourceStmt(String name, String metastoreUris) {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("type", "hudi");
        properties.put("hive.metastore.uris", metastoreUris);
        CreateResourceStmt stmt = new CreateResourceStmt(true, name, properties);
        stmt.setResourceType(Resource.ResourceType.HUDI);
        return stmt;
    }

    // Helper method to create Iceberg resource
    private CreateResourceStmt createIcebergResourceStmt(String name, String metastoreUris) {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("type", "iceberg");
        properties.put("iceberg.catalog.type", "HIVE");
        properties.put("hive.metastore.uris", metastoreUris);
        CreateResourceStmt stmt = new CreateResourceStmt(true, name, properties);
        stmt.setResourceType(Resource.ResourceType.ICEBERG);
        return stmt;
    }

    // Helper method to drop catalog if exists (for test environment where master and follower share GlobalStateMgr)
    private void dropCatalogIfExists(Resource resource) {
        if (resource != null && resource.needMappingCatalog()) {
            String type = resource.getType().name().toLowerCase(java.util.Locale.ROOT);
            String catalogName = com.starrocks.server.CatalogMgr.ResourceMappingCatalog.getResourceMappingCatalogName(
                    resource.getName(), type);
            if (GlobalStateMgr.getCurrentState().getCatalogMgr().catalogExists(catalogName)) {
                com.starrocks.sql.ast.DropCatalogStmt dropCatalogStmt = new com.starrocks.sql.ast.DropCatalogStmt(catalogName);
                try {
                    GlobalStateMgr.getCurrentState().getCatalogMgr().dropCatalog(dropCatalogStmt);
                } catch (Exception e) {
                    // Ignore errors when dropping catalog in test
                }
            }
        }
    }

    @Test
    public void testCreateResourceNormalCase() throws Exception {
        // 1. Prepare test data
        String resourceName = "test_hive_resource";
        String metastoreUris = "thrift://127.0.0.1:9083";
        CreateResourceStmt stmt = createHiveResourceStmt(resourceName, metastoreUris);

        // 2. Verify initial state
        Assertions.assertFalse(masterResourceMgr.containsResource(resourceName));
        Assertions.assertEquals(0, masterResourceMgr.getResourceNum());

        // 3. Execute createResource operation (master side)
        masterResourceMgr.createResource(stmt);

        // 4. Verify master state
        Assertions.assertTrue(masterResourceMgr.containsResource(resourceName));
        Assertions.assertEquals(1, masterResourceMgr.getResourceNum());
        Resource resource = masterResourceMgr.getResource(resourceName);
        Assertions.assertNotNull(resource);
        Assertions.assertEquals(resourceName, resource.getName());
        Assertions.assertEquals(Resource.ResourceType.HIVE, resource.getType());
        
        // Verify catalog exists
        String masterCatalogName = CatalogMgr.ResourceMappingCatalog.getResourceMappingCatalogName(resourceName, "hive");
        Catalog masterCatalog = GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogByName(masterCatalogName);
        Assertions.assertNotNull(masterCatalog);
        Assertions.assertEquals(masterCatalogName, masterCatalog.getName());

        // 5. Test follower replay functionality
        ResourceMgr followerResourceMgr = new ResourceMgr();

        // Verify follower initial state
        Assertions.assertFalse(followerResourceMgr.containsResource(resourceName));
        Assertions.assertEquals(0, followerResourceMgr.getResourceNum());

        Resource replayResource = (Resource) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_RESOURCE);

        // In test environment, master and follower share the same GlobalStateMgr,
        // so we need to drop the catalog first if it exists (created by master)
        dropCatalogIfExists(replayResource);

        // Execute follower replay
        followerResourceMgr.replayCreateResource(replayResource);

        // 6. Verify follower state is consistent with master
        Assertions.assertTrue(followerResourceMgr.containsResource(resourceName));
        Assertions.assertEquals(1, followerResourceMgr.getResourceNum());
        Resource followerResource = followerResourceMgr.getResource(resourceName);
        Assertions.assertNotNull(followerResource);
        Assertions.assertEquals(resourceName, followerResource.getName());
        Assertions.assertEquals(Resource.ResourceType.HIVE, followerResource.getType());
        
        // Verify catalog exists in follower
        String followerCatalogName = CatalogMgr.ResourceMappingCatalog.getResourceMappingCatalogName(resourceName, "hive");
        Catalog followerCatalog = GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogByName(followerCatalogName);
        Assertions.assertNotNull(followerCatalog);
        Assertions.assertEquals(followerCatalogName, followerCatalog.getName());
    }

    @Test
    public void testCreateResourceEditLogException() throws Exception {
        // 1. Prepare test data
        String resourceName = "exception_hive_resource";
        String metastoreUris = "thrift://127.0.0.1:9083";
        CreateResourceStmt stmt = createHiveResourceStmt(resourceName, metastoreUris);

        // 2. Create a separate ResourceMgr for exception testing
        ResourceMgr exceptionResourceMgr = new ResourceMgr();
        // Use the existing EditLog from GlobalStateMgr (which has proper journalQueue)
        EditLog currentEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(currentEditLog);

        // 3. Mock EditLog.logCreateResource to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logCreateResource(any(Resource.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Verify initial state
        Assertions.assertFalse(exceptionResourceMgr.containsResource(resourceName));
        Assertions.assertEquals(0, exceptionResourceMgr.getResourceNum());

        // 4. Execute createResource operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionResourceMgr.createResource(stmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertFalse(exceptionResourceMgr.containsResource(resourceName));
        Assertions.assertEquals(0, exceptionResourceMgr.getResourceNum());
    }

    @Test
    public void testDropResourceNormalCase() throws Exception {
        // 1. Prepare test data - first create a resource
        String resourceName = "test_drop_hive_resource";
        String metastoreUris = "thrift://127.0.0.1:9083";
        CreateResourceStmt createStmt = createHiveResourceStmt(resourceName, metastoreUris);
        masterResourceMgr.createResource(createStmt);

        // Verify resource exists
        Assertions.assertTrue(masterResourceMgr.containsResource(resourceName));
        Assertions.assertEquals(1, masterResourceMgr.getResourceNum());

        // 2. Prepare DropResourceStmt
        DropResourceStmt dropStmt = new DropResourceStmt(resourceName);

        // 3. Execute dropResource operation (master side)
        masterResourceMgr.dropResource(dropStmt);

        // 4. Verify master state
        Assertions.assertFalse(masterResourceMgr.containsResource(resourceName));
        Assertions.assertEquals(0, masterResourceMgr.getResourceNum());
        
        // Verify catalog is dropped
        String masterCatalogName = CatalogMgr.ResourceMappingCatalog.getResourceMappingCatalogName(resourceName, "hive");
        Assertions.assertFalse(GlobalStateMgr.getCurrentState().getCatalogMgr().catalogExists(masterCatalogName));

        // 5. Test follower replay functionality
        ResourceMgr followerResourceMgr = new ResourceMgr();

        // First replay the create resource
        Resource replayCreateResource = (Resource) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_RESOURCE);
        
        // In test environment, master and follower share the same GlobalStateMgr,
        // so we need to drop the catalog first if it exists (created by master)
        dropCatalogIfExists(replayCreateResource);
        
        followerResourceMgr.replayCreateResource(replayCreateResource);

        // Then replay the drop resource
        com.starrocks.persist.DropResourceOperationLog dropLog = 
                (com.starrocks.persist.DropResourceOperationLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_DROP_RESOURCE);

        // Execute follower replay
        followerResourceMgr.replayDropResource(dropLog);

        // 6. Verify follower state is consistent with master
        Assertions.assertFalse(followerResourceMgr.containsResource(resourceName));
        Assertions.assertEquals(0, followerResourceMgr.getResourceNum());
        
        // Verify catalog is dropped in follower
        String followerCatalogName = CatalogMgr.ResourceMappingCatalog.getResourceMappingCatalogName(resourceName, "hive");
        Assertions.assertFalse(GlobalStateMgr.getCurrentState().getCatalogMgr().catalogExists(followerCatalogName));
    }

    @Test
    public void testDropResourceEditLogException() throws Exception {
        // 1. Prepare test data - first create a resource
        String resourceName = "exception_drop_hive_resource";
        String metastoreUris = "thrift://127.0.0.1:9083";
        CreateResourceStmt createStmt = createHiveResourceStmt(resourceName, metastoreUris);

        ResourceMgr exceptionResourceMgr = new ResourceMgr();
        exceptionResourceMgr.createResource(createStmt);

        // Verify resource exists
        Assertions.assertTrue(exceptionResourceMgr.containsResource(resourceName));
        Assertions.assertEquals(1, exceptionResourceMgr.getResourceNum());

        // 2. Prepare DropResourceStmt
        DropResourceStmt dropStmt = new DropResourceStmt(resourceName);

        // 3. Mock EditLog.logDropResource to throw exception
        EditLog currentEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(currentEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logDropResource(any(com.starrocks.persist.DropResourceOperationLog.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute dropResource operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionResourceMgr.dropResource(dropStmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertTrue(exceptionResourceMgr.containsResource(resourceName));
        Assertions.assertEquals(1, exceptionResourceMgr.getResourceNum());
    }

    @Test
    public void testAlterResourceHiveNormalCase() throws Exception {
        // 1. Prepare test data - first create a Hive resource
        String resourceName = "test_alter_hive_resource";
        String initialMetastoreUris = "thrift://127.0.0.1:9083";
        CreateResourceStmt createStmt = createHiveResourceStmt(resourceName, initialMetastoreUris);
        masterResourceMgr.createResource(createStmt);

        // Verify initial state
        Resource resource = masterResourceMgr.getResource(resourceName);
        Assertions.assertNotNull(resource);
        Assertions.assertTrue(resource instanceof HiveResource);
        HiveResource hiveResource = (HiveResource) resource;
        Assertions.assertEquals(initialMetastoreUris, hiveResource.getHiveMetastoreURIs());

        // 2. Prepare AlterResourceStmt
        String newMetastoreUris = "thrift://127.0.0.2:9083";
        Map<String, String> alterProperties = Maps.newHashMap();
        alterProperties.put("hive.metastore.uris", newMetastoreUris);
        AlterResourceStmt alterStmt = new AlterResourceStmt(resourceName, alterProperties);

        // 3. Execute alterResource operation (master side)
        masterResourceMgr.alterResource(alterStmt);

        // 4. Verify master state
        resource = masterResourceMgr.getResource(resourceName);
        Assertions.assertNotNull(resource);
        Assertions.assertTrue(resource instanceof HiveResource);
        hiveResource = (HiveResource) resource;
        Assertions.assertEquals(newMetastoreUris, hiveResource.getHiveMetastoreURIs());
        
        // Verify catalog exists and has updated properties
        String masterCatalogName = CatalogMgr.ResourceMappingCatalog.getResourceMappingCatalogName(resourceName, "hive");
        Catalog masterCatalog = GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogByName(masterCatalogName);
        Assertions.assertNotNull(masterCatalog);
        Assertions.assertEquals(newMetastoreUris, masterCatalog.getConfig().get("hive.metastore.uris"));

        // 5. Test follower replay functionality
        ResourceMgr followerResourceMgr = new ResourceMgr();

        // First replay the create resource
        Resource replayCreateResource = (Resource) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_RESOURCE);
        
        // In test environment, master and follower share the same GlobalStateMgr,
        // so we need to drop the catalog first if it exists (created by master)
        dropCatalogIfExists(replayCreateResource);
        
        followerResourceMgr.replayCreateResource(replayCreateResource);

        // Then replay the alter resource
        AlterResourceInfo alterLog = (AlterResourceInfo) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_RESOURCE);

        // Execute follower replay
        followerResourceMgr.replayAlterResource(alterLog);

        // 6. Verify follower state is consistent with master
        Resource followerResource = followerResourceMgr.getResource(resourceName);
        Assertions.assertNotNull(followerResource);
        Assertions.assertTrue(followerResource instanceof HiveResource);
        HiveResource followerHiveResource = (HiveResource) followerResource;
        Assertions.assertEquals(newMetastoreUris, followerHiveResource.getHiveMetastoreURIs());
        
        // Verify catalog exists in follower and has updated properties
        String followerCatalogName = CatalogMgr.ResourceMappingCatalog.getResourceMappingCatalogName(resourceName, "hive");
        Catalog followerCatalog = GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogByName(followerCatalogName);
        Assertions.assertNotNull(followerCatalog);
        Assertions.assertEquals(newMetastoreUris, followerCatalog.getConfig().get("hive.metastore.uris"));
    }

    @Test
    public void testAlterResourceHiveEditLogException() throws Exception {
        // 1. Prepare test data - first create a Hive resource
        String resourceName = "exception_alter_hive_resource";
        String initialMetastoreUris = "thrift://127.0.0.1:9083";
        CreateResourceStmt createStmt = createHiveResourceStmt(resourceName, initialMetastoreUris);

        ResourceMgr exceptionResourceMgr = new ResourceMgr();
        exceptionResourceMgr.createResource(createStmt);

        // Verify initial state
        Resource resource = exceptionResourceMgr.getResource(resourceName);
        Assertions.assertNotNull(resource);
        Assertions.assertTrue(resource instanceof HiveResource);
        HiveResource hiveResource = (HiveResource) resource;
        String initialUris = hiveResource.getHiveMetastoreURIs();

        // 2. Prepare AlterResourceStmt
        String newMetastoreUris = "thrift://127.0.0.2:9083";
        Map<String, String> alterProperties = Maps.newHashMap();
        alterProperties.put("hive.metastore.uris", newMetastoreUris);
        AlterResourceStmt alterStmt = new AlterResourceStmt(resourceName, alterProperties);

        // 3. Mock EditLog.logAlterResource to throw exception
        EditLog currentEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(currentEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logAlterResource(any(AlterResourceInfo.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute alterResource operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionResourceMgr.alterResource(alterStmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        resource = exceptionResourceMgr.getResource(resourceName);
        Assertions.assertNotNull(resource);
        Assertions.assertTrue(resource instanceof HiveResource);
        hiveResource = (HiveResource) resource;
        Assertions.assertEquals(initialUris, hiveResource.getHiveMetastoreURIs());
    }

    @Test
    public void testAlterResourceHudiNormalCase() throws Exception {
        // 1. Prepare test data - first create a Hudi resource
        String resourceName = "test_alter_hudi_resource";
        String initialMetastoreUris = "thrift://127.0.0.1:9083";
        CreateResourceStmt createStmt = createHudiResourceStmt(resourceName, initialMetastoreUris);
        masterResourceMgr.createResource(createStmt);

        // Verify initial state
        Resource resource = masterResourceMgr.getResource(resourceName);
        Assertions.assertNotNull(resource);
        Assertions.assertTrue(resource instanceof HudiResource);
        HudiResource hudiResource = (HudiResource) resource;
        Assertions.assertEquals(initialMetastoreUris, hudiResource.getHiveMetastoreURIs());

        // 2. Prepare AlterResourceStmt
        String newMetastoreUris = "thrift://127.0.0.2:9083";
        Map<String, String> alterProperties = Maps.newHashMap();
        alterProperties.put("hive.metastore.uris", newMetastoreUris);
        AlterResourceStmt alterStmt = new AlterResourceStmt(resourceName, alterProperties);

        // 3. Execute alterResource operation (master side)
        masterResourceMgr.alterResource(alterStmt);

        // 4. Verify master state
        resource = masterResourceMgr.getResource(resourceName);
        Assertions.assertNotNull(resource);
        Assertions.assertTrue(resource instanceof HudiResource);
        hudiResource = (HudiResource) resource;
        Assertions.assertEquals(newMetastoreUris, hudiResource.getHiveMetastoreURIs());
        
        // Verify catalog exists and has updated properties
        String catalogName = CatalogMgr.ResourceMappingCatalog.getResourceMappingCatalogName(resourceName, "hudi");
        Catalog catalog = GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogByName(catalogName);
        Assertions.assertNotNull(catalog);
        Assertions.assertEquals(newMetastoreUris, catalog.getConfig().get("hive.metastore.uris"));
    }

    @Test
    public void testAlterResourceHudiEditLogException() throws Exception {
        // 1. Prepare test data - first create a Hudi resource
        String resourceName = "exception_alter_hudi_resource";
        String initialMetastoreUris = "thrift://127.0.0.1:9083";
        CreateResourceStmt createStmt = createHudiResourceStmt(resourceName, initialMetastoreUris);

        ResourceMgr exceptionResourceMgr = new ResourceMgr();
        exceptionResourceMgr.createResource(createStmt);

        // Verify initial state
        Resource resource = exceptionResourceMgr.getResource(resourceName);
        Assertions.assertNotNull(resource);
        Assertions.assertTrue(resource instanceof HudiResource);
        HudiResource hudiResource = (HudiResource) resource;
        String initialUris = hudiResource.getHiveMetastoreURIs();

        // 2. Prepare AlterResourceStmt
        String newMetastoreUris = "thrift://127.0.0.2:9083";
        Map<String, String> alterProperties = Maps.newHashMap();
        alterProperties.put("hive.metastore.uris", newMetastoreUris);
        AlterResourceStmt alterStmt = new AlterResourceStmt(resourceName, alterProperties);

        // 3. Mock EditLog.logAlterResource to throw exception
        EditLog currentEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(currentEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logAlterResource(any(AlterResourceInfo.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute alterResource operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionResourceMgr.alterResource(alterStmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        resource = exceptionResourceMgr.getResource(resourceName);
        Assertions.assertNotNull(resource);
        Assertions.assertTrue(resource instanceof HudiResource);
        hudiResource = (HudiResource) resource;
        Assertions.assertEquals(initialUris, hudiResource.getHiveMetastoreURIs());
    }

    @Test
    public void testAlterResourceIcebergNormalCase() throws Exception {
        // 1. Prepare test data - first create an Iceberg resource
        String resourceName = "test_alter_iceberg_resource";
        String initialMetastoreUris = "thrift://127.0.0.1:9083";
        CreateResourceStmt createStmt = createIcebergResourceStmt(resourceName, initialMetastoreUris);
        masterResourceMgr.createResource(createStmt);

        // Verify initial state
        Resource resource = masterResourceMgr.getResource(resourceName);
        Assertions.assertNotNull(resource);
        Assertions.assertTrue(resource instanceof IcebergResource);
        IcebergResource icebergResource = (IcebergResource) resource;
        Assertions.assertEquals(initialMetastoreUris, icebergResource.getHiveMetastoreURIs());

        // 2. Prepare AlterResourceStmt - alter both metastore URIs and catalog-impl
        String newMetastoreUris = "thrift://127.0.0.2:9083";
        String newCatalogImpl = "com.starrocks.connector.iceberg.hive.IcebergHiveCatalog";
        Map<String, String> alterProperties = Maps.newHashMap();
        alterProperties.put("hive.metastore.uris", newMetastoreUris);
        alterProperties.put("iceberg.catalog-impl", newCatalogImpl);
        AlterResourceStmt alterStmt = new AlterResourceStmt(resourceName, alterProperties);

        // 3. Execute alterResource operation (master side)
        masterResourceMgr.alterResource(alterStmt);

        // 4. Verify master state
        resource = masterResourceMgr.getResource(resourceName);
        Assertions.assertNotNull(resource);
        Assertions.assertTrue(resource instanceof IcebergResource);
        icebergResource = (IcebergResource) resource;
        Assertions.assertEquals(newMetastoreUris, icebergResource.getHiveMetastoreURIs());
        Assertions.assertEquals(newCatalogImpl, icebergResource.getIcebergImpl());
        
        // Verify catalog exists and has updated properties
        String catalogName = CatalogMgr.ResourceMappingCatalog.getResourceMappingCatalogName(resourceName, "iceberg");
        Catalog catalog = GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogByName(catalogName);
        Assertions.assertNotNull(catalog);
        Assertions.assertEquals(newMetastoreUris, catalog.getConfig().get("hive.metastore.uris"));
        Assertions.assertEquals(newCatalogImpl, catalog.getConfig().get("iceberg.catalog-impl"));
    }

    @Test
    public void testAlterResourceIcebergEditLogException() throws Exception {
        // 1. Prepare test data - first create an Iceberg resource
        String resourceName = "exception_alter_iceberg_resource";
        String initialMetastoreUris = "thrift://127.0.0.1:9083";
        CreateResourceStmt createStmt = createIcebergResourceStmt(resourceName, initialMetastoreUris);

        ResourceMgr exceptionResourceMgr = new ResourceMgr();
        exceptionResourceMgr.createResource(createStmt);

        // Verify initial state
        Resource resource = exceptionResourceMgr.getResource(resourceName);
        Assertions.assertNotNull(resource);
        Assertions.assertTrue(resource instanceof IcebergResource);
        IcebergResource icebergResource = (IcebergResource) resource;
        String initialUris = icebergResource.getHiveMetastoreURIs();

        // 2. Prepare AlterResourceStmt
        String newMetastoreUris = "thrift://127.0.0.2:9083";
        Map<String, String> alterProperties = Maps.newHashMap();
        alterProperties.put("hive.metastore.uris", newMetastoreUris);
        AlterResourceStmt alterStmt = new AlterResourceStmt(resourceName, alterProperties);

        // 3. Mock EditLog.logAlterResource to throw exception
        EditLog currentEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(currentEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logAlterResource(any(AlterResourceInfo.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute alterResource operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionResourceMgr.alterResource(alterStmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        resource = exceptionResourceMgr.getResource(resourceName);
        Assertions.assertNotNull(resource);
        Assertions.assertTrue(resource instanceof IcebergResource);
        icebergResource = (IcebergResource) resource;
        Assertions.assertEquals(initialUris, icebergResource.getHiveMetastoreURIs());
    }
}
