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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.persist.AlterResourceGroupLog;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.ResourceGroupOpEntry;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.ResourceGroupAnalyzer;
import com.starrocks.sql.ast.AlterResourceGroupStmt;
import com.starrocks.sql.ast.CreateResourceGroupStmt;
import com.starrocks.sql.ast.DropResourceGroupStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class ResourceGroupMgrEditLogTest {
    private ResourceGroupMgr masterResourceGroupMgr;
    private ConnectContext ctx;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        // Create ConnectContext for SQL parsing
        ctx = UtFrameUtils.createDefaultCtx();

        // Create ResourceGroupMgr instance
        masterResourceGroupMgr = new ResourceGroupMgr();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    // Helper method to create a simple resource group statement
    private CreateResourceGroupStmt createResourceGroupStmt(String name, Map<String, String> properties) {
        // Create a simple classifier (empty list means no classifier, but we need at least one for normal type)
        // For MV type, we can have empty classifiers
        List<List<com.starrocks.sql.ast.expression.Predicate>> classifiers = Lists.newArrayList();
        CreateResourceGroupStmt stmt = new CreateResourceGroupStmt(name, false, false, classifiers, properties);
        ResourceGroupAnalyzer.analyzeCreateResourceGroupStmt(stmt);
        return stmt;
    }

    // Helper method to create a resource group with MV type (no classifiers needed)
    private CreateResourceGroupStmt createMVResourceGroupStmt(String name, Map<String, String> properties) {
        List<List<com.starrocks.sql.ast.expression.Predicate>> classifiers = Collections.emptyList();
        CreateResourceGroupStmt stmt = new CreateResourceGroupStmt(name, false, false, classifiers, properties);
        ResourceGroupAnalyzer.analyzeCreateResourceGroupStmt(stmt);
        return stmt;
    }

    @Test
    public void testCreateResourceGroupNormalCase() throws Exception {
        // 1. Prepare test data
        String resourceGroupName = "test_rg";
        Map<String, String> properties = Maps.newHashMap();
        properties.put("cpu_weight", "1");  // Use smaller value to avoid exceeding backend CPU cores
        properties.put("mem_limit", "50%");
        properties.put("type", "mv");
        CreateResourceGroupStmt stmt = createMVResourceGroupStmt(resourceGroupName, properties);

        // 2. Verify initial state
        Assertions.assertNull(masterResourceGroupMgr.getResourceGroup(resourceGroupName));
        Assertions.assertFalse(masterResourceGroupMgr.getAllResourceGroupNames().contains(resourceGroupName));

        // 3. Execute createResourceGroup operation (master side)
        masterResourceGroupMgr.createResourceGroup(stmt);

        // 4. Verify master state
        ResourceGroup resourceGroup = masterResourceGroupMgr.getResourceGroup(resourceGroupName);
        Assertions.assertNotNull(resourceGroup);
        Assertions.assertEquals(resourceGroupName, resourceGroup.getName());
        Assertions.assertTrue(masterResourceGroupMgr.getAllResourceGroupNames().contains(resourceGroupName));

        // 5. Test follower replay functionality
        ResourceGroupMgr followerResourceGroupMgr = new ResourceGroupMgr();

        // Verify follower initial state
        Assertions.assertNull(followerResourceGroupMgr.getResourceGroup(resourceGroupName));
        Assertions.assertFalse(followerResourceGroupMgr.getAllResourceGroupNames().contains(resourceGroupName));

        ResourceGroupOpEntry replayEntry = (ResourceGroupOpEntry) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_RESOURCE_GROUP);

        // Execute follower replay
        followerResourceGroupMgr.replayResourceGroupOp(replayEntry);

        // 6. Verify follower state is consistent with master
        ResourceGroup followerResourceGroup = followerResourceGroupMgr.getResourceGroup(resourceGroupName);
        Assertions.assertNotNull(followerResourceGroup);
        Assertions.assertEquals(resourceGroupName, followerResourceGroup.getName());
        Assertions.assertEquals(resourceGroup.getName(), followerResourceGroup.getName());
    }

    @Test
    public void testCreateResourceGroupEditLogException() throws Exception {
        // 1. Prepare test data
        String resourceGroupName = "exception_rg";
        Map<String, String> properties = Maps.newHashMap();
        properties.put("cpu_weight", "1");  // Use smaller value to avoid exceeding backend CPU cores
        properties.put("mem_limit", "50%");
        properties.put("type", "mv");
        CreateResourceGroupStmt stmt = createMVResourceGroupStmt(resourceGroupName, properties);

        // 2. Create a separate ResourceGroupMgr for exception testing
        ResourceGroupMgr exceptionResourceGroupMgr = new ResourceGroupMgr();
        // Use the existing EditLog from GlobalStateMgr (which has proper journalQueue)
        EditLog currentEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(currentEditLog);

        // 3. Mock EditLog.logResourceGroupOp to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logResourceGroupOp(any(ResourceGroupOpEntry.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Verify initial state
        Assertions.assertNull(exceptionResourceGroupMgr.getResourceGroup(resourceGroupName));

        // 4. Execute createResourceGroup operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionResourceGroupMgr.createResourceGroup(stmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertNull(exceptionResourceGroupMgr.getResourceGroup(resourceGroupName));
        Assertions.assertFalse(exceptionResourceGroupMgr.getAllResourceGroupNames().contains(resourceGroupName));
    }

    @Test
    public void testAlterResourceGroupAlterPropertiesNormalCase() throws Exception {
        // 1. Prepare test data - first create a resource group
        String resourceGroupName = "test_alter_properties_rg";
        Map<String, String> createProperties = Maps.newHashMap();
        createProperties.put("cpu_weight", "1");
        createProperties.put("mem_limit", "50%");
        createProperties.put("type", "mv");
        CreateResourceGroupStmt createStmt = createMVResourceGroupStmt(resourceGroupName, createProperties);
        masterResourceGroupMgr.createResourceGroup(createStmt);

        // Verify initial state
        ResourceGroup resourceGroup = masterResourceGroupMgr.getResourceGroup(resourceGroupName);
        Assertions.assertNotNull(resourceGroup);
        Integer initialCpuWeight = resourceGroup.geNormalizedCpuWeight();
        Double initialMemLimit = resourceGroup.getMemLimit();
        Integer initialConcurrencyLimit = resourceGroup.getConcurrencyLimit();
        Integer initialMaxCpuCores = resourceGroup.getMaxCpuCores();
        Long initialBigQueryMemLimit = resourceGroup.getBigQueryMemLimit();
        Long initialBigQueryScanRowsLimit = resourceGroup.getBigQueryScanRowsLimit();
        Long initialBigQueryCpuSecondLimit = resourceGroup.getBigQueryCpuSecondLimit();
        Double initialSpillMemLimitThreshold = resourceGroup.getSpillMemLimitThreshold();
        long initialVersion = resourceGroup.getVersion();

        // 2. Prepare AlterResourceGroupStmt with AlterProperties - test all modifiable properties
        Map<String, String> alterProperties = Maps.newHashMap();
        alterProperties.put("cpu_weight", "1");  // Keep at 1 (max allowed in test env)
        alterProperties.put("mem_limit", "60%");
        alterProperties.put("concurrency_limit", "5");
        alterProperties.put("max_cpu_cores", "1");  // Set to 1 (max allowed in test env)
        alterProperties.put("big_query_mem_limit", "1024");
        alterProperties.put("big_query_scan_rows_limit", "2048");
        alterProperties.put("big_query_cpu_second_limit", "100");
        alterProperties.put("spill_mem_limit_threshold", "0.3");
        AlterResourceGroupStmt.AlterProperties alterPropertiesCmd = 
                new AlterResourceGroupStmt.AlterProperties(alterProperties);
        AlterResourceGroupStmt alterStmt = new AlterResourceGroupStmt(resourceGroupName, alterPropertiesCmd);

        // 3. Execute alterResourceGroup operation (master side)
        masterResourceGroupMgr.alterResourceGroup(alterStmt);

        // 4. Verify master state - check all modified properties
        resourceGroup = masterResourceGroupMgr.getResourceGroup(resourceGroupName);
        Assertions.assertNotNull(resourceGroup);
        Assertions.assertEquals(1, resourceGroup.geNormalizedCpuWeight());
        Assertions.assertEquals(0.6, resourceGroup.getMemLimit(), 0.001);
        Assertions.assertEquals(5, resourceGroup.getConcurrencyLimit());
        Assertions.assertEquals(1, resourceGroup.getMaxCpuCores());
        Assertions.assertEquals(1024L, resourceGroup.getBigQueryMemLimit());
        Assertions.assertEquals(2048L, resourceGroup.getBigQueryScanRowsLimit());
        Assertions.assertEquals(100L, resourceGroup.getBigQueryCpuSecondLimit());
        Assertions.assertEquals(0.3, resourceGroup.getSpillMemLimitThreshold(), 0.001);
        // Verify version has changed
        Assertions.assertNotEquals(initialVersion, resourceGroup.getVersion());

        // 5. Test follower replay functionality
        ResourceGroupMgr followerResourceGroupMgr = new ResourceGroupMgr();

        // First replay the create resource group
        ResourceGroupOpEntry replayCreateEntry = (ResourceGroupOpEntry) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_RESOURCE_GROUP);
        followerResourceGroupMgr.replayResourceGroupOp(replayCreateEntry);

        // Then replay the alter resource group
        AlterResourceGroupLog alterLog = (AlterResourceGroupLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_RESOURCE_GROUP);

        // Execute follower replay
        followerResourceGroupMgr.replayAlterResourceGroup(alterLog);

        // 6. Verify follower state is consistent with master - check all properties
        ResourceGroup followerResourceGroup = followerResourceGroupMgr.getResourceGroup(resourceGroupName);
        Assertions.assertNotNull(followerResourceGroup);
        Assertions.assertEquals(resourceGroup.geNormalizedCpuWeight(), followerResourceGroup.geNormalizedCpuWeight());
        Assertions.assertEquals(resourceGroup.getMemLimit(), followerResourceGroup.getMemLimit());
        Assertions.assertEquals(resourceGroup.getConcurrencyLimit(), followerResourceGroup.getConcurrencyLimit());
        Assertions.assertEquals(resourceGroup.getMaxCpuCores(), followerResourceGroup.getMaxCpuCores());
        Assertions.assertEquals(resourceGroup.getBigQueryMemLimit(), followerResourceGroup.getBigQueryMemLimit());
        Assertions.assertEquals(resourceGroup.getBigQueryScanRowsLimit(), followerResourceGroup.getBigQueryScanRowsLimit());
        Assertions.assertEquals(resourceGroup.getBigQueryCpuSecondLimit(), followerResourceGroup.getBigQueryCpuSecondLimit());
        Assertions.assertEquals(resourceGroup.getSpillMemLimitThreshold(), followerResourceGroup.getSpillMemLimitThreshold());
        // Verify version is consistent between master and follower
        Assertions.assertEquals(resourceGroup.getVersion(), followerResourceGroup.getVersion());
    }

    @Test
    public void testAlterResourceGroupAlterPropertiesEditLogException() throws Exception {
        // 1. Prepare test data - first create a resource group
        String resourceGroupName = "exception_alter_properties_rg";
        Map<String, String> createProperties = Maps.newHashMap();
        createProperties.put("cpu_weight", "1");
        createProperties.put("mem_limit", "50%");
        createProperties.put("type", "mv");
        CreateResourceGroupStmt createStmt = createMVResourceGroupStmt(resourceGroupName, createProperties);

        ResourceGroupMgr exceptionResourceGroupMgr = new ResourceGroupMgr();
        exceptionResourceGroupMgr.createResourceGroup(createStmt);

        // Verify initial state
        ResourceGroup resourceGroup = exceptionResourceGroupMgr.getResourceGroup(resourceGroupName);
        Assertions.assertNotNull(resourceGroup);
        Integer initialConcurrencyLimit = resourceGroup.getConcurrencyLimit();

        // 2. Prepare AlterResourceGroupStmt with AlterProperties
        // Use concurrency_limit instead of cpu_weight to avoid backend CPU core limit issues
        Map<String, String> alterProperties = Maps.newHashMap();
        alterProperties.put("concurrency_limit", "5");
        AlterResourceGroupStmt.AlterProperties alterPropertiesCmd = 
                new AlterResourceGroupStmt.AlterProperties(alterProperties);
        AlterResourceGroupStmt alterStmt = new AlterResourceGroupStmt(resourceGroupName, alterPropertiesCmd);

        // 3. Mock EditLog.logAlterResourceGroup to throw exception
        EditLog currentEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(currentEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logAlterResourceGroup(any(AlterResourceGroupLog.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute alterResourceGroup operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionResourceGroupMgr.alterResourceGroup(alterStmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        resourceGroup = exceptionResourceGroupMgr.getResourceGroup(resourceGroupName);
        Assertions.assertNotNull(resourceGroup);
        Assertions.assertEquals(initialConcurrencyLimit, resourceGroup.getConcurrencyLimit());
    }

    @Test
    public void testAlterResourceGroupAddClassifiersNormalCase() throws Exception {
        // 1. Prepare test data - first create a resource group with classifiers
        String resourceGroupName = "test_alter_add_classifiers_rg";
        String createSql = "CREATE RESOURCE GROUP " + resourceGroupName + "\n" +
                "TO (user='test_user1')\n" +
                "WITH ('cpu_weight' = '1', 'mem_limit' = '50%', 'type' = 'normal')";
        CreateResourceGroupStmt createStmt = (CreateResourceGroupStmt) UtFrameUtils.parseStmtWithNewParser(createSql, ctx);
        masterResourceGroupMgr.createResourceGroup(createStmt);

        // Verify initial state
        ResourceGroup resourceGroup = masterResourceGroupMgr.getResourceGroup(resourceGroupName);
        Assertions.assertNotNull(resourceGroup);
        int initialClassifierCount = resourceGroup.getClassifiers().size();
        Assertions.assertEquals(1, initialClassifierCount);

        // 2. Prepare AlterResourceGroupStmt with AddClassifiers
        String alterSql = "ALTER RESOURCE GROUP " + resourceGroupName + "\n" +
                "ADD (user='test_user2', query_type in ('select'))";
        AlterResourceGroupStmt alterStmt = (AlterResourceGroupStmt) UtFrameUtils.parseStmtWithNewParser(alterSql, ctx);

        // 3. Execute alterResourceGroup operation (master side)
        masterResourceGroupMgr.alterResourceGroup(alterStmt);

        // 4. Verify master state
        resourceGroup = masterResourceGroupMgr.getResourceGroup(resourceGroupName);
        Assertions.assertNotNull(resourceGroup);
        Assertions.assertEquals(initialClassifierCount + 1, resourceGroup.getClassifiers().size());

        // 5. Test follower replay functionality
        ResourceGroupMgr followerResourceGroupMgr = new ResourceGroupMgr();

        // First replay the create resource group
        ResourceGroupOpEntry replayCreateEntry = (ResourceGroupOpEntry) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_RESOURCE_GROUP);
        followerResourceGroupMgr.replayResourceGroupOp(replayCreateEntry);

        // Then replay the alter resource group
        AlterResourceGroupLog alterLog = (AlterResourceGroupLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_RESOURCE_GROUP);

        // Execute follower replay
        followerResourceGroupMgr.replayAlterResourceGroup(alterLog);

        // 6. Verify follower state is consistent with master
        ResourceGroup followerResourceGroup = followerResourceGroupMgr.getResourceGroup(resourceGroupName);
        Assertions.assertNotNull(followerResourceGroup);
        Assertions.assertEquals(resourceGroup.getClassifiers().size(), followerResourceGroup.getClassifiers().size());
    }

    @Test
    public void testAlterResourceGroupAddClassifiersEditLogException() throws Exception {
        // 1. Prepare test data - first create a resource group with classifiers
        String resourceGroupName = "exception_alter_add_classifiers_rg";
        String createSql = "CREATE RESOURCE GROUP " + resourceGroupName + "\n" +
                "TO (user='test_user1')\n" +
                "WITH ('cpu_weight' = '1', 'mem_limit' = '50%', 'type' = 'normal')";
        CreateResourceGroupStmt createStmt = (CreateResourceGroupStmt) UtFrameUtils.parseStmtWithNewParser(createSql, ctx);

        ResourceGroupMgr exceptionResourceGroupMgr = new ResourceGroupMgr();
        exceptionResourceGroupMgr.createResourceGroup(createStmt);

        // Verify initial state
        ResourceGroup resourceGroup = exceptionResourceGroupMgr.getResourceGroup(resourceGroupName);
        Assertions.assertNotNull(resourceGroup);
        int initialClassifierCount = resourceGroup.getClassifiers().size();
        Assertions.assertEquals(1, initialClassifierCount);

        // 2. Prepare AlterResourceGroupStmt with AddClassifiers
        String alterSql = "ALTER RESOURCE GROUP " + resourceGroupName + "\n" +
                "ADD (user='test_user2', query_type in ('select'))";
        AlterResourceGroupStmt alterStmt = (AlterResourceGroupStmt) UtFrameUtils.parseStmtWithNewParser(alterSql, ctx);

        // 3. Mock EditLog.logAlterResourceGroup to throw exception
        EditLog currentEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(currentEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logAlterResourceGroup(any(AlterResourceGroupLog.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute alterResourceGroup operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionResourceGroupMgr.alterResourceGroup(alterStmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        resourceGroup = exceptionResourceGroupMgr.getResourceGroup(resourceGroupName);
        Assertions.assertNotNull(resourceGroup);
        Assertions.assertEquals(initialClassifierCount, resourceGroup.getClassifiers().size());
    }

    @Test
    public void testAlterResourceGroupDropClassifiersNormalCase() throws Exception {
        // 1. Prepare test data - first create a resource group with multiple classifiers
        String resourceGroupName = "test_alter_drop_classifiers_rg";
        String createSql = "CREATE RESOURCE GROUP " + resourceGroupName + "\n" +
                "TO (user='test_user1'), (user='test_user2'), (user='test_user3')\n" +
                "WITH ('cpu_weight' = '1', 'mem_limit' = '50%', 'type' = 'normal')";
        CreateResourceGroupStmt createStmt = (CreateResourceGroupStmt) UtFrameUtils.parseStmtWithNewParser(createSql, ctx);
        masterResourceGroupMgr.createResourceGroup(createStmt);

        // Verify initial state
        ResourceGroup resourceGroup = masterResourceGroupMgr.getResourceGroup(resourceGroupName);
        Assertions.assertNotNull(resourceGroup);
        int initialClassifierCount = resourceGroup.getClassifiers().size();
        Assertions.assertEquals(3, initialClassifierCount);

        // Get classifier IDs to drop (drop the second one)
        List<ResourceGroupClassifier> classifiers = resourceGroup.getClassifiers();
        long classifierIdToDrop = classifiers.get(1).getId();

        // 2. Prepare AlterResourceGroupStmt with DropClassifiers
        String alterSql = "ALTER RESOURCE GROUP " + resourceGroupName + "\n" +
                "DROP (" + classifierIdToDrop + ")";
        AlterResourceGroupStmt alterStmt = (AlterResourceGroupStmt) UtFrameUtils.parseStmtWithNewParser(alterSql, ctx);

        // 3. Execute alterResourceGroup operation (master side)
        masterResourceGroupMgr.alterResourceGroup(alterStmt);

        // 4. Verify master state
        resourceGroup = masterResourceGroupMgr.getResourceGroup(resourceGroupName);
        Assertions.assertNotNull(resourceGroup);
        Assertions.assertEquals(initialClassifierCount - 1, resourceGroup.getClassifiers().size());

        // 5. Test follower replay functionality
        ResourceGroupMgr followerResourceGroupMgr = new ResourceGroupMgr();

        // First replay the create resource group
        ResourceGroupOpEntry replayCreateEntry = (ResourceGroupOpEntry) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_RESOURCE_GROUP);
        followerResourceGroupMgr.replayResourceGroupOp(replayCreateEntry);

        // Then replay the alter resource group
        AlterResourceGroupLog alterLog = (AlterResourceGroupLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_RESOURCE_GROUP);

        // Execute follower replay
        followerResourceGroupMgr.replayAlterResourceGroup(alterLog);

        // 6. Verify follower state is consistent with master
        ResourceGroup followerResourceGroup = followerResourceGroupMgr.getResourceGroup(resourceGroupName);
        Assertions.assertNotNull(followerResourceGroup);
        Assertions.assertEquals(resourceGroup.getClassifiers().size(), followerResourceGroup.getClassifiers().size());
    }

    @Test
    public void testAlterResourceGroupDropClassifiersEditLogException() throws Exception {
        // 1. Prepare test data - first create a resource group with multiple classifiers
        String resourceGroupName = "exception_alter_drop_classifiers_rg";
        String createSql = "CREATE RESOURCE GROUP " + resourceGroupName + "\n" +
                "TO (user='test_user1'), (user='test_user2'), (user='test_user3')\n" +
                "WITH ('cpu_weight' = '1', 'mem_limit' = '50%', 'type' = 'normal')";
        CreateResourceGroupStmt createStmt = (CreateResourceGroupStmt) UtFrameUtils.parseStmtWithNewParser(createSql, ctx);

        ResourceGroupMgr exceptionResourceGroupMgr = new ResourceGroupMgr();
        exceptionResourceGroupMgr.createResourceGroup(createStmt);

        // Verify initial state
        ResourceGroup resourceGroup = exceptionResourceGroupMgr.getResourceGroup(resourceGroupName);
        Assertions.assertNotNull(resourceGroup);
        int initialClassifierCount = resourceGroup.getClassifiers().size();
        Assertions.assertEquals(3, initialClassifierCount);

        // Get classifier IDs to drop (drop the second one)
        List<ResourceGroupClassifier> classifiers = resourceGroup.getClassifiers();
        long classifierIdToDrop = classifiers.get(1).getId();

        // 2. Prepare AlterResourceGroupStmt with DropClassifiers
        String alterSql = "ALTER RESOURCE GROUP " + resourceGroupName + "\n" +
                "DROP (" + classifierIdToDrop + ")";
        AlterResourceGroupStmt alterStmt = (AlterResourceGroupStmt) UtFrameUtils.parseStmtWithNewParser(alterSql, ctx);

        // 3. Mock EditLog.logAlterResourceGroup to throw exception
        EditLog currentEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(currentEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logAlterResourceGroup(any(AlterResourceGroupLog.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute alterResourceGroup operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionResourceGroupMgr.alterResourceGroup(alterStmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        resourceGroup = exceptionResourceGroupMgr.getResourceGroup(resourceGroupName);
        Assertions.assertNotNull(resourceGroup);
        Assertions.assertEquals(initialClassifierCount, resourceGroup.getClassifiers().size());
    }

    @Test
    public void testAlterResourceGroupDropAllClassifiersNormalCase() throws Exception {
        // 1. Prepare test data - first create a resource group with multiple classifiers
        String resourceGroupName = "test_alter_drop_all_classifiers_rg";
        String createSql = "CREATE RESOURCE GROUP " + resourceGroupName + "\n" +
                "TO (user='test_user1'), (user='test_user2'), (user='test_user3')\n" +
                "WITH ('cpu_weight' = '1', 'mem_limit' = '50%', 'type' = 'normal')";
        CreateResourceGroupStmt createStmt = (CreateResourceGroupStmt) UtFrameUtils.parseStmtWithNewParser(createSql, ctx);
        masterResourceGroupMgr.createResourceGroup(createStmt);

        // Verify initial state
        ResourceGroup resourceGroup = masterResourceGroupMgr.getResourceGroup(resourceGroupName);
        Assertions.assertNotNull(resourceGroup);
        int initialClassifierCount = resourceGroup.getClassifiers().size();
        Assertions.assertEquals(3, initialClassifierCount);

        // 2. Prepare AlterResourceGroupStmt with DropAllClassifiers
        String alterSql = "ALTER RESOURCE GROUP " + resourceGroupName + "\n" +
                "DROP ALL";
        AlterResourceGroupStmt alterStmt = (AlterResourceGroupStmt) UtFrameUtils.parseStmtWithNewParser(alterSql, ctx);

        // 3. Execute alterResourceGroup operation (master side)
        masterResourceGroupMgr.alterResourceGroup(alterStmt);

        // 4. Verify master state
        resourceGroup = masterResourceGroupMgr.getResourceGroup(resourceGroupName);
        Assertions.assertNotNull(resourceGroup);
        Assertions.assertEquals(0, resourceGroup.getClassifiers().size());

        // 5. Test follower replay functionality
        ResourceGroupMgr followerResourceGroupMgr = new ResourceGroupMgr();

        // First replay the create resource group
        ResourceGroupOpEntry replayCreateEntry = (ResourceGroupOpEntry) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_RESOURCE_GROUP);
        followerResourceGroupMgr.replayResourceGroupOp(replayCreateEntry);

        // Then replay the alter resource group
        AlterResourceGroupLog alterLog = (AlterResourceGroupLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_ALTER_RESOURCE_GROUP);

        // Execute follower replay
        followerResourceGroupMgr.replayAlterResourceGroup(alterLog);

        // 6. Verify follower state is consistent with master
        ResourceGroup followerResourceGroup = followerResourceGroupMgr.getResourceGroup(resourceGroupName);
        Assertions.assertNotNull(followerResourceGroup);
        Assertions.assertEquals(resourceGroup.getClassifiers().size(), followerResourceGroup.getClassifiers().size());
        Assertions.assertEquals(0, followerResourceGroup.getClassifiers().size());
    }

    @Test
    public void testAlterResourceGroupDropAllClassifiersEditLogException() throws Exception {
        // 1. Prepare test data - first create a resource group with multiple classifiers
        String resourceGroupName = "exception_alter_drop_all_classifiers_rg";
        String createSql = "CREATE RESOURCE GROUP " + resourceGroupName + "\n" +
                "TO (user='test_user1'), (user='test_user2'), (user='test_user3')\n" +
                "WITH ('cpu_weight' = '1', 'mem_limit' = '50%', 'type' = 'normal')";
        CreateResourceGroupStmt createStmt = (CreateResourceGroupStmt) UtFrameUtils.parseStmtWithNewParser(createSql, ctx);

        ResourceGroupMgr exceptionResourceGroupMgr = new ResourceGroupMgr();
        exceptionResourceGroupMgr.createResourceGroup(createStmt);

        // Verify initial state
        ResourceGroup resourceGroup = exceptionResourceGroupMgr.getResourceGroup(resourceGroupName);
        Assertions.assertNotNull(resourceGroup);
        int initialClassifierCount = resourceGroup.getClassifiers().size();
        Assertions.assertEquals(3, initialClassifierCount);

        // 2. Prepare AlterResourceGroupStmt with DropAllClassifiers
        String alterSql = "ALTER RESOURCE GROUP " + resourceGroupName + "\n" +
                "DROP ALL";
        AlterResourceGroupStmt alterStmt = (AlterResourceGroupStmt) UtFrameUtils.parseStmtWithNewParser(alterSql, ctx);

        // 3. Mock EditLog.logAlterResourceGroup to throw exception
        EditLog currentEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(currentEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logAlterResourceGroup(any(AlterResourceGroupLog.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute alterResourceGroup operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionResourceGroupMgr.alterResourceGroup(alterStmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        resourceGroup = exceptionResourceGroupMgr.getResourceGroup(resourceGroupName);
        Assertions.assertNotNull(resourceGroup);
        Assertions.assertEquals(initialClassifierCount, resourceGroup.getClassifiers().size());
    }

    @Test
    public void testDropResourceGroupNormalCase() throws Exception {
        // 1. Prepare test data - first create a resource group
        String resourceGroupName = "test_drop_rg";
        Map<String, String> properties = Maps.newHashMap();
        properties.put("cpu_weight", "1");
        properties.put("mem_limit", "50%");
        properties.put("type", "mv");
        CreateResourceGroupStmt createStmt = createMVResourceGroupStmt(resourceGroupName, properties);
        masterResourceGroupMgr.createResourceGroup(createStmt);

        // Verify resource group exists
        Assertions.assertNotNull(masterResourceGroupMgr.getResourceGroup(resourceGroupName));
        Assertions.assertTrue(masterResourceGroupMgr.getAllResourceGroupNames().contains(resourceGroupName));

        // 2. Prepare DropResourceGroupStmt
        DropResourceGroupStmt dropStmt = new DropResourceGroupStmt(resourceGroupName, false);

        // 3. Execute dropResourceGroup operation (master side)
        masterResourceGroupMgr.dropResourceGroup(dropStmt);

        // 4. Verify master state
        Assertions.assertNull(masterResourceGroupMgr.getResourceGroup(resourceGroupName));
        Assertions.assertFalse(masterResourceGroupMgr.getAllResourceGroupNames().contains(resourceGroupName));

        // 5. Test follower replay functionality
        ResourceGroupMgr followerResourceGroupMgr = new ResourceGroupMgr();

        // First replay the create resource group
        ResourceGroupOpEntry replayCreateEntry = (ResourceGroupOpEntry) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_RESOURCE_GROUP);
        followerResourceGroupMgr.replayResourceGroupOp(replayCreateEntry);

        // Then replay the drop resource group
        ResourceGroupOpEntry replayDropEntry = (ResourceGroupOpEntry) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_RESOURCE_GROUP);

        // Execute follower replay
        followerResourceGroupMgr.replayResourceGroupOp(replayDropEntry);

        // 6. Verify follower state is consistent with master
        Assertions.assertNull(followerResourceGroupMgr.getResourceGroup(resourceGroupName));
        Assertions.assertFalse(followerResourceGroupMgr.getAllResourceGroupNames().contains(resourceGroupName));
    }

    @Test
    public void testDropResourceGroupEditLogException() throws Exception {
        // 1. Prepare test data - first create a resource group
        String resourceGroupName = "exception_drop_rg";
        Map<String, String> properties = Maps.newHashMap();
        properties.put("cpu_weight", "1");
        properties.put("mem_limit", "50%");
        properties.put("type", "mv");
        CreateResourceGroupStmt createStmt = createMVResourceGroupStmt(resourceGroupName, properties);

        ResourceGroupMgr exceptionResourceGroupMgr = new ResourceGroupMgr();
        exceptionResourceGroupMgr.createResourceGroup(createStmt);

        // Verify resource group exists
        Assertions.assertNotNull(exceptionResourceGroupMgr.getResourceGroup(resourceGroupName));
        Assertions.assertTrue(exceptionResourceGroupMgr.getAllResourceGroupNames().contains(resourceGroupName));

        // 2. Prepare DropResourceGroupStmt
        DropResourceGroupStmt dropStmt = new DropResourceGroupStmt(resourceGroupName, false);

        // 3. Mock EditLog.logResourceGroupOp to throw exception
        EditLog currentEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(currentEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logResourceGroupOp(any(ResourceGroupOpEntry.class), any());

        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Execute dropResourceGroup operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionResourceGroupMgr.dropResourceGroup(dropStmt);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertNotNull(exceptionResourceGroupMgr.getResourceGroup(resourceGroupName));
        Assertions.assertTrue(exceptionResourceGroupMgr.getAllResourceGroupNames().contains(resourceGroupName));
    }
}

