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

package com.starrocks.backup;

import com.starrocks.common.DdlException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.DropSnapshotStmt;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class specifically for BackupHandler.dropSnapshot method coverage
 * Separated from DropSnapshotTest to avoid JMockit conflicts
 */
public class BackupHandlerDropSnapshotTest {

    @Mocked
    private GlobalStateMgr globalStateMgr;
    
    @Injectable
    private BackupHandler backupHandler;
    
    @Mocked
    private RepositoryMgr repoMgr;
    
    @Mocked
    private Repository repository;

    @Before
    public void setUp() {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return globalStateMgr;
            }
        };

        new Expectations() {{
                globalStateMgr.getBackupHandler();
                result = backupHandler;
                
                backupHandler.getRepoMgr();
                result = repoMgr;
            }};
    }

    @Test(expected = DdlException.class)
    public void testDropSnapshotRepositoryNotFound() throws DdlException {
        // Test line 570: Repository does not exist
        String repoName = "nonexistent_repo";
        DropSnapshotStmt stmt = new DropSnapshotStmt(repoName, null);
        stmt.setSnapshotName("test_snapshot");
        
        new Expectations() {{
                repoMgr.getRepo(repoName);
                result = null; // Repository doesn't exist
            }};
        
        backupHandler.dropSnapshot(stmt);
    }

    @Test(expected = DdlException.class)
    public void testDropSnapshotReadOnlyRepository() throws DdlException {
        // Test lines 573-576: Repository is read-only
        String repoName = "readonly_repo";
        DropSnapshotStmt stmt = new DropSnapshotStmt(repoName, null);
        stmt.setSnapshotName("test_snapshot");
        
        new Expectations() {{
                repoMgr.getRepo(repoName);
                result = repository;
                
                repository.isReadOnly();
                result = true; // Repository is read-only
                
                repository.getName();
                result = repoName;
            }};
        
        backupHandler.dropSnapshot(stmt);
    }

    @Test
    public void testDropSnapshotSingleSnapshotSuccess() throws DdlException {
        // Test lines 583-590: Single snapshot deletion success
        String repoName = "test_repo";
        String snapshotName = "test_snapshot";
        DropSnapshotStmt stmt = new DropSnapshotStmt(repoName, null);
        stmt.setSnapshotName(snapshotName);
        
        new Expectations() {{
                repoMgr.getRepo(repoName);
                result = repository;
                
                repository.isReadOnly();
                result = false;
                
                repository.deleteSnapshot(snapshotName);
                result = Status.OK;
            }};
        
        // Mock the tryLock and unlock methods
        new MockUp<BackupHandler>() {
            @Mock
            public void tryLock() throws DdlException {
                // Do nothing for test
            }
        };
        
        backupHandler.dropSnapshot(stmt);
        // If no exception is thrown, the test passes
    }

    @Test(expected = DdlException.class)
    public void testDropSnapshotSingleSnapshotFailure() throws DdlException {
        // Test lines 585-589: Single snapshot deletion failure
        String repoName = "test_repo";
        String snapshotName = "test_snapshot";
        DropSnapshotStmt stmt = new DropSnapshotStmt(repoName, null);
        stmt.setSnapshotName(snapshotName);
        
        new Expectations() {{
                repoMgr.getRepo(repoName);
                result = repository;
                
                repository.isReadOnly();
                result = false;
                
                repository.deleteSnapshot(snapshotName);
                result = new Status(Status.ErrCode.COMMON_ERROR, "Snapshot not found");
            }};
        
        new MockUp<BackupHandler>() {
            @Mock
            public void tryLock() throws DdlException {
                // Do nothing for test
            }
        };
        
        backupHandler.dropSnapshot(stmt);
    }

    @Test
    public void testDropSnapshotMultipleSnapshotsAllSuccess() throws DdlException {
        // Test lines 591-606: Multiple snapshots deletion - all success
        String repoName = "test_repo";
        DropSnapshotStmt stmt = new DropSnapshotStmt(repoName, null);
        stmt.addSnapshotName("snapshot1");
        stmt.addSnapshotName("snapshot2");
        stmt.addSnapshotName("snapshot3");
        
        new Expectations() {{
                repoMgr.getRepo(repoName);
                result = repository;
                
                repository.isReadOnly();
                result = false;
                
                // All deletions succeed
                repository.deleteSnapshot("snapshot1");
                result = Status.OK;
                
                repository.deleteSnapshot("snapshot2");
                result = Status.OK;
                
                repository.deleteSnapshot("snapshot3");
                result = Status.OK;
            }};
        
        new MockUp<BackupHandler>() {
            @Mock
            public void tryLock() throws DdlException {
                // Do nothing for test
            }
        };
        
        backupHandler.dropSnapshot(stmt);
        // If no exception is thrown, the test passes
    }

    @Test
    public void testDropSnapshotMultipleSnapshotsPartialSuccess() throws DdlException {
        // Test lines 591-606: Multiple snapshots deletion - partial success
        String repoName = "test_repo";
        DropSnapshotStmt stmt = new DropSnapshotStmt(repoName, null);
        stmt.addSnapshotName("snapshot1");
        stmt.addSnapshotName("snapshot2");
        stmt.addSnapshotName("snapshot3");
        
        new Expectations() {{
                repoMgr.getRepo(repoName);
                result = repository;
                
                repository.isReadOnly();
                result = false;
                
                // Mixed success/failure
                repository.deleteSnapshot("snapshot1");
                result = Status.OK;
                
                repository.deleteSnapshot("snapshot2");
                result = new Status(Status.ErrCode.COMMON_ERROR, "Snapshot not found");
                
                repository.deleteSnapshot("snapshot3");
                result = Status.OK;
            }};
        
        new MockUp<BackupHandler>() {
            @Mock
            public void tryLock() throws DdlException {
                // Do nothing for test
            }
        };
        
        try {
            backupHandler.dropSnapshot(stmt);
            Assert.fail("Expected DdlException for partial failures");
        } catch (DdlException e) {
            // Test lines 608-611: Exception thrown when some deletions fail
            Assert.assertTrue(e.getMessage().contains("Failed to drop 1 out of 3 snapshots"));
        }
    }

    @Test(expected = DdlException.class)
    public void testDropSnapshotMultipleSnapshotsAllFail() throws DdlException {
        // Test lines 591-611: Multiple snapshots deletion - all fail
        String repoName = "test_repo";
        DropSnapshotStmt stmt = new DropSnapshotStmt(repoName, null);
        stmt.addSnapshotName("snapshot1");
        stmt.addSnapshotName("snapshot2");
        
        new Expectations() {{
                repoMgr.getRepo(repoName);
                result = repository;
                
                repository.isReadOnly();
                result = false;
                
                // All deletions fail
                repository.deleteSnapshot("snapshot1");
                result = new Status(Status.ErrCode.COMMON_ERROR, "Snapshot not found");
                
                repository.deleteSnapshot("snapshot2");
                result = new Status(Status.ErrCode.COMMON_ERROR, "Snapshot not found");
            }};
        
        new MockUp<BackupHandler>() {
            @Mock
            public void tryLock() throws DdlException {
                // Do nothing for test
            }
        };
        
        backupHandler.dropSnapshot(stmt);
    }

    @Test
    public void testDropSnapshotByTimestampSuccess() throws DdlException {
        // Test lines 612-620: Timestamp-based deletion success
        String repoName = "test_repo";
        String timestamp = "2024-01-01-12-00-00";
        String operator = "<=";
        DropSnapshotStmt stmt = new DropSnapshotStmt(repoName, null);
        stmt.setTimestamp(timestamp);
        stmt.setTimestampOperator(operator);
        
        new Expectations() {{
                repoMgr.getRepo(repoName);
                result = repository;
                
                repository.isReadOnly();
                result = false;
                
                repository.deleteSnapshotsByTimestamp(operator, timestamp);
                result = Status.OK;
            }};
        
        new MockUp<BackupHandler>() {
            @Mock
            public void tryLock() throws DdlException {
                // Do nothing for test
            }
        };
        
        backupHandler.dropSnapshot(stmt);
        // If no exception is thrown, the test passes
    }

    @Test(expected = DdlException.class)
    public void testDropSnapshotByTimestampFailure() throws DdlException {
        // Test lines 614-618: Timestamp-based deletion failure
        String repoName = "test_repo";
        String timestamp = "2024-01-01-12-00-00";
        String operator = "<=";
        DropSnapshotStmt stmt = new DropSnapshotStmt(repoName, null);
        stmt.setTimestamp(timestamp);
        stmt.setTimestampOperator(operator);
        
        new Expectations() {{
                repoMgr.getRepo(repoName);
                result = repository;
                
                repository.isReadOnly();
                result = false;
                
                repository.deleteSnapshotsByTimestamp(operator, timestamp);
                result = new Status(Status.ErrCode.COMMON_ERROR, "Invalid timestamp format");
            }};
        
        new MockUp<BackupHandler>() {
            @Mock
            public void tryLock() throws DdlException {
                // Do nothing for test
            }
        };
        
        backupHandler.dropSnapshot(stmt);
    }

    @Test(expected = DdlException.class)
    public void testDropSnapshotNoValidCriteria() throws DdlException {
        // Test lines 621-624: No valid criteria specified
        String repoName = "test_repo";
        DropSnapshotStmt stmt = new DropSnapshotStmt(repoName, null);
        // Don't set any criteria - no snapshot name, no timestamp, no snapshot names list
        
        new Expectations() {{
                repoMgr.getRepo(repoName);
                result = repository;
                
                repository.isReadOnly();
                result = false;
            }};
        
        new MockUp<BackupHandler>() {
            @Mock
            public void tryLock() throws DdlException {
                // Do nothing for test
            }
        };
        
        backupHandler.dropSnapshot(stmt);
    }
}
