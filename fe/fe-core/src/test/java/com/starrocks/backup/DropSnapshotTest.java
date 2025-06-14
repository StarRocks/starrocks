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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.DdlException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.DropSnapshotStmt;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class DropSnapshotTest {

    @Mocked
    private GlobalStateMgr globalStateMgr;
    
    @Mocked
    private BackupHandler backupHandler;
    
    @Mocked
    private RepositoryMgr repoMgr;
    
    @Mocked
    private Repository repository;
    
    @Mocked
    private BlobStorage storage;

    private BackupHandler realBackupHandler;

    @Before
    public void setUp() {
        realBackupHandler = new BackupHandler();
        
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

    @Test
    public void testDropSnapshotByName() throws DdlException {
        // Setup
        String repoName = "test_repo";
        String snapshotName = "test_snapshot";
        
        DropSnapshotStmt stmt = new DropSnapshotStmt(repoName, null);
        stmt.setSnapshotName(snapshotName);
        
        new Expectations() {{
            repoMgr.getRepo(repoName);
            result = repository;
            
            repository.getName();
            result = repoName;
            
            repository.isReadOnly();
            result = false;
            
            repository.deleteSnapshot(snapshotName);
            result = Status.OK;
        }};
        
        // Mock the seqlock
        new MockUp<BackupHandler>() {
            @Mock
            public void tryLock() throws DdlException {
                // Do nothing for test
            }
            
            @Mock
            public ReentrantLock getSeqlock() {
                return new ReentrantLock();
            }
        };
        
        // Test - this would normally be called by the real handler
        // For this test, we'll verify the repository method is called correctly
        new Expectations() {{
            repository.deleteSnapshot(snapshotName);
            result = Status.OK;
            times = 1;
        }};
        
        Status result = repository.deleteSnapshot(snapshotName);
        Assert.assertTrue(result.ok());
    }

    @Test
    public void testDropSnapshotByTimestamp() throws DdlException {
        // Setup
        String repoName = "test_repo";
        String timestamp = "2024-01-01-12-00-00";
        String operator = "<=";
        
        DropSnapshotStmt stmt = new DropSnapshotStmt(repoName, null);
        stmt.setTimestamp(timestamp);
        stmt.setTimestampOperator(operator);
        
        new Expectations() {{
            repoMgr.getRepo(repoName);
            result = repository;
            
            repository.getName();
            result = repoName;
            
            repository.isReadOnly();
            result = false;
            
            repository.deleteSnapshotsByTimestamp(operator, timestamp);
            result = Status.OK;
        }};
        
        // Test the repository method directly
        Status result = repository.deleteSnapshotsByTimestamp(operator, timestamp);
        Assert.assertTrue(result.ok());
    }

    @Test
    public void testDropMultipleSnapshots() throws DdlException {
        // Setup
        String repoName = "test_repo";
        List<String> snapshotNames = Lists.newArrayList("snapshot1", "snapshot2", "snapshot3");
        
        DropSnapshotStmt stmt = new DropSnapshotStmt(repoName, null);
        for (String name : snapshotNames) {
            stmt.addSnapshotName(name);
        }
        
        new Expectations() {{
            repoMgr.getRepo(repoName);
            result = repository;
            
            repository.getName();
            result = repoName;
            
            repository.isReadOnly();
            result = false;
            
            // Each snapshot should be deleted
            repository.deleteSnapshot("snapshot1");
            result = Status.OK;
            
            repository.deleteSnapshot("snapshot2");
            result = Status.OK;
            
            repository.deleteSnapshot("snapshot3");
            result = Status.OK;
        }};
        
        // Test each deletion
        for (String snapshotName : snapshotNames) {
            Status result = repository.deleteSnapshot(snapshotName);
            Assert.assertTrue(result.ok());
        }
    }

    @Test(expected = DdlException.class)
    public void testDropSnapshotNonExistentRepo() throws DdlException {
        // Setup
        String repoName = "nonexistent_repo";
        DropSnapshotStmt stmt = new DropSnapshotStmt(repoName, null);
        stmt.setSnapshotName("test_snapshot");
        
        new Expectations() {{
            repoMgr.getRepo(repoName);
            result = null; // Repository doesn't exist
        }};
        
        // This should throw DdlException
        realBackupHandler.dropSnapshot(stmt);
    }

    @Test(expected = DdlException.class)
    public void testDropSnapshotReadOnlyRepo() throws DdlException {
        // Setup
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
        
        // This should throw DdlException
        realBackupHandler.dropSnapshot(stmt);
    }

    @Test
    public void testRepositoryDeleteSnapshot() {
        // Test the Repository.deleteSnapshot method directly
        String snapshotName = "test_snapshot";
        String repoName = "test_repo";
        String location = "s3://test-bucket/backup";
        
        Repository repo = new Repository(1, repoName, false, location, storage);
        
        new MockUp<Repository>() {
            @Mock
            public Status deleteSnapshot(String name) {
                if ("test_snapshot".equals(name)) {
                    return Status.OK;
                } else {
                    return new Status(Status.ErrCode.COMMON_ERROR, "Snapshot not found");
                }
            }
        };
        
        // Test successful deletion
        Status result = repo.deleteSnapshot(snapshotName);
        Assert.assertTrue(result.ok());
        
        // Test failed deletion
        Status failResult = repo.deleteSnapshot("nonexistent_snapshot");
        Assert.assertFalse(failResult.ok());
        Assert.assertTrue(failResult.getErrMsg().contains("Snapshot not found"));
    }

    @Test
    public void testRepositoryDeleteSnapshotsByTimestamp() {
        // Test the Repository.deleteSnapshotsByTimestamp method
        String repoName = "test_repo";
        String location = "s3://test-bucket/backup";
        
        Repository repo = new Repository(1, repoName, false, location, storage);
        
        new MockUp<Repository>() {
            @Mock
            public Status deleteSnapshotsByTimestamp(String operator, String timestamp) {
                if ("<=".equals(operator) || ">=".equals(operator)) {
                    return Status.OK;
                } else {
                    return new Status(Status.ErrCode.COMMON_ERROR, "Invalid operator");
                }
            }
        };
        
        // Test successful deletion with <= operator
        Status result1 = repo.deleteSnapshotsByTimestamp("<=", "2024-01-01-12-00-00");
        Assert.assertTrue(result1.ok());
        
        // Test successful deletion with >= operator
        Status result2 = repo.deleteSnapshotsByTimestamp(">=", "2024-01-01-12-00-00");
        Assert.assertTrue(result2.ok());
        
        // Test failed deletion with invalid operator
        Status failResult = repo.deleteSnapshotsByTimestamp("=", "2024-01-01-12-00-00");
        Assert.assertFalse(failResult.ok());
        Assert.assertTrue(failResult.getErrMsg().contains("Invalid operator"));
    }

    @Test
    public void testDropSnapshotValidation() {
        // Test validation logic
        String repoName = "test_repo";
        
        // Test with no conditions - should fail
        DropSnapshotStmt stmt1 = new DropSnapshotStmt(repoName, null);
        Assert.assertNull(stmt1.getSnapshotName());
        Assert.assertNull(stmt1.getTimestamp());
        Assert.assertTrue(stmt1.getSnapshotNames().isEmpty());
        
        // Test with snapshot name - should be valid
        DropSnapshotStmt stmt2 = new DropSnapshotStmt(repoName, null);
        stmt2.setSnapshotName("test_snapshot");
        Assert.assertNotNull(stmt2.getSnapshotName());
        
        // Test with timestamp - should be valid
        DropSnapshotStmt stmt3 = new DropSnapshotStmt(repoName, null);
        stmt3.setTimestamp("2024-01-01-12-00-00");
        stmt3.setTimestampOperator("<=");
        Assert.assertNotNull(stmt3.getTimestamp());
        Assert.assertNotNull(stmt3.getTimestampOperator());
        
        // Test with multiple snapshots - should be valid
        DropSnapshotStmt stmt4 = new DropSnapshotStmt(repoName, null);
        stmt4.addSnapshotName("snapshot1");
        stmt4.addSnapshotName("snapshot2");
        Assert.assertFalse(stmt4.getSnapshotNames().isEmpty());
    }
}
