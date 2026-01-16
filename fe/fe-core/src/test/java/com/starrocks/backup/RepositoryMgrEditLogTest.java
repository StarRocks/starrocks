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

import com.starrocks.backup.Status.ErrCode;
import com.starrocks.persist.DropRepositoryLog;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class RepositoryMgrEditLogTest {
    private RepositoryMgr repositoryMgr;
    private static final String REPO_NAME = "test_repo";
    private static final long REPO_ID = 30001L;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        repositoryMgr = new RepositoryMgr();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testAddAndInitRepoIfNotExistNormalCase() throws Exception {
        // 1. Create a mock repository
        BlobStorage storage = new BlobStorage("test_broker", null, false);
        Repository repo = spy(new Repository(REPO_ID, REPO_NAME, false, "test://location", storage));
        
        // Mock initRepository to return OK
        doReturn(Status.OK).when(repo).initRepository();

        // 2. Verify initial state - repository should not exist
        Assertions.assertNull(repositoryMgr.getRepo(REPO_NAME));
        Assertions.assertNull(repositoryMgr.getRepo(REPO_ID));

        // 3. Execute addAndInitRepoIfNotExist
        Status status = repositoryMgr.addAndInitRepoIfNotExist(repo);

        // 4. Verify master state
        Assertions.assertTrue(status.ok());
        Repository addedRepo = repositoryMgr.getRepo(REPO_NAME);
        Assertions.assertNotNull(addedRepo);
        Assertions.assertEquals(REPO_NAME, addedRepo.getName());
        Assertions.assertEquals(REPO_ID, addedRepo.getId());
        Assertions.assertEquals("test://location", addedRepo.getLocation());
        Assertions.assertFalse(addedRepo.isReadOnly());

        Repository addedRepoById = repositoryMgr.getRepo(REPO_ID);
        Assertions.assertNotNull(addedRepoById);
        Assertions.assertEquals(REPO_NAME, addedRepoById.getName());

        // 5. Test follower replay
        Repository replayRepo = (Repository) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_CREATE_REPOSITORY_V2);

        // Verify replay info
        Assertions.assertNotNull(replayRepo);
        Assertions.assertEquals(REPO_ID, replayRepo.getId());
        Assertions.assertEquals(REPO_NAME, replayRepo.getName());
        Assertions.assertEquals("test://location", replayRepo.getLocation());
        Assertions.assertFalse(replayRepo.isReadOnly());

        // Create follower repository manager and replay into it
        RepositoryMgr followerRepositoryMgr = new RepositoryMgr();
        followerRepositoryMgr.replayAddRepo(replayRepo);

        // 6. Verify follower state
        Repository followerRepo = followerRepositoryMgr.getRepo(REPO_NAME);
        Assertions.assertNotNull(followerRepo);
        Assertions.assertEquals(REPO_NAME, followerRepo.getName());
        Assertions.assertEquals(REPO_ID, followerRepo.getId());
        Assertions.assertEquals("test://location", followerRepo.getLocation());
        Assertions.assertFalse(followerRepo.isReadOnly());

        Repository followerRepoById = followerRepositoryMgr.getRepo(REPO_ID);
        Assertions.assertNotNull(followerRepoById);
        Assertions.assertEquals(REPO_NAME, followerRepoById.getName());
    }

    @Test
    public void testAddAndInitRepoIfNotExistEditLogException() throws Exception {
        // 1. Create a mock repository
        BlobStorage storage = new BlobStorage("test_broker", null, false);
        Repository repo = spy(new Repository(REPO_ID, REPO_NAME, false, "test://location", storage));
        
        // Mock initRepository to return OK
        doReturn(Status.OK).when(repo).initRepository();

        // 2. Mock EditLog.logCreateRepository to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logCreateRepository(any(Repository.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute addAndInitRepoIfNotExist and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            repositoryMgr.addAndInitRepoIfNotExist(repo);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed") ||
                            exception.getCause() != null &&
                            exception.getCause().getMessage().contains("EditLog write failed"));

        // 4. Verify repository is not added after exception
        Assertions.assertNull(repositoryMgr.getRepo(REPO_NAME));
        Assertions.assertNull(repositoryMgr.getRepo(REPO_ID));
    }

    @Test
    public void testRemoveRepoNormalCase() throws Exception {
        // 1. First add a repository
        BlobStorage storage = new BlobStorage("test_broker", null, false);
        Repository repo = spy(new Repository(REPO_ID, REPO_NAME, false, "test://location", storage));
        doReturn(Status.OK).when(repo).initRepository();
        
        Status addStatus = repositoryMgr.addAndInitRepoIfNotExist(repo);
        Assertions.assertTrue(addStatus.ok());
        Assertions.assertNotNull(repositoryMgr.getRepo(REPO_NAME));

        // 2. Execute removeRepo
        Status removeStatus = repositoryMgr.removeRepo(REPO_NAME);

        // 3. Verify master state
        Assertions.assertTrue(removeStatus.ok());
        Assertions.assertNull(repositoryMgr.getRepo(REPO_NAME));
        Assertions.assertNull(repositoryMgr.getRepo(REPO_ID));

        // 4. Test follower replay
        DropRepositoryLog replayLog = (DropRepositoryLog) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_DROP_REPOSITORY_V2);

        // Verify replay log
        Assertions.assertNotNull(replayLog);
        Assertions.assertEquals(REPO_NAME, replayLog.getRepositoryName());

        // Create follower repository manager with the same repository, then replay
        RepositoryMgr followerRepositoryMgr = new RepositoryMgr();
        Repository followerRepo = spy(new Repository(REPO_ID, REPO_NAME, false, "test://location", storage));
        followerRepositoryMgr.replayAddRepo(followerRepo);
        Assertions.assertNotNull(followerRepositoryMgr.getRepo(REPO_NAME));

        followerRepositoryMgr.replayRemoveRepo(REPO_NAME);

        // 5. Verify follower state
        Assertions.assertNull(followerRepositoryMgr.getRepo(REPO_NAME));
        Assertions.assertNull(followerRepositoryMgr.getRepo(REPO_ID));
    }

    @Test
    public void testRemoveRepoEditLogException() throws Exception {
        // 1. First add a repository
        BlobStorage storage = new BlobStorage("test_broker", null, false);
        Repository repo = spy(new Repository(REPO_ID, REPO_NAME, false, "test://location", storage));
        doReturn(Status.OK).when(repo).initRepository();
        
        Status addStatus = repositoryMgr.addAndInitRepoIfNotExist(repo);
        Assertions.assertTrue(addStatus.ok());
        Assertions.assertNotNull(repositoryMgr.getRepo(REPO_NAME));

        // 2. Mock EditLog.logDropRepository to throw exception
        EditLog spyEditLog = spy(new EditLog(null));
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logDropRepository(any(String.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 3. Execute removeRepo and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            repositoryMgr.removeRepo(REPO_NAME);
        });
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed") ||
                            exception.getCause() != null &&
                            exception.getCause().getMessage().contains("EditLog write failed"));

        // 4. Verify repository is still present after exception
        Assertions.assertNotNull(repositoryMgr.getRepo(REPO_NAME));
        Assertions.assertNotNull(repositoryMgr.getRepo(REPO_ID));
    }

    @Test
    public void testRemoveRepoNotFound() throws Exception {
        // 1. Try to remove non-existent repository
        Status status = repositoryMgr.removeRepo("non_existent_repo");

        // 2. Verify status
        Assertions.assertFalse(status.ok());
        Assertions.assertEquals(ErrCode.NOT_FOUND, status.getErrCode());
        Assertions.assertTrue(status.getErrMsg().contains("repository does not exist"));
    }
}

