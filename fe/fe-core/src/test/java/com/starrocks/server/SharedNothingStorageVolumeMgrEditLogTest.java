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

import com.starrocks.persist.DropStorageVolumeLog;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.storagevolume.StorageVolume;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_ENDPOINT;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_REGION;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class SharedNothingStorageVolumeMgrEditLogTest {
    private SharedNothingStorageVolumeMgr masterStorageVolumeMgr;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        // Get StorageVolumeMgr instance from GlobalStateMgr
        masterStorageVolumeMgr = (SharedNothingStorageVolumeMgr) GlobalStateMgr.getCurrentState().getStorageVolumeMgr();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    private Map<String, String> createTestParams() {
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "us-west-2");
        storageParams.put(AWS_S3_ENDPOINT, "https://s3.us-west-2.amazonaws.com");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        return storageParams;
    }

    @Test
    public void testCreateInternalNoLockNormalCase() throws Exception {
        // 1. Prepare test data
        String svName = "test_create_sv";
        String svType = "S3";
        List<String> locations = Arrays.asList("s3://test-bucket");
        Map<String, String> params = createTestParams();
        Optional<Boolean> enabled = Optional.of(true);
        String comment = "test storage volume";

        // 2. Verify initial state
        Assertions.assertFalse(masterStorageVolumeMgr.exists(svName));

        // 3. Execute createInternalNoLock operation (master side)
        String storageVolumeId = masterStorageVolumeMgr
                .createInternalNoLock(svName, svType, locations, params, enabled, comment);

        // 4. Verify master state
        Assertions.assertNotNull(storageVolumeId);
        Assertions.assertTrue(masterStorageVolumeMgr.exists(svName));
        StorageVolume sv = masterStorageVolumeMgr.getStorageVolumeByName(svName);
        Assertions.assertNotNull(sv);
        Assertions.assertEquals(svName, sv.getName());
        Assertions.assertEquals(svType, sv.getType());
        Assertions.assertEquals(comment, sv.getComment());
        Assertions.assertTrue(sv.getEnabled());

        // 5. Test follower replay functionality
        SharedNothingStorageVolumeMgr followerStorageVolumeMgr = new SharedNothingStorageVolumeMgr();

        StorageVolume replaySv = (StorageVolume) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_CREATE_STORAGE_VOLUME);

        // Execute follower replay
        followerStorageVolumeMgr.replayCreateStorageVolume(replaySv);

        // 6. Verify follower state is consistent with master
        Assertions.assertTrue(followerStorageVolumeMgr.exists(svName));
        StorageVolume followerSv = followerStorageVolumeMgr.getStorageVolumeByName(svName);
        Assertions.assertNotNull(followerSv);
        Assertions.assertEquals(sv.getId(), followerSv.getId());
        Assertions.assertEquals(svName, followerSv.getName());
    }

    @Test
    public void testCreateInternalNoLockEditLogException() throws Exception {
        // 1. Prepare test data
        String svName = "exception_create_sv";
        String svType = "S3";
        List<String> locations = Arrays.asList("s3://test-bucket");
        Map<String, String> params = createTestParams();
        Optional<Boolean> enabled = Optional.of(true);
        String comment = "test storage volume";

        // 2. Create a separate StorageVolumeMgr for exception testing
        SharedNothingStorageVolumeMgr exceptionStorageVolumeMgr =
                (SharedNothingStorageVolumeMgr) GlobalStateMgr.getCurrentState().getStorageVolumeMgr();

        // Clean up if exists
        if (exceptionStorageVolumeMgr.exists(svName)) {
            try {
                exceptionStorageVolumeMgr.removeStorageVolume(svName);
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }

        EditLog spyEditLog = spy(new EditLog(null));

        // 3. Mock EditLog.logCreateStorageVolume to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logCreateStorageVolume(any(StorageVolume.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Verify initial state
        Assertions.assertFalse(exceptionStorageVolumeMgr.exists(svName));

        // 4. Execute createInternalNoLock operation and expect exception
        Exception exception = Assertions.assertThrows(Exception.class, () -> {
            exceptionStorageVolumeMgr.createInternalNoLock(svName, svType, locations, params, enabled, comment);
        });
        // When using reflection, exceptions are wrapped in InvocationTargetException
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertFalse(exceptionStorageVolumeMgr.exists(svName));
        StorageVolume sv = exceptionStorageVolumeMgr.getStorageVolumeByName(svName);
        Assertions.assertNull(sv);
    }

    @Test
    public void testUpdateInternalNoLockNormalCase() throws Exception {
        // 1. Create a storage volume first
        String svName = "test_update_sv";
        String storageVolumeId = masterStorageVolumeMgr.createStorageVolume(
                svName, "S3", Arrays.asList("s3://test-bucket"),
                createTestParams(), Optional.of(true), "original comment");

        StorageVolume originalSv = masterStorageVolumeMgr.getStorageVolume(storageVolumeId);
        Assertions.assertNotNull(originalSv);

        // 2. Create updated storage volume
        StorageVolume updatedSv = new StorageVolume(originalSv);
        updatedSv.setComment("updated comment");
        Map<String, String> newParams = createTestParams();
        newParams.put(AWS_S3_REGION, "us-east-1");
        updatedSv.setCloudConfiguration(newParams);

        // 3. Execute updateInternalNoLock operation (master side)
        masterStorageVolumeMgr.updateInternalNoLock(updatedSv);

        // 4. Verify master state
        StorageVolume sv = masterStorageVolumeMgr.getStorageVolumeByName(svName);
        Assertions.assertNotNull(sv);
        Assertions.assertEquals("updated comment", sv.getComment());

        // 5. Test follower replay functionality
        SharedNothingStorageVolumeMgr followerStorageVolumeMgr = new SharedNothingStorageVolumeMgr();
        
        StorageVolume replaySv = (StorageVolume) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_STORAGE_VOLUME);
        
        // Create storage volume in follower with the same ID as in the replay log
        // This simulates the state before replay
        StorageVolume originalFollowerSv = new StorageVolume(
                replaySv.getId(), svName, "S3", 
                Arrays.asList("s3://test-bucket"), createTestParams(), true, "original comment");
        followerStorageVolumeMgr.replayCreateStorageVolume(originalFollowerSv);
        Assertions.assertTrue(followerStorageVolumeMgr.exists(svName));

        // Execute follower replay
        followerStorageVolumeMgr.replayUpdateStorageVolume(replaySv);

        // 6. Verify follower state is consistent with master
        StorageVolume followerSv = followerStorageVolumeMgr.getStorageVolumeByName(svName);
        Assertions.assertNotNull(followerSv);
        Assertions.assertEquals("updated comment", followerSv.getComment());
    }

    @Test
    public void testUpdateInternalNoLockEditLogException() throws Exception {
        // 1. Create a storage volume first
        String svName = "exception_update_sv";
        String storageVolumeId = masterStorageVolumeMgr.createStorageVolume(
                svName, "S3", Arrays.asList("s3://test-bucket"),
                createTestParams(), Optional.of(true), "original comment");

        StorageVolume originalSv = masterStorageVolumeMgr.getStorageVolume(storageVolumeId);
        StorageVolume updatedSv = new StorageVolume(originalSv);
        updatedSv.setComment("updated comment");

        // 2. Create a separate StorageVolumeMgr for exception testing
        SharedNothingStorageVolumeMgr exceptionStorageVolumeMgr =
                (SharedNothingStorageVolumeMgr) GlobalStateMgr.getCurrentState().getStorageVolumeMgr();
        
        // Ensure storage volume exists for exception testing
        if (!exceptionStorageVolumeMgr.exists(svName)) {
            exceptionStorageVolumeMgr.createStorageVolume(
                    svName, "S3", Arrays.asList("s3://test-bucket"),
                    createTestParams(), Optional.of(true), "original comment");
        }

        EditLog spyEditLog = spy(new EditLog(null));

        // 3. Mock EditLog.logUpdateStorageVolume to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logUpdateStorageVolume(any(StorageVolume.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Save initial state
        StorageVolume initialSv = exceptionStorageVolumeMgr.getStorageVolumeByName(svName);
        String initialComment = initialSv.getComment();

        // 4. Execute updateInternalNoLock operation and expect exception
        Exception exception = Assertions.assertThrows(Exception.class, () -> {
            exceptionStorageVolumeMgr.updateInternalNoLock(updatedSv);
        });
        // When using reflection, exceptions are wrapped in InvocationTargetException
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));

        // 5. Verify leader memory state remains unchanged after exception
        StorageVolume currentSv = exceptionStorageVolumeMgr.getStorageVolumeByName(svName);
        Assertions.assertEquals(initialComment, currentSv.getComment());
    }

    @Test
    public void testReplaceInternalNoLockNormalCase() throws Exception {
        // 1. Create a storage volume first
        String svName = "test_replace_sv";
        String storageVolumeId = masterStorageVolumeMgr.createStorageVolume(
                svName, "S3", Arrays.asList("s3://test-bucket"),
                createTestParams(), Optional.of(true), "original comment");

        StorageVolume originalSv = masterStorageVolumeMgr.getStorageVolume(storageVolumeId);

        // 2. Create replacement storage volume
        StorageVolume replacedSv = new StorageVolume(originalSv);
        replacedSv.setComment("replaced comment");
        replacedSv.setType("S3");
        List<String> newLocations = Arrays.asList("s3://new-bucket");
        replacedSv.setLocations(newLocations);

        // 3. Execute replaceInternalNoLock operation (master side)
        masterStorageVolumeMgr.updateInternalNoLock(replacedSv);

        // 4. Verify master state
        StorageVolume sv = masterStorageVolumeMgr.getStorageVolumeByName(svName);
        Assertions.assertNotNull(sv);
        Assertions.assertEquals("replaced comment", sv.getComment());
        Assertions.assertEquals(newLocations, sv.getLocations());

        // 5. Test follower replay functionality
        SharedNothingStorageVolumeMgr followerStorageVolumeMgr = new SharedNothingStorageVolumeMgr();
        
        StorageVolume replaySv = (StorageVolume) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_UPDATE_STORAGE_VOLUME);
        
        // Create storage volume in follower with the same ID as in the replay log
        // This simulates the state before replay
        StorageVolume originalFollowerSv = new StorageVolume(
                replaySv.getId(), svName, "S3", 
                Arrays.asList("s3://test-bucket"), createTestParams(), true, "original comment");
        followerStorageVolumeMgr.replayCreateStorageVolume(originalFollowerSv);
        Assertions.assertTrue(followerStorageVolumeMgr.exists(svName));

        // Execute follower replay
        followerStorageVolumeMgr.replayUpdateStorageVolume(replaySv);

        // 6. Verify follower state is consistent with master
        StorageVolume followerSv = followerStorageVolumeMgr.getStorageVolumeByName(svName);
        Assertions.assertNotNull(followerSv);
        Assertions.assertEquals("replaced comment", followerSv.getComment());
        Assertions.assertEquals(newLocations, followerSv.getLocations());
    }

    @Test
    public void testReplaceInternalNoLockEditLogException() throws Exception {
        // 1. Create a storage volume first
        String svName = "exception_replace_sv";
        String storageVolumeId = masterStorageVolumeMgr.createStorageVolume(
                svName, "S3", Arrays.asList("s3://test-bucket"),
                createTestParams(), Optional.of(true), "original comment");

        StorageVolume originalSv = masterStorageVolumeMgr.getStorageVolume(storageVolumeId);
        StorageVolume replacedSv = new StorageVolume(originalSv);
        replacedSv.setComment("replaced comment");

        // 2. Create a separate StorageVolumeMgr for exception testing
        SharedNothingStorageVolumeMgr exceptionStorageVolumeMgr =
                (SharedNothingStorageVolumeMgr) GlobalStateMgr.getCurrentState().getStorageVolumeMgr();
        
        // Ensure storage volume exists for exception testing
        if (!exceptionStorageVolumeMgr.exists(svName)) {
            exceptionStorageVolumeMgr.createStorageVolume(
                    svName, "S3", Arrays.asList("s3://test-bucket"),
                    createTestParams(), Optional.of(true), "original comment");
        }

        EditLog spyEditLog = spy(new EditLog(null));

        // 3. Mock EditLog.logUpdateStorageVolume to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logUpdateStorageVolume(any(StorageVolume.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Save initial state
        StorageVolume initialSv = exceptionStorageVolumeMgr.getStorageVolumeByName(svName);
        String initialComment = initialSv.getComment();

        // 4. Execute replaceInternalNoLock operation and expect exception
        Exception exception = Assertions.assertThrows(Exception.class, () -> {
            exceptionStorageVolumeMgr.replaceInternalNoLock(replacedSv);
        });
        // When using reflection, exceptions are wrapped in InvocationTargetException
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));

        // 5. Verify leader memory state remains unchanged after exception
        StorageVolume currentSv = exceptionStorageVolumeMgr.getStorageVolumeByName(svName);
        Assertions.assertEquals(initialComment, currentSv.getComment());
    }

    @Test
    public void testRemoveInternalNoLockNormalCase() throws Exception {
        // 1. Create a storage volume first
        String svName = "test_remove_sv";
        String storageVolumeId = masterStorageVolumeMgr.createStorageVolume(
                svName, "S3", Arrays.asList("s3://test-bucket"),
                createTestParams(), Optional.of(true), "test storage volume");

        // 2. Verify initial state
        Assertions.assertTrue(masterStorageVolumeMgr.exists(svName));
        StorageVolume sv = masterStorageVolumeMgr.getStorageVolumeByName(svName);
        Assertions.assertNotNull(sv);

        // 3. Execute removeInternalNoLock operation (master side)
        masterStorageVolumeMgr.removeInternalNoLock(sv);

        // 4. Verify master state
        Assertions.assertFalse(masterStorageVolumeMgr.exists(svName));
        StorageVolume removedSv = masterStorageVolumeMgr.getStorageVolumeByName(svName);
        Assertions.assertNull(removedSv);

        // 5. Test follower replay functionality
        SharedNothingStorageVolumeMgr followerStorageVolumeMgr = new SharedNothingStorageVolumeMgr();
        
        DropStorageVolumeLog replayLog = (DropStorageVolumeLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_DROP_STORAGE_VOLUME);
        
        // Create storage volume in follower with the same ID as in the log
        // This simulates the state before replay
        StorageVolume followerSv = new StorageVolume(
                replayLog.getId(), svName, "S3", 
                Arrays.asList("s3://test-bucket"), createTestParams(), true, "test storage volume");
        followerStorageVolumeMgr.replayCreateStorageVolume(followerSv);
        Assertions.assertTrue(followerStorageVolumeMgr.exists(svName));

        // Execute follower replay
        followerStorageVolumeMgr.replayDropStorageVolume(replayLog);

        // 6. Verify follower state is consistent with master
        Assertions.assertFalse(followerStorageVolumeMgr.exists(svName));
        StorageVolume followerRemovedSv = followerStorageVolumeMgr.getStorageVolumeByName(svName);
        Assertions.assertNull(followerRemovedSv);
    }

    @Test
    public void testRemoveInternalNoLockEditLogException() throws Exception {
        // 1. Create a storage volume first
        String svName = "exception_remove_sv";
        String storageVolumeId = masterStorageVolumeMgr.createStorageVolume(
                svName, "S3", Arrays.asList("s3://test-bucket"),
                createTestParams(), Optional.of(true), "test storage volume");

        StorageVolume sv = masterStorageVolumeMgr.getStorageVolumeByName(svName);

        // 2. Create a separate StorageVolumeMgr for exception testing
        SharedNothingStorageVolumeMgr exceptionStorageVolumeMgr =
                (SharedNothingStorageVolumeMgr) GlobalStateMgr.getCurrentState().getStorageVolumeMgr();
        
        // Ensure storage volume exists for exception testing
        if (!exceptionStorageVolumeMgr.exists(svName)) {
            exceptionStorageVolumeMgr.createStorageVolume(
                    svName, "S3", Arrays.asList("s3://test-bucket"),
                    createTestParams(), Optional.of(true), "test storage volume");
        }

        EditLog spyEditLog = spy(new EditLog(null));

        // 3. Mock EditLog.logDropStorageVolume to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logDropStorageVolume(any(DropStorageVolumeLog.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Verify initial state
        Assertions.assertTrue(exceptionStorageVolumeMgr.exists(svName));

        // 4. Execute removeInternalNoLock operation and expect exception
        Exception exception = Assertions.assertThrows(Exception.class, () -> {
            exceptionStorageVolumeMgr.removeInternalNoLock(sv);
        });
        // When using reflection, exceptions are wrapped in InvocationTargetException
        Assertions.assertTrue(exception.getMessage().contains("EditLog write failed"));

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertTrue(exceptionStorageVolumeMgr.exists(svName));
        StorageVolume currentSv = exceptionStorageVolumeMgr.getStorageVolumeByName(svName);
        Assertions.assertNotNull(currentSv);
    }
}

