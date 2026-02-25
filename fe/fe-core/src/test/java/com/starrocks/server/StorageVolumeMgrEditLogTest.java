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

import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.DdlException;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.SetDefaultStorageVolumeLog;
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

public class StorageVolumeMgrEditLogTest {
    private StorageVolumeMgr masterStorageVolumeMgr;

    @BeforeEach
    public void setUp() throws Exception {
        // Initialize test environment
        UtFrameUtils.setUpForPersistTest();

        // Get StorageVolumeMgr instance from GlobalStateMgr
        masterStorageVolumeMgr = GlobalStateMgr.getCurrentState().getStorageVolumeMgr();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    private StorageVolume createTestStorageVolume(String name) throws AlreadyExistsException, DdlException {
        // Check if storage volume already exists and remove it first
        if (masterStorageVolumeMgr.exists(name)) {
            try {
                masterStorageVolumeMgr.removeStorageVolume(name);
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
        List<String> locations = Arrays.asList("s3://test-bucket");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "us-west-2");
        storageParams.put(AWS_S3_ENDPOINT, "https://s3.us-west-2.amazonaws.com");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        String storageVolumeId = masterStorageVolumeMgr.createStorageVolume(
                name, "S3", locations, storageParams, Optional.of(true), "test storage volume");
        return masterStorageVolumeMgr.getStorageVolume(storageVolumeId);
    }

    @Test
    public void testSetDefaultStorageVolumeNormalCase() throws Exception {
        // 1. Prepare test data
        String svName1 = "test_sv_1";
        String svName2 = "test_sv_2";

        // Create storage volumes
        StorageVolume sv1 = createTestStorageVolume(svName1);
        StorageVolume sv2 = createTestStorageVolume(svName2);

        // 2. Verify initial state
        Assertions.assertTrue(masterStorageVolumeMgr.exists(svName1));
        Assertions.assertTrue(masterStorageVolumeMgr.exists(svName2));
        String initialDefaultId = masterStorageVolumeMgr.getDefaultStorageVolumeId();

        // 3. Execute setDefaultStorageVolume operation (master side)
        masterStorageVolumeMgr.setDefaultStorageVolume(svName1);

        // 4. Verify master state
        Assertions.assertEquals(sv1.getId(), masterStorageVolumeMgr.getDefaultStorageVolumeId());
        Assertions.assertNotEquals(initialDefaultId, masterStorageVolumeMgr.getDefaultStorageVolumeId());

        // 5. Test follower replay functionality
        StorageVolumeMgr followerStorageVolumeMgr = new SharedNothingStorageVolumeMgr();

        // Create same storage volumes in follower (using the follower manager)
        List<String> locations = Arrays.asList("s3://test-bucket");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "us-west-2");
        storageParams.put(AWS_S3_ENDPOINT, "https://s3.us-west-2.amazonaws.com");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        
        // Clean up if exists in follower
        if (followerStorageVolumeMgr.exists(svName1)) {
            try {
                followerStorageVolumeMgr.removeStorageVolume(svName1);
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
        if (followerStorageVolumeMgr.exists(svName2)) {
            try {
                followerStorageVolumeMgr.removeStorageVolume(svName2);
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
        
        followerStorageVolumeMgr.createStorageVolume(
                svName1, "S3", locations, storageParams, Optional.of(true), "test storage volume");
        followerStorageVolumeMgr.createStorageVolume(
                svName2, "S3", locations, storageParams, Optional.of(true), "test storage volume");

        SetDefaultStorageVolumeLog replayLog = (SetDefaultStorageVolumeLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_SET_DEFAULT_STORAGE_VOLUME);

        // Execute follower replay
        followerStorageVolumeMgr.replaySetDefaultStorageVolume(replayLog);

        // 6. Verify follower state is consistent with master
        Assertions.assertEquals(sv1.getId(), followerStorageVolumeMgr.getDefaultStorageVolumeId());

        // 7. Set another default and verify
        masterStorageVolumeMgr.setDefaultStorageVolume(svName2);
        Assertions.assertEquals(sv2.getId(), masterStorageVolumeMgr.getDefaultStorageVolumeId());

        SetDefaultStorageVolumeLog replayLog2 = (SetDefaultStorageVolumeLog) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_SET_DEFAULT_STORAGE_VOLUME);
        followerStorageVolumeMgr.replaySetDefaultStorageVolume(replayLog2);
        Assertions.assertEquals(sv2.getId(), followerStorageVolumeMgr.getDefaultStorageVolumeId());
    }

    @Test
    public void testSetDefaultStorageVolumeEditLogException() throws Exception {
        // 1. Prepare test data
        String svName = "exception_sv";
        StorageVolume sv = createTestStorageVolume(svName);

        // 2. Create a separate StorageVolumeMgr for exception testing
        StorageVolumeMgr exceptionStorageVolumeMgr = GlobalStateMgr.getCurrentState().getStorageVolumeMgr();

        EditLog spyEditLog = spy(new EditLog(null));

        // 3. Mock EditLog.logSetDefaultStorageVolume to throw exception
        doThrow(new RuntimeException("EditLog write failed"))
                .when(spyEditLog).logSetDefaultStorageVolume(any(SetDefaultStorageVolumeLog.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // Verify initial state
        String initialDefaultId = exceptionStorageVolumeMgr.getDefaultStorageVolumeId();

        // 4. Execute setDefaultStorageVolume operation and expect exception
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
            exceptionStorageVolumeMgr.setDefaultStorageVolume(svName);
        });
        Assertions.assertEquals("EditLog write failed", exception.getMessage());

        // 5. Verify leader memory state remains unchanged after exception
        Assertions.assertEquals(initialDefaultId, exceptionStorageVolumeMgr.getDefaultStorageVolumeId());
    }

    @Test
    public void testSetDefaultStorageVolumeNonExistent() throws Exception {
        // 1. Test setting default for non-existent storage volume
        String nonExistentSvName = "non_existent_sv";

        // 2. Verify initial state
        Assertions.assertFalse(masterStorageVolumeMgr.exists(nonExistentSvName));

        // 3. Execute setDefaultStorageVolume operation and expect IllegalStateException
        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class, () -> {
            masterStorageVolumeMgr.setDefaultStorageVolume(nonExistentSvName);
        });
        Assertions.assertTrue(exception.getMessage().contains("Storage volume '" + nonExistentSvName + "' does not exist"));
    }

    @Test
    public void testSetDefaultStorageVolumeDisabled() throws Exception {
        // 1. Create a disabled storage volume
        String svName = "disabled_sv";
        List<String> locations = Arrays.asList("s3://test-bucket");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "us-west-2");
        storageParams.put(AWS_S3_ENDPOINT, "https://s3.us-west-2.amazonaws.com");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        masterStorageVolumeMgr.createStorageVolume(
                svName, "S3", locations, storageParams, Optional.of(false), "disabled storage volume");

        // 2. Verify storage volume exists but is disabled
        StorageVolume sv = masterStorageVolumeMgr.getStorageVolumeByName(svName);
        Assertions.assertNotNull(sv);
        Assertions.assertFalse(sv.getEnabled());

        // 3. Execute setDefaultStorageVolume operation and expect IllegalStateException
        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class, () -> {
            masterStorageVolumeMgr.setDefaultStorageVolume(svName);
        });
        Assertions.assertTrue(exception.getMessage().contains("Storage volume '" + svName + "' is disabled"));
    }
}


