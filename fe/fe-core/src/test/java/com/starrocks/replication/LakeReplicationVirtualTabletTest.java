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

package com.starrocks.replication;

import com.staros.client.StarClientException;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.staros.proto.ShardInfo;
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.StorageVolumeMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.storagevolume.StorageVolume;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

public class LakeReplicationVirtualTabletTest {

    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Mocked
    private StorageVolumeMgr storageVolumeMgr;

    @Mocked
    private StarOSAgent starOSAgent;

    private LakeReplicationJob job;

    @BeforeEach
    public void setUp() {
        job = new LakeReplicationJob();

        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return globalStateMgr;
            }

            @Mock
            public StorageVolumeMgr getStorageVolumeMgr() {
                return storageVolumeMgr;
            }

            @Mock
            public StarOSAgent getStarOSAgent() {
                return starOSAgent;
            }
        };
    }

    @AfterEach
    public void tearDown() {
        LakeReplicationJob.storageVolumeNameToShardIdMap.clear();
    }

    @Test
    public void testGetOrCreateVirtualTabletIdStorageVolumeNotExist() {
        String storageVolumeName = "test_sv";
        String srcServiceId = "test_service_id";

        new Expectations() {
            {
                storageVolumeMgr.getStorageVolumeByName(storageVolumeName);
                result = null;
            }
        };

        ExceptionChecker.expectThrowsWithMsg(RuntimeException.class,
                "Unknown src storage volume while creating virtual tablet: " + storageVolumeName,
                () -> job.getVirtualTabletId(storageVolumeName, srcServiceId));
    }

    @Test
    public void testGetOrCreateVirtualTabletId(@Mocked StorageVolume storageVolume) throws DdlException {
        String storageVolumeName = "test_sv";
        String srcServiceId = "test_service_id";

        new Expectations() {
            {
                storageVolumeMgr.getStorageVolumeByName(storageVolumeName);
                result = storageVolume;

                storageVolume.getUniqueId();
                result = -1;

                storageVolumeMgr.resetStorageVolumeUniqueId(storageVolumeName);
                result = new DdlException("reset storage volume unique mocked error");
            }
        };

        ExceptionChecker.expectThrowsWithMsg(RuntimeException.class,
                "Failed to reset storage volume unique id for " + storageVolumeName,
                () -> job.getVirtualTabletId(storageVolumeName, srcServiceId));
    }

    @Test
    public void testGetOrCreateVirtualTabletIdNormalWithShardInfoExisted(@Mocked StorageVolume storageVolume)
            throws DdlException, StarClientException {
        String storageVolumeName = "test_sv";
        String srcServiceId = "test_service_id";
        long expectedVirtualTabletId = 1000L;

        new Expectations() {
            {
                storageVolumeMgr.getStorageVolumeByName(storageVolumeName);
                result = storageVolume;

                storageVolume.getUniqueId();
                result = -1;

                storageVolumeMgr.resetStorageVolumeUniqueId(storageVolumeName);
                result = expectedVirtualTabletId;
            }
        };

        ShardInfo shardInfo = ShardInfo.newBuilder().setShardId(expectedVirtualTabletId).build();
        new Expectations() {
            {
                starOSAgent.getShardInfo(anyLong, StarOSAgent.DEFAULT_WORKER_GROUP_ID);
                result = shardInfo;
            }
        };

        ExceptionChecker.expectThrowsNoException(() -> {
            Assertions.assertEquals(expectedVirtualTabletId, job.getVirtualTabletId(storageVolumeName, srcServiceId));
        });
    }

    @Test
    public void testGetOrCreateVirtualTabletIdNormalWithNoShardInfoFound(@Mocked StorageVolume storageVolume)
            throws DdlException, StarClientException {
        String storageVolumeName = "test_sv";
        String srcServiceId = "test_service_id";
        long expectedVirtualTabletId = 1000L;

        new Expectations() {
            {
                storageVolumeMgr.getStorageVolumeByName(storageVolumeName);
                result = storageVolume;

                storageVolume.getUniqueId();
                result = -1;

                storageVolumeMgr.resetStorageVolumeUniqueId(storageVolumeName);
                result = expectedVirtualTabletId;
            }
        };

        long groupId = 1001L;
        FilePathInfo pathInfo = FilePathInfo.newBuilder().build();
        new Expectations() {
            {
                starOSAgent.getShardInfo(anyLong, StarOSAgent.DEFAULT_WORKER_GROUP_ID);
                result = null;

                starOSAgent.allocateFilePath(anyString, srcServiceId);
                result = pathInfo;

                starOSAgent.createShardGroupForVirtualTablet();
                result = groupId;

                starOSAgent.createShardWithVirtualTabletId(pathInfo, (FileCacheInfo) any, groupId, (HashMap) any,
                        expectedVirtualTabletId, WarehouseManager.DEFAULT_RESOURCE);
                result = null;
            }
        };

        ExceptionChecker.expectThrowsNoException(() -> {
            Assertions.assertEquals(expectedVirtualTabletId, job.getVirtualTabletId(storageVolumeName, srcServiceId));
        });
    }

    @Test
    public void testGetOrCreateVirtualTabletIdException1(@Mocked StorageVolume storageVolume)
            throws DdlException, StarClientException {
        String storageVolumeName = "test_sv";
        String srcServiceId = "test_service_id";
        long expectedVirtualTabletId = 1000L;

        new Expectations() {
            {
                storageVolumeMgr.getStorageVolumeByName(storageVolumeName);
                result = storageVolume;

                storageVolume.getUniqueId();
                result = -1;

                storageVolumeMgr.resetStorageVolumeUniqueId(storageVolumeName);
                result = expectedVirtualTabletId;
            }
        };

        // allocateFilePath throws exception
        new Expectations() {
            {
                starOSAgent.getShardInfo(anyLong, StarOSAgent.DEFAULT_WORKER_GROUP_ID);
                result = null;

                starOSAgent.allocateFilePath(anyString, srcServiceId);
                result = new DdlException("allocate file path mocked error");
            }
        };

        ExceptionChecker.expectThrowsWithMsg(RuntimeException.class,
                "Failed to create shard for storage volume: " + storageVolumeName
                        + ", error: allocate file path mocked error",
                () -> job.getVirtualTabletId(storageVolumeName, srcServiceId)
        );
    }

    @Test
    public void testGetOrCreateVirtualTabletIdException2(@Mocked StorageVolume storageVolume)
            throws DdlException, StarClientException {
        String storageVolumeName = "test_sv";
        String srcServiceId = "test_service_id";
        long expectedVirtualTabletId = 1000L;

        new Expectations() {
            {
                storageVolumeMgr.getStorageVolumeByName(storageVolumeName);
                result = storageVolume;

                storageVolume.getUniqueId();
                result = -1;

                storageVolumeMgr.resetStorageVolumeUniqueId(storageVolumeName);
                result = expectedVirtualTabletId;
            }
        };

        // createShardGroupForVirtualTablet throws exception
        FilePathInfo pathInfo = FilePathInfo.newBuilder().build();
        new Expectations() {
            {
                starOSAgent.getShardInfo(anyLong, StarOSAgent.DEFAULT_WORKER_GROUP_ID);
                result = null;

                starOSAgent.allocateFilePath(anyString, srcServiceId);
                result = pathInfo;

                starOSAgent.createShardGroupForVirtualTablet();
                result = new DdlException("create virtual tablet mocked error");
            }
        };

        ExceptionChecker.expectThrowsWithMsg(RuntimeException.class,
                "Failed to create shard for storage volume: " + storageVolumeName
                        + ", error: create virtual tablet mocked error",
                () -> job.getVirtualTabletId(storageVolumeName, srcServiceId)
        );
    }


}
