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

package com.starrocks.common.proc;

import com.starrocks.common.AnalysisException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.StorageVolumeMgr;
import com.starrocks.storagevolume.StorageVolume;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class StorageVolumeProcNodeTest {

    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Mocked
    private StorageVolumeMgr storageVolumeMgr;

    @Mocked
    private StorageVolume storageVolume;

    private static final String VOLUME_NAME = "test_volume";

    @Before
    public void setUp() {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return globalStateMgr;
            }

            @Mock
            public StorageVolumeMgr getStorageVolumeMgr() {
                return storageVolumeMgr;
            }
        };
    }

    @Test
    public void testFetchResultWithExistingVolume() throws AnalysisException {
        new MockUp<StorageVolumeMgr>() {
            @Mock
            public StorageVolume getStorageVolumeByName(String name) {
                if (VOLUME_NAME.equals(name)) {
                    return storageVolume;
                }
                return null;
            }
        };

        new MockUp<StorageVolume>() {
            @Mock
            public void getProcNodeData(BaseProcResult result) {
                result.addRow(List.of(VOLUME_NAME, "s3", "false", "s3://bucket/path",
                        "{\"max_connections\":20}", "true", "Test storage volume"));
            }
        };

        StorageVolumeProcNode node = new StorageVolumeProcNode(VOLUME_NAME);
        ProcResult result = node.fetchResult();

        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof BaseProcResult);

        Assert.assertEquals(StorageVolumeProcNode.STORAGE_VOLUME_PROC_NODE_TITLE_NAMES, result.getColumnNames());

        List<List<String>> rows = result.getRows();
        Assert.assertEquals(1, rows.size());

        List<String> row = rows.get(0);
        Assert.assertEquals(7, row.size());
        Assert.assertEquals(VOLUME_NAME, row.get(0));
        Assert.assertEquals("s3", row.get(1));
        Assert.assertEquals("false", row.get(2));
        Assert.assertEquals("s3://bucket/path", row.get(3));
        Assert.assertEquals("{\"max_connections\":20}", row.get(4));
        Assert.assertEquals("true", row.get(5));
        Assert.assertEquals("Test storage volume", row.get(6));
    }

    @Test
    public void testFetchResultWithNonExistentVolume() throws AnalysisException {
        new MockUp<StorageVolumeMgr>() {
            @Mock
            public StorageVolume getStorageVolumeByName(String name) {
                return null;
            }
        };

        StorageVolumeProcNode node = new StorageVolumeProcNode("non_existent_volume");
        ProcResult result = node.fetchResult();

        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof BaseProcResult);

        Assert.assertEquals(StorageVolumeProcNode.STORAGE_VOLUME_PROC_NODE_TITLE_NAMES, result.getColumnNames());

        List<List<String>> rows = result.getRows();
        Assert.assertEquals(0, rows.size());
    }
}
