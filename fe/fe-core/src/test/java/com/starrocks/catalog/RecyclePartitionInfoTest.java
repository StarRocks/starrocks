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

import com.google.common.collect.Range;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.starrocks.common.DdlException;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.lake.StorageInfo;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RecyclePartitionInfo#syncDataCacheInfoWithTable}.
 * Tests all branches of the protected method directly via a concrete subclass.
 */
public class RecyclePartitionInfoTest {

    /**
     * A concrete test-only subclass of RecyclePartitionInfo that exposes the
     * protected syncDataCacheInfoWithTable method for direct testing.
     */
    private static class TestRecyclePartitionInfo extends RecyclePartitionInfo {
        private final DataCacheInfo dataCacheInfo;

        TestRecyclePartitionInfo(long dbId, long tableId, Partition partition,
                                 DataProperty dataProperty, short replicationNum,
                                 DataCacheInfo dataCacheInfo) {
            super(dbId, tableId, partition, dataProperty, replicationNum);
            this.dataCacheInfo = dataCacheInfo;
        }

        @Override
        Range<PartitionKey> getRange() {
            return null;
        }

        @Override
        DataCacheInfo getDataCacheInfo() {
            return dataCacheInfo;
        }

        @Override
        void checkRecoverable(OlapTable table) throws DdlException {
        }

        @Override
        void recover(OlapTable table) {
        }

        // Expose the protected method for testing
        public void testSyncDataCacheInfoWithTable(OlapTable table, PartitionInfo partitionInfo, long partitionId) {
            syncDataCacheInfoWithTable(table, partitionInfo, partitionId);
        }
    }

    @Test
    public void testSyncSkipsNonCloudNativeTable(@Mocked OlapTable table, @Mocked RangePartitionInfo partitionInfo) {
        new Expectations() {
            {
                table.isCloudNativeTableOrMaterializedView();
                result = false;
                // Should NOT access tableProperty since it returns early
                table.getTableProperty();
                times = 0;
            }
        };

        TestRecyclePartitionInfo info = new TestRecyclePartitionInfo(
                1L, 2L, null, DataProperty.DEFAULT_DATA_PROPERTY, (short) 1, null);
        // Should return early without any error for non-cloud-native table
        info.testSyncDataCacheInfoWithTable(table, partitionInfo, 100L);
    }

    @Test
    public void testSyncSkipsWhenTablePropertyIsNull(@Mocked OlapTable table, @Mocked RangePartitionInfo partitionInfo) {
        new Expectations() {
            {
                table.isCloudNativeTableOrMaterializedView();
                result = true;
                table.getTableProperty();
                result = null;
                // Should NOT access partitionInfo since it returns early
                partitionInfo.getDataCacheInfo(anyLong);
                times = 0;
            }
        };

        TestRecyclePartitionInfo info = new TestRecyclePartitionInfo(
                1L, 2L, null, DataProperty.DEFAULT_DATA_PROPERTY, (short) 1, null);
        info.testSyncDataCacheInfoWithTable(table, partitionInfo, 100L);
    }

    @Test
    public void testSyncSkipsWhenStorageInfoIsNull(
            @Mocked OlapTable table, @Mocked RangePartitionInfo partitionInfo, @Mocked TableProperty tableProperty) {
        new Expectations() {
            {
                table.isCloudNativeTableOrMaterializedView();
                result = true;
                table.getTableProperty();
                result = tableProperty;
                tableProperty.getStorageInfo();
                result = null;
                // Should NOT access partitionInfo since it returns early
                partitionInfo.getDataCacheInfo(anyLong);
                times = 0;
            }
        };

        TestRecyclePartitionInfo info = new TestRecyclePartitionInfo(
                1L, 2L, null, DataProperty.DEFAULT_DATA_PROPERTY, (short) 1, null);
        info.testSyncDataCacheInfoWithTable(table, partitionInfo, 100L);
    }

    @Test
    public void testSyncCreatesNewDataCacheInfoWhenCurrentIsNull(
            @Mocked OlapTable table, @Mocked TableProperty tableProperty) {
        StorageInfo storageInfo = new StorageInfo(
                FilePathInfo.newBuilder().build(),
                FileCacheInfo.newBuilder().setEnableCache(true).build());

        // Use a real RangePartitionInfo-like approach: track what gets set
        RangePartitionInfo partitionInfo = new RangePartitionInfo();

        new Expectations() {
            {
                table.isCloudNativeTableOrMaterializedView();
                result = true;
                table.getTableProperty();
                result = tableProperty;
                tableProperty.getStorageInfo();
                result = storageInfo;
            }
        };

        long partitionId = 100L;
        // Ensure no DataCacheInfo exists initially for this partitionId
        Assertions.assertNull(partitionInfo.getDataCacheInfo(partitionId));

        TestRecyclePartitionInfo info = new TestRecyclePartitionInfo(
                1L, 2L, null, DataProperty.DEFAULT_DATA_PROPERTY, (short) 1, null);
        info.testSyncDataCacheInfoWithTable(table, partitionInfo, partitionId);

        // Verify that DataCacheInfo was created with table's enableCache=true and asyncWriteBack=false
        DataCacheInfo newDataCacheInfo = partitionInfo.getDataCacheInfo(partitionId);
        Assertions.assertNotNull(newDataCacheInfo, "DataCacheInfo should be created when current is null");
        Assertions.assertTrue(newDataCacheInfo.isEnabled(), "DataCacheInfo should inherit table's enableCache=true");
        Assertions.assertFalse(newDataCacheInfo.isAsyncWriteBack(),
                "asyncWriteBack should default to false when currentDataCacheInfo is null");
    }

    @Test
    public void testSyncUpdatesWhenDataCacheMismatch_EnableToDisable(
            @Mocked OlapTable table, @Mocked TableProperty tableProperty) {
        // Table has datacache disabled
        StorageInfo storageInfo = new StorageInfo(
                FilePathInfo.newBuilder().build(),
                FileCacheInfo.newBuilder().setEnableCache(false).build());

        RangePartitionInfo partitionInfo = new RangePartitionInfo();
        long partitionId = 200L;
        // Partition has datacache enabled (mismatch)
        partitionInfo.setDataCacheInfo(partitionId, new DataCacheInfo(true, true));

        new Expectations() {
            {
                table.isCloudNativeTableOrMaterializedView();
                result = true;
                table.getTableProperty();
                result = tableProperty;
                tableProperty.getStorageInfo();
                result = storageInfo;
            }
        };

        TestRecyclePartitionInfo info = new TestRecyclePartitionInfo(
                1L, 2L, null, DataProperty.DEFAULT_DATA_PROPERTY, (short) 1, null);
        info.testSyncDataCacheInfoWithTable(table, partitionInfo, partitionId);

        DataCacheInfo updatedDataCacheInfo = partitionInfo.getDataCacheInfo(partitionId);
        Assertions.assertNotNull(updatedDataCacheInfo);
        Assertions.assertFalse(updatedDataCacheInfo.isEnabled(),
                "DataCacheInfo should match table's datacache.enable=false");
        // asyncWriteBack should be preserved from the old value
        Assertions.assertTrue(updatedDataCacheInfo.isAsyncWriteBack(),
                "asyncWriteBack should be preserved from original DataCacheInfo");
    }

    @Test
    public void testSyncUpdatesWhenDataCacheMismatch_DisableToEnable(
            @Mocked OlapTable table, @Mocked TableProperty tableProperty) {
        // Table has datacache enabled
        StorageInfo storageInfo = new StorageInfo(
                FilePathInfo.newBuilder().build(),
                FileCacheInfo.newBuilder().setEnableCache(true).build());

        RangePartitionInfo partitionInfo = new RangePartitionInfo();
        long partitionId = 300L;
        // Partition has datacache disabled (mismatch)
        partitionInfo.setDataCacheInfo(partitionId, new DataCacheInfo(false, false));

        new Expectations() {
            {
                table.isCloudNativeTableOrMaterializedView();
                result = true;
                table.getTableProperty();
                result = tableProperty;
                tableProperty.getStorageInfo();
                result = storageInfo;
            }
        };

        TestRecyclePartitionInfo info = new TestRecyclePartitionInfo(
                1L, 2L, null, DataProperty.DEFAULT_DATA_PROPERTY, (short) 1, null);
        info.testSyncDataCacheInfoWithTable(table, partitionInfo, partitionId);

        DataCacheInfo updatedDataCacheInfo = partitionInfo.getDataCacheInfo(partitionId);
        Assertions.assertNotNull(updatedDataCacheInfo);
        Assertions.assertTrue(updatedDataCacheInfo.isEnabled(),
                "DataCacheInfo should match table's datacache.enable=true");
        Assertions.assertFalse(updatedDataCacheInfo.isAsyncWriteBack(),
                "asyncWriteBack should be preserved from original DataCacheInfo");
    }

    @Test
    public void testSyncNoOpWhenAlreadyInSync(
            @Mocked OlapTable table, @Mocked TableProperty tableProperty) {
        // Table has datacache enabled
        StorageInfo storageInfo = new StorageInfo(
                FilePathInfo.newBuilder().build(),
                FileCacheInfo.newBuilder().setEnableCache(true).build());

        RangePartitionInfo partitionInfo = new RangePartitionInfo();
        long partitionId = 400L;
        // Partition also has datacache enabled (in sync)
        DataCacheInfo originalDataCacheInfo = new DataCacheInfo(true, true);
        partitionInfo.setDataCacheInfo(partitionId, originalDataCacheInfo);

        new Expectations() {
            {
                table.isCloudNativeTableOrMaterializedView();
                result = true;
                table.getTableProperty();
                result = tableProperty;
                tableProperty.getStorageInfo();
                result = storageInfo;
            }
        };

        TestRecyclePartitionInfo info = new TestRecyclePartitionInfo(
                1L, 2L, null, DataProperty.DEFAULT_DATA_PROPERTY, (short) 1, null);
        info.testSyncDataCacheInfoWithTable(table, partitionInfo, partitionId);

        // DataCacheInfo should remain unchanged (same object or equivalent)
        DataCacheInfo resultDataCacheInfo = partitionInfo.getDataCacheInfo(partitionId);
        Assertions.assertNotNull(resultDataCacheInfo);
        Assertions.assertTrue(resultDataCacheInfo.isEnabled());
        // The object should be the same since no update was needed
        Assertions.assertSame(originalDataCacheInfo, resultDataCacheInfo,
                "DataCacheInfo should not be replaced when already in sync");
    }

    @Test
    public void testSyncNoOpWhenBothDisabled(
            @Mocked OlapTable table, @Mocked TableProperty tableProperty) {
        // Table has datacache disabled
        StorageInfo storageInfo = new StorageInfo(
                FilePathInfo.newBuilder().build(),
                FileCacheInfo.newBuilder().setEnableCache(false).build());

        RangePartitionInfo partitionInfo = new RangePartitionInfo();
        long partitionId = 500L;
        // Partition also has datacache disabled (in sync)
        DataCacheInfo originalDataCacheInfo = new DataCacheInfo(false, false);
        partitionInfo.setDataCacheInfo(partitionId, originalDataCacheInfo);

        new Expectations() {
            {
                table.isCloudNativeTableOrMaterializedView();
                result = true;
                table.getTableProperty();
                result = tableProperty;
                tableProperty.getStorageInfo();
                result = storageInfo;
            }
        };

        TestRecyclePartitionInfo info = new TestRecyclePartitionInfo(
                1L, 2L, null, DataProperty.DEFAULT_DATA_PROPERTY, (short) 1, null);
        info.testSyncDataCacheInfoWithTable(table, partitionInfo, partitionId);

        // DataCacheInfo should remain unchanged
        DataCacheInfo resultDataCacheInfo = partitionInfo.getDataCacheInfo(partitionId);
        Assertions.assertNotNull(resultDataCacheInfo);
        Assertions.assertFalse(resultDataCacheInfo.isEnabled());
        Assertions.assertSame(originalDataCacheInfo, resultDataCacheInfo,
                "DataCacheInfo should not be replaced when both disabled (in sync)");
    }

    @Test
    public void testSyncPreservesAsyncWriteBackTrue(
            @Mocked OlapTable table, @Mocked TableProperty tableProperty) {
        // Table has datacache enabled
        StorageInfo storageInfo = new StorageInfo(
                FilePathInfo.newBuilder().build(),
                FileCacheInfo.newBuilder().setEnableCache(true).build());

        RangePartitionInfo partitionInfo = new RangePartitionInfo();
        long partitionId = 600L;
        // Partition has datacache disabled but asyncWriteBack=true
        partitionInfo.setDataCacheInfo(partitionId, new DataCacheInfo(false, true));

        new Expectations() {
            {
                table.isCloudNativeTableOrMaterializedView();
                result = true;
                table.getTableProperty();
                result = tableProperty;
                tableProperty.getStorageInfo();
                result = storageInfo;
            }
        };

        TestRecyclePartitionInfo info = new TestRecyclePartitionInfo(
                1L, 2L, null, DataProperty.DEFAULT_DATA_PROPERTY, (short) 1, null);
        info.testSyncDataCacheInfoWithTable(table, partitionInfo, partitionId);

        DataCacheInfo updatedDataCacheInfo = partitionInfo.getDataCacheInfo(partitionId);
        Assertions.assertNotNull(updatedDataCacheInfo);
        Assertions.assertTrue(updatedDataCacheInfo.isEnabled(),
                "DataCacheInfo should match table's datacache.enable=true");
        Assertions.assertTrue(updatedDataCacheInfo.isAsyncWriteBack(),
                "asyncWriteBack=true should be preserved from original DataCacheInfo");
    }

    @Test
    public void testSyncPreservesAsyncWriteBackFalse(
            @Mocked OlapTable table, @Mocked TableProperty tableProperty) {
        // Table has datacache disabled
        StorageInfo storageInfo = new StorageInfo(
                FilePathInfo.newBuilder().build(),
                FileCacheInfo.newBuilder().setEnableCache(false).build());

        RangePartitionInfo partitionInfo = new RangePartitionInfo();
        long partitionId = 700L;
        // Partition has datacache enabled but asyncWriteBack=false
        partitionInfo.setDataCacheInfo(partitionId, new DataCacheInfo(true, false));

        new Expectations() {
            {
                table.isCloudNativeTableOrMaterializedView();
                result = true;
                table.getTableProperty();
                result = tableProperty;
                tableProperty.getStorageInfo();
                result = storageInfo;
            }
        };

        TestRecyclePartitionInfo info = new TestRecyclePartitionInfo(
                1L, 2L, null, DataProperty.DEFAULT_DATA_PROPERTY, (short) 1, null);
        info.testSyncDataCacheInfoWithTable(table, partitionInfo, partitionId);

        DataCacheInfo updatedDataCacheInfo = partitionInfo.getDataCacheInfo(partitionId);
        Assertions.assertNotNull(updatedDataCacheInfo);
        Assertions.assertFalse(updatedDataCacheInfo.isEnabled(),
                "DataCacheInfo should match table's datacache.enable=false");
        Assertions.assertFalse(updatedDataCacheInfo.isAsyncWriteBack(),
                "asyncWriteBack=false should be preserved from original DataCacheInfo");
    }
}
