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

package com.starrocks.connector.iceberg;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class IcebergCatalogPropertiesTest {

    @Test
    public void testAutoMaintenanceDefaults() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(IcebergCatalogProperties.ICEBERG_CATALOG_TYPE, "hive");

        IcebergCatalogProperties catalogProperties = new IcebergCatalogProperties(properties);
        Assertions.assertFalse(catalogProperties.isEnableAutoMaintenance());
        Assertions.assertEquals(24, catalogProperties.getIcebergAutoCleanupIntervalHours());
        Assertions.assertEquals(3, catalogProperties.getIcebergAutoOptimizeIntervalHours());
    }

    @Test
    public void testAutoMaintenanceCustomValues() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(IcebergCatalogProperties.ICEBERG_CATALOG_TYPE, "hive");
        properties.put(IcebergCatalogProperties.ENABLE_ICEBERG_AUTO_MAINTENANCE, "true");
        properties.put(IcebergCatalogProperties.ICEBERG_AUTO_CLEANUP_INTERVAL_HOURS, "12");
        properties.put(IcebergCatalogProperties.ICEBERG_AUTO_OPTIMIZE_INTERVAL_HOURS, "1");

        IcebergCatalogProperties catalogProperties = new IcebergCatalogProperties(properties);
        Assertions.assertTrue(catalogProperties.isEnableAutoMaintenance());
        Assertions.assertEquals(12, catalogProperties.getIcebergAutoCleanupIntervalHours());
        Assertions.assertEquals(1, catalogProperties.getIcebergAutoOptimizeIntervalHours());
    }

    @Test
    public void testMaintenanceTablePropertiesNotSetWithoutAutoMaintenance() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(IcebergCatalogProperties.ICEBERG_CATALOG_TYPE, "hive");

        IcebergCatalogProperties catalogProperties = new IcebergCatalogProperties(properties);
        Assertions.assertTrue(catalogProperties.getMaxSnapshotAgeMs().isEmpty());
        Assertions.assertTrue(catalogProperties.getMinSnapshotsToKeep().isEmpty());
        Assertions.assertTrue(catalogProperties.getManifestTargetSizeBytes().isEmpty());
    }

    @Test
    public void testMaintenanceTablePropertiesDefaultsWhenAutoMaintenanceEnabled() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(IcebergCatalogProperties.ICEBERG_CATALOG_TYPE, "hive");
        properties.put(IcebergCatalogProperties.ENABLE_ICEBERG_AUTO_MAINTENANCE, "true");

        IcebergCatalogProperties catalogProperties = new IcebergCatalogProperties(properties);
        Assertions.assertEquals(5L * 24 * 60 * 60 * 1000,
                catalogProperties.getMaxSnapshotAgeMs().orElse(0L));
        Assertions.assertEquals(10, catalogProperties.getMinSnapshotsToKeep().orElse(0));
        Assertions.assertEquals(8L * 1024 * 1024,
                catalogProperties.getManifestTargetSizeBytes().orElse(0L));
    }

    @Test
    public void testMaintenanceTablePropertiesExplicitWithoutAutoMaintenance() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(IcebergCatalogProperties.ICEBERG_CATALOG_TYPE, "hive");
        properties.put(IcebergCatalogProperties.ICEBERG_MAX_SNAPSHOT_AGE_MS, "86400000");
        properties.put(IcebergCatalogProperties.ICEBERG_MIN_SNAPSHOTS_TO_KEEP, "5");
        properties.put(IcebergCatalogProperties.ICEBERG_MANIFEST_TARGET_SIZE_BYTES, "16777216");

        IcebergCatalogProperties catalogProperties = new IcebergCatalogProperties(properties);
        Assertions.assertFalse(catalogProperties.isEnableAutoMaintenance());
        Assertions.assertEquals(86400000L, catalogProperties.getMaxSnapshotAgeMs().orElse(0L));
        Assertions.assertEquals(5, catalogProperties.getMinSnapshotsToKeep().orElse(0));
        Assertions.assertEquals(16777216L, catalogProperties.getManifestTargetSizeBytes().orElse(0L));
    }

    @Test
    public void testMaintenanceTablePropertiesExplicitOverridesAutoMaintenanceDefaults() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(IcebergCatalogProperties.ICEBERG_CATALOG_TYPE, "hive");
        properties.put(IcebergCatalogProperties.ENABLE_ICEBERG_AUTO_MAINTENANCE, "true");
        properties.put(IcebergCatalogProperties.ICEBERG_MAX_SNAPSHOT_AGE_MS, "172800000");
        properties.put(IcebergCatalogProperties.ICEBERG_MIN_SNAPSHOTS_TO_KEEP, "20");
        properties.put(IcebergCatalogProperties.ICEBERG_MANIFEST_TARGET_SIZE_BYTES, "4194304");

        IcebergCatalogProperties catalogProperties = new IcebergCatalogProperties(properties);
        Assertions.assertTrue(catalogProperties.isEnableAutoMaintenance());
        Assertions.assertEquals(172800000L, catalogProperties.getMaxSnapshotAgeMs().orElse(0L));
        Assertions.assertEquals(20, catalogProperties.getMinSnapshotsToKeep().orElse(0));
        Assertions.assertEquals(4194304L, catalogProperties.getManifestTargetSizeBytes().orElse(0L));
    }
}

