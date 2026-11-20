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

import com.starrocks.connector.PlanMode;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;

import static com.starrocks.connector.iceberg.IcebergMetadataTest.DEFAULT_CATALOG_PROPERTIES;
import static com.starrocks.connector.iceberg.IcebergMetadataTest.DEFAULT_CONFIG;

public class IcebergDeleteFileIndexCacheTest extends TableTestBase {
    private static final String CATALOG_NAME = "iceberg_catalog";
    private static final String DB_NAME = "db";
    private static final String TABLE_NAME = "ta";

    private static final DeleteFile FILE_A_PATH_DELETES = pathDeletes("/path/to/data-a-path-deletes.parquet",
            FILE_A.location(), "data_bucket=0");
    private static final DeleteFile FILE_A_2_PATH_DELETES = pathDeletes("/path/to/data-a2-path-deletes.parquet",
            FILE_A_2.location(), "data_bucket=1");

    private CachingIcebergCatalog catalog;

    private static DeleteFile pathDeletes(String path, String dataFile, String partitionPath) {
        return FileMetadata.deleteFileBuilder(SPEC_A)
                .ofPositionDeletes()
                .withPath(path)
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(10)
                .withPartitionPath(partitionPath)
                .withRecordCount(1)
                .withReferencedDataFile(dataFile)
                .build();
    }

    @BeforeEach
    public void setUpCatalog() {
        IcebergHiveCatalog hiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        catalog = new CachingIcebergCatalog(CATALOG_NAME, hiveCatalog, DEFAULT_CATALOG_PROPERTIES,
                Executors.newSingleThreadExecutor());
    }

    // data file location -> sorted delete file locations
    private Map<String, List<String>> planDeletes(Table table, Expression filter) throws IOException {
        StarRocksIcebergTableScanContext scanContext =
                new StarRocksIcebergTableScanContext(CATALOG_NAME, DB_NAME, TABLE_NAME, PlanMode.LOCAL);
        TableScan scan = catalog.getTableScan(table, scanContext)
                .useSnapshot(table.currentSnapshot().snapshotId())
                .filter(filter);
        Map<String, List<String>> deletesByDataFile = new TreeMap<>();
        try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
            for (FileScanTask task : tasks) {
                deletesByDataFile.put(task.file().location(),
                        task.deletes().stream().map(ContentFile::location).sorted().toList());
            }
        }
        return deletesByDataFile;
    }

    private long cachedIndexes() {
        return catalog.getDeleteFileIndexCache().estimatedSize();
    }

    @Test
    public void testPositionDeleteIndexIsSharedAcrossScans() throws IOException {
        mockedNativeTableA.newAppend().appendFile(FILE_A).appendFile(FILE_A_2).commit();
        mockedNativeTableA.newRowDelta().addDeletes(FILE_A_PATH_DELETES).addDeletes(FILE_A_DELETES).commit();
        mockedNativeTableA.newRowDelta().addDeletes(FILE_A_2_PATH_DELETES).commit();

        Map<String, List<String>> expected = Map.of(
                FILE_A.location(), List.of(FILE_A_PATH_DELETES.location(), FILE_A_DELETES.location()).stream()
                        .sorted().toList(),
                FILE_A_2.location(), List.of(FILE_A_2_PATH_DELETES.location()));

        // The first scan reads the delete manifests and fills the delete file cache; the index is built from
        // complete cache entries only, so it is cached by the second scan and reused from then on.
        Assertions.assertEquals(expected, planDeletes(mockedNativeTableA, Expressions.alwaysTrue()));
        Assertions.assertEquals(0, cachedIndexes());
        Assertions.assertEquals(expected, planDeletes(mockedNativeTableA, Expressions.alwaysTrue()));
        Assertions.assertEquals(1, cachedIndexes());
        Assertions.assertEquals(expected, planDeletes(mockedNativeTableA, Expressions.alwaysTrue()));

        // A filter that keeps the same delete manifests hits the filter-free index and still gets exact deletes.
        Assertions.assertEquals(expected, planDeletes(mockedNativeTableA, Expressions.greaterThan("id", 0)));
        Assertions.assertEquals(1, cachedIndexes());

        // A filter that prunes a partition keeps exact deletes for the data file it keeps.
        Map<String, List<String>> bucket0 = planDeletes(mockedNativeTableA, Expressions.equal("data", "a"));
        bucket0.forEach((dataFile, deletes) -> Assertions.assertEquals(expected.get(dataFile), deletes));

        catalog.invalidateCache(DB_NAME, TABLE_NAME);
        Assertions.assertEquals(0, cachedIndexes());
    }

    @Test
    public void testNewDeleteManifestBuildsNewIndex() throws IOException {
        mockedNativeTableA.newAppend().appendFile(FILE_A).appendFile(FILE_A_2).commit();
        mockedNativeTableA.newRowDelta().addDeletes(FILE_A_PATH_DELETES).commit();
        planDeletes(mockedNativeTableA, Expressions.alwaysTrue());
        planDeletes(mockedNativeTableA, Expressions.alwaysTrue());
        Assertions.assertEquals(1, cachedIndexes());

        // A new snapshot with another delete manifest must not see the old index.
        mockedNativeTableA.newRowDelta().addDeletes(FILE_A_2_PATH_DELETES).commit();
        Map<String, List<String>> expected = Map.of(
                FILE_A.location(), List.of(FILE_A_PATH_DELETES.location()),
                FILE_A_2.location(), List.of(FILE_A_2_PATH_DELETES.location()));
        Assertions.assertEquals(expected, planDeletes(mockedNativeTableA, Expressions.alwaysTrue()));
        Assertions.assertEquals(expected, planDeletes(mockedNativeTableA, Expressions.alwaysTrue()));
        Assertions.assertEquals(2, cachedIndexes());
    }

    @Test
    public void testEqualityDeletesAreNotCached() throws IOException {
        mockedNativeTableA.newAppend().appendFile(FILE_A).commit();
        mockedNativeTableA.newRowDelta().addDeletes(FILE_A_DELETES).addDeletes(FILE_A2_DELETES).commit();

        Map<String, List<String>> expected = Map.of(FILE_A.location(),
                List.of(FILE_A_DELETES.location(), FILE_A2_DELETES.location()).stream().sorted().toList());
        for (int i = 0; i < 3; i++) {
            Assertions.assertEquals(expected, planDeletes(mockedNativeTableA, Expressions.alwaysTrue()));
        }
        Assertions.assertEquals(0, cachedIndexes());
    }
}
