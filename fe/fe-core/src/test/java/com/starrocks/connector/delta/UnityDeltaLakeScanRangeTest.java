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

package com.starrocks.connector.delta;

import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.RemoteFileInfoDefaultSource;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.planner.PartitionIdGenerator;
import com.starrocks.thrift.THdfsScanRange;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.utils.FileStatus;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starrocks.connector.delta.UnityCatalogTestSupport.STORAGE;

public class UnityDeltaLakeScanRangeTest {
    @Mocked
    private DeltaLakeTable table;

    @Test
    public void testScopedScanPreservesPlusAndDecodesEscapedSpacesOnce() {
        DeltaConnectorScanRangeSource source = source(STORAGE + "/alpha/part+name%20one.parquet", null, true);
        THdfsScanRange range = source.getOutputs(1).get(0).getScan_range().getHdfs_scan_range();
        Assertions.assertEquals(STORAGE + "/alpha/part+name one.parquet", range.getFull_path());
        Assertions.assertFalse(range.isSetRelative_path());
        Assertions.assertEquals(123L, range.getFile_length());
        Assertions.assertFalse(source.hasMoreOutput());
    }

    @Test
    public void testEscapedTableRootUsesDecodedAbsoluteScanPath() {
        String root = STORAGE + "/alpha+root%20one";
        THdfsScanRange range = source(root, root + "/part+name%20one.parquet", null, true)
                .getOutputs(1).get(0).getScan_range().getHdfs_scan_range();
        Assertions.assertEquals(STORAGE + "/alpha+root one/part+name one.parquet", range.getFull_path());
        Assertions.assertFalse(range.isSetRelative_path());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "/alpha-other/file.parquet", "/alpha/../outside.parquet", "/alpha/%2e%2e/outside.parquet",
            "/alpha/%252e%252e/outside.parquet", "/alpha/part%2ffile.parquet", "/alpha/file.parquet?sig=untrusted"
    })
    public void testRejectsDataOutsideTableScopeOrAmbiguousPaths(String suffix) {
        DeltaConnectorScanRangeSource source = source(STORAGE + suffix, null, true);
        Assertions.assertThrows(StarRocksConnectorException.class, () -> source.getOutputs(1));
    }

    @Test
    public void testRejectsDataFromAnotherStorageAccount() {
        DeltaConnectorScanRangeSource source = source(
                "abfss://container@otheraccount.dfs.core.windows.net/alpha/file.parquet", null, true);
        Assertions.assertThrows(StarRocksConnectorException.class, () -> source.getOutputs(1));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "abfss://container@account.dfs.core.windows.net/alpha/deletion+vector%20one.bin",
            "https://account.dfs.core.windows.net/container/alpha/deletion+vector%20one.bin"
    })
    public void testSerializesCanonicalInScopeDeletionVector(String location) {
        DeletionVectorDescriptor vector = new DeletionVectorDescriptor(
                "p", location, Optional.of(4), 10, 1);
        THdfsScanRange range = source(STORAGE + "/alpha/file.parquet", vector, true)
                .getOutputs(1).get(0).getScan_range().getHdfs_scan_range();
        Assertions.assertEquals(STORAGE + "/alpha/deletion+vector one.bin",
                range.getDeletion_vector_descriptor().getPathOrInlineDv());
        Assertions.assertEquals(4, range.getDeletion_vector_descriptor().getOffset());
    }

    @ParameterizedTest
    @ValueSource(strings = {"alpha", "alpha+root%20one"})
    public void testUuidDeletionVectorBecomesAbsoluteBackendPath(String directory) {
        String root = STORAGE + "/" + directory;
        DeletionVectorDescriptor vector = new DeletionVectorDescriptor(
                "u", "dv+prefix" + "0".repeat(20), Optional.of(4), 10, 1);
        THdfsScanRange range = source(root, root + "/file.parquet", vector, true)
                .getOutputs(1).get(0).getScan_range().getHdfs_scan_range();
        Assertions.assertEquals("p", range.getDeletion_vector_descriptor().getStorageType());
        Assertions.assertEquals(STORAGE + "/" + directory.replace("%20", " ")
                        + "/dv+prefix/deletion_vector_00000000-0000-0000-0000-000000000000.bin",
                range.getDeletion_vector_descriptor().getPathOrInlineDv());
        Assertions.assertEquals(4, range.getDeletion_vector_descriptor().getOffset());
    }

    @Test
    public void testRejectsDeletionVectorOutsideTableScope() {
        DeletionVectorDescriptor vector = new DeletionVectorDescriptor(
                "p", STORAGE + "/other/deletion_vector.bin", Optional.empty(), 10, 1);
        DeltaConnectorScanRangeSource source = source(STORAGE + "/alpha/file.parquet", vector, true);
        Assertions.assertThrows(StarRocksConnectorException.class, () -> source.getOutputs(1));
    }

    @Test
    public void testRejectsUuidDeletionVectorTraversalBeforeKernelNormalizesIt() {
        DeletionVectorDescriptor vector = new DeletionVectorDescriptor(
                "u", "../" + "0".repeat(20), Optional.empty(), 10, 1);
        DeltaConnectorScanRangeSource source = source(STORAGE + "/alpha/file.parquet", vector, true);
        Assertions.assertThrows(StarRocksConnectorException.class, () -> source.getOutputs(1));
    }

    @ParameterizedTest
    @ValueSource(strings = {"s3://bucket", STORAGE})
    public void testExistingHmsCredentialsDoNotEnableUnityPathRules(String storage) {
        String externalPath = storage + "/other/file.parquet";
        THdfsScanRange range = source(storage + "/alpha", externalPath, null, false)
                .getOutputs(1).get(0).getScan_range().getHdfs_scan_range();
        Assertions.assertEquals(externalPath, range.getFull_path());
        Assertions.assertFalse(range.isSetRelative_path());
    }

    private DeltaConnectorScanRangeSource source(String file, DeletionVectorDescriptor vector, boolean unityCatalogTable) {
        return source(STORAGE + "/alpha", file, vector, unityCatalogTable);
    }

    private DeltaConnectorScanRangeSource source(String root, String file, DeletionVectorDescriptor vector,
                                                 boolean unityCatalogTable) {
        CloudConfiguration configuration = new CloudConfiguration();
        new Expectations() {
            {
                table.getDeltaMetadata().getFormat().getProvider();
                result = "parquet";
                table.getTableLocation();
                result = root;
                table.getCloudConfiguration();
                result = configuration;
                minTimes = 0;
                table.isUnityCatalogTable();
                result = unityCatalogTable;
                table.getType();
                result = Table.TableType.DELTALAKE;
                minTimes = 0;
                table.getPartitionColumnNames();
                result = List.of();
                minTimes = 0;
                table.getPartitionColumns();
                result = List.of();
                minTimes = 0;
            }
        };
        FileScanTask task = new FileScanTask(FileStatus.of(file, 123, 0), 1, Map.of(), vector);
        return new DeltaConnectorScanRangeSource(table,
                new RemoteFileInfoDefaultSource(List.of(new DeltaRemoteFileInfo(task))), PartitionIdGenerator.of());
    }
}
