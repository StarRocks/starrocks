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

package com.starrocks.connector.hudi;

import com.starrocks.common.Config;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileScanContext;
import com.starrocks.connector.RemotePathKey;
import com.starrocks.connector.exception.StarRocksConnectorException;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Covers the file system view {@link HudiRemoteFileIO} builds for a scan.
 *
 * The view has to be backed by metadata readers created with reuse enabled. Without it every
 * partition lookup reopens them, and reopening them recomputes the valid instant set by re-reading
 * the data table's completed rollback metadata, so a scan of N partitions on a table with R
 * rollback instants pays about N x R metadata reads.
 */
public class HudiRemoteFileIOTest {

    @TempDir
    private Path tableDir;

    private String tableLocation;
    private boolean savedMetadataTableFlag;

    @BeforeEach
    public void setUp() throws IOException {
        savedMetadataTableFlag = Config.enable_hudi_lib_internal_metadata_table;
        tableLocation = "file://" + tableDir.toAbsolutePath();
        initTableWithOneCommit();
    }

    @AfterEach
    public void tearDown() {
        Config.enable_hudi_lib_internal_metadata_table = savedMetadataTableFlag;
    }

    /**
     * A bare Hudi table carrying a single completed commit, which is the minimum for
     * createHudiContext to get past its "timeline has a last instant" guard and build the view.
     */
    private void initTableWithOneCommit() throws IOException {
        HadoopStorageConfiguration storageConf = new HadoopStorageConfiguration(new Configuration());
        HoodieTableMetaClient metaClient = HoodieTableMetaClient.newTableBuilder()
                .setTableType(HoodieTableType.COPY_ON_WRITE)
                .setTableName("hudi_remote_file_io_test")
                .setRecordKeyFields("id")
                .setPartitionFields("part")
                // Declares the files partition of the metadata table as available, which is what
                // makes Hudi read listings through it rather than falling back to the file system.
                .setMetadataPartitions("files")
                .initTable(storageConf, tableLocation);

        InstantGenerator instants = metaClient.getInstantGenerator();
        HoodieInstant commit = instants.createNewInstant(
                HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "20260101000000000");
        metaClient.getActiveTimeline().createNewInstant(commit);
        metaClient.getActiveTimeline().transitionRequestedToInflight(commit, Option.empty());
        metaClient.getActiveTimeline().saveAsComplete(
                instants.createNewInstant(
                        HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, commit.requestedTime()),
                Option.empty());
    }

    /**
     * The metadata table is itself a Hudi table living under .hoodie/metadata. It has to exist and
     * carry a completed instant before Hudi will read through it; without one the view silently
     * falls back to listing the file system, and the reuse flag this test is about never applies.
     */
    private void initMetadataTable() throws IOException {
        HadoopStorageConfiguration storageConf = new HadoopStorageConfiguration(new Configuration());
        String metadataLocation = tableLocation + "/.hoodie/metadata";
        HoodieTableMetaClient metadataMetaClient = HoodieTableMetaClient.newTableBuilder()
                .setTableType(HoodieTableType.MERGE_ON_READ)
                .setTableName("hudi_remote_file_io_test_metadata")
                .setRecordKeyFields("key")
                .initTable(storageConf, metadataLocation);

        InstantGenerator instants = metadataMetaClient.getInstantGenerator();
        HoodieInstant deltaCommit = instants.createNewInstant(
                HoodieInstant.State.REQUESTED, HoodieTimeline.DELTA_COMMIT_ACTION, "20260101000000000");
        metadataMetaClient.getActiveTimeline().createNewInstant(deltaCommit);
        metadataMetaClient.getActiveTimeline().transitionRequestedToInflight(deltaCommit, Option.empty());
        metadataMetaClient.getActiveTimeline().saveAsComplete(
                instants.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION,
                        deltaCommit.requestedTime()),
                Option.empty());
    }

    /**
     * Runs one partition lookup. The scan context is closed again on the way out, since the lookup
     * releases the last reference to it, so anything worth asserting has to be captured while the
     * call is still in flight.
     */
    private RemoteFileScanContext scanOnePartition() {
        HudiRemoteFileIO io = new HudiRemoteFileIO(new Configuration());
        RemoteFileScanContext ctx = new RemoteFileScanContext(tableLocation);
        RemotePathKey pathKey = RemotePathKey.of(tableLocation + "/part=1", false);
        pathKey.setScanContext(ctx);

        Map<RemotePathKey, List<RemoteFileDesc>> files = io.getRemoteFiles(pathKey);
        // The partition holds no data files, but the lookup itself has to succeed.
        Assertions.assertTrue(files.containsKey(pathKey));
        return ctx;
    }

    /** Keeps the scan context's view alive past the lookup so the test can inspect it. */
    private static void keepScanContextOpen() {
        new MockUp<RemoteFileScanContext>() {
            @Mock
            public void close() {
            }
        };
    }

    private static Object readField(Object target, String name) throws Exception {
        Class<?> clazz = target.getClass();
        while (clazz != null) {
            try {
                Field field = clazz.getDeclaredField(name);
                field.setAccessible(true);
                return field.get(target);
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        throw new NoSuchFieldException(name + " not found on " + target.getClass());
    }

    @Test
    public void testMetadataReadersAreBuiltForReuse() throws Exception {
        Config.enable_hudi_lib_internal_metadata_table = true;
        initMetadataTable();
        keepScanContextOpen();

        HudiRemoteFileIO io = new HudiRemoteFileIO(new Configuration());
        RemoteFileScanContext ctx = new RemoteFileScanContext(tableLocation);
        RemotePathKey pathKey = RemotePathKey.of(tableLocation + "/part=1", false);
        pathKey.setScanContext(ctx);
        try {
            io.getRemoteFiles(pathKey);
        } catch (StarRocksConnectorException expected) {
            // The metadata table here carries a timeline but no records, so serving a lookup from
            // it fails. The view is built before that, which is the part under test.
        }

        Assertions.assertNotNull(ctx.hudiFsView, "the scan should have built a file system view");

        Object metadata = readField(ctx.hudiFsView, "tableMetadata");
        Assertions.assertInstanceOf(HoodieBackedTableMetadata.class, metadata,
                "with the metadata table enabled the view should read through it");
        Assertions.assertTrue((Boolean) readField(metadata, "reuse"),
                "metadata readers must be reusable across the partitions of one scan, otherwise every "
                        + "partition reopens them and re-reads the table's rollback metadata");
    }

    @Test
    public void testScanSucceedsWithMetadataTableDisabled() {
        Config.enable_hudi_lib_internal_metadata_table = false;
        scanOnePartition();
    }
}
