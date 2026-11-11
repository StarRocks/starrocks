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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.NodeMgr;
import com.starrocks.system.Frontend;
import com.starrocks.thrift.TIcebergDataFile;
import com.starrocks.thrift.TSinkCommitInfo;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.starrocks.type.IntegerType.INT;
import static com.starrocks.type.VarcharType.VARCHAR;

// StarRocks writes to Iceberg format-version 4 tables. Iceberg 1.12 commits them like v3 tables except that every
// manifest it writes is Parquet. Each statement is checked against both Iceberg's own scan and StarRocks planning.
public class IcebergFormatV4WriteTest extends TableTestBase {
    private static final String CATALOG_NAME = "iceberg_catalog";

    private TestTables.TestTable table;
    private IcebergMetadata metadata;

    @BeforeEach
    public void setUpV4Table() {
        table = create(SCHEMA_A, SPEC_A, "tv4_write", 4);
        IcebergHiveCatalog catalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(),
                IcebergMetadataTest.DEFAULT_CONFIG);
        metadata = new IcebergMetadata(CATALOG_NAME, IcebergMetadataTest.HDFS_ENVIRONMENT, catalog,
                Executors.newSingleThreadExecutor(), IcebergMetadataTest.DEFAULT_CATALOG_PROPERTIES);
        IcebergTable icebergTable = icebergTable();
        new Expectations(metadata) {
            {
                metadata.getTable((ConnectContext) any, anyString, anyString);
                result = icebergTable;
                minTimes = 0;
            }
        };
        new MockUp<NodeMgr>() {
            @Mock
            public Frontend getMySelf() {
                return new Frontend(FrontendNodeType.LEADER, "test-fe", "127.0.0.1", 9010);
            }
        };
        new MockUp<Frontend>() {
            @Mock
            public String getFeVersion() {
                return "test-version-v4";
            }
        };
    }

    private IcebergTable icebergTable() {
        return new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db", "iceberg_table", "",
                Lists.newArrayList(new Column("id", INT), new Column("data", VARCHAR)), table, Maps.newHashMap());
    }

    private void commit(List<TSinkCommitInfo> commitInfos, IcebergMetadata.IcebergSinkExtra extra) {
        metadata.finishSink("iceberg_db", "iceberg_table", commitInfos, null, extra,
                UtFrameUtils.createDefaultCtx());
        table.refresh();
    }

    private TSinkCommitInfo dataFileCommit(String name, long rows, boolean overwrite) {
        TIcebergDataFile file = new TIcebergDataFile();
        file.setPath(table.location() + "/data/data_bucket=0/" + name);
        file.setFormat("parquet");
        file.setRecord_count(rows);
        file.setSplit_offsets(Lists.newArrayList(4L));
        file.setPartition_path(table.location() + "/data/data_bucket=0/");
        file.setFile_size_in_bytes(2000);
        file.setPartition_null_fingerprint("0");
        TSinkCommitInfo commitInfo = new TSinkCommitInfo();
        commitInfo.setIs_overwrite(overwrite);
        commitInfo.setIceberg_data_file(file);
        return commitInfo;
    }

    private static IcebergMetadata.IcebergSinkExtra scanned(DataFile... files) {
        IcebergMetadata.IcebergSinkExtra extra = new IcebergMetadata.IcebergSinkExtra();
        extra.addScannedDataFiles(Sets.newHashSet(files));
        return extra;
    }

    // Data file location -> locations of its delete files, from Iceberg's scan of the current snapshot.
    private Map<String, List<String>> icebergPlan() {
        return Lists.newArrayList(table.newScan().planFiles()).stream()
                .collect(Collectors.toMap((FileScanTask task) -> task.file().location(),
                        task -> task.deletes().stream().map(DeleteFile::location).sorted().collect(Collectors.toList())));
    }

    // The same, as planned by StarRocks.
    private Map<String, List<String>> starRocksPlan() {
        List<RemoteFileInfo> infos = metadata.getRemoteFiles(icebergTable(), GetRemoteFilesParams.newBuilder()
                .setTableVersionRange(TvrTableSnapshot.of(Optional.of(table.currentSnapshot().snapshotId())))
                .setFieldNames(Lists.newArrayList()).build());
        return infos.stream().map(info -> ((IcebergRemoteFileInfo) info).getFileScanTask())
                .collect(Collectors.toMap(task -> task.file().location(),
                        task -> task.deletes().stream().map(DeleteFile::location).sorted().collect(Collectors.toList())));
    }

    private void assertPlan(Map<String, List<String>> expected) {
        Assertions.assertEquals(expected, icebergPlan());
        Assertions.assertEquals(expected, starRocksPlan());
        List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
        Assertions.assertTrue(manifests.stream().allMatch(m -> FileFormat.fromFileName(m.path()) == FileFormat.PARQUET),
                "every manifest of a v4 table is Parquet");
    }

    @Test
    public void testInsert() {
        TSinkCommitInfo first = dataFileCommit("a.parquet", 10, false);
        commit(Lists.newArrayList(first), null);
        TSinkCommitInfo second = dataFileCommit("b.parquet", 5, false);
        commit(Lists.newArrayList(second), null);
        assertPlan(Map.of(first.getIceberg_data_file().getPath(), List.of(),
                second.getIceberg_data_file().getPath(), List.of()));
        Assertions.assertEquals(DataOperations.APPEND, table.currentSnapshot().operation());
    }

    @Test
    public void testInsertOverwrite() {
        table.newFastAppend().appendFile(FILE_A).commit();
        TSinkCommitInfo overwrite = dataFileCommit("c.parquet", 7, true);
        commit(Lists.newArrayList(overwrite), null);
        assertPlan(Map.of(overwrite.getIceberg_data_file().getPath(), List.of()));
    }

    // UPDATE and MERGE are rejected at analysis for any format version but 2, so v4 tables need no write test here.

    @Test
    public void testRewriteReplacesScannedFiles() {
        table.newFastAppend().appendFile(FILE_A).appendFile(FILE_A_2).commit();
        long base = table.currentSnapshot().snapshotId();
        IcebergMetadata.IcebergSinkExtra extra = scanned(FILE_A, FILE_A_2);
        extra.setRewrite(true);
        extra.setBaseSnapshotId(base);
        TSinkCommitInfo compacted = dataFileCommit("compacted.parquet", 4, false);
        compacted.unsetIs_overwrite();
        compacted.setIs_rewrite(true);
        commit(Lists.newArrayList(compacted), extra);
        assertPlan(Map.of(compacted.getIceberg_data_file().getPath(), List.of()));
        Assertions.assertEquals(DataOperations.REPLACE, table.currentSnapshot().operation());
    }
}
