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

import org.apache.iceberg.ConvertEqualityDeleteRewriteFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class IcebergConvertEqualityDeleteCommitTest {
    @Test
    public void testAtomicReplacement() throws Exception {
        try (Fixture f = new Fixture()) {
            f.rewrite().addFile(f.positionDelete(), f.eq.dataSequenceNumber()).deleteFile(f.eq).commit();
            Assertions.assertEquals(Set.of(f.positionDelete().location()), f.deletePaths());
        }
    }

    @Test
    public void testRemovalOnly() throws Exception {
        try (Fixture f = new Fixture()) {
            new ConvertEqualityDeleteRewriteFiles(f.table.name(), f.table.ops(), Set.of(), f.snapshot)
                    .deleteFile(f.eq).commit();
            Assertions.assertTrue(f.deletePaths().isEmpty());
        }
    }

    @Test
    public void testConcurrentAppend() throws Exception {
        try (Fixture f = new Fixture()) {
            f.table.newAppend().appendFile(f.dataFile("new.parquet")).commit();
            f.rewrite().addFile(f.positionDelete(), f.eq.dataSequenceNumber()).deleteFile(f.eq).commit();
            Assertions.assertEquals(Set.of(f.positionDelete().location()), f.deletePaths());
        }
    }

    @Test
    public void testConcurrentDataRemoval() throws Exception {
        try (Fixture f = new Fixture()) {
            f.table.newDelete().deleteFile(f.data).commit();
            Assertions.assertThrows(ValidationException.class, () -> f.rewrite()
                    .addFile(f.positionDelete(), f.eq.dataSequenceNumber()).deleteFile(f.eq).commit());
        }
    }

    @Test
    public void testConcurrentEqualityDeleteRemoval() throws Exception {
        try (Fixture f = new Fixture()) {
            f.table.newRewrite().deleteFile(f.eq).commit();
            Assertions.assertThrows(ValidationException.class, () -> f.rewrite()
                    .addFile(f.positionDelete(), f.eq.dataSequenceNumber()).deleteFile(f.eq).commit());
        }
    }

    @Test
    public void testRevalidatesAfterCommitConflict() throws Exception {
        try (Fixture f = new Fixture()) {
            ConvertEqualityDeleteRewriteFiles rewrite = new ConvertEqualityDeleteRewriteFiles(
                    f.table.name(), f.table.ops(), Set.of(f.data.location()), f.snapshot) {
                private boolean concurrentCommit = false;

                @Override
                protected void validate(TableMetadata base, Snapshot parent) {
                    super.validate(base, parent);
                    if (!concurrentCommit) {
                        concurrentCommit = true;
                        f.table.newDelete().deleteFile(f.data).commit();
                    }
                }
            };
            Assertions.assertThrows(ValidationException.class, () -> rewrite
                    .addFile(f.positionDelete(), f.eq.dataSequenceNumber()).deleteFile(f.eq).commit());
        }
    }

    private static class Fixture implements AutoCloseable {
        private final Path dir = Files.createTempDirectory("eq-delete-commit-");
        private final PartitionSpec spec = PartitionSpec.unpartitioned();
        private final TestTables.TestTable table;
        private final DataFile data;
        private final DeleteFile eq;
        private final long snapshot;

        Fixture() throws Exception {
            Schema schema = new Schema(Types.NestedField.optional(1, "k", Types.LongType.get()));
            table = TestTables.create(dir.toFile(), "eq-" + UUID.randomUUID(), schema, spec, 2);
            data = dataFile("data.parquet");
            table.newAppend().appendFile(data).commit();
            DeleteFile delete = FileMetadata.deleteFileBuilder(spec).ofEqualityDeletes(1)
                    .withPath(dir.resolve("eq.parquet").toString()).withFormat(FileFormat.PARQUET)
                    .withFileSizeInBytes(100).withRecordCount(1).build();
            table.newRowDelta().addDeletes(delete).commit();
            snapshot = table.currentSnapshot().snapshotId();
            try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
                eq = tasks.iterator().next().deletes().get(0);
            }
        }

        DataFile dataFile(String name) {
            return DataFiles.builder(spec).withPath(dir.resolve(name).toString())
                    .withFileSizeInBytes(100).withRecordCount(10).build();
        }

        DeleteFile positionDelete() {
            return FileMetadata.deleteFileBuilder(spec).ofPositionDeletes()
                    .withPath(dir.resolve("pos.parquet").toString()).withFormat(FileFormat.PARQUET)
                    .withReferencedDataFile(data.location()).withFileSizeInBytes(100).withRecordCount(1).build();
        }

        ConvertEqualityDeleteRewriteFiles rewrite() {
            return new ConvertEqualityDeleteRewriteFiles(table.name(), table.ops(), Set.of(data.location()), snapshot);
        }

        Set<String> deletePaths() throws Exception {
            Set<String> paths = new HashSet<>();
            try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
                for (FileScanTask task : tasks) {
                    task.deletes().forEach(file -> paths.add(file.location()));
                }
            }
            return paths;
        }

        @Override
        public void close() throws Exception {
            try (var paths = Files.walk(dir)) {
                for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                    Files.delete(path);
                }
            }
        }
    }
}
