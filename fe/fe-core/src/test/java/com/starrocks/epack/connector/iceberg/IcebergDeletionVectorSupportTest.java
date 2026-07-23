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

package com.starrocks.epack.connector.iceberg;

import com.starrocks.connector.iceberg.IcebergMetadata;
import com.starrocks.connector.iceberg.TableTestBase;
import com.starrocks.connector.iceberg.TestTables;
import com.starrocks.thrift.TIcebergPreviousDeleteFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class IcebergDeletionVectorSupportTest extends TableTestBase {

    @Test
    public void testBuildPreviousDeleteFilesExpandsAssociations() {
        DeleteFile dv = FileMetadata.deleteFileBuilder(SPEC_A)
                .ofPositionDeletes()
                .withPath("/path/to/old_dv.puffin")
                .withFormat(FileFormat.PUFFIN)
                .withFileSizeInBytes(100)
                .withRecordCount(2)
                .withReferencedDataFile(FILE_A.location())
                .withContentOffset(4)
                .withContentSizeInBytes(40)
                .withPartitionPath("data_bucket=0")
                .build();
        // FILE_A_DELETES: partition-scoped positional PD (orc, no referencedDataFile). Planning
        // associates it with each data file it applies to; the thrift form carries ONE entry
        // per delete file with the associated-file list, never one entry per association.
        Map<String, List<DeleteFile>> byRef = new LinkedHashMap<>();
        byRef.put(FILE_A.location(), List.of(dv, FILE_A_DELETES));
        byRef.put("/path/to/data-x.parquet", List.of(FILE_A_DELETES));

        IcebergMetadata.IcebergSinkExtra extra = new IcebergMetadata.IcebergSinkExtra();
        List<TIcebergPreviousDeleteFile> previous =
                IcebergDeletionVectorSupport.buildPreviousDeleteFiles(byRef, extra);

        Assertions.assertEquals(2, previous.size());
        TIcebergPreviousDeleteFile dvPrev = previous.get(0);
        Assertions.assertEquals("puffin", dvPrev.getFormat());
        Assertions.assertTrue(dvPrev.isFile_scoped());
        Assertions.assertEquals(List.of(FILE_A.location()), dvPrev.getReferenced_data_files());
        Assertions.assertTrue(dvPrev.isSetContent_offset());
        TIcebergPreviousDeleteFile pd = previous.get(1);
        Assertions.assertEquals("orc", pd.getFormat());
        Assertions.assertFalse(pd.isFile_scoped());
        Assertions.assertEquals(List.of(FILE_A.location(), "/path/to/data-x.parquet"),
                pd.getReferenced_data_files());
        // Only file-scoped originals enter the removeDeletes round-trip index; a
        // partition-scoped file reported as rewritten must fail the commit's planned-set
        // lookup rather than be removed.
        Assertions.assertNotNull(extra.getPreviousDeleteFile(
                IcebergDeletionVectorSupport.uniqueDeleteFileKey(dv.location(), dv.contentOffset())));
        Assertions.assertNull(extra.getPreviousDeleteFile(
                IcebergDeletionVectorSupport.uniqueDeleteFileKey(FILE_A_DELETES.location(), null)));
    }

    @Test
    public void testPlanningRejectsDuplicateDeletionVectors() {
        // The previous-delete association relies on scan planning's DeleteFileIndex, which
        // guards the one-DV-per-data-file invariant. Pin that guard: two live DVs referencing
        // the same data file must fail planning, not be silently unioned.
        TestTables.TestTable v3Table = create(SCHEMA_A, SPEC_A, "ta_v3_dup_live_dv", 3);
        v3Table.newFastAppend().appendFile(FILE_A).commit();
        DeleteFile dv1 = FileMetadata.deleteFileBuilder(SPEC_A)
                .ofPositionDeletes()
                .withPath(v3Table.location() + "/data/dv1.puffin")
                .withFormat(FileFormat.PUFFIN)
                .withFileSizeInBytes(100)
                .withRecordCount(2)
                .withReferencedDataFile(FILE_A.location())
                .withContentOffset(4)
                .withContentSizeInBytes(40)
                .withPartitionPath("data_bucket=0")
                .build();
        DeleteFile dv2 = FileMetadata.deleteFileBuilder(SPEC_A)
                .ofPositionDeletes()
                .withPath(v3Table.location() + "/data/dv2.puffin")
                .withFormat(FileFormat.PUFFIN)
                .withFileSizeInBytes(100)
                .withRecordCount(3)
                .withReferencedDataFile(FILE_A.location())
                .withContentOffset(4)
                .withContentSizeInBytes(40)
                .withPartitionPath("data_bucket=0")
                .build();
        v3Table.newRowDelta().addDeletes(dv1).commit();
        // validateFromSnapshot narrows the concurrent-DV commit check to an empty range so the
        // duplicate DV can land; only read-side planning rejects it.
        v3Table.newRowDelta().addDeletes(dv2)
                .validateFromSnapshot(v3Table.currentSnapshot().snapshotId())
                .commit();

        ValidationException ex = Assertions.assertThrows(ValidationException.class, () -> {
            try (CloseableIterable<FileScanTask> tasks = v3Table.newScan().planFiles()) {
                tasks.forEach(task -> task.deletes());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        Assertions.assertTrue(ex.getMessage().contains("Can't index multiple DVs"),
                "expected DeleteFileIndex duplicate-DV rejection, got: " + ex.getMessage());
    }
}
