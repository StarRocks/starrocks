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

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

public class IcebergRewriteFileSelectorTest {
    private static final int FILE_PATH_FIELD_ID = 2147483546;
    private static final String DATA_PATH = "s3://bucket/t/data/f1.parquet";
    private static final long BIG = 1000L;
    private static final long SIZE_THRESHOLD = 100L;

    // A 1000-byte, 100-row data file: above the 100-byte size threshold, so only deletes can select it.
    private static FileScanTask task(DeleteFile... deletes) {
        DataFile file = Mockito.mock(DataFile.class);
        Mockito.when(file.location()).thenReturn(DATA_PATH);
        Mockito.when(file.path()).thenReturn(DATA_PATH);
        Mockito.when(file.fileSizeInBytes()).thenReturn(BIG);
        Mockito.when(file.recordCount()).thenReturn(100L);
        FileScanTask task = Mockito.mock(FileScanTask.class);
        Mockito.when(task.file()).thenReturn(file);
        Mockito.when(task.deletes()).thenReturn(Arrays.asList(deletes));
        return task;
    }

    private static DeleteFile deletionVector(String referencedDataFile, long rows) {
        DeleteFile del = Mockito.mock(DeleteFile.class);
        Mockito.when(del.content()).thenReturn(FileContent.POSITION_DELETES);
        Mockito.when(del.format()).thenReturn(FileFormat.PUFFIN);
        Mockito.when(del.referencedDataFile()).thenReturn(referencedDataFile);
        Mockito.when(del.recordCount()).thenReturn(rows);
        return del;
    }

    private static DeleteFile fileScopedPositionDelete(String referencedDataFile, long rows) {
        DeleteFile del = Mockito.mock(DeleteFile.class);
        Mockito.when(del.content()).thenReturn(FileContent.POSITION_DELETES);
        Mockito.when(del.format()).thenReturn(FileFormat.PARQUET);
        Mockito.when(del.referencedDataFile()).thenReturn(referencedDataFile);
        Mockito.when(del.recordCount()).thenReturn(rows);
        return del;
    }

    private static DeleteFile unreferencedPositionDelete(String lowerPath, String upperPath, long rows) {
        DeleteFile del = Mockito.mock(DeleteFile.class);
        Mockito.when(del.content()).thenReturn(FileContent.POSITION_DELETES);
        Mockito.when(del.format()).thenReturn(FileFormat.PARQUET);
        Mockito.when(del.referencedDataFile()).thenReturn(null);
        Mockito.when(del.recordCount()).thenReturn(rows);
        Map<Integer, ByteBuffer> lower =
                Map.of(FILE_PATH_FIELD_ID, ByteBuffer.wrap(lowerPath.getBytes(StandardCharsets.UTF_8)));
        Map<Integer, ByteBuffer> upper =
                Map.of(FILE_PATH_FIELD_ID, ByteBuffer.wrap(upperPath.getBytes(StandardCharsets.UTF_8)));
        Mockito.when(del.lowerBounds()).thenReturn(lower);
        Mockito.when(del.upperBounds()).thenReturn(upper);
        return del;
    }

    private static DeleteFile equalityDelete(long rows) {
        DeleteFile del = Mockito.mock(DeleteFile.class);
        Mockito.when(del.content()).thenReturn(FileContent.EQUALITY_DELETES);
        Mockito.when(del.format()).thenReturn(FileFormat.PARQUET);
        Mockito.when(del.recordCount()).thenReturn(rows);
        return del;
    }

    private static IcebergRewriteFileSelector selector(double ratio, long deleteFiles) {
        return new IcebergRewriteFileSelector(false, SIZE_THRESHOLD, ratio, deleteFiles);
    }

    @Test
    public void testSizeAndRewriteAll() {
        IcebergRewriteFileSelector defaults = IcebergRewriteFileSelector.withDefaults(false, SIZE_THRESHOLD);
        Assertions.assertFalse(defaults.shouldRewrite(task()));
        Assertions.assertTrue(IcebergRewriteFileSelector.withDefaults(false, BIG).shouldRewrite(task()));
        Assertions.assertTrue(IcebergRewriteFileSelector.withDefaults(true, SIZE_THRESHOLD).shouldRewrite(task()));
    }

    @Test
    public void testDeletionVectorRatio() {
        IcebergRewriteFileSelector selector = selector(0.3, Long.MAX_VALUE);
        Assertions.assertFalse(selector.shouldRewrite(task(deletionVector(DATA_PATH, 29))));
        Assertions.assertTrue(selector.shouldRewrite(task(deletionVector(DATA_PATH, 30))));
        Assertions.assertTrue(selector.shouldRewrite(task(deletionVector(DATA_PATH, 100))));
        // A DV for some other data file never counts.
        Assertions.assertFalse(selector.shouldRewrite(task(deletionVector("s3://bucket/t/data/f2.parquet", 100))));
    }

    @Test
    public void testFileScopedPositionDeleteRatioSumsAllDeletesOfTheFile() {
        IcebergRewriteFileSelector selector = selector(0.3, Long.MAX_VALUE);
        Assertions.assertFalse(selector.shouldRewrite(task(fileScopedPositionDelete(DATA_PATH, 20))));
        Assertions.assertTrue(selector.shouldRewrite(task(
                fileScopedPositionDelete(DATA_PATH, 20), fileScopedPositionDelete(DATA_PATH, 10))));
    }

    @Test
    public void testZeroRatioPicksAnyFileScopedDeleteOnly() {
        IcebergRewriteFileSelector selector = selector(0.0, Long.MAX_VALUE);
        Assertions.assertTrue(selector.shouldRewrite(task(deletionVector(DATA_PATH, 1))));
        Assertions.assertFalse(selector.shouldRewrite(task(equalityDelete(50))));
        Assertions.assertFalse(selector.shouldRewrite(task(deletionVector("s3://bucket/t/data/f2.parquet", 1))));
    }

    @Test
    public void testPartitionScopedPositionDeleteAlwaysPicks() {
        IcebergRewriteFileSelector selector = selector(1.0, Long.MAX_VALUE);
        Assertions.assertTrue(selector.shouldRewrite(task(
                unreferencedPositionDelete("s3://bucket/t/data/a.parquet", "s3://bucket/t/data/z.parquet", 1))));
    }

    @Test
    public void testEqualBoundsPositionDeleteCountsTowardsRatio() {
        IcebergRewriteFileSelector selector = selector(0.3, Long.MAX_VALUE);
        // Equal file_path bounds without a reference: the delete holds this data file's positions only.
        Assertions.assertFalse(selector.shouldRewrite(task(unreferencedPositionDelete(DATA_PATH, DATA_PATH, 29))));
        Assertions.assertTrue(selector.shouldRewrite(task(unreferencedPositionDelete(DATA_PATH, DATA_PATH, 30))));
        Assertions.assertTrue(selector.shouldRewrite(task(
                unreferencedPositionDelete(DATA_PATH, DATA_PATH, 20), fileScopedPositionDelete(DATA_PATH, 10))));
        // Equal bounds naming another data file, or a truncated bound, do not count.
        String other = "s3://bucket/t/data/f2.parquet";
        Assertions.assertFalse(selector.shouldRewrite(task(unreferencedPositionDelete(other, other, 100))));
        String truncated = DATA_PATH.substring(0, 16);
        Assertions.assertFalse(selector.shouldRewrite(task(unreferencedPositionDelete(truncated, truncated, 100))));
    }

    @Test
    public void testEqualityDeletesDoNotCountTowardsRatio() {
        IcebergRewriteFileSelector selector = selector(0.3, Long.MAX_VALUE);
        Assertions.assertFalse(selector.shouldRewrite(task(equalityDelete(100), equalityDelete(100))));
    }

    @Test
    public void testDeleteFileThresholdCountsEveryDeleteKind() {
        IcebergRewriteFileSelector selector = selector(1.0, 3);
        Assertions.assertFalse(selector.shouldRewrite(task(equalityDelete(1), deletionVector(DATA_PATH, 1))));
        Assertions.assertTrue(selector.shouldRewrite(task(
                equalityDelete(1), equalityDelete(1), deletionVector(DATA_PATH, 1))));
    }
}
