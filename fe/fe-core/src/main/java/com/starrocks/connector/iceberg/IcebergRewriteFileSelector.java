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
import org.apache.iceberg.FileScanTask;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

/**
 * Decides which data files `rewrite_data_files` rewrites.
 *
 * <p>A data file is picked when any of the following holds:
 * <ul>
 *   <li>{@code rewriteAll} is set;</li>
 *   <li>its size is at or below {@code minFileSizeBytes};</li>
 *   <li>a partition-scoped position delete applies to it (rewriting every file such a delete covers is the only way
 *   to make it dangling);</li>
 *   <li>at least {@code deleteFileThreshold} delete files apply to it;</li>
 *   <li>the rows deleted by its file-scoped position deletes reach {@code deleteRatioThreshold} of its rows. A
 *   position delete is file-scoped when it references the data file (deletion vectors included) or when both
 *   bounds of its file_path column are the data file's path. Equality deletes and partition-scoped position
 *   deletes are not attributable to a single data file, so they do not count towards the ratio.</li>
 * </ul>
 * The thresholds mirror Spark's {@code delete-ratio-threshold} and {@code delete-file-threshold}.
 */
public record IcebergRewriteFileSelector(boolean rewriteAll,
                                         long minFileSizeBytes,
                                         double deleteRatioThreshold,
                                         long deleteFileThreshold) {
    public static final long DEFAULT_MIN_FILE_SIZE_BYTES = 256L * 1024 * 1024;
    public static final double DEFAULT_DELETE_RATIO_THRESHOLD = 0.3;
    // No limit by default, as in Spark.
    public static final long DEFAULT_DELETE_FILE_THRESHOLD = Long.MAX_VALUE;

    // https://iceberg.apache.org/spec/#reserved-field-ids
    private static final int FILE_PATH_FIELD_ID = 2147483546;

    public static IcebergRewriteFileSelector withDefaults(boolean rewriteAll, long minFileSizeBytes) {
        return new IcebergRewriteFileSelector(rewriteAll, minFileSizeBytes,
                DEFAULT_DELETE_RATIO_THRESHOLD, DEFAULT_DELETE_FILE_THRESHOLD);
    }

    public boolean shouldRewrite(FileScanTask task) {
        DataFile file = task.file();
        if (rewriteAll || file.fileSizeInBytes() <= minFileSizeBytes) {
            return true;
        }
        if (task.deletes().isEmpty()) {
            return false;
        }
        if (task.deletes().size() >= deleteFileThreshold) {
            return true;
        }
        long deletedRows = 0;
        for (DeleteFile del : task.deletes()) {
            if (del.content() != FileContent.POSITION_DELETES) {
                continue;
            }
            if (isPartitionScoped(del)) {
                return true;
            }
            if (IcebergConnectorScanRangeSource.referencesOnlyDataFile(del, file)) {
                deletedRows += del.recordCount();
            }
        }
        // Divide rather than multiply the threshold: 0.3 * 10 is 3.0000000000000004, so 3 of 10 rows would miss 0.3.
        return deletedRows > 0 && file.recordCount() > 0
                && (double) deletedRows / file.recordCount() >= deleteRatioThreshold;
    }

    // A position delete without a referenced data file whose file_path bounds differ spans several data files.
    // One with equal bounds holds a single data file's positions and counts towards that file's ratio. The commit
    // drops only position deletes that carry the reference, so such a delete outlives the rewrite, but it names
    // the old data file's path and no longer applies to any live data file.
    private static boolean isPartitionScoped(DeleteFile del) {
        if (del.referencedDataFile() != null) {
            return false;
        }
        Map<Integer, ByteBuffer> lower = del.lowerBounds();
        Map<Integer, ByteBuffer> upper = del.upperBounds();
        return lower != null && upper != null
                && !Objects.equals(lower.get(FILE_PATH_FIELD_ID), upper.get(FILE_PATH_FIELD_ID));
    }
}
