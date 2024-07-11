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

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Pair;
import com.starrocks.persist.gson.GsonUtils;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.utils.FileStatus;

import java.util.Map;

import static io.delta.kernel.internal.InternalScanFileUtils.ADD_FILE_ORDINAL;
import static io.delta.kernel.internal.InternalScanFileUtils.ADD_FILE_STATS_ORDINAL;

public class ScanFileUtils {
    public static class Records {
        @SerializedName(value = "numRecords")
        public long numRecords;
    }

    public static long getFileRows(Row file, FileStatus fileStatus, long estimateRowSize) {
        String stats = file.getString(ADD_FILE_STATS_ORDINAL);
        if (stats != null) {
            Records records = GsonUtils.GSON.fromJson(stats, Records.class);
            if (records != null) {
                return records.numRecords;
            }
        }

        return fileStatus.getSize() / estimateRowSize;
    }

    public static DeltaLakeAddFileStatsSerDe getColumnStatistics(Row file, FileStatus fileStatus,
                                                                 long estimateRowSize) {
        String stats = file.getString(ADD_FILE_STATS_ORDINAL);
        if (stats != null) {
            DeltaLakeAddFileStatsSerDe fileStatsSerDe = GsonUtils.GSON.fromJson(
                    stats, DeltaLakeAddFileStatsSerDe.class);
            if (fileStatsSerDe != null) {
                return fileStatsSerDe;
            }
        }

        long estimateRowCount = fileStatus.getSize() / estimateRowSize;
        return new DeltaLakeAddFileStatsSerDe(estimateRowCount, null, null, null);
    }

    private static Row getAddFileEntry(Row scanFileInfo) {
        if (scanFileInfo.isNullAt(ADD_FILE_ORDINAL)) {
            throw new IllegalArgumentException("There is no `add` entry in the scan file row");
        }
        return scanFileInfo.getStruct(ADD_FILE_ORDINAL);
    }

    public static Pair<FileScanTask, DeltaLakeAddFileStatsSerDe> convertFromRowToFileScanTask(
            boolean needStats, Row file, long estimateRowSize) {
        FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(file);
        Map<String, String> partitionValues = InternalScanFileUtils.getPartitionValues(file);
        Row addFileRow = getAddFileEntry(file);

        FileScanTask fileScanTask;
        if (needStats) {
            DeltaLakeAddFileStatsSerDe stats = ScanFileUtils.getColumnStatistics(
                    addFileRow, fileStatus, estimateRowSize);
            fileScanTask = new FileScanTask(fileStatus, stats.numRecords, partitionValues);
            return new Pair<>(fileScanTask, stats);
        } else {
            long records = ScanFileUtils.getFileRows(addFileRow, fileStatus, estimateRowSize);
            fileScanTask = new FileScanTask(fileStatus, records, partitionValues);
            return new Pair<>(fileScanTask, null);
        }
    }
}
