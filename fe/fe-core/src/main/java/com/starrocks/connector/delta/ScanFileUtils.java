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
import com.starrocks.persist.gson.GsonUtils;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.utils.FileStatus;

import java.util.Map;

import static io.delta.kernel.internal.InternalScanFileUtils.ADD_FILE_STATS_ORDINAL;

public class ScanFileUtils {
    public static class Records {
        @SerializedName(value = "numRecords")
        public long numRecords;
    }

    public static long getFileRows(Row file) {
        String stats = file.getString(ADD_FILE_STATS_ORDINAL);
        if (stats == null) {
            throw new IllegalArgumentException("There is no `stats` entry in the add file row");
        }

        Records records = GsonUtils.GSON.fromJson(stats, Records.class);
        if (records == null) {
            throw new IllegalArgumentException("There is no `records` entry in the stats row");
        }

        return records.numRecords;
    }

    public static DeltaLakeStatsStruct getColumnStatistics(Row file) {
        String stats = file.getString(ADD_FILE_STATS_ORDINAL);
        if (stats == null) {
            throw new IllegalArgumentException("There is no `stats` entry in the add file row");
        }

        DeltaLakeStatsStruct statistics = GsonUtils.GSON.fromJson(stats, DeltaLakeStatsStruct.class);
        if (statistics == null) {
            throw new IllegalArgumentException("There is no entry in the stats row");
        }

        return statistics;
    }

    public static FileScanTask convertFromRowToFileScanTask(boolean needStats, Row file) {
        FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(file);
        Map<String, String> partitionValues = InternalScanFileUtils.getPartitionValues(file);
        FileScanTask fileScanTask;
        if (needStats) {
            DeltaLakeStatsStruct stats = ScanFileUtils.getColumnStatistics(file);
            fileScanTask = new FileScanTask(fileStatus, stats.numRecords, partitionValues, stats);
        } else {
            long records = ScanFileUtils.getFileRows(file);
            fileScanTask = new FileScanTask(fileStatus, records, partitionValues);
        }
        return fileScanTask;
    }
}
