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

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Pair;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.persist.gson.GsonUtils;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.types.StructField;
import io.delta.kernel.utils.FileStatus;
import org.apache.commons.collections4.map.CaseInsensitiveMap;

import java.util.List;
import java.util.Map;
import java.util.Set;

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

    private static Map<String, StructField> buildCaseInsensitiveSchema(List<StructField> fields) {
        Map<String, StructField> caseInsensitiveMap = new CaseInsensitiveMap<>();
        for (StructField field : fields) {
            caseInsensitiveMap.put(field.getName(), field);
        }
        return caseInsensitiveMap;
    }

    public static Pair<FileScanTask, DeltaLakeAddFileStatsSerDe> convertFromRowToFileScanTask(
            boolean needStats, Row file, Metadata metadata, long estimateRowSize, DeletionVectorDescriptor dv) {
        Set<String> partitionColumns = metadata.getPartitionColNames();
        Map<String, StructField> schema = buildCaseInsensitiveSchema(metadata.getSchema().fields());

        FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(file);
        Map<String, String> partitionValues = InternalScanFileUtils.getPartitionValues(file);
        Map<String, String> physicalNameToPartitionNameMap = Maps.newHashMap();
        // partition Values use column physical name(using in column mapping) as key, we need to convert it to logical name
        for (String partitionColumn : partitionColumns) {
            if (schema.get(partitionColumn) == null) {
                throw new StarRocksConnectorException("Partition column " + partitionColumn + " not found in schema");
            }
            physicalNameToPartitionNameMap.put(ColumnMapping.getPhysicalName(schema.get(partitionColumn)), partitionColumn);
        }
        // convert physical column name to partition logical column name
        Map<String, String> logicalPartitionValues = Maps.newHashMap();
        for (Map.Entry<String, String> entry : partitionValues.entrySet()) {
            logicalPartitionValues.put(physicalNameToPartitionNameMap.get(entry.getKey()), entry.getValue());
        }

        Row addFileRow = getAddFileEntry(file);
        FileScanTask fileScanTask;
        if (needStats) {
            DeltaLakeAddFileStatsSerDe stats = ScanFileUtils.getColumnStatistics(
                    addFileRow, fileStatus, estimateRowSize);
            fileScanTask = new FileScanTask(fileStatus, stats.numRecords, logicalPartitionValues, dv);
            return new Pair<>(fileScanTask, stats);
        } else {
            long records = ScanFileUtils.getFileRows(addFileRow, fileStatus, estimateRowSize);
            fileScanTask = new FileScanTask(fileStatus, records, logicalPartitionValues, dv);
            return new Pair<>(fileScanTask, null);
        }
    }
}
