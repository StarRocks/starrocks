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
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.FileStatus;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.hadoop.fs.Path;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.delta.kernel.internal.InternalScanFileUtils.ADD_FILE_ORDINAL;
import static io.delta.kernel.internal.InternalScanFileUtils.ADD_FILE_STATS_ORDINAL;

public class ScanFileUtils {
    private static final int TABLE_ROOT_ORDINAL = InternalScanFileUtils.SCAN_FILE_SCHEMA.indexOf("tableRoot");
    private static final int ADD_FILE_PATH_ORDINAL =
            ((StructType) InternalScanFileUtils.SCAN_FILE_SCHEMA.get("add").getDataType()).indexOf("path");

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

    // delta-kernel 4.2 URI-decodes the path inside getAddFileStatus(), but BE opens the object key
    // verbatim and the physical key keeps the URI-encoded add.path. Rebuild the absolute path from the
    // raw add.path so the encoding is preserved (matching pre-4.2 behavior).
    private static FileStatus getEncodedAddFileStatus(Row scanFileInfo) {
        Row addFile = getAddFileEntry(scanFileInfo);
        if (addFile.isNullAt(ADD_FILE_PATH_ORDINAL) || scanFileInfo.isNullAt(TABLE_ROOT_ORDINAL)) {
            throw new IllegalArgumentException("There is no `add.path` or `tableRoot` entry in the scan file row");
        }
        FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(scanFileInfo);
        String encodedPath = addFile.getString(ADD_FILE_PATH_ORDINAL);
        String tableRoot = scanFileInfo.getString(TABLE_ROOT_ORDINAL);
        String encodedAbsolutePath = new Path(tableRoot, encodedPath).toString();
        return FileStatus.of(encodedAbsolutePath, fileStatus.getSize(), fileStatus.getModificationTime());
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
        return new FileScanTaskConverter(metadata, estimateRowSize).convert(needStats, file, dv);
    }

    // One converter belongs to one snapshot scan. Schema and column mapping never change between its files.
    public static final class FileScanTaskConverter {
        private final Map<String, String> physicalToLogicalPartitionNames;
        private final long estimateRowSize;

        public FileScanTaskConverter(Metadata metadata, long estimateRowSize) {
            this.estimateRowSize = estimateRowSize;
            Map<String, String> names = Maps.newHashMap();
            if (!metadata.getPartitionColNames().isEmpty()) {
                Map<String, StructField> schema = buildCaseInsensitiveSchema(metadata.getSchema().fields());
                for (String partitionColumn : metadata.getPartitionColNames()) {
                    StructField field = schema.get(partitionColumn);
                    if (field == null) {
                        throw new StarRocksConnectorException("Partition column " + partitionColumn + " not found in schema");
                    }
                    names.put(ColumnMapping.getPhysicalName(field), partitionColumn);
                }
            }
            physicalToLogicalPartitionNames = Collections.unmodifiableMap(names);
        }

        public Pair<FileScanTask, DeltaLakeAddFileStatsSerDe> convert(
                boolean needStats, Row file, DeletionVectorDescriptor dv) {
            FileStatus fileStatus = getEncodedAddFileStatus(file);
            Map<String, String> partitionValues = InternalScanFileUtils.getPartitionValues(file);
            // convert physical column name to partition logical column name
            Map<String, String> logicalPartitionValues = Maps.newHashMap();
            for (Map.Entry<String, String> entry : partitionValues.entrySet()) {
                logicalPartitionValues.put(physicalToLogicalPartitionNames.get(entry.getKey()), entry.getValue());
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
}
