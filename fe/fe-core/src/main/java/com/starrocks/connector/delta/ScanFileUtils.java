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
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
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

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;

import static io.delta.kernel.internal.InternalScanFileUtils.ADD_FILE_ORDINAL;
import static io.delta.kernel.internal.InternalScanFileUtils.ADD_FILE_STATS_ORDINAL;

public class ScanFileUtils {
    private static final int TABLE_ROOT_ORDINAL = InternalScanFileUtils.SCAN_FILE_SCHEMA.indexOf("tableRoot");
    private static final StructType ADD_FILE_SCHEMA =
            (StructType) InternalScanFileUtils.SCAN_FILE_SCHEMA.get("add").getDataType();
    private static final int ADD_FILE_PATH_ORDINAL = ADD_FILE_SCHEMA.indexOf("path");
    private static final int ADD_FILE_SIZE_ORDINAL = ADD_FILE_SCHEMA.indexOf("size");
    private static final int ADD_FILE_MOD_TIME_ORDINAL = ADD_FILE_SCHEMA.indexOf("modificationTime");

    public static class Records {
        @SerializedName(value = "numRecords")
        public long numRecords;
    }

    public static long getFileRows(Row file, FileStatus fileStatus, long estimateRowSize) {
        String stats = file.getString(ADD_FILE_STATS_ORDINAL);
        if (stats != null) {
            Long numRecords = readNumRecords(stats);
            if (numRecords != null) {
                return numRecords;
            }
            Records records = GsonUtils.GSON.fromJson(stats, Records.class);
            if (records != null) {
                return records.numRecords;
            }
        }

        return fileStatus.getSize() / estimateRowSize;
    }

    // Reads only the top-level numRecords of an add-file stats object, stopping as soon as it is found (writers put
    // it first, ahead of the per-column min/max/null maps). Returns null whenever this shortcut cannot answer
    // exactly what binding the whole object to Records would -- not an object, malformed, or numRecords absent --
    // so the caller falls back to that full parse.
    static Long readNumRecords(String stats) {
        try (JsonReader reader = new JsonReader(new StringReader(stats))) {
            if (reader.peek() != JsonToken.BEGIN_OBJECT) {
                return null;
            }
            reader.beginObject();
            while (reader.hasNext()) {
                if ("numRecords".equals(reader.nextName())) {
                    if (reader.peek() == JsonToken.NULL) {
                        return null;
                    }
                    return reader.nextLong();
                }
                reader.skipValue();
            }
            return null;
        } catch (IOException | RuntimeException e) {
            return null;
        }
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
    // raw add.path so the encoding is preserved (matching pre-4.2 behavior). Size and modification time are read
    // straight from the add struct: getAddFileStatus() would also build and discard a decoded path per file.
    private static FileStatus getEncodedAddFileStatus(Row scanFileInfo, RowContext context) {
        Row addFile = getAddFileEntry(scanFileInfo);
        if (addFile.isNullAt(ADD_FILE_PATH_ORDINAL) || scanFileInfo.isNullAt(TABLE_ROOT_ORDINAL)) {
            throw new IllegalArgumentException("There is no `add.path` or `tableRoot` entry in the scan file row");
        }
        String encodedPath = addFile.getString(ADD_FILE_PATH_ORDINAL);
        String tableRoot = scanFileInfo.getString(TABLE_ROOT_ORDINAL);
        String encodedAbsolutePath = context.resolve(tableRoot, encodedPath);
        return FileStatus.of(encodedAbsolutePath, addFile.getLong(ADD_FILE_SIZE_ORDINAL),
                addFile.getLong(ADD_FILE_MOD_TIME_ORDINAL));
    }

    // Same result as new Path(tableRoot, child).toString(), which parses both as URIs and normalizes the result.
    // A plain relative child -- no scheme, no leading or doubled slash, no dot segment, only characters that URI
    // quoting leaves as they are or round-trips (% included) -- resolves to root + "/" + child. Anything else, and
    // any root rootPrefix() turns down, takes the Path route.
    static String resolveEncodedPath(String tableRoot, String child, String rootPrefix) {
        if (rootPrefix != null && isPlainRelativePath(child)) {
            return rootPrefix + child;
        }
        return new Path(tableRoot, child).toString();
    }

    // The prefix the fast path appends a child to, or null when the root has no path (a bare "s3://bucket"):
    // Path(root, child) then glues the child onto the authority and fails, and the fast path must not paper over that.
    static String rootPrefix(String tableRoot) {
        try {
            Path root = new Path(tableRoot);
            if (root.toUri().getPath().isEmpty()) {
                return null;
            }
            String s = root.toString();
            return s.endsWith("/") ? s : s + "/";
        } catch (RuntimeException e) {
            return null;
        }
    }

    static boolean isPlainRelativePath(String child) {
        int n = child.length();
        if (n == 0 || child.charAt(0) == '/' || child.charAt(n - 1) == '/') {
            return false;
        }
        int segmentStart = 0;
        for (int i = 0; i <= n; i++) {
            char c = i < n ? child.charAt(i) : '/';
            if (c == '/') {
                int len = i - segmentStart;
                if (len == 0) {
                    return false;
                }
                if (child.charAt(segmentStart) == '.' && (len == 1 || (len == 2 && child.charAt(segmentStart + 1) == '.'))) {
                    return false;
                }
                segmentStart = i + 1;
            } else if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
                    || c == '.' || c == '_' || c == '-' || c == '=' || c == '%' || c == '+' || c == '~')) {
                return false;
            }
        }
        return true;
    }

    // Per-snapshot state of the conversion, derived once per scan-file iteration instead of once per file: the
    // logical name of each partition column's physical name, and the resolved prefix of the last table root seen.
    // Not thread-safe; one per iterator.
    public static final class RowContext {
        private final Map<String, String> physicalToLogicalPartitionName;
        private String lastRoot;
        private String lastRootPrefix;

        public RowContext(Metadata metadata) {
            Map<String, StructField> schema = buildCaseInsensitiveSchema(metadata.getSchema().fields());
            Map<String, String> names = Maps.newHashMap();
            // partition Values use column physical name(using in column mapping) as key, we need to convert it to
            // logical name
            for (String partitionColumn : metadata.getPartitionColNames()) {
                if (schema.get(partitionColumn) == null) {
                    throw new StarRocksConnectorException("Partition column " + partitionColumn + " not found in schema");
                }
                names.put(ColumnMapping.getPhysicalName(schema.get(partitionColumn)), partitionColumn);
            }
            this.physicalToLogicalPartitionName = names;
        }

        private String resolve(String tableRoot, String child) {
            if (!tableRoot.equals(lastRoot)) {
                lastRootPrefix = rootPrefix(tableRoot);
                lastRoot = tableRoot;
            }
            return resolveEncodedPath(tableRoot, child, lastRootPrefix);
        }
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
        return convertFromRowToFileScanTask(needStats, file, new RowContext(metadata), estimateRowSize, dv);
    }

    public static Pair<FileScanTask, DeltaLakeAddFileStatsSerDe> convertFromRowToFileScanTask(
            boolean needStats, Row file, RowContext context, long estimateRowSize, DeletionVectorDescriptor dv) {
        FileStatus fileStatus = getEncodedAddFileStatus(file, context);
        Map<String, String> partitionValues = InternalScanFileUtils.getPartitionValues(file);
        // convert physical column name to partition logical column name
        Map<String, String> logicalPartitionValues = Maps.newHashMap();
        for (Map.Entry<String, String> entry : partitionValues.entrySet()) {
            logicalPartitionValues.put(context.physicalToLogicalPartitionName.get(entry.getKey()), entry.getValue());
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
