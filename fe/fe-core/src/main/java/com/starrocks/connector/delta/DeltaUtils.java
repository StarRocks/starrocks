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

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.Type;
import com.starrocks.connector.ColumnTypeConverter;
import com.starrocks.connector.delta.cache.CachingDeltaLogImpl;
import com.starrocks.connector.delta.cache.DeltaLakeTableName;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.IHiveMetastore;
import com.starrocks.connector.hive.RemoteFileInputFormat;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.DataType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;

public class DeltaUtils {
    private static final Logger LOG = LogManager.getLogger(DeltaUtils.class);

    private static final Pattern DELTA_FILE_PATTERN = Pattern
            .compile("\\d+\\.json");
    private static final Pattern CHECKPOINT_FILE_PATTERN = Pattern
            .compile("\\d+\\.checkpoint(\\.\\d+\\.\\d+)?\\.parquet");

    public static DeltaLakeTable convertDeltaToSRTable(String catalog, String dbName, String tblName, String path,
                                                       Configuration configuration, long createTime, IHiveMetastore metastore) {
        DeltaLog deltaLog;
        if (metastore instanceof CachingDeltaLakeMetadata) {
            try {
                DeltaLakeTableName tbl = new DeltaLakeTableName(catalog, dbName, tblName, path);
                deltaLog = CachingDeltaLogImpl.forTable(configuration, (CachingDeltaLakeMetadata) metastore, tbl);
            } catch (IOException e) {
                throw new IllegalArgumentException(String
                        .format("Deltalake Table load fail for %s.%s.%s. error message:%s",
                                catalog, dbName, tblName, e.getMessage()));
            }
        } else {
            deltaLog = DeltaLog.forTable(configuration, path);
        }

        if (!deltaLog.tableExists()) {
            throw new IllegalArgumentException(String.format("Delta log not exist for %s.%s.%s",
                    catalog, dbName, tblName));
        }

        Metadata metadata = deltaLog.snapshot().getMetadata();
        StructType tableSchema = metadata.getSchema();
        List<Column> fullSchema = Lists.newArrayList();

        if (tableSchema == null) {
            throw new IllegalArgumentException(String.format("Unable to find Schema information in Delta log for " +
                    "%s.%s.%s", catalog, dbName, tblName));
        }

        for (StructField field : metadata.getSchema().getFields()) {
            DataType dataType = field.getDataType();
            Type type;
            try {
                type = ColumnTypeConverter.fromDeltaLakeType(dataType);
            } catch (InternalError | Exception e) {
                LOG.error("Failed to convert delta type {} on {}.{}.{}", dataType.getTypeName(), catalog, dbName, tblName, e);
                type = Type.UNKNOWN_TYPE;
            }
            Column column = new Column(field.getName(), type, true);
            fullSchema.add(column);
        }

        return new DeltaLakeTable(CONNECTOR_ID_GENERATOR.getNextId().asInt(), catalog, dbName, tblName,
                fullSchema, metadata.getPartitionColumns(), deltaLog, createTime);
    }

    public static RemoteFileInputFormat getRemoteFileFormat(String format) {
        if (format.equalsIgnoreCase("ORC")) {
            return RemoteFileInputFormat.ORC;
        } else if (format.equalsIgnoreCase("PARQUET")) {
            return RemoteFileInputFormat.PARQUET;
        } else {
            throw new StarRocksConnectorException("Unexpected file format: " + format);
        }
    }

    public static long lastSnapshotVersion(Configuration hadoopConf, String path) throws IOException {
        Path logPath = new Path(path, "_delta_log");
        FileSystem fs = FileSystem.get(logPath.toUri(), hadoopConf);
        long lastSnapshotVersion = 0;
        long fileVersion;
        FileStatus[] fileStatuses = fs.listStatus(logPath);
        Path file;
        for (FileStatus fileStatus : fileStatuses) {
            file = fileStatus.getPath();
            if (isCheckpointFile(file) || isDeltaFile(file)) {
                fileVersion = getFileVersion(file);
                lastSnapshotVersion = Math.max(fileVersion, lastSnapshotVersion);
            }
        }

        return lastSnapshotVersion;

    }

    private static long getFileVersion(Path path) {
        String version = null;
        if (isCheckpointFile(path)) {
            version = path.getName().split("\\.")[0];
        } else if (isDeltaFile(path)) {
            version = path.getName().replace(".json", "");
        }
        if (null == version) {
            throw new AssertionError(
                    "Unexpected file type found in transaction log: " + path);
        }
        return Long.parseLong(version);
    }

    private static boolean isCheckpointFile(Path path) {
        return CHECKPOINT_FILE_PATTERN.matcher(path.getName()).matches();
    }

    private static boolean isDeltaFile(Path path) {
        return DELTA_FILE_PATTERN.matcher(path.getName()).matches();
    }
}
