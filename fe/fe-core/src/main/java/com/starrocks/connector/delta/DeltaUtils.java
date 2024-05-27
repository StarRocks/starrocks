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
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.ColumnTypeConverter;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.RemoteFileInputFormat;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.common.ErrorType;
import io.delta.kernel.Table;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;
import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;

public class DeltaUtils {
    private static final Logger LOG = LogManager.getLogger(DeltaUtils.class);

    public static void checkTableFeatureSupported(Protocol protocol, Metadata metadata) {
        if (protocol == null || metadata == null) {
            LOG.error("Delta table is missing protocol or metadata information.");
            ErrorReport.reportValidateException(ErrorCode.ERR_BAD_TABLE_ERROR, ErrorType.UNSUPPORTED,
                    "Delta table is missing protocol or metadata information.");
        }
        // check column mapping
        String columnMappingMode = ColumnMapping.getColumnMappingMode(metadata.getConfiguration());
        if (!columnMappingMode.equals(ColumnMapping.COLUMN_MAPPING_MODE_NONE)) {
            LOG.error("Delta table feature column mapping is not supported");
            ErrorReport.reportValidateException(ErrorCode.ERR_BAD_TABLE_ERROR, ErrorType.UNSUPPORTED,
                    "Delta table feature [column mapping] is not supported");
        }
        // check timestampNtz type
        if (protocol.getReaderFeatures().contains("timestampNtz")) {
            LOG.error("Delta table feature timestampNtz is not supported");
            ErrorReport.reportValidateException(ErrorCode.ERR_BAD_TABLE_ERROR, ErrorType.UNSUPPORTED,
                    "Delta table feature [timestampNtz] is not supported");
        }
    }

    public static DeltaLakeTable convertDeltaToSRTable(String catalog, String dbName, String tblName, String path,
                                                       Configuration configuration, long createTime) {
        DefaultEngine deltaEngine = DefaultEngine.create(configuration);

        Table deltaTable = null;
        SnapshotImpl snapshot = null;

        try (Timer ignored = Tracers.watchScope(EXTERNAL, "DeltaLake.getSnapshot")) {
            deltaTable = Table.forPath(deltaEngine, path);
            snapshot = (SnapshotImpl) deltaTable.getLatestSnapshot(deltaEngine);
        } catch (TableNotFoundException e) {
            LOG.error("Failed to find Delta table for {}.{}.{}, {}", catalog, dbName, tblName, e.getMessage());
            throw new SemanticException("Failed to find Delta table for " + catalog + "." + dbName + "." + tblName);
        } catch (Exception e) {
            LOG.error("Failed to get latest snapshot for {}.{}.{}, {}", catalog, dbName, tblName, e.getMessage());
            throw new SemanticException("Failed to get latest snapshot for " + catalog + "." + dbName + "." + tblName);
        }

        StructType deltaSchema = snapshot.getSchema(deltaEngine);
        if (deltaSchema == null) {
            throw new IllegalArgumentException(String.format("Unable to find Schema information in Delta log for " +
                    "%s.%s.%s", catalog, dbName, tblName));
        }

        List<Column> fullSchema = Lists.newArrayList();
        for (StructField field : deltaSchema.fields()) {
            DataType dataType = field.getDataType();
            Type type;
            try {
                type = ColumnTypeConverter.fromDeltaLakeType(dataType);
            } catch (InternalError | Exception e) {
                LOG.error("Failed to convert delta type {} on {}.{}.{}", dataType.toString(), catalog, dbName, tblName, e);
                type = Type.UNKNOWN_TYPE;
            }
            Column column = new Column(field.getName(), type, true);
            fullSchema.add(column);
        }

        return new DeltaLakeTable(CONNECTOR_ID_GENERATOR.getNextId().asInt(), catalog, dbName, tblName,
                fullSchema, Lists.newArrayList(snapshot.getMetadata().getPartitionColNames()), snapshot, path,
                deltaEngine, createTime);
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
}
