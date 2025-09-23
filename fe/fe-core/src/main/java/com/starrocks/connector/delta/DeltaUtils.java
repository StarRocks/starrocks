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
import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.internal.util.Preconditions;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

import static com.starrocks.catalog.Column.COLUMN_UNIQUE_ID_INIT_VALUE;
import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;
import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;

public class DeltaUtils {
    private static final Logger LOG = LogManager.getLogger(DeltaUtils.class);

    public static void checkProtocolAndMetadata(Protocol protocol, Metadata metadata) {
        if (protocol == null || metadata == null) {
            LOG.error("Delta table is missing protocol or metadata information.");
            ErrorReport.reportValidateException(ErrorCode.ERR_BAD_TABLE_ERROR, ErrorType.UNSUPPORTED,
                    "Delta table is missing protocol or metadata information.");
        }
    }

    public static DeltaLakeTable convertDeltaToSRTable(String catalog, String dbName, String tblName, String path,
                                                       Engine deltaEngine, long createTime) {
        SnapshotImpl snapshot;

        try (Timer ignored = Tracers.watchScope(EXTERNAL, "DeltaLake.getSnapshot")) {
            Table deltaTable = Table.forPath(deltaEngine, path);
            snapshot = (SnapshotImpl) deltaTable.getLatestSnapshot(deltaEngine);
        } catch (TableNotFoundException e) {
            LOG.error("Failed to find Delta table for {}.{}.{}, {}. caused by : {}", catalog, dbName, tblName,
                    e.getMessage(), e.getCause());
            throw new SemanticException("Failed to find Delta table for %s.%s.%s, %s. caused by : %s", catalog,
                    dbName, tblName, e.getMessage(), e.getCause());
        } catch (Exception e) {
            LOG.error("Failed to get latest snapshot for {}.{}.{}, {}. caused by : {}", catalog, dbName,
                    tblName, e.getMessage(), e.getCause());
            throw new SemanticException("Failed to get latest snapshot for %s.%s.%s, %s. caused by : %s",
                    catalog, dbName, tblName, e.getMessage(), e.getCause());
        }

        StructType deltaSchema = snapshot.getSchema(deltaEngine);
        if (deltaSchema == null) {
            throw new IllegalArgumentException(String.format("Unable to find Schema information in Delta log for " +
                    "%s.%s.%s", catalog, dbName, tblName));
        }

        String columnMappingMode = ColumnMapping.getColumnMappingMode(snapshot.getMetadata().getConfiguration());
        List<Column> fullSchema = Lists.newArrayList();
        for (StructField field : deltaSchema.fields()) {
            DataType dataType = field.getDataType();
            Type type;
            try {
                type = ColumnTypeConverter.fromDeltaLakeType(dataType, columnMappingMode);
            } catch (InternalError | Exception e) {
                LOG.error("Failed to convert delta type {} on {}.{}.{}", dataType.toString(), catalog, dbName, tblName, e);
                type = Type.UNKNOWN_TYPE;
            }
            Column column = buildColumnWithColumnMapping(field, type, columnMappingMode);
            fullSchema.add(column);
        }

        return new DeltaLakeTable(CONNECTOR_ID_GENERATOR.getNextId().asInt(), catalog, dbName, tblName, fullSchema,
                loadPartitionColumnNames(snapshot), snapshot, path,
                deltaEngine, createTime);
    }

    public static List<String> loadPartitionColumnNames(SnapshotImpl snapshot) {
        ArrayValue partitionColumns = snapshot.getMetadata().getPartitionColumns();
        ColumnVector partitionColNameVector = partitionColumns.getElements();
        List<String> partitionColumnNames = Lists.newArrayList();
        for (int i = 0; i < partitionColumns.getSize(); i++) {
            Preconditions.checkArgument(!partitionColNameVector.isNullAt(i),
                    "Expected a non-null partition column name");
            String partitionColName = partitionColNameVector.getString(i);
            Preconditions.checkArgument(partitionColName != null && !partitionColName.isEmpty(),
                    "Expected non-null and non-empty partition column name");
            partitionColumnNames.add(partitionColName);
        }
        return partitionColumnNames;
    }

    public static Column buildColumnWithColumnMapping(StructField field, Type type, String columnMappingMode) {
        String columnName = field.getName();
        int columnUniqueId = COLUMN_UNIQUE_ID_INIT_VALUE;
        String physicalName = "";

        if (columnMappingMode.equalsIgnoreCase(ColumnMapping.COLUMN_MAPPING_MODE_ID) &&
                field.getMetadata().contains(ColumnMapping.COLUMN_MAPPING_ID_KEY)) {
            columnUniqueId = ((Long)  field.getMetadata().get(ColumnMapping.COLUMN_MAPPING_ID_KEY)).intValue();
        }
        if (columnMappingMode.equalsIgnoreCase(ColumnMapping.COLUMN_MAPPING_MODE_NAME) &&
                field.getMetadata().contains(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY)) {
            physicalName = (String) field.getMetadata().get(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY);
        }
        return new Column(columnName, type, false, null, true,
                null, "", columnUniqueId, physicalName);
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
