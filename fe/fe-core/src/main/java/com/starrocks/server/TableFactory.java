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


package com.starrocks.server;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FileTable;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.HudiTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Resource;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.connector.ColumnTypeConverter;
import com.starrocks.sql.ast.CreateTableStmt;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.iceberg.types.Types;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.catalog.Resource.ResourceType.HIVE;
import static com.starrocks.catalog.Resource.ResourceType.HUDI;
import static com.starrocks.catalog.Resource.ResourceType.ICEBERG;
import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.getResourceMappingCatalogName;

public class TableFactory {
    public static final String DB = "database";
    public static final String TABLE = "table";
    public static final String RESOURCE = "resource";
    public static final String PROPERTY_MISSING_MSG =
            "Hive %s is null. Please add properties('%s'='xxx') when create table";

    public static Table createTable(CreateTableStmt stmt, Table.TableType type) throws DdlException {
        Table table;
        switch (type) {
            case HIVE:
                table = createHiveTable(stmt);
                break;
            case ICEBERG:
                table = createIcebergTable(stmt);
                break;
            case HUDI:
                table = createHudiTable(stmt);
                break;
            case FILE:
                table = createFileTable(stmt);
                break;
            default:
                throw new DdlException("Unsupported table type " + type);
        }
        return table;
    }

    private static HiveTable createHiveTable(CreateTableStmt stmt) throws DdlException {
        GlobalStateMgr gsm = GlobalStateMgr.getCurrentState();
        String tableName = stmt.getTableName();
        List<Column> columns = stmt.getColumns();
        Map<String, String> properties = stmt.getProperties();
        long tableId = gsm.getNextId();
        Table table = getTableFromResourceMappingCatalog(properties, Table.TableType.HIVE, HIVE);
        if (table == null) {
            throw new DdlException("Can not find hive table "
                    + properties.get(DB) + "." + properties.get(TABLE)
                    + " from the resource " + properties.get(RESOURCE));
        }
        HiveTable oHiveTable = (HiveTable) table;

        validateHiveColumnType(columns, oHiveTable);

        HiveTable.Builder tableBuilder = HiveTable.builder()
                .setId(tableId)
                .setTableName(tableName)
                .setCatalogName(oHiveTable.getCatalogName())
                .setResourceName(oHiveTable.getResourceName())
                .setHiveDbName(oHiveTable.getDbName())
                .setHiveTableName(oHiveTable.getTableName())
                .setPartitionColumnNames(oHiveTable.getPartitionColumnNames())
                .setDataColumnNames(oHiveTable.getDataColumnNames())
                .setFullSchema(columns)
                .setTableLocation(oHiveTable.getTableLocation())
                .setCreateTime(oHiveTable.getCreateTime());

        HiveTable hiveTable = tableBuilder.build();

        // partition key, commented for show partition key
        if (Strings.isNullOrEmpty(stmt.getComment()) && hiveTable.getPartitionColumnNames().size() > 0) {
            String partitionCmt = "PARTITION BY (" + String.join(", ", hiveTable.getPartitionColumnNames()) + ")";
            hiveTable.setComment(partitionCmt);
        } else if (!Strings.isNullOrEmpty(stmt.getComment())) {
            hiveTable.setComment(stmt.getComment());
        }
        return hiveTable;
    }

    private static IcebergTable createIcebergTable(CreateTableStmt stmt) throws DdlException {
        GlobalStateMgr gsm = GlobalStateMgr.getCurrentState();
        String tableName = stmt.getTableName();
        List<Column> columns = stmt.getColumns();
        Map<String, String> properties = stmt.getProperties();
        long tableId = gsm.getNextId();

        IcebergTable oIcebergTable = (IcebergTable) getTableFromResourceMappingCatalog(
                properties, Table.TableType.ICEBERG, ICEBERG);

        if (oIcebergTable == null) {
            throw new DdlException("Can not find iceberg table "
                    + properties.get(DB) + "." + properties.get(TABLE)
                    + " from the resource " + properties.get(RESOURCE));
        }

        validateIcebergColumnType(columns, oIcebergTable);

        IcebergTable.Builder tableBuilder = IcebergTable.builder()
                .setId(tableId)
                .setSrTableName(tableName)
                .setCatalogName(oIcebergTable.getCatalogName())
                .setResourceName(oIcebergTable.getResourceName())
                .setRemoteDbName(oIcebergTable.getRemoteDbName())
                .setRemoteTableName(oIcebergTable.getRemoteTableName())
                .setFullSchema(columns)
                .setIcebergProperties(properties)
                .setNativeTable(oIcebergTable.getNativeTable());

        IcebergTable icebergTable = tableBuilder.build();

        // partition key, commented for show partition key
        if (Strings.isNullOrEmpty(stmt.getComment()) && !icebergTable.isUnPartitioned()) {
            String partitionCmt = "PARTITION BY (" + String.join(", ", icebergTable.getPartitionColumnNames()) + ")";
            icebergTable.setComment(partitionCmt);
        } else if (!Strings.isNullOrEmpty(stmt.getComment())) {
            icebergTable.setComment(stmt.getComment());
        }
        return icebergTable;
    }

    private static HudiTable createHudiTable(CreateTableStmt stmt) throws DdlException {
        GlobalStateMgr gsm = GlobalStateMgr.getCurrentState();
        String tableName = stmt.getTableName();
        List<Column> columns = stmt.getColumns();
        long tableId = gsm.getNextId();
        Map<String, String> properties = stmt.getProperties();

        Set<String> metaFields = new HashSet<>(Arrays.asList(
                HoodieRecord.COMMIT_TIME_METADATA_FIELD,
                HoodieRecord.COMMIT_SEQNO_METADATA_FIELD,
                HoodieRecord.RECORD_KEY_METADATA_FIELD,
                HoodieRecord.PARTITION_PATH_METADATA_FIELD,
                HoodieRecord.FILENAME_METADATA_FIELD));
        Set<String> includedMetaFields = columns.stream().map(Column::getName)
                .filter(metaFields::contains).collect(Collectors.toSet());
        metaFields.removeAll(includedMetaFields);
        metaFields.forEach(f -> columns.add(new Column(f, Type.STRING, true)));

        Table table = getTableFromResourceMappingCatalog(properties, Table.TableType.HUDI, HUDI);
        if (table == null) {
            throw new DdlException("Can not find hudi table "
                    + properties.get(DB) + "." + properties.get(TABLE)
                    + " from the resource " + properties.get(RESOURCE));
        }
        HudiTable oHudiTable = (HudiTable) table;
        validateHudiColumnType(columns, oHudiTable);

        HudiTable.Builder tableBuilder = HudiTable.builder()
                .setId(tableId)
                .setTableName(tableName)
                .setCatalogName(oHudiTable.getCatalogName())
                .setResourceName(oHudiTable.getResourceName())
                .setHiveDbName(oHudiTable.getDbName())
                .setHiveTableName(oHudiTable.getTableName())
                .setFullSchema(columns)
                .setPartitionColNames(oHudiTable.getPartitionColumnNames())
                .setDataColNames(oHudiTable.getDataColumnNames())
                .setHudiProperties(oHudiTable.getProperties())
                .setCreateTime(oHudiTable.getCreateTime());

        HudiTable hudiTable = tableBuilder.build();

        // partition key, commented for show partition key
        if (Strings.isNullOrEmpty(stmt.getComment()) && hudiTable.getPartitionColumnNames().size() > 0) {
            String partitionCmt = "PARTITION BY (" + String.join(", ", hudiTable.getPartitionColumnNames()) + ")";
            hudiTable.setComment(partitionCmt);
        } else if (!Strings.isNullOrEmpty(stmt.getComment())) {
            hudiTable.setComment(stmt.getComment());
        }
        return hudiTable;
    }

    private static FileTable createFileTable(CreateTableStmt stmt) throws DdlException {
        GlobalStateMgr gsm = GlobalStateMgr.getCurrentState();
        String tableName = stmt.getTableName();
        List<Column> columns = stmt.getColumns();
        Map<String, String> properties = stmt.getProperties();
        long tableId = gsm.getNextId();

        FileTable.Builder tableBuilder = FileTable.builder()
                .setId(tableId)
                .setTableName(tableName)
                .setFullSchema(columns)
                .setProperties(properties);

        FileTable fileTable = tableBuilder.build();

        if (!Strings.isNullOrEmpty(stmt.getComment())) {
            fileTable.setComment(stmt.getComment());
        }
        return fileTable;
    }

    private static void validateHiveColumnType(List<Column> columns, Table oTable) throws DdlException {
        for (Column column : columns) {
            Column oColumn = oTable.getColumn(column.getName());
            if (oColumn == null) {
                throw new DdlException("column [" + column.getName() + "] not exists in hive");
            }

            if (oColumn.getType() == Type.UNKNOWN_TYPE) {
                throw new DdlException("Column type convert failed on column: " + column.getName());
            }

            if (!ColumnTypeConverter.validateColumnType(column.getType(), oColumn.getType()) &&
                    !FeConstants.runningUnitTest) {
                throw new DdlException("can not convert hive external table column type [" + column.getType() + "] " +
                        "to correct type [" + oColumn.getType() + "]");
            }

            for (String partName : oTable.getPartitionColumnNames()) {
                if (!columns.stream().map(Column::getName).collect(Collectors.toList()).contains(partName)) {
                    throw new DdlException("partition column [" + partName + "] must exist in column list");
                }
            }
        }
    }

    private static void validateIcebergColumnType(List<Column> columns, IcebergTable oTable) throws DdlException {
        for (Column column : columns) {
            Map<String, Types.NestedField> icebergColumns = oTable.getNativeTable().schema().columns().stream()
                    .collect(Collectors.toMap(Types.NestedField::name, field -> field));
            if (!icebergColumns.containsKey(column.getName())) {
                throw new DdlException("column [" + column.getName() + "] not exists in iceberg");
            }

            Column oColumn = oTable.getColumn(column.getName());
            if (oColumn.getType() == Type.UNKNOWN_TYPE) {
                throw new DdlException("Column type convert failed on column: " + column.getName());
            }

            if (!ColumnTypeConverter.validateColumnType(column.getType(), oColumn.getType()) &&
                    !FeConstants.runningUnitTest) {
                throw new DdlException("can not convert iceberg external table column type [" + column.getType() + "] " +
                        "to correct type [" + oColumn.getType() + "]");
            }

            if (!column.isAllowNull()) {
                throw new DdlException("iceberg extern table not support no-nullable column: [" + column.getName() + "]");
            }

            for (String partName : oTable.getPartitionColumnNames()) {
                if (!columns.stream().map(Column::getName).collect(Collectors.toList()).contains(partName)) {
                    throw new DdlException("partition column [" + partName + "] must exist in column list");
                }
            }
        }
    }

    private static void validateHudiColumnType(List<Column> columns, HudiTable oTable) throws DdlException {
        String hudiBasePath = oTable.getTableLocation();
        Configuration conf = new Configuration();
        HoodieTableMetaClient metaClient =
                HoodieTableMetaClient.builder().setConf(conf).setBasePath(hudiBasePath).build();
        TableSchemaResolver schemaUtil = new TableSchemaResolver(metaClient);
        Schema hudiTableSchema;
        try {
            hudiTableSchema = HoodieAvroUtils.createHoodieWriteSchema(schemaUtil.getTableAvroSchema());
        } catch (Exception e) {
            throw new DdlException("Cannot get hudi table schema.");
        }

        for (Column column : columns) {
            if (!column.isAllowNull()) {
                throw new DdlException(
                        "Hudi extern table does not support no-nullable column: [" + column.getName() + "]");
            }
            Schema.Field hudiColumn = hudiTableSchema.getField(column.getName());
            if (hudiColumn == null) {
                throw new DdlException("Column [" + column.getName() + "] not exists in hudi.");
            }
            Column oColumn = oTable.getColumn(column.getName());

            if (oColumn.getType() == Type.UNKNOWN_TYPE) {
                throw new DdlException("Column type convert failed on column: " + column.getName());
            }

            if (!ColumnTypeConverter.validateColumnType(column.getType(), oColumn.getType())) {
                throw new DdlException("can not convert hudi external table column type [" + column.getPrimitiveType() + "] " +
                        "to correct type [" + oColumn.getPrimitiveType() + "]");
            }
        }

        List<String> columnNames = columns.stream().map(Column::getName).collect(Collectors.toList());
        for (String partitionName : oTable.getPartitionColumnNames()) {
            if (!columnNames.contains(partitionName)) {
                throw new DdlException("Partition column [" + partitionName + "] must exist in column list");
            }
        }
    }

    public static void checkResource(String resourceName, Resource.ResourceType type) throws DdlException {
        Resource resource = GlobalStateMgr.getCurrentState().getResourceMgr().getResource(resourceName);
        if (resource == null) {
            throw new DdlException(type + " resource [" + resourceName + "] not exists");
        }

        if (resource.getType() != type) {
            throw new DdlException(resource.getType().name() + " resource [" + resourceName + "] is not " + type + " resource");
        }
    }

    public static Table getTableFromResourceMappingCatalog(Map<String, String> properties,
                                                           Table.TableType tableType,
                                                           Resource.ResourceType resourceType) throws DdlException {
        GlobalStateMgr gsm = GlobalStateMgr.getCurrentState();

        if (properties == null) {
            throw new DdlException("Please set properties of " + tableType.name() + " table, "
                    + "they are: database, table and resource");
        }

        Map<String, String> copiedProps = Maps.newHashMap(properties);
        String remoteDbName = copiedProps.get(DB);
        if (Strings.isNullOrEmpty(remoteDbName)) {
            throw new DdlException(String.format(PROPERTY_MISSING_MSG, DB, DB));
        }

        String remoteTableName = copiedProps.get(TABLE);
        if (Strings.isNullOrEmpty(remoteTableName)) {
            throw new DdlException(String.format(PROPERTY_MISSING_MSG, TABLE, TABLE));
        }

        // check properties
        // resource must be set
        String resourceName = copiedProps.get(RESOURCE);
        if (Strings.isNullOrEmpty(resourceName)) {
            throw new DdlException("property " + RESOURCE + " must be set");
        }

        checkResource(resourceName, resourceType);
        String resourceMappingCatalogName = getResourceMappingCatalogName(resourceName, resourceType.name());
        return gsm.getMetadataMgr().getTable(resourceMappingCatalogName, remoteDbName, remoteTableName);
    }
}