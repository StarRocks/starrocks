// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveMetaStoreTableInfo;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.HudiTable;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.common.IdGenerator;
import com.starrocks.connector.ConnectorDatabaseId;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.external.hive.HiveColumnStats;
import com.starrocks.external.hive.HivePartition;
import com.starrocks.external.hive.HivePartitionStats;
import com.starrocks.external.hive.HiveTableStats;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.PlannerProfile;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HiveMetaStoreTableUtils {
    private static final Logger LOG = LogManager.getLogger(HiveMetaStoreTableUtils.class);
    public static final IdGenerator<ConnectorTableId> CONNECTOR_TABLE_ID_ID_GENERATOR =
            ConnectorTableId.createGenerator();
    public static final IdGenerator<ConnectorDatabaseId> CONNECTOR_DATABASE_ID_ID_GENERATOR =
            ConnectorDatabaseId.createGenerator();
    protected static final List<String> HIVE_UNSUPPORTED_TYPES = Arrays.asList("STRUCT", "BINARY", "MAP", "UNIONTYPE");

    public static Map<String, HiveColumnStats> getTableLevelColumnStats(HiveMetaStoreTableInfo hmsTable,
                                                                        List<String> columnNames) throws DdlException {
        try (PlannerProfile.ScopedTimer _ = PlannerProfile.getScopedTimer("HMS.tableColumnStats")) {
            Map<String, HiveColumnStats> allColumnStats = GlobalStateMgr.getCurrentState().getHiveRepository()
                    .getTableLevelColumnStats(hmsTable);
            Map<String, HiveColumnStats> result = Maps.newHashMapWithExpectedSize(columnNames.size());
            for (String columnName : columnNames) {
                result.put(columnName, allColumnStats.get(columnName));
            }
            return result;
        }
    }

    public static List<Column> getPartitionColumns(HiveMetaStoreTableInfo hmsTable) {
        List<Column> partColumns = Lists.newArrayList();
        for (String columnName : hmsTable.getPartColumnNames()) {
            partColumns.add(hmsTable.getNameToColumn().get(columnName));
        }
        return partColumns;
    }

    public static List<String> getAllColumnNames(HiveMetaStoreTableInfo hmsTable) {
        return new ArrayList<>(hmsTable.getNameToColumn().keySet());
    }

    public static List<HivePartitionStats> getPartitionsStats(HiveMetaStoreTableInfo hmsTable,
                                                              List<PartitionKey> partitionKeys) throws DdlException {
        try (PlannerProfile.ScopedTimer _ = PlannerProfile.getScopedTimer("HMS.partitionStats")) {
            return GlobalStateMgr.getCurrentState().getHiveRepository().getPartitionsStats(hmsTable, partitionKeys);
        }
    }

    public static HiveTableStats getTableStats(HiveMetaStoreTableInfo hmsTable) throws DdlException {
        try (PlannerProfile.ScopedTimer _ = PlannerProfile.getScopedTimer("HMS.tableStats")) {
            return GlobalStateMgr.getCurrentState().getHiveRepository().getTableStats(hmsTable.getResourceName(),
                    hmsTable.getDb(), hmsTable.getTable());
        }
    }

    public static List<HivePartition> getPartitions(HiveMetaStoreTableInfo hmsTable,
                                                    List<PartitionKey> partitionKeys) throws DdlException {
        try (PlannerProfile.ScopedTimer _ = PlannerProfile.getScopedTimer("HMS.partitions")) {
            return GlobalStateMgr.getCurrentState().getHiveRepository()
                    .getPartitions(hmsTable, partitionKeys);
        }
    }

    public static Map<PartitionKey, Long> getPartitionKeys(HiveMetaStoreTableInfo hmsTable) throws DdlException {
        try (PlannerProfile.ScopedTimer _ = PlannerProfile.getScopedTimer("HMS.partitionKeys")) {
            return GlobalStateMgr.getCurrentState().getHiveRepository().getPartitionKeys(hmsTable);
        }
    }

    public static long getPartitionStatsRowCount(HiveMetaStoreTableInfo hmsTable,
                                                 List<PartitionKey> partitions) {
        try (PlannerProfile.ScopedTimer _ = PlannerProfile.getScopedTimer("HMS.partitionRowCount")) {
            return doGetPartitionStatsRowCount(hmsTable, partitions);
        }
    }

    public static List<FieldSchema> getAllHiveColumns(Table table) {
        ImmutableList.Builder<FieldSchema> allColumns = ImmutableList.builder();
        List<FieldSchema> unHivePartColumns = table.getSd().getCols();
        List<FieldSchema> partHiveColumns = table.getPartitionKeys();
        return allColumns.addAll(unHivePartColumns).addAll(partHiveColumns).build();
    }

    public static boolean validateColumnType(String hiveType, Type type) {
        if (hiveType == null) {
            return false;
        }

        // for type with length, like char(10), we only check the type and ignore the length
        String typeUpperCase = ColumnTypeConverter.getTypeKeyword(hiveType).toUpperCase();
        PrimitiveType primitiveType = type.getPrimitiveType();
        switch (typeUpperCase) {
            case "TINYINT":
                return primitiveType == PrimitiveType.TINYINT;
            case "SMALLINT":
                return primitiveType == PrimitiveType.SMALLINT;
            case "INT":
            case "INTEGER":
                return primitiveType == PrimitiveType.INT;
            case "BIGINT":
                return primitiveType == PrimitiveType.BIGINT;
            case "FLOAT":
                return primitiveType == PrimitiveType.FLOAT;
            case "DOUBLE":
            case "DOUBLE PRECISION":
                return primitiveType == PrimitiveType.DOUBLE;
            case "DECIMAL":
            case "NUMERIC":
                return primitiveType == PrimitiveType.DECIMALV2 || primitiveType == PrimitiveType.DECIMAL32 ||
                        primitiveType == PrimitiveType.DECIMAL64 || primitiveType == PrimitiveType.DECIMAL128;
            case "TIMESTAMP":
                return primitiveType == PrimitiveType.DATETIME;
            case "DATE":
                return primitiveType == PrimitiveType.DATE;
            case "STRING":
            case "VARCHAR":
                return primitiveType == PrimitiveType.VARCHAR;
            case "CHAR":
                return primitiveType == PrimitiveType.CHAR ||
                        primitiveType == PrimitiveType.VARCHAR;
            case "BOOLEAN":
                return primitiveType == PrimitiveType.BOOLEAN;
            case "ARRAY":
                if (type.equals(Type.UNKNOWN_TYPE)) {
                    return !HIVE_UNSUPPORTED_TYPES.stream().filter(hiveType.toUpperCase()::contains)
                            .collect(Collectors.toList()).isEmpty();
                }
                if (!type.isArrayType()) {
                    return false;
                }
                return validateColumnType(hiveType.substring(hiveType.indexOf('<') + 1, hiveType.length() - 1),
                        ((ArrayType) type).getItemType());
            case "MAP":
                if (!type.isMapType()) {
                    return false;
                }
                String[] kvStr = ColumnTypeConverter.getKeyValueStr(hiveType);
                return validateColumnType(kvStr[0], ((MapType) type).getKeyType()) &&
                        validateColumnType(kvStr[1], ((MapType) type).getValueType());

            default:
                // for BINARY and other types, we transfer it to UNKNOWN_TYPE
                return primitiveType == PrimitiveType.UNKNOWN_TYPE;
        }
    }

    // this func targets at convert hudi column type(avroSchema) to starrocks column type(primitiveType)
    public static Type convertHudiTableColumnType(Schema avroSchema) throws DdlException {
        Schema.Type columnType = avroSchema.getType();
        LogicalType logicalType = avroSchema.getLogicalType();
        PrimitiveType primitiveType = null;
        boolean isConvertedFailed = false;

        switch (columnType) {
            case BOOLEAN:
                primitiveType = PrimitiveType.BOOLEAN;
                break;
            case INT:
                if (logicalType instanceof LogicalTypes.Date) {
                    primitiveType = PrimitiveType.DATE;
                } else if (logicalType instanceof LogicalTypes.TimeMillis) {
                    primitiveType = PrimitiveType.TIME;
                } else {
                    primitiveType = PrimitiveType.INT;
                }
                break;
            case LONG:
                if (logicalType instanceof LogicalTypes.TimeMicros) {
                    primitiveType = PrimitiveType.TIME;
                } else if (logicalType instanceof LogicalTypes.TimestampMillis
                        || logicalType instanceof LogicalTypes.TimestampMicros) {
                    primitiveType = PrimitiveType.DATETIME;
                } else {
                    primitiveType = PrimitiveType.BIGINT;
                }
                break;
            case FLOAT:
                primitiveType = PrimitiveType.FLOAT;
                break;
            case DOUBLE:
                primitiveType = PrimitiveType.DOUBLE;
                break;
            case STRING:
                return ScalarType.createDefaultString();
            case ARRAY:
                Type type = convertToArrayType(avroSchema);
                if (type.isArrayType()) {
                    return type;
                } else {
                    isConvertedFailed = false;
                    break;
                }
            case FIXED:
            case BYTES:
                if (logicalType instanceof LogicalTypes.Decimal) {
                    int precision = 0;
                    int scale = 0;
                    if (avroSchema.getObjectProp("precision") instanceof Integer) {
                        precision = (int) avroSchema.getObjectProp("precision");
                    }
                    if (avroSchema.getObjectProp("scale") instanceof Integer) {
                        scale = (int) avroSchema.getObjectProp("scale");
                    }
                    return ScalarType.createUnifiedDecimalType(precision, scale);
                } else {
                    primitiveType = PrimitiveType.VARCHAR;
                    break;
                }
            case UNION:
                List<Schema> nonNullMembers = avroSchema.getTypes().stream()
                        .filter(schema -> !Schema.Type.NULL.equals(schema.getType()))
                        .collect(Collectors.toList());

                if (nonNullMembers.size() == 1) {
                    return convertHudiTableColumnType(nonNullMembers.get(0));
                } else {
                    isConvertedFailed = true;
                    break;
                }
            case ENUM:
            case MAP:
            default:
                isConvertedFailed = true;
                break;
        }

        if (isConvertedFailed) {
            primitiveType = PrimitiveType.UNKNOWN_TYPE;
        }

        return ScalarType.createType(primitiveType);
    }

    private static ArrayType convertToArrayType(Schema typeSchema) throws DdlException {
        return new ArrayType(convertHudiTableColumnType(typeSchema.getElementType()));
    }

    // In the first phase of connector, in order to reduce changes, we use `hive.metastore.uris` as resource name
    // for table of external catalog. The table of external catalog will not create a real resource.
    // We will reconstruct this part later. The concept of resource will not be used for external catalog in the future.
    public static HiveTable convertHiveConnTableToSRTable(Table hiveTable, String resoureName) throws DdlException {
        if (hiveTable.getTableType().equals("VIRTUAL_VIEW")) {
            throw new DdlException("Hive view table is not supported.");
        }

        List<FieldSchema> allHiveColumns = getAllHiveColumns(hiveTable);
        List<Column> fullSchema = Lists.newArrayList();
        for (FieldSchema fieldSchema : allHiveColumns) {
            Type srType = ColumnTypeConverter.fromHiveType(fieldSchema.getType());
            Column column = new Column(fieldSchema.getName(), srType, true);
            fullSchema.add(column);
        }

        Map<String, String> properties = Maps.newHashMap();
        properties.put(HiveTable.HIVE_DB, hiveTable.getDbName());
        properties.put(HiveTable.HIVE_TABLE, hiveTable.getTableName());
        properties.put(HiveTable.HIVE_METASTORE_URIS, resoureName);
        properties.put(HiveTable.HIVE_RESOURCE, resoureName);

        return new HiveTable(CONNECTOR_TABLE_ID_ID_GENERATOR.getNextId().asInt(), hiveTable.getTableName(),
                fullSchema, properties, hiveTable);
    }

    public static HudiTable convertHudiConnTableToSRTable(Table hmsTable, String resourceName)
            throws DdlException {
        Schema hudiSchema = HudiTable.loadHudiSchema(hmsTable);
        List<Schema.Field> allHudiColumns = hudiSchema.getFields();
        List<Column> fullSchema = Lists.newArrayList();
        for (Schema.Field fieldSchema : allHudiColumns) {
            Type srType = convertHudiTableColumnType(fieldSchema.schema());
            if (srType == null) {
                throw new DdlException("Can not convert hudi column type [" + fieldSchema.schema().toString() + "] " +
                        "to starrocks type.");
            }
            Column column = new Column(fieldSchema.name(), srType, true);
            fullSchema.add(column);
        }

        Map<String, String> properties = Maps.newHashMap();
        properties.put(HudiTable.HUDI_DB, hmsTable.getDbName());
        properties.put(HudiTable.HUDI_TABLE, hmsTable.getTableName());
        properties.put(HudiTable.HUDI_RESOURCE, resourceName);

        return new HudiTable(CONNECTOR_TABLE_ID_ID_GENERATOR.getNextId().asInt(), hudiSchema.getName(),
                fullSchema, properties);
    }

    public static Database convertToSRDatabase(String dbName) {
        return new Database(CONNECTOR_DATABASE_ID_ID_GENERATOR.getNextId().asInt(), dbName);
    }

    public static long doGetPartitionStatsRowCount(HiveMetaStoreTableInfo hmsTable,
                                                   List<PartitionKey> partitions) {
        if (partitions == null) {
            try {
                partitions = Lists.newArrayList(getPartitionKeys(hmsTable).keySet());
            } catch (DdlException e) {
                LOG.warn("Failed to get table {} partitions.", hmsTable.getTable(), e);
                return -1;
            }
        }
        if (partitions.isEmpty()) {
            return 0;
        }

        long numRows = -1;

        List<HivePartitionStats> partitionsStats = Lists.newArrayList();
        try {
            partitionsStats = getPartitionsStats(hmsTable, partitions);
        } catch (DdlException e) {
            LOG.warn("Failed to get table {} partitions stats.", hmsTable.getTable(), e);
        }

        for (int i = 0; i < partitionsStats.size(); i++) {
            long partNumRows = partitionsStats.get(i).getNumRows();
            long partTotalFileBytes = partitionsStats.get(i).getTotalFileBytes();
            // -1: missing stats
            if (partNumRows > -1) {
                if (numRows == -1) {
                    numRows = 0;
                }
                numRows += partNumRows;
            } else {
                LOG.debug("Table {} partition {} stats is invalid. num rows: {}, total file bytes: {}",
                        hmsTable.getTable(), partitions.get(i), partNumRows, partTotalFileBytes);
            }
        }
        return numRows;
    }

    // In the first phase of connector, in order to reduce changes, we use `hive.metastore.uris` as resource name
    // for table of external catalog. The table of external catalog will not create a real resource.
    // We will reconstruct this part later. The concept of resource will not be used for external catalog
    public static boolean isInternalCatalog(String resourceName) {
        return !resourceName.startsWith("thrift://");
    }
}
