// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveMetaStoreTableInfo;
import com.starrocks.catalog.HiveTable;
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
import com.starrocks.external.hive.Utils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.PlannerProfile;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HiveMetaStoreTableUtils {
    private static final Logger LOG = LogManager.getLogger(HiveMetaStoreTableUtils.class);
    private static final IdGenerator<ConnectorTableId> connectorTableIdIdGenerator = ConnectorTableId.createGenerator();
    private static final IdGenerator<ConnectorDatabaseId> connectorDbIdIdGenerator = ConnectorDatabaseId.createGenerator();

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

    public static Map<String, FieldSchema> getAllHiveColumns(Table table) {
        List<FieldSchema> unPartHiveColumns = table.getSd().getCols();
        List<FieldSchema> partHiveColumns = table.getPartitionKeys();
        Map<String, FieldSchema> allHiveColumns = unPartHiveColumns.stream()
                .collect(Collectors.toMap(FieldSchema::getName, fieldSchema -> fieldSchema));
        for (FieldSchema hiveColumn : partHiveColumns) {
            allHiveColumns.put(hiveColumn.getName(), hiveColumn);
        }
        return allHiveColumns;
    }

    public static List<FieldSchema> getAllColumns(Table table) {
        List<FieldSchema> allColumns = table.getSd().getCols();
        List<FieldSchema> partHiveColumns = table.getPartitionKeys();
        allColumns.addAll(partHiveColumns);
        return allColumns;
    }

    public static boolean validateColumnType(String hiveType, Type type) {
        if (hiveType == null) {
            return false;
        }

        // for type with length, like char(10), we only check the type and ignore the length
        String typeUpperCase = Utils.getTypeKeyword(hiveType).toUpperCase();
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
            case "BINARY":
                return primitiveType == PrimitiveType.VARCHAR;
            case "CHAR":
                return primitiveType == PrimitiveType.CHAR ||
                        primitiveType == PrimitiveType.VARCHAR;
            case "BOOLEAN":
                return primitiveType == PrimitiveType.BOOLEAN;
            case "ARRAY":
                if (!type.isArrayType()) {
                    return false;
                }
                return validateColumnType(hiveType.substring(hiveType.indexOf('<') + 1, hiveType.length() - 1),
                        ((ArrayType) type).getItemType());
            default:
                // for UNION and other types, we transfer it to UNKNOWN_TYPE
                return primitiveType == PrimitiveType.UNKNOWN_TYPE;
        }
    }

    public static Type convertColumnType(String hiveType) throws DdlException {
        String typeUpperCase = Utils.getTypeKeyword(hiveType).toUpperCase();
        PrimitiveType primitiveType;
        switch (typeUpperCase) {
            case "TINYINT":
                primitiveType = PrimitiveType.TINYINT;
                break;
            case "SMALLINT":
                primitiveType = PrimitiveType.SMALLINT;
                break;
            case "INT":
            case "INTEGER":
                primitiveType = PrimitiveType.INT;
                break;
            case "BIGINT":
                primitiveType = PrimitiveType.BIGINT;
                break;
            case "FLOAT":
                primitiveType = PrimitiveType.FLOAT;
                break;
            case "DOUBLE":
            case "DOUBLE PRECISION":
                primitiveType = PrimitiveType.DOUBLE;
                break;
            case "DECIMAL":
            case "NUMERIC":
                primitiveType = PrimitiveType.DECIMAL32;
                break;
            case "TIMESTAMP":
                primitiveType = PrimitiveType.DATETIME;
                break;
            case "DATE":
                primitiveType = PrimitiveType.DATE;
                break;
            case "STRING":
            case "BINARY":
                return ScalarType.createDefaultString();
            case "VARCHAR":
                return ScalarType.createVarcharType(Utils.getVarcharLength(hiveType));
            case "CHAR":
                return ScalarType.createCharType(Utils.getCharLength(hiveType));
            case "BOOLEAN":
                primitiveType = PrimitiveType.BOOLEAN;
                break;
            case "ARRAY":
                Type type = Utils.convertToArrayType(hiveType);
                if (type.isArrayType()) {
                    return type;
                }
            default:
                primitiveType = PrimitiveType.UNKNOWN_TYPE;
                break;
        }

        if (primitiveType != PrimitiveType.DECIMAL32) {
            return ScalarType.createType(primitiveType);
        } else {
            int[] parts = Utils.getPrecisionAndScale(hiveType);
            return ScalarType.createUnifiedDecimalType(parts[0], parts[1]);
        }
    }

    // In the first phase of connector, in order to reduce changes, we use `hive.metastore.uris` as resource name
    // for table of external catalog. The table of external catalog will not create a real resource.
    // We will reconstruct this part later. The concept of resource will not be used for external catalog in the future.
    public static HiveTable convertToSRTable(Table hiveTable, String resoureName) throws DdlException {
        if (hiveTable.getTableType().equals("VIRTUAL_VIEW")) {
            throw new DdlException("Hive view table is not supported.");
        }

        List<FieldSchema> allHiveColumns = getAllColumns(hiveTable);
        List<Column> fullSchema = Lists.newArrayList();
        for (FieldSchema fieldSchema : allHiveColumns) {
            Type srType = convertColumnType(fieldSchema.getType());
            Column column = new Column(fieldSchema.getName(), srType, true);
            fullSchema.add(column);
        }

        // Adding some necessary properties to adapt initialization of HiveTable.
        Map<String, String> properties = Maps.newHashMap();
        properties.put(HiveTable.HIVE_DB, hiveTable.getDbName());
        properties.put(HiveTable.HIVE_TABLE, hiveTable.getTableName());
        properties.put(HiveTable.HIVE_METASTORE_URIS, resoureName);
        properties.put(HiveTable.HIVE_RESOURCE, resoureName);

        return new HiveTable(connectorTableIdIdGenerator.getNextId().asInt(), hiveTable.getTableName(),
                fullSchema, properties, hiveTable);
    }

    public static Database convertToSRDatabase(String dbName) {
        return new Database(connectorDbIdIdGenerator.getNextId().asInt(), dbName);
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
