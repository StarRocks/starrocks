// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveMetaStoreTableInfo;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
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

    public static PrimitiveType convertColumnType(String hiveType) throws DdlException {
        String typeUpperCase = Utils.getTypeKeyword(hiveType).toUpperCase();
        switch (typeUpperCase) {
            case "TINYINT":
                return PrimitiveType.TINYINT;
            case "SMALLINT":
                return PrimitiveType.SMALLINT;
            case "INT":
            case "INTEGER":
                return PrimitiveType.INT;
            case "BIGINT":
                return PrimitiveType.BIGINT;
            case "FLOAT":
                return PrimitiveType.FLOAT;
            case "DOUBLE":
            case "DOUBLE PRECISION":
                return PrimitiveType.DOUBLE;
            case "DECIMAL":
            case "NUMERIC":
                return PrimitiveType.DECIMAL32;
            case "TIMESTAMP":
                return PrimitiveType.DATETIME;
            case "DATE":
                return PrimitiveType.DATE;
            case "STRING":
            case "VARCHAR":
            case "BINARY":
                return PrimitiveType.VARCHAR;
            case "CHAR":
                return PrimitiveType.CHAR;
            case "BOOLEAN":
                return PrimitiveType.BOOLEAN;
            default:
                throw new DdlException("hive table column type [" + typeUpperCase + "] transform failed.");
        }
    }

    // In the first phase of connector, in order to reduce changes, we use `hive.metastore.uris` as resource name
    // for table of external catalog. The table of external catalog will not create a real resource.
    // We will reconstruct this part later. The concept of resource will not be used for external catalog in the future.
    public static HiveTable convertToSRTable(Table hiveTable, String resoureName) throws DdlException {
        if (hiveTable.getTableType().equals("VIRTUAL_VIEW")) {
            throw new DdlException("Hive view table is not supported.");
        }

        Map<String, FieldSchema> allHiveColumns = getAllHiveColumns(hiveTable);
        List<Column> fullSchema = Lists.newArrayList();
        for (Map.Entry<String, FieldSchema> entry : allHiveColumns.entrySet()) {
            FieldSchema fieldSchema = entry.getValue();
            PrimitiveType srType = convertColumnType(fieldSchema.getType());
            Column column = new Column(fieldSchema.getName(), ScalarType.createType(srType), true);
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
