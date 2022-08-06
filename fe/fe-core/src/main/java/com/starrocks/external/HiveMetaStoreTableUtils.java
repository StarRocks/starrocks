// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveMetaStoreTableInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.external.hive.HiveColumnStats;
import com.starrocks.external.hive.HivePartition;
import com.starrocks.external.hive.HivePartitionStats;
import com.starrocks.external.hive.HiveTableStats;
import com.starrocks.external.hive.Utils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HiveMetaStoreTableUtils {
    private static final Logger LOG = LogManager.getLogger(HiveMetaStoreTableUtils.class);

    public static Map<String, HiveColumnStats> getTableLevelColumnStats(HiveMetaStoreTableInfo hmsTable,
                                                                        List<String> columnNames) throws DdlException {
        Map<String, HiveColumnStats> allColumnStats = Catalog.getCurrentCatalog().getHiveRepository()
                .getTableLevelColumnStats(hmsTable);
        Map<String, HiveColumnStats> result = Maps.newHashMapWithExpectedSize(columnNames.size());
        for (String columnName : columnNames) {
            result.put(columnName, allColumnStats.get(columnName));
        }
        return result;
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
        return Catalog.getCurrentCatalog().getHiveRepository().getPartitionsStats(hmsTable, partitionKeys);
    }

    public static HiveTableStats getTableStats(HiveMetaStoreTableInfo hmsTable) throws DdlException {
        return Catalog.getCurrentCatalog().getHiveRepository().getTableStats(hmsTable.getResourceName(),
                hmsTable.getDb(), hmsTable.getTable());
    }

    public static List<HivePartition> getPartitions(HiveMetaStoreTableInfo hmsTable,
                                                    List<PartitionKey> partitionKeys) throws DdlException {
        return Catalog.getCurrentCatalog().getHiveRepository().getPartitions(hmsTable, partitionKeys);
    }

    public static Map<PartitionKey, Long> getPartitionKeys(HiveMetaStoreTableInfo hmsTable) throws DdlException {
        return Catalog.getCurrentCatalog().getHiveRepository().getPartitionKeys(hmsTable);
    }

    public static long getPartitionStatsRowCount(HiveMetaStoreTableInfo hmsTable, List<PartitionKey> partitions) {
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

    public static List<FieldSchema> getAllHiveColumns(Table table) {
        ImmutableList.Builder<FieldSchema> allColumns = ImmutableList.builder();
        List<FieldSchema> unHivePartColumns = table.getSd().getCols();
        List<FieldSchema> partHiveColumns = table.getPartitionKeys();
        return allColumns.addAll(unHivePartColumns).addAll(partHiveColumns).build();
    }

    public static Type convertColumnType(String hiveType) throws DdlException {
        String typeUpperCase = Utils.getTypeKeyword(hiveType).toUpperCase();
        PrimitiveType primitiveType = null;
        Type type = null;
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
                type = Utils.convertToArrayType(hiveType);
                break;
            default:
                throw new DdlException("hive table column type [" + typeUpperCase + "] transform failed.");
        }

        if (type != null && type.isArrayType()) {
            return type;
        } else if (primitiveType != PrimitiveType.DECIMAL32) {
            return ScalarType.createType(primitiveType);
        } else {
            int[] parts = Utils.getPrecisionAndScale(hiveType);
            return ScalarType.createUnifiedDecimalType(parts[0], parts[1]);
        }
    }
}
