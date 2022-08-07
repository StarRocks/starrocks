package com.starrocks.external.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.ColumnTypeConverter;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.HudiTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.common.IdGenerator;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.external.HiveColumnStatistics;
import com.starrocks.external.hive.text.TextFileFormatDesc;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hudi.common.model.HoodieFileFormat;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.server.CatalogMgr.INTERNAL_RESOURCE_TO_CATALOG_NAME_PREFIX;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.common.StatsSetupConst.ROW_COUNT;
import static org.apache.hadoop.hive.common.StatsSetupConst.TOTAL_SIZE;

public class HiveMetastoreApiConverter {
    public static final IdGenerator<ConnectorTableId> connectorIdIdGenerator = ConnectorTableId.createGenerator();

    public static Database toDatabase(org.apache.hadoop.hive.metastore.api.Database database) {
        if (database == null || database.getName() == null) {
            throw new StarRocksConnectorException("Hive db not exists");
        }
        return new Database(connectorIdIdGenerator.getNextId().asInt(), database.getName());
    }

    public static HiveTable toHiveTable(Table table, String catalogName) {
        if (table.getTableType().equals("VIRTUAL_VIEW")) {
            throw new StarRocksConnectorException("Hive view table is not supported");
        }

        if (table.getSd() == null) {
            throw new StarRocksConnectorException("miss sd");
        }

        HiveTable.Builder tableBuilder = HiveTable.builder()
                .setId(connectorIdIdGenerator.getNextId().asInt())
                .setCatalogName(catalogName)
                .setResourceName(toResourceName(catalogName))
                .setHiveDbName(table.getDbName())
                .setHiveTableName(table.getTableName())
                .setPartitionColumnNames(table.getPartitionKeys().stream()
                        .map(FieldSchema::getName)
                        .collect(Collectors.toList()))
                .setDataColumnNames(table.getSd().getCols().stream()
                        .map(FieldSchema::getName)
                        .collect(Collectors.toList()))
                .setFullSchema(toFullSchemas(table))
                .setHdfsPath(table.getSd().getLocation());
        return tableBuilder.build();
    }

    public static Partition toPartition(StorageDescriptor sd) {
        requireNonNull(sd, "StorageDescriptor is null");
        Partition.Builder partitionBuilder = Partition.builder()
                .setParams(sd.getParameters())
                .setFullPath(sd.getLocation())
                .setInputFormat(toRemoteFileInputFormat(sd.getInputFormat()))
                .setTextFileFormatDesc(toTextFileFormatDesc(sd.getSerdeInfo().getParameters()))
                .setSplittable(RemoteFileInputFormat.isSplittable(sd.getInputFormat()));

        return partitionBuilder.build();
    }


    public static List<Column> toFullSchemas(Table table) {
        List<FieldSchema> fieldSchemas = getAllFieldSchemas(table);
        List<Column> fullSchema = Lists.newArrayList();
        for (FieldSchema fieldSchema : fieldSchemas) {
            Type type = ColumnTypeConverter.fromHiveType(fieldSchema.getType());
            Column column = new Column(fieldSchema.getName(), type, true);
            fullSchema.add(column);
        }
        return fullSchema;
    }


    public static List<FieldSchema> getAllFieldSchemas(Table table) {
        ImmutableList.Builder<FieldSchema> allColumns =  ImmutableList.builder();
        List<FieldSchema> unHivePartColumns = table.getSd().getCols();
        List<FieldSchema> partHiveColumns = table.getPartitionKeys();
        return allColumns.addAll(unHivePartColumns).addAll(partHiveColumns).build();
    }


    public static String toResourceName(String catalogName) {
        return catalogName.startsWith(INTERNAL_RESOURCE_TO_CATALOG_NAME_PREFIX) ?
                catalogName.substring(INTERNAL_RESOURCE_TO_CATALOG_NAME_PREFIX.length()) :
                catalogName;
    }

    public static RemoteFileInputFormat toRemoteFileInputFormat(String inputFormat) {
        if (HudiTable.isHudiTable(inputFormat)) {
            return RemoteFileInputFormat.fromHdfsInputFormatClass(inputFormat);
        } else {
            return RemoteFileInputFormat.PARQUET;
        }
    }

    public static TextFileFormatDesc toTextFileFormatDesc(Map<String, String> serdeParams) {
        // Get properties 'field.delim', 'line.delim', 'collection.delim' and 'mapkey.delim' from StorageDescriptor
        // Detail refer to:
        // https://github.com/apache/hive/blob/90428cc5f594bd0abb457e4e5c391007b2ad1cb8/serde/src/gen/thrift/gen-javabean/org/apache/hadoop/hive/serde/serdeConstants.java#L34-L40

        // Here is for compatibility with Hive 2.x version.
        // There is a typo in Hive 2.x version, and fixed in Hive 3.x version.
        // https://issues.apache.org/jira/browse/HIVE-16922
        String collectionDelim;
        if (serdeParams.containsKey("colelction.delim")) {
            collectionDelim = serdeParams.get("colelction.delim");
        } else {
            collectionDelim = serdeParams.getOrDefault("collection.delim", "\002");
        }

        return new TextFileFormatDesc(
                serdeParams.getOrDefault("field.delim", "\001"),
                serdeParams.getOrDefault("line.delim", "\n"),
                collectionDelim,
                serdeParams.getOrDefault("mapkey.delim", "\003"));
    }

    public static HiveCommonStats toHiveCommonStats(Map<String, String> params) {
        long numRows = getLongParam(ROW_COUNT, params);
        long totalSize = getLongParam(TOTAL_SIZE, params);
        return new HiveCommonStats(numRows, totalSize);
    }

    public static Map<String, Map<String, HiveColumnStatistics>> toPartitionColumnStatistics(
            Map<String, List<ColumnStatisticsObj>> partitionNameToColumnStats,
            Map<String, Long> partToRowNum) {
        return partitionNameToColumnStats.entrySet().stream()
                .filter(entry -> !entry.getValue().isEmpty())
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> toSinglePartitionColumnStats(
                                entry.getValue(),
                                partToRowNum.getOrDefault(entry.getKey(), -1L))));
    }

    public static Map<String, HiveColumnStatistics> toSinglePartitionColumnStats(
            List<ColumnStatisticsObj> statisticsObjs,
            long partitionRowNum) {
        return statisticsObjs.stream()
                .collect(Collectors.toMap(
                        ColumnStatisticsObj::getColName,
                        statisticsObj -> toHiveColumnStatistics(statisticsObj, partitionRowNum)));
    }

    public static HiveColumnStatistics toHiveColumnStatistics(ColumnStatisticsObj columnStatisticsObj, long rowNums) {
        HiveColumnStatistics hiveColumnStatistics = new HiveColumnStatistics();
        hiveColumnStatistics.initialize(columnStatisticsObj.getStatsData(), rowNums);
        return hiveColumnStatistics;
    }

    private static long getLongParam(String key, Map<String, String> parameters) {
        if (parameters == null) {
            return -1;
        }

        String value = parameters.get(key);
        if (value == null) {
            return -1;
        }

        try {
            return Long.parseLong(value);
        } catch (NumberFormatException exc) {
            // ignore
        }
        return -1;
    }
}
