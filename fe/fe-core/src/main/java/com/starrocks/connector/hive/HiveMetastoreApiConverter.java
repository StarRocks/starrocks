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


package com.starrocks.connector.hive;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.HiveView;
import com.starrocks.catalog.HudiTable;
import com.starrocks.catalog.Type;
import com.starrocks.connector.ColumnTypeConverter;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.trino.TrinoViewColumnTypeConverter;
import com.starrocks.connector.trino.TrinoViewDefinition;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.StarRocksPlannerException;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.Option;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.starrocks.catalog.HudiTable.HUDI_BASE_PATH;
import static com.starrocks.catalog.HudiTable.HUDI_TABLE_COLUMN_NAMES;
import static com.starrocks.catalog.HudiTable.HUDI_TABLE_COLUMN_TYPES;
import static com.starrocks.catalog.HudiTable.HUDI_TABLE_INPUT_FOAMT;
import static com.starrocks.catalog.HudiTable.HUDI_TABLE_SERDE_LIB;
import static com.starrocks.catalog.HudiTable.HUDI_TABLE_TYPE;
import static com.starrocks.connector.ColumnTypeConverter.fromHudiType;
import static com.starrocks.connector.ColumnTypeConverter.fromHudiTypeToHiveTypeString;
import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.toResourceName;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.common.StatsSetupConst.ROW_COUNT;
import static org.apache.hadoop.hive.common.StatsSetupConst.TOTAL_SIZE;

public class HiveMetastoreApiConverter {
    private static final Logger LOG = LogManager.getLogger(HiveMetastoreApiConverter.class);
    private static final String SPARK_SQL_SOURCE_PROVIDER = "spark.sql.sources.provider";

    private static boolean isDeltaLakeTable(Map<String, String> tableParams) {
        return tableParams.containsKey(SPARK_SQL_SOURCE_PROVIDER) &&
                tableParams.get(SPARK_SQL_SOURCE_PROVIDER).equalsIgnoreCase("delta");
    }

    public static boolean isHudiTable(String inputFormat) {
        return inputFormat != null && HudiTable.fromInputFormat(inputFormat) != HudiTable.HudiTableType.UNKNOWN;
    }

    public static String toTableLocation(StorageDescriptor sd, Map<String, String> tableParams) {
        Optional<Map<String, String>> tableParamsOptional = Optional.ofNullable(tableParams);
        if (isDeltaLakeTable(tableParamsOptional.orElse(ImmutableMap.of()))) {
            return sd.getSerdeInfo().getParameters().get("path");
        }
        return sd.getLocation();
    }

    public static Database toDatabase(org.apache.hadoop.hive.metastore.api.Database database) {
        if (database == null || database.getName() == null) {
            throw new StarRocksConnectorException("Hive database [%s] doesn't exist");
        }
        return new Database(ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt(), database.getName(),
                database.getLocationUri());
    }

    public static org.apache.hadoop.hive.metastore.api.Database toMetastoreApiDatabase(Database database) {
        org.apache.hadoop.hive.metastore.api.Database result = new org.apache.hadoop.hive.metastore.api.Database();
        result.setName(database.getFullName());
        if (!Strings.isNullOrEmpty(database.getLocation())) {
            result.setLocationUri(database.getLocation());
        }

        return result;
    }

    public static HiveTable toHiveTable(Table table, String catalogName) {
        validateHiveTableType(table.getTableType());

        HiveTable.Builder tableBuilder = HiveTable.builder()
                .setId(ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt())
                .setTableName(table.getTableName())
                .setCatalogName(catalogName)
                .setResourceName(toResourceName(catalogName, "hive"))
                .setHiveDbName(table.getDbName())
                .setHiveTableName(table.getTableName())
                .setPartitionColumnNames(table.getPartitionKeys().stream()
                        .map(FieldSchema::getName)
                        .collect(Collectors.toList()))
                .setDataColumnNames(table.getSd().getCols().stream()
                        .map(FieldSchema::getName)
                        .collect(Collectors.toList()))
                .setFullSchema(toFullSchemasForHiveTable(table))
                .setTableLocation(toTableLocation(table.getSd(), table.getParameters()))
                .setCreateTime(table.getCreateTime());
        return tableBuilder.build();
    }

    public static HiveView toHiveView(Table table, String catalogName) {
        HiveView hiveView;
        if (table.getViewOriginalText() != null && table.getViewOriginalText().startsWith(HiveView.PRESTO_VIEW_PREFIX)
                && table.getViewOriginalText().endsWith(HiveView.PRESTO_VIEW_SUFFIX)) {
            // for trino view
            String hiveViewText = table.getViewOriginalText();
            hiveViewText = hiveViewText.substring(HiveView.PRESTO_VIEW_PREFIX.length());
            hiveViewText = hiveViewText.substring(0, hiveViewText.length() - HiveView.PRESTO_VIEW_SUFFIX.length());
            byte[] bytes = Base64.getDecoder().decode(hiveViewText);
            TrinoViewDefinition trinoViewDefinition = GsonUtils.GSON.fromJson(new String(bytes),
                    TrinoViewDefinition.class);
            hiveViewText = trinoViewDefinition.getOriginalSql();
            hiveView = new HiveView(ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt(), catalogName,
                    table.getTableName(), toFullSchemasForTrinoView(table, trinoViewDefinition), hiveViewText);
        } else {
            hiveView = new HiveView(ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt(), catalogName,
                    table.getTableName(), toFullSchemasForHiveTable(table), table.getViewExpandedText());
        }

        try {
            hiveView.getQueryStatementWithSRParser();
        } catch (StarRocksPlannerException e) {
            throw new StarRocksConnectorException("failed to parse hive view text", e);
        }
        return hiveView;
    }

    public static HudiTable toHudiTable(Table table, String catalogName) {
        String hudiBasePath = table.getSd().getLocation();
        // using hadoop properties from catalog definition
        Configuration configuration = new Configuration();
        if (catalogName != null) {
            Connector connector = GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalogName);
            CloudConfiguration cloudConfiguration = connector.getCloudConfiguration();
            HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(cloudConfiguration);
            configuration = hdfsEnvironment.getConfiguration();
        }

        HoodieTableMetaClient metaClient =
                HoodieTableMetaClient.builder().setConf(configuration).setBasePath(hudiBasePath).build();
        HoodieTableConfig hudiTableConfig = metaClient.getTableConfig();
        TableSchemaResolver schemaUtil = new TableSchemaResolver(metaClient);
        Schema hudiSchema;
        try {
            hudiSchema = HoodieAvroUtils.createHoodieWriteSchema(schemaUtil.getTableAvroSchema());
        } catch (Exception e) {
            throw new StarRocksConnectorException("Cannot get hudi table schema.");
        }

        List<String> partitionColumnNames = toPartitionColumnNamesForHudiTable(table, hudiTableConfig);

        HudiTable.Builder tableBuilder = HudiTable.builder()
                .setId(ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt())
                .setTableName(table.getTableName())
                .setCatalogName(catalogName)
                .setResourceName(toResourceName(catalogName, "hudi"))
                .setHiveDbName(table.getDbName())
                .setHiveTableName(table.getTableName())
                .setFullSchema(toFullSchemasForHudiTable(hudiSchema))
                .setPartitionColNames(partitionColumnNames)
                .setDataColNames(toDataColumnNamesForHudiTable(hudiSchema, partitionColumnNames))
                .setHudiProperties(toHudiProperties(table, metaClient, hudiSchema))
                .setCreateTime(table.getCreateTime());

        return tableBuilder.build();
    }

    public static Partition toPartition(StorageDescriptor sd, Map<String, String> params) {
        requireNonNull(sd, "StorageDescriptor is null");
        Partition.Builder partitionBuilder = Partition.builder()
                .setParams(params)
                .setFullPath(sd.getLocation())
                .setInputFormat(toRemoteFileInputFormat(sd.getInputFormat()))
                .setTextFileFormatDesc(toTextFileFormatDesc(sd.getSerdeInfo().getParameters()))
                .setSplittable(RemoteFileInputFormat.isSplittable(sd.getInputFormat()));

        return partitionBuilder.build();
    }

    public static List<Column> toFullSchemasForHiveTable(Table table) {
        List<FieldSchema> fieldSchemas = getAllFieldSchemas(table);
        List<Column> fullSchema = Lists.newArrayList();
        for (FieldSchema fieldSchema : fieldSchemas) {
            Type type;
            try {
                type = ColumnTypeConverter.fromHiveType(fieldSchema.getType());
            } catch (InternalError | Exception e) {
                LOG.error("Failed to convert hive type {} on {}", fieldSchema.getType(), table.getTableName(), e);
                type = Type.UNKNOWN_TYPE;
            }
            Column column = new Column(fieldSchema.getName(), type, true);
            fullSchema.add(column);
        }
        return fullSchema;
    }

    public static List<Column> toFullSchemasForTrinoView(Table table, TrinoViewDefinition definition) {
        List<TrinoViewDefinition.ViewColumn> viewColumns = definition.getColumns();
        List<Column> fullSchema = Lists.newArrayList();
        for (TrinoViewDefinition.ViewColumn col : viewColumns) {
            Type type;
            try {
                type = TrinoViewColumnTypeConverter.fromTrinoType(col.getType());
            } catch (InternalError | Exception e) {
                LOG.error("Failed to convert trino view type {} on {}", col.getType(), table.getTableName(), e);
                type = Type.UNKNOWN_TYPE;
            }
            Column column = new Column(col.getName(), type, true);
            fullSchema.add(column);
        }
        return fullSchema;
    }

    public static List<Column> toFullSchemasForHudiTable(Schema hudiSchema) {
        List<Schema.Field> allHudiColumns = hudiSchema.getFields();
        List<Column> fullSchema = Lists.newArrayList();
        for (Schema.Field fieldSchema : allHudiColumns) {
            Type type;
            try {
                type = fromHudiType(fieldSchema.schema());
            } catch (InternalError | Exception e) {
                LOG.error("Failed to convert hudi type {}", fieldSchema.schema().getType().getName(), e);
                type = Type.UNKNOWN_TYPE;
            }
            Column column = new Column(fieldSchema.name(), type, true);
            fullSchema.add(column);
        }
        return fullSchema;
    }

    public static List<FieldSchema> getAllFieldSchemas(Table table) {
        ImmutableList.Builder<FieldSchema> allColumns = ImmutableList.builder();
        List<FieldSchema> unHivePartColumns = table.getSd().getCols();
        List<FieldSchema> partHiveColumns = table.getPartitionKeys();
        return allColumns.addAll(unHivePartColumns).addAll(partHiveColumns).build();
    }

    public static List<String> toPartitionColumnNamesForHudiTable(Table table, HoodieTableConfig tableConfig) {
        List<String> partitionColumnNames = Lists.newArrayList();
        Option<String[]> hudiPartitionFields = tableConfig.getPartitionFields();

        if (hudiPartitionFields.isPresent()) {
            partitionColumnNames.addAll(Arrays.asList(hudiPartitionFields.get()));
        } else if (!table.getPartitionKeys().isEmpty()) {
            for (FieldSchema fieldSchema : table.getPartitionKeys()) {
                String partField = fieldSchema.getName();
                partitionColumnNames.add(partField);
            }
        }
        return partitionColumnNames;
    }

    public static List<String> toDataColumnNamesForHudiTable(Schema schema, List<String> partitionColumnNames) {
        List<String> dataColumnNames = Lists.newArrayList();
        for (Schema.Field hudiField : schema.getFields()) {
            if (partitionColumnNames.stream().noneMatch(p -> p.equals(hudiField.name()))) {
                dataColumnNames.add(hudiField.name());
            }
        }
        return dataColumnNames;
    }

    public static Map<String, String> toHudiProperties(Table metastoreTable,
                                                       HoodieTableMetaClient metaClient,
                                                       Schema tableSchema) {
        Map<String, String> hudiProperties = Maps.newHashMap();

        String hudiBasePath = metastoreTable.getSd().getLocation();
        if (!Strings.isNullOrEmpty(hudiBasePath)) {
            hudiProperties.put(HUDI_BASE_PATH, hudiBasePath);
        }
        String serdeLib = metastoreTable.getSd().getSerdeInfo().getSerializationLib();
        if (!Strings.isNullOrEmpty(serdeLib)) {
            hudiProperties.put(HUDI_TABLE_SERDE_LIB, serdeLib);
        }
        String inputFormat = metastoreTable.getSd().getInputFormat();
        if (!Strings.isNullOrEmpty(inputFormat)) {
            hudiProperties.put(HUDI_TABLE_INPUT_FOAMT, inputFormat);
        }

        hudiProperties.put(HUDI_TABLE_TYPE, metaClient.getTableType().name());

        StringBuilder columnNamesBuilder = new StringBuilder();
        StringBuilder columnTypesBuilder = new StringBuilder();
        List<FieldSchema> allFields = metastoreTable.getSd().getCols();
        allFields.addAll(metastoreTable.getPartitionKeys());

        boolean isFirst = true;
        for (Schema.Field hudiField : tableSchema.getFields()) {
            if (!isFirst) {
                columnNamesBuilder.append(",");
                columnTypesBuilder.append("#");
            }

            String columnName = hudiField.name().toLowerCase(Locale.ROOT);
            Optional<FieldSchema> field = allFields.stream()
                    .filter(f -> f.getName().equals(columnName)).findFirst();
            if (!field.isPresent()) {
                throw new StarRocksConnectorException(
                        "Hudi column [" + hudiField.name() + "] not exists in hive metastore.");
            }
            columnNamesBuilder.append(columnName);
            String columnType = fromHudiTypeToHiveTypeString(hudiField.schema());
            columnTypesBuilder.append(columnType);

            isFirst = false;
        }
        hudiProperties.put(HUDI_TABLE_COLUMN_NAMES, columnNamesBuilder.toString());
        hudiProperties.put(HUDI_TABLE_COLUMN_TYPES, columnTypesBuilder.toString());

        return hudiProperties;
    }

    public static RemoteFileInputFormat toRemoteFileInputFormat(String inputFormat) {
        if (!isHudiTable(inputFormat)) {
            return RemoteFileInputFormat.fromHdfsInputFormatClass(inputFormat);
        } else {
            // Currently, we only support parquet on hudi format.
            return RemoteFileInputFormat.PARQUET;
        }
    }

    public static TextFileFormatDesc toTextFileFormatDesc(Map<String, String> serdeParams) {
        final String DEFAULT_FIELD_DELIM = "\001";
        final String DEFAULT_COLLECTION_DELIM = "\002";
        final String DEFAULT_MAPKEY_DELIM = "\003";
        final String DEFAULT_LINE_DELIM = "\n";


        // Get properties 'field.delim', 'line.delim', 'collection.delim' and 'mapkey.delim' from StorageDescriptor
        // Detail refer to:
        // https://github.com/apache/hive/blob/90428cc5f594bd0abb457e4e5c391007b2ad1cb8/serde/src/gen/thrift/gen-javabean/org/apache/hadoop/hive/serde/serdeConstants.java#L34-L40

        // Here is for compatibility with Hive 2.x version.
        // There is a typo in Hive 2.x version, and fixed in Hive 3.x version.
        // https://issues.apache.org/jira/browse/HIVE-16922
        String collectionDelim;
        if (serdeParams.containsKey("colelction.delim")) {
            collectionDelim = serdeParams.getOrDefault("colelction.delim", "");
        } else {
            collectionDelim = serdeParams.getOrDefault("collection.delim", "");
        }

        String fieldDelim = serdeParams.getOrDefault("field.delim", "");
        if (fieldDelim.isEmpty()) {
            // Support for hive org.apache.hadoop.hive.serde2.OpenCSVSerde
            // https://cwiki.apache.org/confluence/display/hive/csv+serde
            fieldDelim = serdeParams.getOrDefault("separatorChar", "");
        }
        String lineDelim = serdeParams.getOrDefault("line.delim", "");
        String mapkeyDelim = serdeParams.getOrDefault("mapkey.delim", "");

        // check is empty
        fieldDelim = fieldDelim.isEmpty() ? DEFAULT_FIELD_DELIM : fieldDelim;
        lineDelim = lineDelim.isEmpty() ? DEFAULT_LINE_DELIM : lineDelim;
        collectionDelim = collectionDelim.isEmpty() ? DEFAULT_COLLECTION_DELIM : collectionDelim;
        mapkeyDelim = mapkeyDelim.isEmpty() ? DEFAULT_MAPKEY_DELIM : mapkeyDelim;

        return new TextFileFormatDesc(fieldDelim, lineDelim, collectionDelim, mapkeyDelim);
    }

    public static HiveCommonStats toHiveCommonStats(Map<String, String> params) {
        long numRows = getLongParam(ROW_COUNT, params);
        long totalSize = getLongParam(TOTAL_SIZE, params);
        return new HiveCommonStats(numRows, totalSize);
    }

    public static Map<String, Map<String, HiveColumnStats>> toPartitionColumnStatistics(
            Map<String, List<ColumnStatisticsObj>> partitionNameToColumnStats,
            Map<String, Long> partToRowNum) {
        return partitionNameToColumnStats.entrySet().stream()
                .filter(entry -> !entry.getValue().isEmpty())
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> toSinglePartitionColumnStats(
                                entry.getValue(),
                                partToRowNum.getOrDefault(entry.getKey(), -1L))));
    }

    public static Map<String, HiveColumnStats> toSinglePartitionColumnStats(
            List<ColumnStatisticsObj> statisticsObjs,
            long partitionRowNum) {
        return statisticsObjs.stream().collect(Collectors.toMap(
                ColumnStatisticsObj::getColName,
                statisticsObj -> toHiveColumnStatistics(statisticsObj, partitionRowNum)));
    }

    public static HiveColumnStats toHiveColumnStatistics(ColumnStatisticsObj columnStatisticsObj, long rowNums) {
        HiveColumnStats hiveColumnStatistics = new HiveColumnStats();
        hiveColumnStatistics.initialize(columnStatisticsObj.getStatsData(), rowNums);
        return hiveColumnStatistics;
    }

    public static void validateHiveTableType(String hiveTableType) {
        if (hiveTableType == null) {
            throw new StarRocksConnectorException("Unknown hive table type.");
        }
        switch (hiveTableType.toUpperCase(Locale.ROOT)) {
            case "VIRTUAL_VIEW": // hive view supported
            case "EXTERNAL_TABLE": // hive external table supported
            case "MANAGED_TABLE": // basic hive table supported
            case "MATERIALIZED_VIEW": // hive materialized view table supported
                break;
            default:
                throw new StarRocksConnectorException("unsupported hive table type [" + hiveTableType + "].");
        }
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
