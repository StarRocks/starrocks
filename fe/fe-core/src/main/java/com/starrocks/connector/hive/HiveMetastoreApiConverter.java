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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.HiveView;
import com.starrocks.catalog.HudiTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.Version;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.ColumnTypeConverter;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.trino.TrinoViewColumnTypeConverter;
import com.starrocks.connector.trino.TrinoViewDefinition;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.StarRocksPlannerException;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
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
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.starrocks.catalog.HiveTable.HIVE_TABLE_COLUMN_NAMES;
import static com.starrocks.catalog.HiveTable.HIVE_TABLE_COLUMN_TYPES;
import static com.starrocks.catalog.HiveTable.HIVE_TABLE_INPUT_FORMAT;
import static com.starrocks.catalog.HiveTable.HIVE_TABLE_SERDE_LIB;
import static com.starrocks.catalog.HudiTable.HUDI_BASE_PATH;
import static com.starrocks.catalog.HudiTable.HUDI_TABLE_COLUMN_NAMES;
import static com.starrocks.catalog.HudiTable.HUDI_TABLE_COLUMN_TYPES;
import static com.starrocks.catalog.HudiTable.HUDI_TABLE_INPUT_FOAMT;
import static com.starrocks.catalog.HudiTable.HUDI_TABLE_SERDE_LIB;
import static com.starrocks.catalog.HudiTable.HUDI_TABLE_TYPE;
import static com.starrocks.connector.AvroUtils.getAvroFields;
import static com.starrocks.connector.AvroUtils.isHiveTableWithAvroSchemas;
import static com.starrocks.connector.ColumnTypeConverter.fromHudiType;
import static com.starrocks.connector.ColumnTypeConverter.fromHudiTypeToHiveTypeString;
import static com.starrocks.connector.hive.HiveMetadata.STARROCKS_QUERY_ID;
import static com.starrocks.connector.hive.RemoteFileInputFormat.fromHdfsInputFormatClass;
import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.toResourceName;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.common.StatsSetupConst.NUM_FILES;
import static org.apache.hadoop.hive.common.StatsSetupConst.ROW_COUNT;
import static org.apache.hadoop.hive.common.StatsSetupConst.TOTAL_SIZE;

public class HiveMetastoreApiConverter {
    private static final Logger LOG = LogManager.getLogger(HiveMetastoreApiConverter.class);
    private static final String SPARK_SQL_SOURCE_PROVIDER = "spark.sql.sources.provider";
    private static final Set<String> STATS_PROPERTIES = ImmutableSet.of(ROW_COUNT, TOTAL_SIZE, NUM_FILES);

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

    /**
     * Convert org.apache.hadoop.hive.metastore.api.table to internal representation
     * @param table org.apache.hadoop.hive.metastore.api.table
     * @param catalogName catalog name
     * @param schemas we use it to represent fieldSchemas fetched from avro_schema_url other than HMS only when it is
     *                an Avro table, and isAvroTableWithSchemas returns True as Avro schemas provides more accurate
     *                column info.
     * @return HiveTable
     */
    public static HiveTable toHiveTable(Table table, String catalogName) {
        validateHiveTableType(table.getTableType());

        List<FieldSchema> fieldSchemas = table.getSd().getCols();

        if (isHiveTableWithAvroSchemas(table)) {
            HashMap<String, String> tableProperties = new HashMap<>();
            table.getSd().getSerdeInfo().getParameters().forEach((key, value) ->
                    tableProperties.put(key, value != null ? value : ""));
            tableProperties.putAll(table.getParameters());
            List<FieldSchema> avroFields = getAvroFields(catalogName, table.getDbName(), table.getTableName(),
                    tableProperties);
            if (!avroFields.isEmpty()) {
                fieldSchemas = avroFields;
            }
        }

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
                .setDataColumnNames(fieldSchemas.stream()
                        .map(FieldSchema::getName)
                        .collect(Collectors.toList()))
                .setFullSchema(toFullSchemasForHiveTable(table, fieldSchemas))
                .setTableLocation(toTableLocation(table.getSd(), table.getParameters()))
                .setProperties(toHiveProperties(table,
                        HiveStorageFormat.get(fromHdfsInputFormatClass(table.getSd().getInputFormat()).name()),
                        fieldSchemas))
                .setStorageFormat(
                        HiveStorageFormat.get(fromHdfsInputFormatClass(table.getSd().getInputFormat()).name()))
                .setCreateTime(table.getCreateTime())
                .setHiveTableType(HiveTable.HiveTableType.fromString(table.getTableType()));

        return tableBuilder.build();
    }

    public static Table toMetastoreApiTable(HiveTable table) {
        Table apiTable = new Table();
        apiTable.setDbName(table.getDbName());
        apiTable.setTableName(table.getTableName());
        apiTable.setTableType("MANAGED_TABLE");
        apiTable.setOwner(System.getenv("HADOOP_USER_NAME"));
        apiTable.setParameters(toApiTableProperties(table));
        apiTable.setPartitionKeys(table.getPartitionColumns().stream()
                .map(HiveMetastoreApiConverter::toMetastoreApiFieldSchema)
                .collect(Collectors.toList()));
        apiTable.setSd(makeStorageDescriptorFromHiveTable(table));
        return apiTable;
    }

    private static StorageDescriptor makeStorageDescriptorFromHiveTable(HiveTable table) {
        SerDeInfo serdeInfo = new SerDeInfo();
        serdeInfo.setName(table.getTableName());
        HiveStorageFormat storageFormat = table.getStorageFormat();
        serdeInfo.setSerializationLib(storageFormat.getSerde());

        StorageDescriptor sd = new StorageDescriptor();
        sd.setLocation(table.getTableLocation());
        sd.setCols(table.getDataColumnNames().stream()
                .map(table::getColumn)
                .map(HiveMetastoreApiConverter::toMetastoreApiFieldSchema)
                .collect(toImmutableList()));
        sd.setSerdeInfo(serdeInfo);
        sd.setInputFormat(storageFormat.getInputFormat());
        sd.setOutputFormat(storageFormat.getOutputFormat());
        sd.setParameters(ImmutableMap.of());

        return sd;
    }

    private static StorageDescriptor makeStorageDescriptorFromHivePartition(HivePartition partition) {
        SerDeInfo serdeInfo = new SerDeInfo();
        HiveStorageFormat hiveSd = partition.getStorage();
        serdeInfo.setName(partition.getTableName());
        serdeInfo.setSerializationLib(hiveSd.getSerde());

        StorageDescriptor sd = new StorageDescriptor();
        sd.setLocation(emptyToNull(partition.getLocation()));
        sd.setCols(partition.getColumns().stream()
                .map(HiveMetastoreApiConverter::toMetastoreApiFieldSchema)
                .collect(toImmutableList()));
        sd.setSerdeInfo(serdeInfo);
        sd.setInputFormat(hiveSd.getInputFormat());
        sd.setOutputFormat(hiveSd.getOutputFormat());
        sd.setParameters(ImmutableMap.of());

        return sd;
    }

    public static FieldSchema toMetastoreApiFieldSchema(Column column) {
        return new FieldSchema(column.getName(), ColumnTypeConverter.toHiveType(column.getType()), column.getComment());
    }

    public static Map<String, String> toApiTableProperties(HiveTable table) {
        ImmutableMap.Builder<String, String> tableProperties = ImmutableMap.builder();

        tableProperties.put(ROW_COUNT, "0");
        tableProperties.put(TOTAL_SIZE, "0");
        tableProperties.put("comment", table.getComment());
        tableProperties.put("starrocks_version", Version.STARROCKS_VERSION + "-" + Version.STARROCKS_COMMIT_HASH);
        if (ConnectContext.get() != null && ConnectContext.get().getQueryId() != null) {
            tableProperties.put(STARROCKS_QUERY_ID, ConnectContext.get().getQueryId().toString());
        }

        return tableProperties.build();
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
                    table.getTableName(), toFullSchemasForTrinoView(table, trinoViewDefinition), hiveViewText,
                    HiveView.Type.Trino);
        } else {
            hiveView = new HiveView(ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt(), catalogName,
                    table.getTableName(), toFullSchemasForHiveTable(table), table.getViewExpandedText(),
                    HiveView.Type.Hive);
        }

        try {
            hiveView.getQueryStatement();
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
            CatalogConnector connector = GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalogName);
            CloudConfiguration cloudConfiguration = connector.getMetadata().getCloudConfiguration();
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

    public static org.apache.hadoop.hive.metastore.api.Partition toMetastoreApiPartition(
            HivePartitionWithStats partitionWithStatistics) {
        org.apache.hadoop.hive.metastore.api.Partition partition =
                toMetastoreApiPartition(partitionWithStatistics.getHivePartition());
        partition.setParameters(updateStatisticsParameters(
                partition.getParameters(), partitionWithStatistics.getHivePartitionStats().getCommonStats()));
        return partition;
    }

    public static org.apache.hadoop.hive.metastore.api.Partition toMetastoreApiPartition(HivePartition hivePartition) {
        org.apache.hadoop.hive.metastore.api.Partition result = new org.apache.hadoop.hive.metastore.api.Partition();
        result.setDbName(hivePartition.getDatabaseName());
        result.setTableName(hivePartition.getTableName());
        result.setValues(hivePartition.getValues());
        result.setSd(makeStorageDescriptorFromHivePartition(hivePartition));
        result.setParameters(hivePartition.getParameters());
        return result;
    }

    public static Map<String, String> toHiveProperties(Table metastoreTable, HiveStorageFormat storageFormat,
                                                       List<FieldSchema> fieldSchemas) {
        Map<String, String> hiveProperties = Maps.newHashMap();

        String serdeLib = storageFormat.getSerde();
        if (!Strings.isNullOrEmpty(serdeLib)) {
            // metaStore has more accurate information about serde
            if (metastoreTable.getSd() != null && metastoreTable.getSd().getSerdeInfo() != null &&
                    metastoreTable.getSd().getSerdeInfo().getSerializationLib() != null) {
                serdeLib = metastoreTable.getSd().getSerdeInfo().getSerializationLib();
            }
            hiveProperties.put(HIVE_TABLE_SERDE_LIB, serdeLib);
        }

        String inputFormat = storageFormat.getInputFormat();
        if (!Strings.isNullOrEmpty(inputFormat)) {
            hiveProperties.put(HIVE_TABLE_INPUT_FORMAT, inputFormat);
        }

        String dataColumnNames = fieldSchemas.stream()
                .map(FieldSchema::getName).collect(Collectors.joining(","));

        if (!Strings.isNullOrEmpty(dataColumnNames)) {
            hiveProperties.put(HIVE_TABLE_COLUMN_NAMES, dataColumnNames);
        }

        String dataColumnTypes = fieldSchemas.stream()
                .map(FieldSchema::getType).collect(Collectors.joining("#"));

        if (!Strings.isNullOrEmpty(dataColumnTypes)) {
            hiveProperties.put(HIVE_TABLE_COLUMN_TYPES, dataColumnTypes);
        }

        if (metastoreTable.getParameters() != null) {
            hiveProperties.putAll(metastoreTable.getParameters());
        }

        return hiveProperties;
    }

    public static List<Column> toFullSchemasForHiveTable(Table table) {
        return toFullSchemasForHiveTable(table, ImmutableList.of());
    }

    public static List<Column> toFullSchemasForHiveTable(Table table, List<FieldSchema> schemas) {
        ImmutableList.Builder<FieldSchema> allColumns = ImmutableList.builder();
        List<FieldSchema> unHivePartColumns = schemas.isEmpty() ? table.getSd().getCols() : schemas;
        List<FieldSchema> partHiveColumns = table.getPartitionKeys();
        ImmutableList<FieldSchema> allFieldSchemas = allColumns.addAll(unHivePartColumns).addAll(partHiveColumns).build();

        List<Column> fullSchema = Lists.newArrayList();
        for (FieldSchema fieldSchema : allFieldSchemas) {
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
        if (isHudiTable(inputFormat)) {
            // Currently, we only support parquet on hudi format.
            return RemoteFileInputFormat.PARQUET;
        }
        return RemoteFileInputFormat.fromHdfsInputFormatClass(inputFormat);
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

        // check delim is empty, if it's empty, we convert it to null
        fieldDelim = fieldDelim.isEmpty() ? null : fieldDelim;
        lineDelim = lineDelim.isEmpty() ? null : lineDelim;
        collectionDelim = collectionDelim.isEmpty() ? null : collectionDelim;
        mapkeyDelim = mapkeyDelim.isEmpty() ? null : mapkeyDelim;

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

    public static Map<String, String> updateStatisticsParameters(Map<String, String> parameters,
                                                                 HiveCommonStats statistics) {
        ImmutableMap.Builder<String, String> result = ImmutableMap.builder();

        parameters.forEach((key, value) -> {
            if (!(STATS_PROPERTIES.contains(key))) {
                result.put(key, value);
            }
        });

        result.put(ROW_COUNT, String.valueOf(statistics.getRowNums()));
        result.put(TOTAL_SIZE, String.valueOf(statistics.getTotalFileBytes()));

        if (!parameters.containsKey("STATS_GENERATED_VIA_STATS_TASK")) {
            result.put("STATS_GENERATED_VIA_STATS_TASK", "workaround for potential lack of HIVE-12730");
        }

        return result.buildOrThrow();
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
