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
import com.starrocks.catalog.KuduTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.Version;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.ColumnTypeConverter;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.metastore.MetastoreTable;
import com.starrocks.connector.trino.TrinoViewColumnTypeConverter;
import com.starrocks.connector.trino.TrinoViewDefinition;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.StarRocksPlannerException;
import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.OpenCSVSerde;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
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
import static com.starrocks.catalog.HudiTable.HUDI_HMS_TABLE_TYPE;
import static com.starrocks.catalog.HudiTable.HUDI_TABLE_COLUMN_NAMES;
import static com.starrocks.catalog.HudiTable.HUDI_TABLE_COLUMN_TYPES;
import static com.starrocks.catalog.HudiTable.HUDI_TABLE_INPUT_FOAMT;
import static com.starrocks.catalog.HudiTable.HUDI_TABLE_SERDE_LIB;
import static com.starrocks.catalog.HudiTable.HUDI_TABLE_TYPE;
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

    public static boolean isKuduTable(String inputFormat) {
        return inputFormat != null && KuduTable.isKuduInputFormat(inputFormat);
    }

    public static String toTableLocation(StorageDescriptor sd, Map<String, String> tableParams) {
        Optional<Map<String, String>> tableParamsOptional = Optional.ofNullable(tableParams);
        if (isDeltaLakeTable(tableParamsOptional.orElse(ImmutableMap.of()))) {
            return sd.getSerdeInfo().getParameters().get("path");
        }
        return sd.getLocation();
    }

    public static String toComment(Map<String, String> tableParams) {
        if (tableParams != null && tableParams.containsKey("comment")) {
            return tableParams.getOrDefault("comment", "");
        }
        return "";
    }

    public static Database toDatabase(org.apache.hadoop.hive.metastore.api.Database database, String dbName) {
        if (database == null || database.getName() == null) {
            throw new StarRocksConnectorException("Hive database [%s] doesn't exist");
        }
        return new Database(ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt(), dbName.toLowerCase(),
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
                .setComment(toComment(table.getParameters()))
                .setTableLocation(toTableLocation(table.getSd(), table.getParameters()))
                .setProperties(toHiveProperties(table,
                        HiveStorageFormat.get(fromHdfsInputFormatClass(table.getSd().getInputFormat()).name())))
                .setSerdeProperties(toSerDeProperties(table))
                .setStorageFormat(
                        HiveStorageFormat.get(fromHdfsInputFormatClass(table.getSd().getInputFormat()).name()))
                .setCreateTime(table.getCreateTime())
                .setHiveTableType(HiveTable.HiveTableType.fromString(table.getTableType()));

        return tableBuilder.build();
    }

    public static MetastoreTable toMetastoreTable(Table table) {
        String tableLocation = toTableLocation(table.getSd(), table.getParameters());
        return new MetastoreTable(table.getDbName(), table.getTableName(), tableLocation, table.getCreateTime());
    }

    public static Table toMetastoreApiTable(HiveTable table) {
        Table apiTable = new Table();
        apiTable.setDbName(table.getCatalogDBName());
        apiTable.setTableName(table.getCatalogTableName());
        apiTable.setTableType(table.getHiveTableType().name());
        apiTable.setOwner(System.getenv("HADOOP_USER_NAME"));
        apiTable.setParameters(toApiTableProperties(table));
        apiTable.setPartitionKeys(table.getPartitionColumns().stream()
                .map(HiveMetastoreApiConverter::toMetastoreApiFieldSchema)
                .collect(Collectors.toList()));
        apiTable.setSd(makeStorageDescriptorFromHiveTable(table));
        return apiTable;
    }

    public static KuduTable toKuduTable(Table table, String catalogName) {
        List<Column> fullSchema = toFullSchemasForHiveTable(table);
        List<String> partColNames = table.getPartitionKeys().stream()
                .map(FieldSchema::getName)
                .collect(Collectors.toList());
        return KuduTable.fromMetastoreTable(table, catalogName, fullSchema, partColNames);
    }

    private static StorageDescriptor makeStorageDescriptorFromHiveTable(HiveTable table) {
        SerDeInfo serdeInfo = new SerDeInfo();
        serdeInfo.setName(table.getCatalogTableName());
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
        String colComment = StringUtils.defaultIfBlank(column.getComment(), null);
        return new FieldSchema(column.getName(), ColumnTypeConverter.toHiveType(column.getType()), colComment);
    }

    public static Map<String, String> toApiTableProperties(HiveTable table) {
        ImmutableMap.Builder<String, String> tableProperties = ImmutableMap.builder();

        tableProperties.put(ROW_COUNT, "0");
        tableProperties.put(TOTAL_SIZE, "0");
        if (!Strings.isNullOrEmpty(table.getComment())) {
            tableProperties.put("comment", table.getComment());
        }
        tableProperties.put("starrocks_version", Version.STARROCKS_VERSION + "-" + Version.STARROCKS_COMMIT_HASH);
        if (ConnectContext.get() != null && ConnectContext.get().getQueryId() != null) {
            tableProperties.put(STARROCKS_QUERY_ID, ConnectContext.get().getQueryId().toString());
        }
        if (table.getHiveTableType() == HiveTable.HiveTableType.EXTERNAL_TABLE) {
            tableProperties.put("EXTERNAL", "TRUE");
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
                    table.getDbName(), table.getTableName(), toFullSchemasForTrinoView(table, trinoViewDefinition),
                    hiveViewText, HiveView.Type.Trino);
        } else {
            hiveView = new HiveView(ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt(), catalogName,
                    table.getDbName(), table.getTableName(), toFullSchemasForHiveTable(table),
                    table.getViewExpandedText(), HiveView.Type.Hive);
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
                HoodieTableMetaClient.builder().setConf(new HadoopStorageConfiguration(configuration))
                        .setBasePath(hudiBasePath).build();
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
                .setFullSchema(toFullSchemasForHudiTable(table, hudiSchema))
                .setComment(toComment(table.getParameters()))
                .setPartitionColNames(partitionColumnNames)
                .setDataColNames(toDataColumnNamesForHudiTable(hudiSchema, partitionColumnNames))
                .setHudiProperties(toHudiProperties(table, metaClient, hudiSchema))
                .setCreateTime(table.getCreateTime())
                .setTableType(HudiTable.fromInputFormat(table.getSd().getInputFormat()));

        return tableBuilder.build();
    }

    public static Partition toPartition(StorageDescriptor sd, Map<String, String> params) {
        requireNonNull(sd, "StorageDescriptor is null");
        Map<String, String> textFileParameters = Maps.newHashMap();
        textFileParameters.putAll(sd.getSerdeInfo().getParameters());
        // "skip.header.line.count" is set in TBLPROPERTIES
        textFileParameters.putAll(params);
        Partition.Builder partitionBuilder = Partition.builder()
                .setParams(params)
                .setFullPath(sd.getLocation())
                .setInputFormat(toRemoteFileInputFormat(sd.getInputFormat()))
                .setTextFileFormatDesc(toTextFileFormatDesc(textFileParameters))
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

    public static Map<String, String> toHiveProperties(Table metastoreTable, HiveStorageFormat storageFormat) {
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

        String dataColumnNames = metastoreTable.getSd().getCols().stream()
                .map(FieldSchema::getName).collect(Collectors.joining(","));

        if (!Strings.isNullOrEmpty(dataColumnNames)) {
            hiveProperties.put(HIVE_TABLE_COLUMN_NAMES, dataColumnNames);
        }

        String dataColumnTypes = metastoreTable.getSd().getCols().stream()
                .map(FieldSchema::getType).collect(Collectors.joining("#"));

        if (!Strings.isNullOrEmpty(dataColumnTypes)) {
            hiveProperties.put(HIVE_TABLE_COLUMN_TYPES, dataColumnTypes);
        }

        if (metastoreTable.getParameters() != null) {
            hiveProperties.putAll(metastoreTable.getParameters());
        }

        return hiveProperties;
    }

    public static Map<String, String> toSerDeProperties(Table table) {
        Map<String, String> serdeProperties = new HashMap<>();
        if (table.getSd() != null && table.getSd().getSerdeInfo() != null &&
                table.getSd().getSerdeInfo().getParameters() != null) {
            serdeProperties = table.getSd().getSerdeInfo().getParameters();
        }
        return serdeProperties;
    }

    public static List<Column> toFullSchemasForHiveTable(Table table) {
        List<FieldSchema> fieldSchemas = getAllFieldSchemas(table);
        List<Column> fullSchema = Lists.newArrayList();
        for (FieldSchema fieldSchema : fieldSchemas) {
            Type type;
            String comment = "";
            try {
                comment = fieldSchema.getComment();
                type = ColumnTypeConverter.fromHiveType(fieldSchema.getType());
            } catch (InternalError | Exception e) {
                LOG.error("Failed to convert hive type {} on {}", fieldSchema.getType(), table.getTableName(), e);
                type = Type.UNKNOWN_TYPE;
            }
            Column column = new Column(fieldSchema.getName(), type, true, comment);
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

    public static List<Column> toFullSchemasForHudiTable(Table table, Schema hudiSchema) {
        List<Schema.Field> allHudiColumns = hudiSchema.getFields();
        List<FieldSchema> allFieldSchemas = getAllFieldSchemas(table);
        Map<String, String> schemaCommentMap = new HashMap<>();
        for (FieldSchema schema : allFieldSchemas) {
            schemaCommentMap.put(schema.getName(), schema.getComment());
        }
        List<Column> fullSchema = Lists.newArrayList();
        for (int i = 0; i < allHudiColumns.size(); i++) {
            Schema.Field fieldSchema = allHudiColumns.get(i);
            Type type;
            try {
                type = fromHudiType(fieldSchema.schema());
            } catch (InternalError | Exception e) {
                LOG.error("Failed to convert hudi type {}", fieldSchema.schema().getType().getName(), e);
                type = Type.UNKNOWN_TYPE;
            }
            String fieldName = fieldSchema.name();
            Column column = new Column(fieldName, type, true, schemaCommentMap.get(fieldName));
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
        hudiProperties.put(HUDI_HMS_TABLE_TYPE, metastoreTable.getTableType());

        StringBuilder columnNamesBuilder = new StringBuilder();
        StringBuilder columnTypesBuilder = new StringBuilder();

        boolean isFirst = true;
        for (Schema.Field hudiField : tableSchema.getFields()) {
            if (!isFirst) {
                columnNamesBuilder.append(",");
                columnTypesBuilder.append("#");
            }

            String columnName = hudiField.name().toLowerCase(Locale.ROOT);
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

    public static TextFileFormatDesc toTextFileFormatDesc(Map<String, String> parameters) {
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
        if (parameters.containsKey("colelction.delim")) {
            collectionDelim = parameters.getOrDefault("colelction.delim", "");
        } else {
            collectionDelim = parameters.getOrDefault(serdeConstants.COLLECTION_DELIM, "");
        }

        String fieldDelim = parameters.getOrDefault(serdeConstants.FIELD_DELIM, "");
        if (fieldDelim.isEmpty()) {
            // Support for hive org.apache.hadoop.hive.serde2.OpenCSVSerde
            // https://cwiki.apache.org/confluence/display/hive/csv+serde
            fieldDelim = parameters.getOrDefault(OpenCSVSerde.SEPARATORCHAR, "");
        }
        String lineDelim = parameters.getOrDefault(serdeConstants.LINE_DELIM, "");
        String mapkeyDelim = parameters.getOrDefault(serdeConstants.MAPKEY_DELIM, "");
        int skipHeaderLineCount = Integer.parseInt(parameters.getOrDefault(serdeConstants.HEADER_COUNT, "0"));
        if (skipHeaderLineCount < 0) {
            skipHeaderLineCount = 0;
        }

        // check delim is empty, if it's empty, we convert it to null
        fieldDelim = fieldDelim.isEmpty() ? null : fieldDelim;
        lineDelim = lineDelim.isEmpty() ? null : lineDelim;
        collectionDelim = collectionDelim.isEmpty() ? null : collectionDelim;
        mapkeyDelim = mapkeyDelim.isEmpty() ? null : mapkeyDelim;

        return new TextFileFormatDesc(fieldDelim, lineDelim, collectionDelim, mapkeyDelim, skipHeaderLineCount);
    }

    public static HiveCommonStats toHiveCommonStats(Map<String, String> params) {
        long numRows = getLongParam(ROW_COUNT, params);
        if (numRows == -1 && Config.enable_reuse_spark_column_statistics) {
            numRows = getLongParam("spark.sql.statistics.numRows", params);
        }
        long totalSize = getLongParam(TOTAL_SIZE, params);
        if (totalSize == -1 && Config.enable_reuse_spark_column_statistics) {
            totalSize = getLongParam("spark.sql.statistics.totalSize", params);
        }
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

    public static List<ColumnStatisticsObj> getColStatsFromSparkParams(org.apache.hadoop.hive.metastore.api.Table table) {
        return table.getSd().getCols().stream()
                .map(fieldSchema -> convertSparkColumnStatistics(fieldSchema, table.getParameters()))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * Refer to https://github.com/apache/hive/blob/rel/release-3.1.3/ql/src/java/org/apache/hadoop/hive/ql/exec/ColumnStatsUpdateTask.java#L77
     */
    public static ColumnStatisticsObj convertSparkColumnStatistics(FieldSchema fieldSchema, Map<String, String> parameters) {

        String columnName = fieldSchema.getName();
        String columnType = fieldSchema.getType().toLowerCase();
        String colStatsPrefix = "spark.sql.statistics.colStats." + columnName + ".";
        if (parameters.keySet().stream().noneMatch(k -> k.startsWith(colStatsPrefix))) {
            // return early if no stats for this column
            return null;
        }
        long distinctCount = Long.parseLong(parameters.get(colStatsPrefix + "distinctCount"));
        long nullsCount = Long.parseLong(parameters.get(colStatsPrefix + "nullCount"));
        ColumnStatisticsObj colStatsObj = new ColumnStatisticsObj();
        colStatsObj.setColName(columnName);
        colStatsObj.setColType(columnType);
        ColumnStatisticsData colStatsData = new ColumnStatisticsData();
        switch (columnType) {
            case "long":
            case "tinyint":
            case "smallint":
            case "int":
            case "bigint":
            case "timestamp":
                LongColumnStatsData longColStats = new LongColumnStatsData();
                longColStats.setNumNulls(nullsCount);
                longColStats.setLowValue(Long.parseLong(parameters.get(colStatsPrefix + "min")));
                longColStats.setHighValue(Long.parseLong(parameters.get(colStatsPrefix + "max")));
                longColStats.setNumDVs(distinctCount);
                colStatsData.setLongStats(longColStats);
                break;
            case "double":
            case "float":
                DoubleColumnStatsData doubleColStats = new DoubleColumnStatsData();
                doubleColStats.setNumNulls(nullsCount);
                doubleColStats.setLowValue(Double.parseDouble(parameters.get(colStatsPrefix + "min")));
                doubleColStats.setHighValue(Double.parseDouble(parameters.get(colStatsPrefix + "max")));
                doubleColStats.setNumDVs(distinctCount);
                colStatsData.setDoubleStats(doubleColStats);
                break;
            case "string":
            case "char":
            case "varchar":
                StringColumnStatsData stringColStats = new StringColumnStatsData();
                stringColStats.setNumNulls(nullsCount);
                stringColStats.setAvgColLen(Double.parseDouble(parameters.get(colStatsPrefix + "avgLen")));
                stringColStats.setMaxColLen(Long.parseLong(parameters.get(colStatsPrefix + "maxLen")));
                stringColStats.setNumDVs(distinctCount);
                colStatsData.setStringStats(stringColStats);
                break;
            case "boolean":
                BooleanColumnStatsData booleanColStats = new BooleanColumnStatsData();
                booleanColStats.setNumNulls(nullsCount);
                colStatsData.setBooleanStats(booleanColStats);
                break;
            case "decimal":
                DecimalColumnStatsData decimalColStats = new DecimalColumnStatsData();
                decimalColStats.setNumNulls(nullsCount);
                BigDecimal lowVal = new BigDecimal(parameters.get(colStatsPrefix + "min"));
                decimalColStats.setLowValue(getHiveDecimal(ByteBuffer.wrap(lowVal.unscaledValue().toByteArray()),
                        (short) lowVal.scale()));
                BigDecimal highVal = new BigDecimal(parameters.get(colStatsPrefix + "max"));
                decimalColStats.setHighValue(getHiveDecimal(ByteBuffer.wrap(highVal.unscaledValue().toByteArray()),
                        (short) highVal.scale()));
                decimalColStats.setNumDVs(distinctCount);
                colStatsData.setDecimalStats(decimalColStats);
                break;
            case "date":
                DateColumnStatsData dateColStats = new DateColumnStatsData();
                dateColStats.setNumNulls(nullsCount);
                dateColStats.setLowValue(readDateValue(parameters.get(colStatsPrefix + "min")));
                dateColStats.setHighValue(readDateValue(parameters.get(colStatsPrefix + "max")));
                dateColStats.setNumDVs(distinctCount);
                colStatsData.setDateStats(dateColStats);
                break;
            default:
                LOG.warn("Unsupported column statistics type: {}", columnType);
                return null;
        }
        colStatsObj.setStatsData(colStatsData);
        return colStatsObj;
    }

    private static Decimal getHiveDecimal(ByteBuffer unscaled, short scale) {
        return new Decimal(scale, unscaled);
    }

    /**
     * Refer to https://github.com/apache/hive/blob/rel/release-3.1.3/ql/src/java/org/apache/hadoop/hive/ql/exec/ColumnStatsUpdateTask.java#L318
     */
    private static Date readDateValue(String dateStr) {
        // try either yyyy-mm-dd, or integer representing days since epoch
        try {
            DateWritableV2 writableVal = new DateWritableV2(org.apache.hadoop.hive.common.type.Date.valueOf(dateStr));
            return new Date(writableVal.getDays());
        } catch (IllegalArgumentException err) {
            // Fallback to integer parsing
            LOG.debug("Reading date value as days since epoch: {}", dateStr);
            return new Date(Long.parseLong(dateStr));
        }
    }

}
