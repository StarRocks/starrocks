// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.catalog;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.external.HiveMetaStoreTableUtils;
import com.starrocks.external.hive.HiveColumnStats;
import com.starrocks.external.hive.HivePartition;
import com.starrocks.external.hive.HiveRepository;
import com.starrocks.external.hive.HiveTableStats;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TColumn;
import com.starrocks.thrift.THdfsPartition;
import com.starrocks.thrift.THdfsPartitionLocation;
import com.starrocks.thrift.THudiTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.external.HiveMetaStoreTableUtils.isInternalCatalog;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils.getTypeInfoFromTypeString;

/**
 * Currently, we depend on Hive metastore to obtain table/partition path and statistics.
 * This logic should be decoupled from metastore when the related interfaces are ready.
 */
public class HudiTable extends Table implements HiveMetaStoreTable {
    private static final Logger LOG = LogManager.getLogger(HudiTable.class);

    private static final String PROPERTY_MISSING_MSG =
            "Hudi %s is null. Please add properties('%s'='xxx') when create table";
    private static final String JSON_KEY_HUDI_DB = "database";
    private static final String JSON_KEY_HUDI_TABLE = "table";
    private static final String JSON_KEY_RESOURCE_NAME = "resource";
    private static final String JSON_KEY_PART_COLUMN_NAMES = "partColumnNames";
    private static final String JSON_KEY_DATA_COLUMN_NAMES = "dataColumnNames";
    private static final String JSON_KEY_HUDI_PROPERTIES = "hudiProperties";

    public static final String HUDI_TABLE_TYPE = "hudi.table.type";
    public static final String HUDI_BASE_PATH = "hudi.table.base.path";
    public static final String HUDI_TABLE_SERDE_LIB = "hudi.table.serde.lib";
    public static final String HUDI_TABLE_INPUT_FOAMT = "hudi.table.input.format";
    public static final String HUDI_TABLE_COLUMN_NAMES = "hudi.table.column.names";
    public static final String HUDI_TABLE_COLUMN_TYPES = "hudi.table.column.types";
    public static final String HUDI_DB = "database";
    public static final String HUDI_TABLE = "table";
    public static final String HUDI_RESOURCE = "resource";

    public static final String COW_INPUT_FORMAT = "org.apache.hudi.hadoop.HoodieParquetInputFormat";
    public static final String COW_INPUT_FORMAT_LEGACY = "com.uber.hoodie.hadoop.HoodieInputFormat";
    public static final String MOR_RO_INPUT_FORMAT = "org.apache.hudi.hadoop.HoodieParquetInputFormat";
    public static final String MOR_RO_INPUT_FORMAT_LEGACY = "com.uber.hoodie.hadoop.HoodieInputFormat";
    public static final String MOR_RT_INPUT_FORMAT = "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat";
    public static final String MOR_RT_INPUT_FORMAT_LEGACY = "com.uber.hoodie.hadoop.realtime.HoodieRealtimeInputFormat";

    public enum HudiTableType {
        COW, MOR, UNKNOWN
    }

    private String db;
    private String table;
    private String resourceName;

    private List<String> partColumnNames = Lists.newArrayList();
    // dataColumnNames stores all the non-partition columns of the hudi table,
    // consistent with the order defined in the hudi table
    private List<String> dataColumnNames = Lists.newArrayList();

    private Map<String, String> hudiProperties = Maps.newHashMap();

    private HiveMetaStoreTableInfo hmsTableInfo;

    public HudiTable() {
        super(TableType.HUDI);
    }

    public HudiTable(long id, String name, List<Column> schema, Map<String, String> properties) throws DdlException {
        super(id, name, TableType.HUDI, schema);
        initProps(properties);
        initHmsTableInfo();
    }

    public String getDbName() {
        return db;
    }

    public String getTable() {
        return table;
    }

    public String getResourceName() {
        return resourceName;
    }

    @Override
    public String getCatalogName() {
        return null;
    }

    public HoodieTableType getTableType() {
        return HoodieTableType.valueOf(hudiProperties.get(HUDI_TABLE_TYPE));
    }

    public String getHudiBasePath() {
        return hudiProperties.get(HUDI_BASE_PATH);
    }

    public String getHudiInputFormat() {
        return hudiProperties.get(HUDI_TABLE_INPUT_FOAMT);
    }

    @Override
    public String getTableName() {
        return table;
    }

    public HiveMetaStoreTableInfo getHmsTableInfo() {
        return hmsTableInfo;
    }

    @Override
    public List<Column> getPartitionColumns() {
        return HiveMetaStoreTableUtils.getPartitionColumns(hmsTableInfo);
    }

    @Override
    public List<String> getPartitionColumnNames() {
        return partColumnNames;
    }

    public List<String> getDataColumnNames() {
        return dataColumnNames;
    }

    @Override
    public boolean isUnPartitioned() {
        return partColumnNames.size() == 0;
    }

    @Override
    public String getTableIdentifier() {
        return Joiner.on(":").join(table, createTime);
    }

    public static boolean isHudiTable(String inputFormat) {
        return HudiTable.fromInputFormat(inputFormat) != HudiTable.HudiTableType.UNKNOWN;
    }

    public static HudiTableType fromInputFormat(String inputFormat) {
        switch (inputFormat) {
            case COW_INPUT_FORMAT:
            case COW_INPUT_FORMAT_LEGACY:
                return HudiTableType.COW;
            case MOR_RT_INPUT_FORMAT:
            case MOR_RT_INPUT_FORMAT_LEGACY:
                return HudiTableType.MOR;
            default:
                return HudiTableType.UNKNOWN;
        }
    }

    public void initHmsTableInfo() {
        if (hmsTableInfo == null) {
            hmsTableInfo =
                    new HiveMetaStoreTableInfo(resourceName, db, table, partColumnNames, dataColumnNames, nameToColumn,
                            type);
        }
    }

    public Map<PartitionKey, Long> getPartitionKeys() throws DdlException {
        return HiveMetaStoreTableUtils.getPartitionKeys(hmsTableInfo);
    }

    @Override
    public List<HivePartition> getPartitions(List<PartitionKey> partitionKeys) throws DdlException {
        return HiveMetaStoreTableUtils.getPartitions(hmsTableInfo, partitionKeys);
    }

    @Override
    public HiveTableStats getTableStats() throws DdlException {
        return HiveMetaStoreTableUtils.getTableStats(hmsTableInfo);
    }

    @Override
    public Map<String, HiveColumnStats> getTableLevelColumnStats(List<String> columnNames) throws DdlException {
        return HiveMetaStoreTableUtils.getTableLevelColumnStats(hmsTableInfo, columnNames);
    }

    // same as hive table, refresh table meta
    // if table is internal table, refresh table cache
    // if table is connector table, refresh table meta in hiveMetaCache
    @Override
    public void refreshTableCache(String dbName, String tableName) throws DdlException {
        HiveRepository hiveRepository = GlobalStateMgr.getCurrentState().getHiveRepository();
        try {
            if (HiveMetaStoreTableUtils.isInternalCatalog(resourceName)) {
                hiveRepository.refreshTableCache(hmsTableInfo);
            } else {
                hiveRepository.refreshConnectorTable(resourceName, dbName, tableName);
            }
        } catch (Exception e) {
            hiveRepository.clearCache(hmsTableInfo);
            LOG.warn("Failed to refresh [{}.{}.{}]. Invalidate all cache on it", resourceName, dbName, tableName);
            throw new DdlException(e.getMessage());
        }
    }

    // same as hive table, refresh partition meta
    @Override
    public void refreshPartCache(List<String> partNames) throws DdlException {
        GlobalStateMgr.getCurrentState().getHiveRepository().refreshPartitionCache(hmsTableInfo, partNames);
    }

    @Override
    public void refreshTableColumnStats() throws DdlException {
        GlobalStateMgr.getCurrentState().getHiveRepository().refreshTableColumnStats(hmsTableInfo);
    }

    /**
     * Computes and returns the number of rows scanned based on the per-partition row count stats
     * TODO: consider missing or corrupted partition stats
     */
    @Override
    public long getPartitionStatsRowCount(List<PartitionKey> partitions) {
        return HiveMetaStoreTableUtils.getPartitionStatsRowCount(hmsTableInfo, partitions);
    }

    private void initProps(Map<String, String> properties) throws DdlException {
        if (properties == null) {
            throw new DdlException("Please set properties of hudi table.");
        }

        Map<String, String> copiedProps = Maps.newHashMap(properties);
        this.db = copiedProps.remove(HUDI_DB);
        if (Strings.isNullOrEmpty(db)) {
            throw new DdlException(String.format(PROPERTY_MISSING_MSG, HUDI_DB, HUDI_DB));
        }

        this.table = copiedProps.remove(HUDI_TABLE);
        if (Strings.isNullOrEmpty(table)) {
            throw new DdlException(String.format(PROPERTY_MISSING_MSG, HUDI_TABLE, HUDI_TABLE));
        }

        String resourceName = copiedProps.remove(HUDI_RESOURCE);
        // trick like hive table, only external table has resourceName
        if (isInternalCatalog(resourceName)) {
            Resource resource = GlobalStateMgr.getCurrentState().getResourceMgr().getResource(resourceName);
            if (resource == null) {
                throw new DdlException("hive resource [" + resourceName + "] not exists");
            }
            if (resource.getType() != Resource.ResourceType.HUDI) {
                throw new DdlException("Resource [" + resourceName + "] is not hudi resource");
            }
        }
        this.resourceName = resourceName;

        org.apache.hadoop.hive.metastore.api.Table metastoreTable =
                GlobalStateMgr.getCurrentState().getHiveRepository().getTable(resourceName, this.db, this.table);
        Schema tableSchema = HudiTable.loadHudiSchema(metastoreTable);
        this.createTime = metastoreTable.getCreateTime();

        for (Column column : this.fullSchema) {
            if (!column.isAllowNull()) {
                throw new DdlException(
                        "Hudi extern table does not support no-nullable column: [" + column.getName() + "]");
            }
            Schema.Field hudiColumn = tableSchema.getField(column.getName());
            if (hudiColumn == null) {
                throw new DdlException("Column [" + column.getName() + "] not exists in hudi.");
            }
            // for each column in hudi schema, we should transfer hudi column type to starrocks type
            // after that, we should check column type whether is same
            // Only internal catalog like hudi external table need to validate column type
            if (HiveMetaStoreTableUtils.isInternalCatalog(resourceName) &&
                    !validColumnType(hudiColumn.schema(), column.getType())) {
                throw new DdlException("Can not convert hudi column type [" + hudiColumn.schema().toString() + "] " +
                        "to starrocks type [" + column.getPrimitiveType() + "], column name: " + column.getName()
                        + ", starrocks type should be "
                        + HiveMetaStoreTableUtils.convertHudiTableColumnType(hudiColumn.schema()).toSql());
            }
        }

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

        Configuration conf = new Configuration();
        HoodieTableMetaClient metaClient =
                HoodieTableMetaClient.builder().setConf(conf).setBasePath(hudiBasePath).build();
        HoodieTableConfig hudiTableConfig = metaClient.getTableConfig();

        HoodieTableType hudiTableType = metaClient.getTableType();
        hudiProperties.put(HUDI_TABLE_TYPE, hudiTableType.name());

        StringBuilder columnNamesBuilder = new StringBuilder();
        StringBuilder columnTypesBuilder = new StringBuilder();
        List<FieldSchema> allFields = metastoreTable.getSd().getCols();
        allFields.addAll(metastoreTable.getPartitionKeys());

        boolean isFirst = true;
        for (Schema.Field hudiField : tableSchema.getFields()) {
            if (!isFirst) {
                columnNamesBuilder.append(",");
                columnTypesBuilder.append(":");
            }
            Optional<FieldSchema> field = allFields.stream()
                    .filter(f -> f.getName().equals(hudiField.name().toLowerCase(Locale.ROOT))).findFirst();
            if (!field.isPresent()) {
                throw new DdlException("Hudi column [" + hudiField.name() + "] not exists in hive metastore.");
            }
            TypeInfo fieldInfo = getTypeInfoFromTypeString(field.get().getType());
            columnNamesBuilder.append(field.get().getName());
            columnTypesBuilder.append(fieldInfo.getTypeName());
            isFirst = false;
        }
        hudiProperties.put(HUDI_TABLE_COLUMN_NAMES, columnNamesBuilder.toString());
        hudiProperties.put(HUDI_TABLE_COLUMN_TYPES, columnTypesBuilder.toString());

        Option<String[]> hudiPartitionFields = hudiTableConfig.getPartitionFields();
        if (hudiPartitionFields.isPresent()) {
            for (String partField : hudiPartitionFields.get()) {
                Column partColumn = this.nameToColumn.get(partField);
                if (partColumn == null) {
                    throw new DdlException("Partition column [" + partField + "] must exist in column list");
                } else {
                    this.partColumnNames.add(partField);
                }
            }
        } else if (!metastoreTable.getPartitionKeys().isEmpty()) {
            for (FieldSchema fieldSchema : metastoreTable.getPartitionKeys()) {
                String partField = fieldSchema.getName();
                Column partColumn = this.nameToColumn.get(partField);
                if (partColumn == null) {
                    throw new DdlException("Partition column [" + partField + "] must exist in column list");
                } else {
                    this.partColumnNames.add(partField);
                }
            }
        }

        for (Schema.Field hudiField : tableSchema.getFields()) {
            if (this.partColumnNames.stream().noneMatch(p -> p.equals(hudiField.name()))) {
                this.dataColumnNames.add(hudiField.name());
            }
        }
        if (!copiedProps.isEmpty()) {
            throw new DdlException("Unknown table properties: " + copiedProps);
        }
    }

    // validate hudi table column type
    public static org.apache.hadoop.hive.metastore.api.Table validate(String resourceName, String db, String table,
                                                                      List<Column> fullSchema) throws DdlException {
        org.apache.hadoop.hive.metastore.api.Table metastoreTable =
                GlobalStateMgr.getCurrentState().getHiveRepository().getTable(resourceName, db, table);

        Schema tableSchema = HudiTable.loadHudiSchema(metastoreTable);

        for (Column column : fullSchema) {
            if (!column.isAllowNull()) {
                throw new DdlException(
                        "Hudi extern table does not support no-nullable column: [" + column.getName() + "]");
            }
            Schema.Field hudiColumn = tableSchema.getField(column.getName());
            if (hudiColumn == null) {
                throw new DdlException("Column [" + column.getName() + "] not exists in hudi.");
            }
            // for each column in hudi schema, we should transfer hudi column type to starrocks type
            // after that, we should check column type whether is same
            // Only internal catalog like hudi external table need to validate column type
            if (HiveMetaStoreTableUtils.isInternalCatalog(resourceName) &&
                    !validColumnType(hudiColumn.schema(), column.getType())) {
                throw new DdlException("Can not convert hudi column type [" + hudiColumn.schema().toString() + "] " +
                        "to starrocks type [" + column.getPrimitiveType() + "], column name: " + column.getName()
                        + ", starrocks type should be "
                        + HiveMetaStoreTableUtils.convertHudiTableColumnType(hudiColumn.schema()).toSql());
            }
        }
        return metastoreTable;
    }

    // load hudi table schema
    public static Schema loadHudiSchema(org.apache.hadoop.hive.metastore.api.Table hiveTable) throws DdlException {
        String hudiBasePath = hiveTable.getSd().getLocation();
        Configuration conf = new Configuration();
        HoodieTableMetaClient metaClient =
                HoodieTableMetaClient.builder().setConf(conf).setBasePath(hudiBasePath).build();
        TableSchemaResolver schemaUtil = new TableSchemaResolver(metaClient);
        Schema hudiTable;
        try {
            hudiTable = HoodieAvroUtils.createHoodieWriteSchema(schemaUtil.getTableAvroSchema());
        } catch (Exception e) {
            throw new DdlException("Cannot get hudi table schema.");
        }
        return hudiTable;
    }

    private static boolean validColumnType(Schema avroSchema, Type srType) {
        Schema.Type columnType = avroSchema.getType();
        if (columnType == null) {
            return false;
        }

        PrimitiveType primitiveType = srType.getPrimitiveType();
        LogicalType logicalType = avroSchema.getLogicalType();

        switch (columnType) {
            case BOOLEAN:
                return primitiveType == PrimitiveType.BOOLEAN;
            case INT:
                if (logicalType instanceof LogicalTypes.Date) {
                    return primitiveType == PrimitiveType.DATE;
                } else if (logicalType instanceof LogicalTypes.TimeMillis) {
                    return primitiveType == PrimitiveType.TIME;
                } else {
                    return primitiveType == PrimitiveType.INT;
                }
            case LONG:
                if (logicalType instanceof LogicalTypes.TimeMicros) {
                    return primitiveType == PrimitiveType.TIME;
                } else if (logicalType instanceof LogicalTypes.TimestampMillis
                        || logicalType instanceof LogicalTypes.TimestampMicros) {
                    return primitiveType == PrimitiveType.DATETIME;
                } else {
                    return primitiveType == PrimitiveType.BIGINT;
                }
            case FLOAT:
                return primitiveType == PrimitiveType.FLOAT;
            case DOUBLE:
                return primitiveType == PrimitiveType.DOUBLE;
            case STRING:
                return primitiveType == PrimitiveType.VARCHAR ||
                        primitiveType == PrimitiveType.CHAR;
            case ARRAY:
                return validColumnType(avroSchema.getElementType(), ((ArrayType) srType).getItemType());
            case FIXED:
            case BYTES:
                if (logicalType instanceof LogicalTypes.Decimal) {
                    return primitiveType == PrimitiveType.DECIMALV2 || primitiveType == PrimitiveType.DECIMAL32 ||
                            primitiveType == PrimitiveType.DECIMAL64 || primitiveType == PrimitiveType.DECIMAL128;
                } else {
                    return primitiveType == PrimitiveType.VARCHAR;
                }
            case UNION:
                List<Schema> nonNullMembers = avroSchema.getTypes().stream()
                        .filter(schema -> !Schema.Type.NULL.equals(schema.getType()))
                        .collect(Collectors.toList());

                if (nonNullMembers.size() == 1) {
                    return validColumnType(nonNullMembers.get(0), srType);
                } else {
                    // UNION type is not supported in Starrocks
                    return primitiveType == PrimitiveType.UNKNOWN_TYPE;
                }
            case ENUM:
            case MAP:
            default:
                return primitiveType == PrimitiveType.UNKNOWN_TYPE;
        }
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        Preconditions.checkNotNull(partitions);

        THudiTable tHudiTable = new THudiTable();
        tHudiTable.setLocation(getHudiBasePath());

        // columns and partition columns
        Set<String> partitionColumnNames = Sets.newHashSet();
        List<TColumn> tPartitionColumns = Lists.newArrayList();
        List<TColumn> tColumns = Lists.newArrayList();

        for (Column column : getPartitionColumns()) {
            tPartitionColumns.add(column.toThrift());
            partitionColumnNames.add(column.getName());
        }
        for (Column column : getBaseSchema()) {
            if (partitionColumnNames.contains(column.getName())) {
                continue;
            }
            tColumns.add(column.toThrift());
        }
        tHudiTable.setColumns(tColumns);
        if (!tPartitionColumns.isEmpty()) {
            tHudiTable.setPartition_columns(tPartitionColumns);
        }

        // partitions
        List<PartitionKey> partitionKeys = Lists.newArrayList();
        for (DescriptorTable.ReferencedPartitionInfo partition : partitions) {
            partitionKeys.add(partition.getKey());
        }
        List<HivePartition> hudiPartitions;
        try {
            hudiPartitions = getPartitions(partitionKeys);
        } catch (DdlException e) {
            LOG.warn("Table {} gets partition info failed.", name, e);
            return null;
        }

        for (int i = 0; i < hudiPartitions.size(); i++) {
            DescriptorTable.ReferencedPartitionInfo info = partitions.get(i);
            PartitionKey key = info.getKey();
            long partitionId = info.getId();

            THdfsPartition tPartition = new THdfsPartition();
            tPartition.setFile_format(hudiPartitions.get(i).getFormat().toThrift());

            List<LiteralExpr> keys = key.getKeys();
            tPartition.setPartition_key_exprs(keys.stream().map(Expr::treeToThrift).collect(Collectors.toList()));

            THdfsPartitionLocation tPartitionLocation = new THdfsPartitionLocation();
            tPartitionLocation.setPrefix_index(-1);
            tPartitionLocation.setSuffix(hudiPartitions.get(i).getFullPath());
            tPartition.setLocation(tPartitionLocation);
            tHudiTable.putToPartitions(partitionId, tPartition);
        }

        Configuration conf = new Configuration();
        HoodieTableMetaClient metaClient =
                HoodieTableMetaClient.builder().setConf(conf).setBasePath(getHudiBasePath()).build();
        HoodieTimeline timeline = metaClient.getActiveTimeline().filterCompletedAndCompactionInstants();
        String queryInstant = timeline.lastInstant().get().getTimestamp();
        tHudiTable.setInstant_time(queryInstant);
        tHudiTable.setHive_column_names(hudiProperties.get(HUDI_TABLE_COLUMN_NAMES));
        tHudiTable.setHive_column_types(hudiProperties.get(HUDI_TABLE_COLUMN_TYPES));
        tHudiTable.setInput_format(hudiProperties.get(HUDI_TABLE_INPUT_FOAMT));
        tHudiTable.setSerde_lib(hudiProperties.get(HUDI_TABLE_SERDE_LIB));
        tHudiTable.setIs_mor_table(getTableType() == HoodieTableType.MERGE_ON_READ);

        TTableDescriptor tTableDescriptor =
                new TTableDescriptor(id, TTableType.HUDI_TABLE, fullSchema.size(), 0, table, db);
        tTableDescriptor.setHudiTable(tHudiTable);
        return tTableDescriptor;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty(JSON_KEY_HUDI_DB, db);
        jsonObject.addProperty(JSON_KEY_HUDI_TABLE, table);
        if (!Strings.isNullOrEmpty(resourceName)) {
            jsonObject.addProperty(JSON_KEY_RESOURCE_NAME, resourceName);
        }
        if (!partColumnNames.isEmpty()) {
            JsonArray jPartColumnNames = new JsonArray();
            for (String partColName : partColumnNames) {
                jPartColumnNames.add(partColName);
            }
            jsonObject.add(JSON_KEY_PART_COLUMN_NAMES, jPartColumnNames);
        }
        if (!dataColumnNames.isEmpty()) {
            JsonArray jDataColumnNames = new JsonArray();
            for (String dataColumnName : dataColumnNames) {
                jDataColumnNames.add(dataColumnName);
            }
            jsonObject.add(JSON_KEY_DATA_COLUMN_NAMES, jDataColumnNames);
        }
        if (!hudiProperties.isEmpty()) {
            JsonObject jHudiProperties = new JsonObject();
            for (Map.Entry<String, String> entry : hudiProperties.entrySet()) {
                jHudiProperties.addProperty(entry.getKey(), entry.getValue());
            }
            jsonObject.add(JSON_KEY_HUDI_PROPERTIES, jHudiProperties);
        }
        Text.writeString(out, jsonObject.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        String json = Text.readString(in);
        JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();
        db = jsonObject.getAsJsonPrimitive(JSON_KEY_HUDI_DB).getAsString();
        table = jsonObject.getAsJsonPrimitive(JSON_KEY_HUDI_TABLE).getAsString();
        if (jsonObject.has(JSON_KEY_RESOURCE_NAME)) {
            resourceName = jsonObject.getAsJsonPrimitive(JSON_KEY_RESOURCE_NAME).getAsString();
        }
        if (jsonObject.has(JSON_KEY_PART_COLUMN_NAMES)) {
            JsonArray jPartColumnNames = jsonObject.getAsJsonArray(JSON_KEY_PART_COLUMN_NAMES);
            for (int i = 0; i < jPartColumnNames.size(); i++) {
                partColumnNames.add(jPartColumnNames.get(i).getAsString());
            }
        }
        if (jsonObject.has(JSON_KEY_HUDI_PROPERTIES)) {
            JsonObject jHudiProperties = jsonObject.getAsJsonObject(JSON_KEY_HUDI_PROPERTIES);
            for (Map.Entry<String, JsonElement> entry : jHudiProperties.entrySet()) {
                hudiProperties.put(entry.getKey(), entry.getValue().getAsString());
            }
        }
        if (jsonObject.has(JSON_KEY_DATA_COLUMN_NAMES)) {
            JsonArray jDataColumnNames = jsonObject.getAsJsonArray(JSON_KEY_DATA_COLUMN_NAMES);
            for (int i = 0; i < jDataColumnNames.size(); i++) {
                dataColumnNames.add(jDataColumnNames.get(i).getAsString());
            }
        } else {
            // In order to be compatible with the case where JSON_KEY_DATA_COLUMN_NAMES does not exist.
            // Just put (full schema - partition columns) to dataColumnNames.
            // But there may be errors, because fullSchema may not store all the non-partition columns of the hive table
            // and the order may be inconsistent with that in hive

            // full schema - partition columns = data columns
            HashSet<String> partColumnSet = new HashSet<>(partColumnNames);
            for (Column col : fullSchema) {
                if (!partColumnSet.contains(col.getName())) {
                    dataColumnNames.add(col.getName());
                }
            }
        }
        initHmsTableInfo();
    }

    @Override
    public void onDrop(Database db, boolean force, boolean replay) {
        if (this.resourceName != null) {
            GlobalStateMgr.getCurrentState().getHiveRepository().clearCache(hmsTableInfo);
        }
    }

    @Override
    public boolean isSupported() {
        return true;
    }
}
