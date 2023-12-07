// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.catalog;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.iceberg.IcebergCatalog;
import com.starrocks.connector.iceberg.IcebergCatalogType;
import com.starrocks.connector.iceberg.IcebergUtil;
import com.starrocks.connector.iceberg.StarRocksIcebergException;
import com.starrocks.connector.iceberg.io.IcebergCachingFileIO;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TColumn;
import com.starrocks.thrift.TIcebergTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IcebergTable extends Table {
    private static final Logger LOG = LogManager.getLogger(IcebergTable.class);

    private static final String PROPERTY_MISSING_MSG =
            "Iceberg %s is null. Please add properties('%s'='xxx') when create table";
    private static final String JSON_KEY_ICEBERG_DB = "database";
    private static final String JSON_KEY_ICEBERG_TABLE = "table";
    private static final String JSON_KEY_RESOURCE_NAME = "resource";
    private static final String JSON_KEY_ICEBERG_PROPERTIES = "icebergProperties";

    public static final String ICEBERG_CATALOG_TYPE = "iceberg.catalog.type";
    @Deprecated
    public static final String ICEBERG_CATALOG_LEGACY = "starrocks.catalog-type";
    public static final String ICEBERG_METASTORE_URIS = "iceberg.catalog.hive.metastore.uris";
    public static final String ICEBERG_IMPL = "iceberg.catalog-impl";
    public static final String ICEBERG_CATALOG = "catalog";
    public static final String ICEBERG_DB = "database";
    public static final String ICEBERG_TABLE = "table";
    public static final String ICEBERG_RESOURCE = "resource";
    public static final String PARTITION_NULL_VALUE = "null";

    private org.apache.iceberg.Table icbTbl; // actual iceberg table
    private boolean isCatalogTbl = false;
    private String catalog;
    private String db;
    private String table;
    private String resourceName;

    private final List<String> columnNames = Lists.newArrayList();

    private final Map<String, String> icebergProperties = Maps.newHashMap();

    public IcebergTable() {
        super(TableType.ICEBERG);
    }

    public IcebergTable(long id, org.apache.iceberg.Table icbTbl, boolean isCatalogTbl, String name,
                        List<Column> schema, Map<String, String> properties) throws DdlException {
        this(id, name, schema, properties);
        this.icbTbl = icbTbl;
        this.isCatalogTbl = isCatalogTbl;
    }

    public IcebergTable(long id, String name, List<Column> schema, Map<String, String> properties) throws DdlException {
        super(id, name, TableType.ICEBERG, schema);
        catalog = properties.get(ICEBERG_CATALOG);
        db = properties.get(ICEBERG_DB);
        table = properties.get(ICEBERG_TABLE);

        String catalogType = properties.get(ICEBERG_CATALOG_TYPE);
        if (catalogType != null && IcebergCatalogType.GLUE_CATALOG == IcebergCatalogType.valueOf(catalogType)) {
            setGlueCatalogProperties();
            return;
        }
        String metastoreURI = properties.get(ICEBERG_METASTORE_URIS);
        if (null != metastoreURI && !isInternalCatalog(metastoreURI)) {
            setHiveCatalogProperties(metastoreURI);
            return;
        }
        if (null != properties.get(ICEBERG_IMPL)) {
            setCustomCatalogProperties(properties);
            return;
        }
        validate(properties);
    }

    public String getCatalog() {
        return catalog;
    }

    public String getDb() {
        return db;
    }

    public String getTable() {
        return table;
    }

    public List<Column> getPartitionColumns() {
        List<PartitionField> identityPartitionFields = this.getIcebergTable().spec().fields().stream().
                filter(partitionField -> partitionField.transform().isIdentity()).collect(Collectors.toList());
        return identityPartitionFields.stream().map(partitionField -> getColumn(partitionField.name())).collect(
                Collectors.toList());
    }

    public boolean isUnPartitioned() {
        return getPartitionColumns().size() == 0;
    }

    public List<String> getPartitionColumnNames() {
        return getPartitionColumns().stream().filter(java.util.Objects::nonNull).map(Column::getName)
                .collect(Collectors.toList());
    }

    public String getFileIOMaxTotalBytes() {
        return icebergProperties.get(IcebergCachingFileIO.FILEIO_CACHE_MAX_TOTAL_BYTES);
    }

    public String getResourceName() {
        return resourceName;
    }

    @Override
    public String getTableIdentifier() {
        return Joiner.on(":").join(table, ((BaseTable) getIcebergTable()).operations().current().uuid());
    }

    public IcebergCatalogType getCatalogType() {
        if (icebergProperties.containsKey(ICEBERG_CATALOG_TYPE)) {
            return IcebergCatalogType.valueOf(icebergProperties.get(ICEBERG_CATALOG_TYPE));
        }
        return IcebergCatalogType.valueOf(icebergProperties.get(ICEBERG_CATALOG_LEGACY));
    }

    public String getCatalogImpl() {
        return icebergProperties.get(ICEBERG_IMPL);
    }

    public Map<String, String> getIcebergProperties() {
        return icebergProperties;
    }

    public String getIcebergHiveMetastoreUris() {
        return icebergProperties.get(ICEBERG_METASTORE_URIS);
    }

    public boolean isCatalogTbl() {
        return isCatalogTbl;
    }

    public void refreshTable() {
        IcebergUtil.refreshTable(this.getIcebergTable());
    }

    public String getTableLocation() {
        return this.getIcebergTable().location();
    }

    // icbTbl is used for caching
    public synchronized org.apache.iceberg.Table getIcebergTable() {
        try {
            if (isCatalogTbl) {
                GlobalStateMgr.getCurrentState().getIcebergRepository().getTable(icbTbl).get();
            } else {
                if (this.icbTbl == null) {
                    IcebergCatalog catalog = IcebergUtil.getIcebergCatalog(this);
                    this.icbTbl = catalog.loadTable(this);
                }
            }
        } catch (StarRocksIcebergException e) {
            LOG.error("Load iceberg table failure!", e);
            throw e;
        } catch (Exception e) {
            LOG.error("Load iceberg table failure!", e);
        }
        return icbTbl;
    }

    private void setGlueCatalogProperties() {
        icebergProperties.put(ICEBERG_CATALOG_TYPE, "GLUE_CATALOG");
    }

    private void setHiveCatalogProperties(String metastoreURI) {
        icebergProperties.put(ICEBERG_METASTORE_URIS, metastoreURI);
        icebergProperties.put(ICEBERG_CATALOG_TYPE, "HIVE_CATALOG");
    }

    private void setCustomCatalogProperties(Map<String, String> properties) {
        icebergProperties.put(ICEBERG_CATALOG_TYPE, "CUSTOM_CATALOG");
        icebergProperties.put(ICEBERG_IMPL, properties.remove(ICEBERG_IMPL));
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            icebergProperties.put(entry.getKey(), entry.getValue());
        }
    }

    private void validate(Map<String, String> properties) throws DdlException {
        if (properties == null) {
            throw new DdlException("Please set properties of iceberg table, they are: database, table.");
        }

        Map<String, String> copiedProps = Maps.newHashMap(properties);
        db = copiedProps.remove(ICEBERG_DB);
        if (Strings.isNullOrEmpty(db)) {
            throw new DdlException(String.format(PROPERTY_MISSING_MSG, ICEBERG_DB, ICEBERG_DB));
        }

        table = copiedProps.get(ICEBERG_TABLE);
        if (Strings.isNullOrEmpty(table)) {
            throw new DdlException(String.format(PROPERTY_MISSING_MSG, ICEBERG_TABLE, ICEBERG_TABLE));
        }
        copiedProps.remove(ICEBERG_TABLE);

        String resourceName = copiedProps.get(ICEBERG_RESOURCE);
        if (Strings.isNullOrEmpty(resourceName)) {
            throw new DdlException("property " + ICEBERG_RESOURCE + " must be set");
        }

        copiedProps.remove(ICEBERG_RESOURCE);
        Resource resource = GlobalStateMgr.getCurrentState().getResourceMgr().getResource(resourceName);
        if (resource == null) {
            throw new DdlException("iceberg resource [" + resourceName + "] not exists");
        }
        if (resource.getType() != Resource.ResourceType.ICEBERG) {
            throw new DdlException("resource [" + resourceName + "] is not iceberg resource");
        }
        IcebergResource icebergResource = (IcebergResource) resource;
        IcebergCatalogType type = icebergResource.getCatalogType();
        icebergProperties.put(ICEBERG_CATALOG_TYPE, type.name());
        LOG.info("Iceberg table type is " + type.name());

        String fileIOCacheMaxTotalBytes = copiedProps.get(IcebergCachingFileIO.FILEIO_CACHE_MAX_TOTAL_BYTES);
        if (!Strings.isNullOrEmpty(fileIOCacheMaxTotalBytes)) {
            icebergProperties.put(IcebergCachingFileIO.FILEIO_CACHE_MAX_TOTAL_BYTES, fileIOCacheMaxTotalBytes);
            copiedProps.remove(IcebergCachingFileIO.FILEIO_CACHE_MAX_TOTAL_BYTES);
        }

        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(icebergProperties, null);
        IcebergCatalog icebergCatalog;
        switch (type) {
            case HIVE_CATALOG:
                icebergProperties.put(ICEBERG_METASTORE_URIS, icebergResource.getHiveMetastoreURIs());
                icebergCatalog =
                        IcebergUtil.getIcebergHiveCatalog(icebergResource.getHiveMetastoreURIs(), icebergProperties,
                                hdfsEnvironment);
                break;
            case CUSTOM_CATALOG:
                icebergProperties.put(ICEBERG_IMPL, icebergResource.getIcebergImpl());
                for (String key : copiedProps.keySet()) {
                    icebergProperties.put(key, copiedProps.remove(key));
                }
                icebergCatalog =
                        IcebergUtil.getIcebergCustomCatalog(icebergResource.getIcebergImpl(), icebergProperties,
                                hdfsEnvironment);
                break;
            default:
                throw new DdlException("unsupported catalog type " + type.name());
        }
        this.resourceName = resourceName;

        validateColumn(icebergCatalog);

        if (!copiedProps.isEmpty()) {
            throw new DdlException("Unknown table properties: " + copiedProps.toString());
        }
    }

    private void validateColumn(IcebergCatalog catalog) throws DdlException {
        org.apache.iceberg.Table icebergTable = catalog.loadTable(IcebergUtil.getIcebergTableIdentifier(db, table));
        try {
            // TODO: use TypeUtil#indexByName to handle nested field
            Map<String, Types.NestedField> icebergColumns = icebergTable.schema().columns().stream()
                    .collect(Collectors.toMap(Types.NestedField::name, field -> field));
            for (Column column : this.fullSchema) {
                Types.NestedField icebergColumn = icebergColumns.get(column.getName());
                if (icebergColumn == null) {
                    throw new DdlException("column [" + column.getName() + "] not exists in iceberg");
                }
                if (!validateColumnType(icebergColumn.type(), column.getType())) {
                    throw new DdlException("can not convert iceberg column type [" + icebergColumn.type() + "] to " +
                            "starrocks type [" + column.getPrimitiveType() + "], column name: " + column.getName());
                }
                if (!column.isAllowNull()) {
                    throw new DdlException(
                            "iceberg extern table not support no-nullable column: [" + icebergColumn.name() + "]");
                }
            }
            LOG.debug("successfully validating columns for " + catalog);
        } catch (NullPointerException e) {
            throw new DdlException("Can not find iceberg table " + db + "." + table + " from the resource " + resourceName);
        }
    }

    private boolean validateColumnType(Type icebergType, com.starrocks.catalog.Type type) {
        if (icebergType == null) {
            return false;
        }

        if (icebergType.isListType()) {
            return validateColumnType(icebergType.asListType().elementType(), ((ArrayType) type).getItemType());
        }

        if (!icebergType.isPrimitiveType()) {
            return false;
        }
        PrimitiveType primitiveType = type.getPrimitiveType();
        // for type with length, like char(10), we only check the type and ignore the length
        // TODO: fixed and binary should be considered as binary
        switch (icebergType.typeId()) {
            case BOOLEAN:
                return primitiveType == PrimitiveType.BOOLEAN;
            case INTEGER:
                return primitiveType == PrimitiveType.INT;
            case LONG:
                return primitiveType == PrimitiveType.BIGINT;
            case FLOAT:
                return primitiveType == PrimitiveType.FLOAT;
            case DOUBLE:
                return primitiveType == PrimitiveType.DOUBLE;
            case DATE:
                return primitiveType == PrimitiveType.DATE;
            case TIMESTAMP:
                return primitiveType == PrimitiveType.DATETIME;
            case STRING:
            case UUID:
                return primitiveType == PrimitiveType.VARCHAR ||
                        primitiveType == PrimitiveType.CHAR;
            case DECIMAL:
                return primitiveType == PrimitiveType.DECIMALV2 ||
                        primitiveType == PrimitiveType.DECIMAL32 ||
                        primitiveType == PrimitiveType.DECIMAL64 ||
                        primitiveType == PrimitiveType.DECIMAL128;
            case TIME:
            case FIXED:
            case BINARY:
            case STRUCT:
            case LIST:
            case MAP:
            default:
                return primitiveType == PrimitiveType.UNKNOWN_TYPE;
        }
    }

    // In the first phase of connector, in order to reduce changes, we use `hive.metastore.uris` as resource name
    // for table of external catalog. The table of external catalog will not create a real resource.
    // We will reconstruct this part later. The concept of resource will not be used for external catalog

    // Only iceberg catalog use this method at present. it will be removed after refactoring iceberg catalog.
    public static boolean isInternalCatalog(String resourceName) {
        return !resourceName.startsWith("thrift://");
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        Preconditions.checkNotNull(partitions);

        TIcebergTable tIcebergTable = new TIcebergTable();

        List<TColumn> tColumns = Lists.newArrayList();
        for (Column column : getBaseSchema()) {
            tColumns.add(column.toThrift());
        }
        tIcebergTable.setColumns(tColumns);

        TTableDescriptor tTableDescriptor = new TTableDescriptor(id, TTableType.ICEBERG_TABLE,
                fullSchema.size(), 0, table, db);
        tTableDescriptor.setIcebergTable(tIcebergTable);
        return tTableDescriptor;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty(JSON_KEY_ICEBERG_DB, db);
        jsonObject.addProperty(JSON_KEY_ICEBERG_TABLE, table);
        if (!Strings.isNullOrEmpty(resourceName)) {
            jsonObject.addProperty(JSON_KEY_RESOURCE_NAME, resourceName);
        }
        if (!icebergProperties.isEmpty()) {
            JsonObject jIcebergProperties = new JsonObject();
            for (Map.Entry<String, String> entry : icebergProperties.entrySet()) {
                jIcebergProperties.addProperty(entry.getKey(), entry.getValue());
            }
            jsonObject.add(JSON_KEY_ICEBERG_PROPERTIES, jIcebergProperties);
        }
        Text.writeString(out, jsonObject.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        String json = Text.readString(in);
        JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();
        db = jsonObject.getAsJsonPrimitive(JSON_KEY_ICEBERG_DB).getAsString();
        table = jsonObject.getAsJsonPrimitive(JSON_KEY_ICEBERG_TABLE).getAsString();
        resourceName = jsonObject.getAsJsonPrimitive(JSON_KEY_RESOURCE_NAME).getAsString();
        if (jsonObject.has(JSON_KEY_ICEBERG_PROPERTIES)) {
            JsonObject jIcebergProperties = jsonObject.getAsJsonObject(JSON_KEY_ICEBERG_PROPERTIES);
            for (Map.Entry<String, JsonElement> entry : jIcebergProperties.entrySet()) {
                icebergProperties.put(entry.getKey(), entry.getValue().getAsString());
            }
        }
        {
            for (Column col : fullSchema) {
                columnNames.add(col.getName());
            }
        }
    }

    @Override
    public boolean isSupported() {
        return true;
    }

    @Override
    public int hashCode() {
        return com.google.common.base.Objects.hashCode(getCatalog(), db, getTableIdentifier());
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof IcebergTable)) {
            return false;
        }

        IcebergTable otherTable = (IcebergTable) other;
        String catalogName = getCatalog();
        String tableIdentifier = getTableIdentifier();
        return Objects.equal(catalogName, otherTable.getCatalog()) &&
                Objects.equal(db, otherTable.db) &&
                Objects.equal(tableIdentifier, otherTable.getTableIdentifier());
    }
}
