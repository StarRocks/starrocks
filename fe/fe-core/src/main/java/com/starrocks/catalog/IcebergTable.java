// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

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
import com.starrocks.external.iceberg.IcebergCatalog;
import com.starrocks.external.iceberg.IcebergCatalogType;
import com.starrocks.external.iceberg.IcebergUtil;
import com.starrocks.external.iceberg.StarRocksIcebergException;
import com.starrocks.thrift.TColumn;
import com.starrocks.thrift.TIcebergTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
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

    private static final String ICEBERG_CATALOG = "starrocks.catalog-type";
    private static final String ICEBERG_METASTORE_URIS = "iceberg.catalog.hive.metastore.uris";
    private static final String ICEBERG_DB = "database";
    private static final String ICEBERG_TABLE = "table";
    private static final String ICEBERG_RESOURCE = "resource";

    private org.apache.iceberg.Table icbTbl; // actual iceberg table

    private String db;
    private String table;
    private String resourceName;
    private String tableLocation;

    private final List<String> columnNames = Lists.newArrayList();

    private final Map<String, String> icebergProperties = Maps.newHashMap();

    public IcebergTable() {
        super(TableType.ICEBERG);
    }

    public IcebergTable(long id, String name, List<Column> schema, Map<String, String> properties) throws DdlException {
        super(id, name, TableType.ICEBERG, schema);
        validate(properties);
    }

    public String getDb() {
        return db;
    }

    public String getTable() {
        return table;
    }

    public String getResourceName() {
        return resourceName;
    }

    public IcebergCatalogType getCatalogType() {
        return IcebergCatalogType.valueOf(icebergProperties.get(ICEBERG_CATALOG));
    }

    public String getIcebergHiveMetastoreUris() {
        return icebergProperties.get(ICEBERG_METASTORE_URIS);
    }

    public void setTableLocation(String location) {
        this.tableLocation = location;
    }

    public void refreshTable() {
        IcebergUtil.refreshTable(this.getIcebergTable());
    }

    // icbTbl is used for caching
    public synchronized org.apache.iceberg.Table getIcebergTable() {
        try {
            if (this.icbTbl == null) {
                IcebergCatalog catalog = IcebergUtil.getIcebergCatalog(this);
                this.icbTbl = catalog.loadTable(this);
            }
        } catch (StarRocksIcebergException e) {
            LOG.error("Load iceberg table failure!", e);
            throw e;
        }
        return icbTbl;
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
        Resource resource = Catalog.getCurrentCatalog().getResourceMgr().getResource(resourceName);
        if (resource == null) {
            throw new DdlException("iceberg resource [" + resourceName + "] not exists");
        }
        if (resource.getType() != Resource.ResourceType.ICEBERG) {
            throw new DdlException("resource [" + resourceName + "] is not iceberg resource");
        }
        IcebergResource icebergResource = (IcebergResource) resource;
        IcebergCatalogType type = icebergResource.getCatalogType();
        icebergProperties.put(ICEBERG_CATALOG, type.name());
        LOG.info("Iceberg table type is " + type.name());
        switch (type) {
            case HIVE_CATALOG:
                icebergProperties.put(ICEBERG_METASTORE_URIS, icebergResource.getHiveMetastoreURIs());
                break;
            default:
                throw new DdlException("unsupported catalog type " + type.name());
        }
        this.resourceName = resourceName;

        IcebergCatalog catalog = IcebergUtil.getIcebergCatalog(type, icebergResource.getHiveMetastoreURIs());
        org.apache.iceberg.Table icebergTable = catalog.loadTable(IcebergUtil.getIcebergTableIdentifier(db, table));
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

        if (!copiedProps.isEmpty()) {
            throw new DdlException("Unknown table properties: " + copiedProps.toString());
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
                return false;
        }
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        Preconditions.checkNotNull(partitions);

        TIcebergTable tIcebergTable = new TIcebergTable();
        tIcebergTable.setLocation(tableLocation);

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
}
