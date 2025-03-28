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

package com.starrocks.catalog;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.common.util.Util;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergApiConverter;
import com.starrocks.connector.iceberg.IcebergCatalogType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.rpc.ConfigurableSerDesFactory;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TColumn;
import com.starrocks.thrift.TCompressedPartitionMap;
import com.starrocks.thrift.THdfsPartition;
import com.starrocks.thrift.TIcebergTable;
import com.starrocks.thrift.TPartitionMap;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortField;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.starrocks.connector.iceberg.IcebergCatalogProperties.ICEBERG_CATALOG_TYPE;
import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.getResourceMappingCatalogName;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

public class IcebergTable extends Table {
    private static final Logger LOG = LogManager.getLogger(IcebergTable.class);

    private static final String PARQUET_FORMAT = "parquet";
    public static final String DATA_SEQUENCE_NUMBER = "$data_sequence_number";
    public static final String SPEC_ID = "$spec_id";
    public static final String EQUALITY_DELETE_TABLE_COMMENT = "equality_delete_table_comment";

    private String catalogName;
    @SerializedName(value = "dn")
    protected String catalogDBName;
    @SerializedName(value = "tn")
    protected String catalogTableName;
    @SerializedName(value = "rn")
    private String resourceName;
    @SerializedName(value = "prop")
    private Map<String, String> icebergProperties = Maps.newHashMap();

    private org.apache.iceberg.Table nativeTable; // actual iceberg table
    private List<Column> partitionColumns;

    private final AtomicLong partitionIdGen = new AtomicLong(0L);

    public IcebergTable() {
        super(TableType.ICEBERG);
    }

    public IcebergTable(long id, String srTableName, String catalogName, String resourceName, String catalogDBName,
                        String catalogTableName, String comment, List<Column> schema,
                        org.apache.iceberg.Table nativeTable, Map<String, String> icebergProperties) {
        super(id, srTableName, TableType.ICEBERG, schema);
        this.catalogName = catalogName;
        this.resourceName = resourceName;
        this.catalogDBName = catalogDBName;
        this.catalogTableName = catalogTableName;
        this.comment = comment;
        this.nativeTable = nativeTable;
        this.icebergProperties = icebergProperties;
    }

    @Override
    public String getCatalogName() {
        return catalogName == null ? getResourceMappingCatalogName(resourceName, "iceberg") : catalogName;
    }

    @Override
    public String getResourceName() {
        return resourceName;
    }

    @Override
    public String getCatalogDBName() {
        return catalogDBName;
    }

    @Override
    public String getCatalogTableName() {
        return catalogTableName;
    }

    @Override
    public String getUUID() {
        if (CatalogMgr.isExternalCatalog(catalogName)) {
            String uuid = ((BaseTable) getNativeTable()).operations().current().uuid();
            return String.join(".", catalogName, catalogDBName, catalogTableName,
                    uuid == null ? "" : uuid);
        } else {
            return Long.toString(id);
        }
    }

    @Override
    public List<Column> getPartitionColumns() {
        if (partitionColumns == null) {
            List<PartitionField> partitionFields = this.getNativeTable().spec().fields();
            Schema schema = this.getNativeTable().schema();
            partitionColumns = partitionFields.stream().map(partitionField ->
                    getColumn(getPartitionSourceName(schema, partitionField))).collect(Collectors.toList());
        }
        return partitionColumns;
    }

    public List<Column> getPartitionColumnsIncludeTransformed() {
        List<Column> allPartitionColumns = new ArrayList<>();
        for (PartitionField field : getNativeTable().spec().fields()) {
            if (!field.transform().isIdentity() && hasPartitionTransformedEvolution()) {
                continue;
            }
            String baseColumnName = nativeTable.schema().findColumnName(field.sourceId());
            Column partitionCol = getColumn(baseColumnName);
            allPartitionColumns.add(partitionCol);
        }
        return allPartitionColumns;
    }

    public boolean isAllPartitionColumnsAlwaysIdentity() {
        // now we are sure we have never applied transformation,
        // we check if all partition columns are identity.
        for (PartitionField field : getNativeTable().spec().fields()) {
            if (!field.transform().isIdentity()) {
                return false;
            }
        }
        return true;
    }

    public PartitionField getPartitionField(String partitionColumnName) {
        List<PartitionField> allPartitionFields = getNativeTable().spec().fields();
        Schema schema = this.getNativeTable().schema();
        for (PartitionField field : allPartitionFields) {
            if (getPartitionSourceName(schema, field).equalsIgnoreCase(partitionColumnName)) {
                return field;
            }
        }
        return null;
    }

    public long nextPartitionId() {
        return partitionIdGen.getAndIncrement();
    }

    public List<Integer> partitionColumnIndexes() {
        List<Column> partitionCols = getPartitionColumns();
        return partitionCols.stream().map(col -> fullSchema.indexOf(col)).collect(Collectors.toList());
    }

    public List<Integer> getSortKeyIndexes() {
        List<Integer> indexes = new ArrayList<>();
        org.apache.iceberg.Table nativeTable = getNativeTable();
        List<Types.NestedField> fields = nativeTable.schema().asStruct().fields();
        List<Integer> sortFieldSourceIds = nativeTable.sortOrder().fields().stream()
                .map(SortField::sourceId)
                .collect(Collectors.toList());

        for (int i = 0; i < fields.size(); i++) {
            Types.NestedField field = fields.get(i);
            if (sortFieldSourceIds.contains(field.fieldId())) {
                indexes.add(i);
            }
        }

        return indexes;
    }

    // TODO(stephen): we should refactor this part to be compatible with cases of different transform result types
    //  in the same partition column.
    // day(dt) -> identity dt
    public boolean hasPartitionTransformedEvolution() {
        return (!isV2Format() && getNativeTable().spec().fields().stream().anyMatch(field -> field.transform().isVoid())) ||
                (isV2Format() && getNativeTable().spec().specId() > 0);
    }

    public boolean isV2Format() {
        return ((BaseTable) getNativeTable()).operations().current().formatVersion() > 1;
    }

    /**
     * <p>
     *     In the Iceberg Partition Evolution scenario, 'org.apache.iceberg.PartitionField#name' only represents the
     *     name of a partition in the Iceberg table's Partition Spec. This name is used when trying to obtain the
     *     names of Partition Spec partitions. e.g.
     * </p>
     * <p>
     *     {
     *   "source-id": 4,
     *   "field-id": 1000,
     *   "name": "ts_day",
     *   "transform": "day"
     *   }
     * </p>
     * <p>
     *     column id is '4', column name is 'ts', but 'PartitionField#name' is 'ts_day', 'PartitionField#fieldId'
     *     is '1000', 'PartitionField#name' default is 'columnName_transformName', and we can customize this name.
     *     So even for an Identity Transform, this name doesn't necessarily have to match the schema column name,
     *     because we can customize this name. But in general, nobody customize an Identity Transform Partition name.
     * </p>
     * <p>
     *     To obtain the table columns for Iceberg tables, we use 'org.apache.iceberg.Schema#findColumnName'.
     * </p>
     *<br>
     * refs:<br>
     * - https://iceberg.apache.org/spec/#partition-evolution<br>
     * - https://iceberg.apache.org/spec/#partition-specs<br>
     * - https://iceberg.apache.org/spec/#partition-transforms
     */
    public String getPartitionSourceName(Schema schema, PartitionField partition) {
        return schema.findColumnName(partition.sourceId());
    }

    @Override
    public boolean isUnPartitioned() {
        return ((BaseTable) getNativeTable()).operations().current().spec().isUnpartitioned();
    }

    public boolean isPartitioned() {
        return !isUnPartitioned();
    }

    public List<String> getPartitionColumnNames() {
        return getPartitionColumns().stream().filter(java.util.Objects::nonNull).map(Column::getName)
                .collect(Collectors.toList());
    }

    public List<String> getPartitionColumnNamesWithTransform() {
        PartitionSpec partitionSpec = getNativeTable().spec();
        return IcebergApiConverter.toPartitionFields(partitionSpec);
    }

    @Override
    public String getTableIdentifier() {
        String uuid = ((BaseTable) getNativeTable()).operations().current().uuid();
        return Joiner.on(":").join(name, uuid == null ? "" : uuid);
    }

    public IcebergCatalogType getCatalogType() {
        return IcebergCatalogType.valueOf(icebergProperties.get(ICEBERG_CATALOG_TYPE));
    }

    public String getTableLocation() {
        return getNativeTable().location();
    }

    @Override
    public Map<String, String> getProperties() {
        return getNativeTable().properties();
    }

    public PartitionField getPartitionFiled(String colName) {
        org.apache.iceberg.Table nativeTable = getNativeTable();
        return nativeTable.spec().fields().stream()
                .filter(field -> nativeTable.schema().findColumnName(field.sourceId()).equalsIgnoreCase(colName))
                .findFirst()
                .orElse(null);
    }

    public org.apache.iceberg.Table getNativeTable() {
        // For compatibility with the resource iceberg table. native table is lazy. Prevent failure during fe restarting.
        if (nativeTable == null) {
            IcebergTable resourceMappingTable = (IcebergTable) GlobalStateMgr.getCurrentState().getMetadataMgr()
                    .getTable(new ConnectContext(), getCatalogName(), catalogDBName, catalogTableName);
            if (resourceMappingTable == null) {
                throw new StarRocksConnectorException("Can't find table %s.%s.%s",
                        getCatalogName(), catalogDBName, catalogTableName);
            }
            nativeTable = resourceMappingTable.getNativeTable();
        }
        return nativeTable;
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        Preconditions.checkNotNull(partitions);

        TIcebergTable tIcebergTable = new TIcebergTable();
        tIcebergTable.setLocation(getNativeTable().location());

        List<TColumn> tColumns = Lists.newArrayList();
        for (Column column : getBaseSchema()) {
            tColumns.add(column.toThrift());
        }
        tIcebergTable.setColumns(tColumns);

        tIcebergTable.setIceberg_schema(IcebergApiConverter.getTIcebergSchema(nativeTable.schema()));
        tIcebergTable.setPartition_column_names(getPartitionColumnNames());

        if (!partitions.isEmpty()) {
            TPartitionMap tPartitionMap = new TPartitionMap();
            for (int i = 0; i < partitions.size(); i++) {
                DescriptorTable.ReferencedPartitionInfo info = partitions.get(i);
                PartitionKey key = info.getKey();
                long partitionId = info.getId();
                THdfsPartition tPartition = new THdfsPartition();
                List<LiteralExpr> keys = key.getKeys();
                tPartition.setPartition_key_exprs(keys.stream().map(Expr::treeToThrift).collect(Collectors.toList()));
                tPartitionMap.putToPartitions(partitionId, tPartition);
            }

            // partition info may be very big, and it is the same in plan fragment send to every be.
            // extract and serialize it as a string, will get better performance(about 3x in test).
            try {
                TSerializer serializer = ConfigurableSerDesFactory.getTSerializer();
                byte[] bytes = serializer.serialize(tPartitionMap);
                byte[] compressedBytes = Util.compress(bytes);
                TCompressedPartitionMap tCompressedPartitionMap = new TCompressedPartitionMap();
                tCompressedPartitionMap.setOriginal_len(bytes.length);
                tCompressedPartitionMap.setCompressed_len(compressedBytes.length);
                tCompressedPartitionMap.setCompressed_serialized_partitions(Base64.getEncoder().encodeToString(compressedBytes));
                tIcebergTable.setCompressed_partitions(tCompressedPartitionMap);
            } catch (TException | IOException ignore) {
                tIcebergTable.setPartitions(tPartitionMap.getPartitions());
            }
        }

        TTableDescriptor tTableDescriptor = new TTableDescriptor(id, TTableType.ICEBERG_TABLE,
                fullSchema.size(), 0, catalogTableName, catalogDBName);
        tTableDescriptor.setIcebergTable(tIcebergTable);
        return tTableDescriptor;
    }

    @Override
    public boolean isSupported() {
        return true;
    }

    @Override
    public boolean supportInsert() {
        // for now, only support writing iceberg table with parquet file format
        return getNativeTable().properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT)
                .equalsIgnoreCase(PARQUET_FORMAT);
    }

    @Override
    public boolean supportPreCollectMetadata() {
        return true;
    }

    @Override
    public boolean isTemporal() {
        return true;
    }

    @Override
    public int hashCode() {
        return com.google.common.base.Objects.hashCode(getCatalogName(), catalogDBName, getTableIdentifier());
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof IcebergTable)) {
            return false;
        }

        IcebergTable otherTable = (IcebergTable) other;
        String catalogName = getCatalogName();
        String tableIdentifier = getTableIdentifier();
        return Objects.equal(catalogName, otherTable.getCatalogName()) &&
                Objects.equal(catalogDBName, otherTable.catalogDBName) &&
                Objects.equal(tableIdentifier, otherTable.getTableIdentifier());
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private long id;
        private String srTableName;
        private String catalogName;
        private String resourceName;
        private String catalogDBName;
        private String catalogTableName;

        private String comment;
        private List<Column> fullSchema;
        private Map<String, String> icebergProperties;
        private org.apache.iceberg.Table nativeTable;

        public Builder() {
        }

        public Builder setId(long id) {
            this.id = id;
            return this;
        }

        public Builder setSrTableName(String srTableName) {
            this.srTableName = srTableName;
            return this;
        }

        public Builder setCatalogName(String catalogName) {
            this.catalogName = catalogName;
            return this;
        }

        public Builder setComment(String comment) {
            this.comment = comment;
            return this;
        }

        public Builder setResourceName(String resourceName) {
            this.resourceName = resourceName;
            return this;
        }

        public Builder setCatalogDBName(String catalogDbName) {
            this.catalogDBName = catalogDbName;
            return this;
        }

        public Builder setCatalogTableName(String catalogTableName) {
            this.catalogTableName = catalogTableName;
            return this;
        }

        public Builder setFullSchema(List<Column> fullSchema) {
            this.fullSchema = fullSchema;
            return this;
        }

        public Builder setIcebergProperties(Map<String, String> icebergProperties) {
            this.icebergProperties = icebergProperties;
            return this;
        }

        public Builder setNativeTable(org.apache.iceberg.Table nativeTable) {
            this.nativeTable = nativeTable;
            return this;
        }

        public IcebergTable build() {
            return new IcebergTable(id, srTableName, catalogName, resourceName, catalogDBName, catalogTableName,
                    comment, fullSchema, nativeTable, icebergProperties);
        }
    }
}
