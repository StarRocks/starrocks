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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.connector.delta.DeltaUtils;
import com.starrocks.server.CatalogMgr;
import com.starrocks.thrift.TColumn;
import com.starrocks.thrift.TDeltaLakeTable;
import com.starrocks.thrift.THdfsPartition;
import com.starrocks.thrift.THdfsPartitionLocation;
import com.starrocks.thrift.TSlotDescriptor;
import com.starrocks.thrift.TStructField;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import com.starrocks.thrift.TTypeDesc;
import com.starrocks.thrift.TTypeNode;
import com.starrocks.thrift.TTypeNodeType;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DeltaLakeTable extends Table {
    private String catalogName;
    private String dbName;
    private String tableName;
    private List<String> partColumnNames;
    private SnapshotImpl deltaSnapshot;
    private String tableLocation;
    private Engine deltaEngine;
    private Map<String, StructField> physicalSchema;

    public static final String PARTITION_NULL_VALUE = "null";

    public DeltaLakeTable() {
        super(TableType.DELTALAKE);
    }

    public DeltaLakeTable(long id, String catalogName, String dbName, String tableName, List<Column> logicalSchema,
                          StructType physicalSchema, List<String> partitionNames,
                          SnapshotImpl deltaSnapshot, String tableLocation, Engine deltaEngine, long createTime) {
        super(id, tableName, TableType.DELTALAKE, logicalSchema);
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.partColumnNames = partitionNames;
        this.deltaSnapshot = deltaSnapshot;
        this.tableLocation = tableLocation;
        this.deltaEngine = deltaEngine;
        this.createTime = createTime;
        this.physicalSchema = physicalSchema.fields().stream()
                .collect(Collectors.toMap(StructField::getName, field -> field));
    }

    @Override
    public boolean isSupported() {
        return true;
    }

    public String getTableLocation() {
        return tableLocation;
    }

    public Metadata getDeltaMetadata() {
        return deltaSnapshot.getMetadata();
    }

    public Snapshot getDeltaSnapshot() {
        return deltaSnapshot;
    }

    public Engine getDeltaEngine() {
        return deltaEngine;
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public String getUUID() {
        if (CatalogMgr.isExternalCatalog(catalogName)) {
            return String.join(".", catalogName, dbName, tableName, Long.toString(createTime));
        } else {
            return Long.toString(id);
        }
    }

    public List<Column> getPartitionColumns() {
        return partColumnNames.stream()
                .map(name -> nameToColumn.get(name))
                .collect(Collectors.toList());
    }

    public List<String> getPartitionColumnNames() {
        return partColumnNames;
    }

    public boolean isUnPartitioned() {
        return partColumnNames.isEmpty();
    }

    public THdfsPartition toHdfsPartition(DescriptorTable.ReferencedPartitionInfo info) {
        Metadata deltaMetadata = getDeltaMetadata();
        PartitionKey key = info.getKey();
        THdfsPartition tPartition = new THdfsPartition();
        tPartition.setFile_format(DeltaUtils.getRemoteFileFormat(deltaMetadata.getFormat().getProvider()).toThrift());

        List<LiteralExpr> keys = key.getKeys();
        tPartition.setPartition_key_exprs(keys.stream().map(Expr::treeToThrift).collect(Collectors.toList()));

        THdfsPartitionLocation tPartitionLocation = new THdfsPartitionLocation();
        tPartitionLocation.setPrefix_index(-1);
        tPartitionLocation.setSuffix(info.getPath());
        tPartition.setLocation(tPartitionLocation);
        return tPartition;
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        Preconditions.checkNotNull(partitions);

        TDeltaLakeTable tDeltaLakeTable = new TDeltaLakeTable();
        tDeltaLakeTable.setLocation(getTableLocation());

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
        tDeltaLakeTable.setColumns(tColumns);
        if (!tPartitionColumns.isEmpty()) {
            tDeltaLakeTable.setPartition_columns(tPartitionColumns);
        }

        for (DescriptorTable.ReferencedPartitionInfo info : partitions) {
            long partitionId = info.getId();
            THdfsPartition tPartition = toHdfsPartition(info);
            tDeltaLakeTable.putToPartitions(partitionId, tPartition);
        }

        TTableDescriptor tTableDescriptor = new TTableDescriptor(id, TTableType.DELTALAKE_TABLE,
                fullSchema.size(), 0, tableName, dbName);
        tTableDescriptor.setDeltaLakeTable(tDeltaLakeTable);
        return tTableDescriptor;
    }

    public void doColumnMapping(TSlotDescriptor slotDescriptor, Column column) {
        StructField physicalSchemaField = physicalSchema.get(column.getName());
        String columnMappingMode = ColumnMapping.getColumnMappingMode(getDeltaMetadata().getConfiguration());

        if (!columnMappingMode.equals(ColumnMapping.COLUMN_MAPPING_MODE_ID) &&
                !columnMappingMode.equals(ColumnMapping.COLUMN_MAPPING_MODE_NAME)) {
            return;
        }
        // For scalar column, we can directly set column physical id and name in slot descriptor
        FieldMetadata fieldMetadata = physicalSchemaField.getMetadata();
        if (columnMappingMode.equals(ColumnMapping.COLUMN_MAPPING_MODE_ID) &&
                fieldMetadata.contains(ColumnMapping.COLUMN_MAPPING_ID_KEY)) {
            slotDescriptor.setCol_unique_id(((Long) fieldMetadata.get(ColumnMapping.COLUMN_MAPPING_ID_KEY)).intValue());
        }

        if (columnMappingMode.equals(ColumnMapping.COLUMN_MAPPING_MODE_NAME) &&
                fieldMetadata.contains(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY)) {
            slotDescriptor.setCol_physical_name(
                    (String) fieldMetadata.get(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY));
        }

        // For Column with complex type, we need to do column mapping recursively
        Integer typeDescIndex = 0;
        doColumnMappingForComplexColumn(slotDescriptor, physicalSchemaField, typeDescIndex, columnMappingMode);
    }

    /**
     * set column physical id and name for complex column (struct, map, array).
     * slot descriptor's slotType is A flattened representation of a tree of column types obtained by depth-first
     * traversal. so we need use typeDescIndex to track the current type node.
     */
    public void doColumnMappingForComplexColumn(TSlotDescriptor slotDescriptor, StructField physicalSchemaField,
                                                Integer typeDescIndex, String columnMappingMode) {
        TTypeDesc typeDesc = slotDescriptor.getSlotType();
        if (physicalSchemaField.getDataType() instanceof StructType) {
            TTypeNode structNode = typeDesc.getTypes().get(typeDescIndex);
            Preconditions.checkState(structNode.type.equals(TTypeNodeType.STRUCT));
            StructType physicalStruct = (StructType) physicalSchemaField.getDataType();

            for (int childIndex = 0; childIndex < structNode.struct_fields.size(); childIndex++) {
                TStructField typeNodeStructField = structNode.struct_fields.get(childIndex);
                StructField physicalStructFiled = physicalStruct.fields().get(childIndex);
                typeDescIndex++;
                doColumnMappingForStructField(typeDesc, typeDescIndex, typeNodeStructField, physicalStructFiled,
                        columnMappingMode);
            }
        } else if (physicalSchemaField.getDataType() instanceof MapType) {
            MapType physicalMapType = (MapType) physicalSchemaField.getDataType();
            StructField keyField = new StructField("key", physicalMapType.getKeyType(), true, FieldMetadata.empty());
            StructField valueField = new StructField("value", physicalMapType.getValueType(), true, FieldMetadata.empty());

            typeDescIndex++;
            doColumnMappingForComplexColumn(slotDescriptor, keyField, typeDescIndex, columnMappingMode);

            typeDescIndex++;
            doColumnMappingForComplexColumn(slotDescriptor, valueField, typeDescIndex, columnMappingMode);
        } else if (physicalSchemaField.getDataType() instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) physicalSchemaField.getDataType();
            StructField elementField = new StructField("element", arrayType.getElementType(), true, FieldMetadata.empty());
            typeDescIndex++;
            doColumnMappingForComplexColumn(slotDescriptor, elementField, typeDescIndex, columnMappingMode);
        }
    }

    // set column physical id and name for struct field
    public void doColumnMappingForStructField(TTypeDesc typeDesc, Integer typeDescIndex, TStructField tField,
                                              StructField physicalStructFiled, String columnMappingMode) {
        TTypeNode typeNode = typeDesc.getTypes().get(typeDescIndex);
        if (columnMappingMode.equals(ColumnMapping.COLUMN_MAPPING_MODE_ID) &&
                physicalStructFiled.getMetadata().contains(ColumnMapping.COLUMN_MAPPING_ID_KEY)) {
            tField.setId(((Long) physicalStructFiled.getMetadata().get(ColumnMapping.COLUMN_MAPPING_ID_KEY)).intValue());
        }

        if (columnMappingMode.equals(ColumnMapping.COLUMN_MAPPING_MODE_NAME) &&
                physicalStructFiled.getMetadata().contains(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY)) {
            tField.setPhysical_name(
                    (String) physicalStructFiled.getMetadata().get(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY));
        }
        // For struct field, we need to do column mapping recursively
        if (typeNode.type.equals(TTypeNodeType.STRUCT)) {
            for (int childIndex = 0; childIndex < typeNode.struct_fields.size(); childIndex++) {
                TStructField typeNodeStructField = typeNode.struct_fields.get(childIndex);
                StructField childField = ((StructType) physicalStructFiled.getDataType()).fields().get(childIndex);
                typeDescIndex++;
                doColumnMappingForStructField(typeDesc, typeDescIndex, typeNodeStructField, childField, columnMappingMode);
            }
        }
    }
}
