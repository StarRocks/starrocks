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

import com.starrocks.analysis.DescriptorTable;
import com.starrocks.planner.PaimonScanNode;
import com.starrocks.thrift.TPaimonTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import org.apache.paimon.table.AbstractFileStoreTable;
import org.apache.paimon.types.DataField;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;


public class PaimonTable extends Table {
    private final String catalogName;
    private final String databaseName;
    private final String tableName;
    private final org.apache.paimon.table.Table paimonNativeTable;
    private final List<String> partColumnNames;
    private final List<String> paimonFieldNames;

    public PaimonTable(String catalogName, String dbName, String tblName, List<Column> schema,
                       org.apache.paimon.table.Table paimonNativeTable, long createTime) {
        super(CONNECTOR_ID_GENERATOR.getNextId().asInt(), tblName, TableType.PAIMON, schema);
        this.catalogName = catalogName;
        this.databaseName = dbName;
        this.tableName = tblName;
        this.paimonNativeTable = paimonNativeTable;
        this.partColumnNames = paimonNativeTable.partitionKeys();
        this.paimonFieldNames = paimonNativeTable.rowType().getFields().stream()
                .map(DataField::name)
                .collect(Collectors.toList());
        this.createTime = createTime;
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    public String getDbName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public org.apache.paimon.table.Table getNativeTable() {
        return paimonNativeTable;
    }

    @Override
    public String getUUID() {
        return String.join(".", catalogName, databaseName, tableName, Long.toString(createTime));
    }

    @Override
    public String getTableLocation() {
        if (paimonNativeTable instanceof AbstractFileStoreTable) {
            return ((AbstractFileStoreTable) paimonNativeTable).location().toString();
        } else {
            return paimonNativeTable.name().toString();
        }
    }

    @Override
    public List<String> getPartitionColumnNames() {
        return partColumnNames;
    }

    @Override
    public List<Column> getPartitionColumns() {
        List<Column> partitionColumns = new ArrayList<>();
        if (!partColumnNames.isEmpty()) {
            partitionColumns = partColumnNames.stream().map(this::getColumn)
                    .collect(Collectors.toList());
        }
        return partitionColumns;
    }

    public List<String> getFieldNames() {
        return paimonFieldNames;
    }

    @Override
    public boolean isUnPartitioned() {
        return partColumnNames.isEmpty();
    }

    @Override
    public boolean isSupported() {
        return true;
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        TPaimonTable tPaimonTable = new TPaimonTable();
        String encodedTable = PaimonScanNode.encodeObjectToString(paimonNativeTable);
        tPaimonTable.setPaimon_native_table(encodedTable);
        TTableDescriptor tTableDescriptor = new TTableDescriptor(id, TTableType.PAIMON_TABLE,
                fullSchema.size(), 0, tableName, databaseName);
        tTableDescriptor.setPaimonTable(tPaimonTable);
        return tTableDescriptor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PaimonTable that = (PaimonTable) o;
        return catalogName.equals(that.catalogName) &&
                databaseName.equals(that.databaseName) &&
                tableName.equals(that.tableName) &&
                createTime == that.createTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogName, databaseName, tableName, createTime);
    }
}
