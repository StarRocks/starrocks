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
import com.starrocks.common.util.TimeUtils;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.thrift.TFlussTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;
import static com.starrocks.planner.PaimonScanNode.encodeObjectToString;

public class FlussTable extends Table {
    private String catalogName;
    private String databaseName;
    private String tableName;
    private TableInfo tableInfo;
    private List<String> partColumnNames;
    private List<String> flussFieldNames;
    // Catalog-level Fluss/lake options copied from CREATE EXTERNAL CATALOG
    private Configuration catalogConf;
    private String tableNameSuffix = "";

    public FlussTable() {
        super(TableType.FLUSS);
    }

    public FlussTable(String catalogName, String dbName, String tblName, List<Column> schema,
                      TableInfo tableInfo, Configuration catalogConf) {
        super(CONNECTOR_ID_GENERATOR.getNextId().asLong(), tblName, TableType.FLUSS, schema);
        this.catalogName = catalogName;
        this.databaseName = dbName;
        this.tableName = tblName;
        this.tableInfo = tableInfo;
        this.partColumnNames = tableInfo.getPartitionKeys();
        this.flussFieldNames = tableInfo.getSchema().getColumnNames();
        this.catalogConf = catalogConf;
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public String getCatalogDBName() {
        return databaseName;
    }

    @Override
    public String getCatalogTableName() {
        return tableName;
    }

    public TableInfo getTableInfo() {
        return tableInfo;
    }

    public Configuration buildRuntimeConf() {
        Configuration runtimeConf = new Configuration();
        for (Map.Entry<String, String> entry : getProperties().entrySet()) {
            runtimeConf.setString(entry.getKey(), entry.getValue());
        }
        if (catalogConf != null) {
            runtimeConf.addAll(catalogConf);
        }
        return runtimeConf;
    }

    @Override
    public String getUUID() {
        return String.join(".", catalogName, databaseName, tableName + tableNameSuffix,
                String.valueOf(tableInfo.getTableId()));
    }

    @Override
    public String getTableLocation() {
        return this.tableInfo.getTablePath().toString();
    }

    @Override
    public Map<String, String> getProperties() {
        if (tableInfo == null) {
            return new HashMap<>();
        }
        return new HashMap<>(tableInfo.getProperties().toMap());
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
        return flussFieldNames;
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
        TFlussTable tFlussTable = new TFlussTable();
        tFlussTable.setRuntime_conf(encodeObjectToString(buildRuntimeConf()));
        tFlussTable.setTime_zone(TimeUtils.getSessionTimeZone());
        tFlussTable.setCatalog_name(this.catalogName);

        TTableDescriptor tTableDescriptor = new TTableDescriptor(id, TTableType.FLUSS_TABLE,
                fullSchema.size(), 0, tableName, databaseName);
        tTableDescriptor.setFlussTable(tFlussTable);
        return tTableDescriptor;
    }

    @Override
    public String getTableIdentifier() {
        return Joiner.on(":").join(name, getUUID());
    }

    public void setTableNameSuffix(String suffix) {
        this.tableNameSuffix = suffix;
    }

    public String getTableNameSuffix() {
        return tableNameSuffix;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FlussTable that = (FlussTable) o;
        return catalogName.equals(that.catalogName) &&
                databaseName.equals(that.databaseName) &&
                tableName.equals(that.tableName) &&
                Objects.equals(getTableIdentifier(), that.getTableIdentifier());
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogName, databaseName, tableName, getTableIdentifier());
    }
}
