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
import com.google.common.base.Strings;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.thrift.TFlussTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.utils.PushdownUtils;
import org.apache.fluss.metadata.TableInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;
import static com.starrocks.planner.PaimonScanNode.encodeObjectToString;

public class FlussTable extends Table {
    private static final Logger LOG = LogManager.getLogger(FlussTable.class);
    private String catalogName;
    private String databaseName;
    private String tableName;
    private TableInfo tableInfo;
    private String uuid;
    private List<String> partColumnNames;
    private List<String> flussFieldNames;
    private Map<String, String> properties;
    private Configuration configuration;
    private Map<String, String> tableProperties;
    private String tableNamePrefix = "";

    public FlussTable() {
        super(TableType.FLUSS);
    }

    public FlussTable(String catalogName, String dbName, String tblName, List<Column> schema,
                      org.apache.fluss.client.table.Table nativeFlussTable, Configuration configuration,
                      Map<String, String> tableProperties) {
        super(CONNECTOR_ID_GENERATOR.getNextId().asInt(), tblName, TableType.FLUSS, schema);
        this.catalogName = catalogName;
        this.databaseName = dbName;
        this.tableName = tblName;
        this.tableInfo = nativeFlussTable.getTableInfo();
        this.partColumnNames = tableInfo.getPartitionKeys();
        this.flussFieldNames = tableInfo.getSchema().getColumnNames();
        this.configuration = configuration;
        this.tableProperties = tableProperties;
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

    public TableInfo getTableInfo() {
        return tableInfo;
    }

    @Override
    public String getUUID() {
        if (Strings.isNullOrEmpty(this.uuid)) {
            this.uuid = String.join(".", catalogName, databaseName, tableName,
                    String.valueOf(tableInfo.getTableId()));
        }
        return this.uuid;
    }

    @Override
    public String getTableLocation() {
        return this.tableInfo.getTablePath().toString();
    }

    @Override
    public Map<String, String> getProperties() {
        if (properties == null) {
            this.properties = new HashMap<>();
        }
        return properties;
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

    public long getRTCount() {
        return PushdownUtils.countLogTable(this.tableInfo.getTablePath(), this.configuration);
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

        Configuration conf = this.configuration;
        for (Map.Entry<String, String> entry : tableProperties.entrySet()) {
            conf.setString(entry.getKey(), entry.getValue());
        }
        tFlussTable.setTable_conf(encodeObjectToString(conf));
        tFlussTable.setTime_zone(TimeUtils.getSessionTimeZone());

        TTableDescriptor tTableDescriptor = new TTableDescriptor(id, TTableType.FLUSS_TABLE,
                fullSchema.size(), 0, tableName, databaseName);
        tTableDescriptor.setFlussTable(tFlussTable);
        return tTableDescriptor;
    }

    @Override
    public String getTableIdentifier() {
        String uuid = getUUID();
        return Joiner.on(":").join(name, uuid == null ? "" : uuid);
    }

    public void setTableNamePrefix(String prefix) {
        this.tableNamePrefix = prefix;
    }

    public String getTableNamePrefix() {
        return tableNamePrefix;
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
