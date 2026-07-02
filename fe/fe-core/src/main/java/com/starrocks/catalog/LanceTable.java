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
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.thrift.THdfsTable;
import com.starrocks.thrift.TLanceTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;

public class LanceTable extends Table {
    public static final String DATASET_URI = "dataset.uri";

    @SerializedName(value = "cn")
    private String catalogName;
    @SerializedName(value = "dn")
    private String databaseName;
    @SerializedName(value = "tn")
    private String tableName;
    @SerializedName(value = "prop")
    private Map<String, String> lanceProperties;

    public LanceTable() {
        super(TableType.LANCE);
    }

    public LanceTable(String catalogName, String dbName, String tblName, List<Column> schema,
                      Map<String, String> lanceProperties) {
        super(CONNECTOR_ID_GENERATOR.getNextId().asLong(), tblName, TableType.LANCE, schema);
        this.catalogName = catalogName;
        this.databaseName = dbName;
        this.tableName = tblName;
        this.lanceProperties = new HashMap<>(lanceProperties);
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

    @Override
    public String getUUID() {
        return String.join(".", catalogName, databaseName, tableName, getDatasetURI());
    }

    @Override
    public String getTableLocation() {
        return getDatasetURI();
    }

    public String getDatasetURI() {
        return lanceProperties.get(DATASET_URI);
    }

    @Override
    public Map<String, String> getProperties() {
        return lanceProperties;
    }

    @Override
    public List<Column> getPartitionColumns() {
        return List.of();
    }

    @Override
    public boolean isUnPartitioned() {
        return true;
    }

    @Override
    public boolean isSupported() {
        return true;
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        TLanceTable tLanceTable = new TLanceTable();
        tLanceTable.setDataset_uri(getDatasetURI());

        TTableDescriptor tTableDescriptor = new TTableDescriptor(id, TTableType.LANCE_TABLE,
                fullSchema.size(), 0, tableName, databaseName);
        THdfsTable tHdfsTable = new THdfsTable();
        tHdfsTable.setHdfs_base_dir(getDatasetURI());
        tHdfsTable.setColumns(getColumns().stream().map(Column::toThrift).collect(Collectors.toList()));
        tHdfsTable.setPartition_columnsIsSet(false);
        tHdfsTable.setTime_zone(TimeUtils.getSessionTimeZone());
        tTableDescriptor.setHdfsTable(tHdfsTable);
        tTableDescriptor.setLanceTable(tLanceTable);
        return tTableDescriptor;
    }

    @Override
    public String getTableIdentifier() {
        return Joiner.on(":").join(name, getUUID());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(catalogName, databaseName, tableName, getDatasetURI());
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof LanceTable)) {
            return false;
        }
        LanceTable otherTable = (LanceTable) other;
        return Objects.equal(catalogName, otherTable.catalogName)
                && Objects.equal(databaseName, otherTable.databaseName)
                && Objects.equal(tableName, otherTable.tableName)
                && Objects.equal(getDatasetURI(), otherTable.getDatasetURI());
    }
}
