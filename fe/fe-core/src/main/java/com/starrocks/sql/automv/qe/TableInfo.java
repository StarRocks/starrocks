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

package com.starrocks.sql.automv.qe;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.sql.automv.pieces.FQTable;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class TableInfo {
    String catalogName;
    Long catalogId;
    String catalogType;
    String databaseName;
    String databaseUUID;
    Long databaseId;
    String databaseLocation;
    String tableName;
    String tableType;
    String tableEngine;
    String tableUUID;
    Long tableId;
    Boolean partitioned;
    List<String> tablePartitionKey;

    public static List<String> getPartitionKey(Table table) {
        List<Column> partitionColumns = null;
        try {
            if (table instanceof OlapTable) {
                OlapTable olapTable = (OlapTable) table;
                partitionColumns = olapTable.getPartitionInfo().getPartitionColumns(olapTable.getIdToColumn());
            } else {
                partitionColumns = table.getPartitionColumns();
            }
        } catch (Exception ignored) {

        }
        return Optional.ofNullable(partitionColumns)
                .map(columns -> columns.stream().map(Column::getName).collect(Collectors.toList()))
                .orElseGet(Collections::emptyList);
    }

    public static TableInfo from(FQTable fqTable) {
        TableInfo tableInfo = new TableInfo();
        if (fqTable.getCatalog() != null) {
            tableInfo.setCatalogName(fqTable.getCatalog().getName());
            tableInfo.setCatalogId(fqTable.getCatalog().getId());
            tableInfo.setCatalogType(fqTable.getCatalog().getType());
        }
        tableInfo.setDatabaseName(fqTable.getDatabase().getFullName());
        tableInfo.setDatabaseId(fqTable.getDatabase().getId());
        tableInfo.setDatabaseUUID(fqTable.getDatabase().getUUID());
        tableInfo.setDatabaseLocation(fqTable.getDatabase().getLocation());
        tableInfo.setTableName(fqTable.getTable().getName());
        tableInfo.setTableUUID(fqTable.getTable().getUUID());
        tableInfo.setTableId(fqTable.getTable().getId());
        tableInfo.setTableType(fqTable.getTable().getType().toString());
        tableInfo.setTableEngine(fqTable.getTable().getEngine());

        tableInfo.setTablePartitionKey(getPartitionKey(fqTable.getTable()));
        tableInfo.setPartitioned(fqTable.getTable().isPartitioned());
        return tableInfo;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    public Long getCatalogId() {
        return catalogId;
    }

    public void setCatalogId(Long catalogId) {
        this.catalogId = catalogId;
    }

    public String getCatalogType() {
        return catalogType;
    }

    public void setCatalogType(String catalogType) {
        this.catalogType = catalogType;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getDatabaseUUID() {
        return databaseUUID;
    }

    public void setDatabaseUUID(String databaseUUID) {
        this.databaseUUID = databaseUUID;
    }

    public Long getDatabaseId() {
        return databaseId;
    }

    public void setDatabaseId(Long databaseId) {
        this.databaseId = databaseId;
    }

    public String getDatabaseLocation() {
        return databaseLocation;
    }

    public void setDatabaseLocation(String databaseLocation) {
        this.databaseLocation = databaseLocation;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableType() {
        return tableType;
    }

    public void setTableType(String tableType) {
        this.tableType = tableType;
    }

    public String getTableEngine() {
        return tableEngine;
    }

    public void setTableEngine(String tableEngine) {
        this.tableEngine = tableEngine;
    }

    public String getTableUUID() {
        return tableUUID;
    }

    public void setTableUUID(String tableUUID) {
        this.tableUUID = tableUUID;
    }

    public Long getTableId() {
        return tableId;
    }

    public void setTableId(Long tableId) {
        this.tableId = tableId;
    }

    public Boolean getPartitioned() {
        return partitioned;
    }

    public void setPartitioned(Boolean partitioned) {
        this.partitioned = partitioned;
    }

    public List<String> getTablePartitionKey() {
        return tablePartitionKey;
    }

    public void setTablePartitionKey(List<String> tablePartitionKey) {
        this.tablePartitionKey = tablePartitionKey;
    }
}
