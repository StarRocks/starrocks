// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.catalog;

import com.starrocks.catalog.Table.TableType;

import java.util.List;
import java.util.Map;

public class HiveMetaStoreTableInfo {

    private final String resourceName;
    private final String db;
    private final String table;
    private final List<String> partColumnNames;
    private final List<String> dataColumnNames;
    private final Map<String, Column> nameToColumn;
    private final TableType tableType;

    public HiveMetaStoreTableInfo(String resourceName, String db, String table,
                                  List<String> partColumnNames,
                                  List<String> dataColumnNames,
                                  Map<String, Column> nameToColumn,
                                  TableType tableType) {
        this.resourceName = resourceName;
        this.db = db;
        this.table = table;
        this.partColumnNames = partColumnNames;
        this.dataColumnNames = dataColumnNames;
        this.nameToColumn = nameToColumn;
        this.tableType = tableType;
    }

    public String getResourceName() {
        return resourceName;
    }

    public String getDb() {
        return db;
    }

    public String getTable() {
        return table;
    }

    public List<String> getPartColumnNames() {
        return partColumnNames;
    }

    public List<String> getDataColumnNames() {
        return dataColumnNames;
    }

    public Map<String, Column> getNameToColumn() {
        return nameToColumn;
    }

    public TableType getTableType() {
        return tableType;
    }
}
