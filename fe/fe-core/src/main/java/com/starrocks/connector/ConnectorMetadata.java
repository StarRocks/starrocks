// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.connector;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Table;

import java.util.List;

public interface ConnectorMetadata {
    /**
     * List all database names of connector
     * @return a list of string containing all database names of connector
     */
    default List<String> listDatabaseNames() {
        return Lists.newArrayList();
    };

    /**
     * List all table names of the database specific by `dbName`
     * @param dbName - the string of which all table names are listed
     * @return a list of string containing all table names of `dbName`
     */
    default List<String> listTableNames(String dbName) {
        return Lists.newArrayList();
    }

    /**
     * Get Table descriptor for the table specific by `dbName`.`tblName`
     * @param dbName - the string represents the database name
     * @param tblName - the string represents the table name
     * @return a Table instance
     */
    default Table getTable(String dbName, String tblName) {
        return null;
    }
}
