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
import com.google.common.collect.Lists;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

// unique constraint is used to guide optimizer rewrite for now,
// and is not enforced during ingestion.
// the unique property of data should be guaranteed by user.
//
// a table may have multi unique constraints.
public class UniqueConstraint {
    private static final Logger LOG = LogManager.getLogger(UniqueConstraint.class);
    // here id is preferred, but meta of column does not have id.
    // have to use name here, so column rename is not supported
    private final List<String> uniqueColumns;

    private final String catalogName;
    private final String dbName;
    private final String tableName;

    public UniqueConstraint(String catalogName, String dbName, String tableName, List<String> uniqueColumns) {
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.uniqueColumns = uniqueColumns;
    }

    public List<String> getUniqueColumns() {
        return uniqueColumns;
    }

    // foreignKeys must be in lower case for case-insensitive
    public boolean isMatch(Table parentTable, Set<String> foreignKeys) {
        if (catalogName != null && dbName != null && tableName != null) {
            Table uniqueTable = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(catalogName, dbName, tableName);
            if (uniqueTable == null) {
                LOG.warn("can not find unique constraint table: {}.{}.{}", catalogName, dbName, tableName);
                return false;
            }
            if (!uniqueTable.equals(parentTable)) {
                return false;
            }
        }
        Set<String> uniqueColumnSet = uniqueColumns.stream().map(String::toLowerCase).collect(Collectors.toSet());
        return uniqueColumnSet.equals(foreignKeys);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (catalogName != null) {
            sb.append(catalogName).append(".");
        }
        if (dbName != null) {
            sb.append(dbName).append(".");
        }
        if (tableName != null) {
            sb.append(tableName).append(".");
        }
        sb.append(Joiner.on(",").join(uniqueColumns));
        return sb.toString();
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public static List<UniqueConstraint> parse(String catalogName, String dbName, String tableName,
                                               String constraintDescs) {
        if (Strings.isNullOrEmpty(constraintDescs)) {
            return null;
        }
        String[] constraintArray = constraintDescs.split(";");
        List<UniqueConstraint> uniqueConstraints = Lists.newArrayList();
        for (String constraintDesc : constraintArray) {
            if (Strings.isNullOrEmpty(constraintDesc)) {
                continue;
            }
            String[] uniqueColumns = constraintDesc.split(",");
            List<String> columnNames =
                    Arrays.stream(uniqueColumns).map(String::trim).collect(Collectors.toList());
            parseUniqueConstraintColumns(catalogName, dbName, tableName, columnNames, uniqueConstraints);
        }
        return uniqueConstraints;
    }

    private static void parseUniqueConstraintColumns(String defaultCatalogName, String defaultDbName,
                                                     String defaultTableName, List<String> columnNames,
                                                     List<UniqueConstraint> uniqueConstraints) {
        String catalogName = null;
        String dbName = null;
        String tableName = null;
        List<String> uniqueConstraintColumns = Lists.newArrayList();
        for (String columnName : columnNames) {
            String[] parts = columnName.split("\\.");
            if (parts.length == 1) {
                uniqueConstraintColumns.add(parts[0]);
            } else if (parts.length == 2) {
                if (tableName != null && !tableName.equals(parts[0])) {
                    throw new SemanticException("unique constraint column should be in same table");
                }
                tableName = parts[0];
                uniqueConstraintColumns.add(parts[1]);
            } else if (parts.length == 3) {
                if (dbName != null && !dbName.equals(parts[0])) {
                    throw new SemanticException("unique constraint column should be in same table");
                }
                if (tableName != null && !tableName.equals(parts[1])) {
                    throw new SemanticException("unique constraint column should be in same table");
                }
                dbName = parts[0];
                tableName = parts[1];
                uniqueConstraintColumns.add(parts[2]);
            } else if (parts.length == 4) {
                if (catalogName != null && !catalogName.equals(parts[0])) {
                    throw new SemanticException("unique constraint column should be in same table");
                }
                if (dbName != null && !dbName.equals(parts[1])) {
                    throw new SemanticException("unique constraint column should be in same table");
                }
                if (tableName != null && !tableName.equals(parts[2])) {
                    throw new SemanticException("unique constraint column should be in same table");
                }
                catalogName = parts[0];
                dbName = parts[1];
                tableName = parts[2];
                uniqueConstraintColumns.add(parts[3]);
            } else {
                throw new SemanticException("invalid unique constraint" + columnName);
            }
        }

        if (catalogName == null) {
            catalogName = defaultCatalogName;
        }
        if (dbName == null) {
            dbName = defaultDbName;
        }
        if (tableName == null) {
            tableName = defaultTableName;
        }

        uniqueConstraints.add(new UniqueConstraint(catalogName, dbName, tableName, uniqueConstraintColumns));
    }
}
