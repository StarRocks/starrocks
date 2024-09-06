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

package com.starrocks.catalog.constraint;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

// Unique constraint is used to guide optimizer rewrite for now, and is not enforced during ingestion.
// User should guarantee the unique property of data.
// A table may have multi unique constraints.
public class UniqueConstraint extends Constraint {
    private static final Logger LOG = LogManager.getLogger(UniqueConstraint.class);
    private final List<ColumnId> uniqueColumns;

    private String catalogName;
    private String dbName;
    private String tableName;

    private Table referencedTable;

    public UniqueConstraint(String catalogName, String dbName, String tableName, List<ColumnId> uniqueColumns) {
        super(ConstraintType.UNIQUE, TABLE_PROPERTY_CONSTRAINT);
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.uniqueColumns = uniqueColumns;
    }

    // Used for primaryKey/uniqueKey table to create default uniqueConstraints.
    public UniqueConstraint(Table referencedTable, List<ColumnId> uniqueColumns) {
        super(ConstraintType.UNIQUE, TABLE_PROPERTY_CONSTRAINT);
        this.referencedTable = referencedTable;
        this.uniqueColumns = uniqueColumns;
    }

    public List<String> getUniqueColumnNames() {
        Table targetTable;
        if (referencedTable != null) {
            targetTable = referencedTable;
        } else {
            targetTable = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(catalogName, dbName, tableName);
            if (targetTable == null) {
                throw new SemanticException("Table %s is not found", tableName);
            }
        }
        List<String> result = new ArrayList<>(uniqueColumns.size());
        for (ColumnId columnId : uniqueColumns) {
            Column column = targetTable.getColumn(columnId);
            if (column == null) {
                LOG.warn("Can not find column by column id: {}, the column may have been dropped", columnId);
                continue;
            }
            result.add(column.getName());
        }
        return result;
    }

    public List<ColumnId> getUniqueColumns() {
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
        Set<String> uniqueColumnSet = getUniqueColumnNames().stream().map(String::toLowerCase)
                .collect(Collectors.toSet());
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
        sb.append(Joiner.on(",").join(getUniqueColumns()));
        return sb.toString();
    }

    public static String getShowCreateTableConstraintDesc(List<UniqueConstraint> constraints) {
        List<String> constraintStrs = Lists.newArrayList();
        for (UniqueConstraint constraint : constraints) {
            StringBuilder constraintSb = new StringBuilder();
            if (constraint.catalogName != null) {
                constraintSb.append(constraint.catalogName).append(".");
            }
            if (constraint.dbName != null) {
                constraintSb.append(constraint.dbName).append(".");
            }
            if (constraint.tableName != null) {
                constraintSb.append(constraint.tableName).append(".");
            }
            constraintSb.append(Joiner.on(",").join(constraint.getUniqueColumnNames()));
            constraintStrs.add(constraintSb.toString());
        }

        return Joiner.on(";").join(constraintStrs);
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

    public static List<UniqueConstraint> parse(String defaultCatalogName, String defaultDbName,
                                               String defaultTableName, String constraintDescs) {
        if (Strings.isNullOrEmpty(constraintDescs)) {
            return null;
        }
        String[] constraintArray = constraintDescs.split(";");
        List<UniqueConstraint> uniqueConstraints = Lists.newArrayList();
        for (String constraintDesc : constraintArray) {
            if (Strings.isNullOrEmpty(constraintDesc)) {
                continue;
            }
            Pair<TableName, List<String>> descResult = parseUniqueConstraintDesc(
                    defaultCatalogName, defaultDbName, defaultTableName, constraintDesc);
            uniqueConstraints.add(new UniqueConstraint(descResult.first.getCatalog(),
                    descResult.first.getDb(), descResult.first.getTbl(),
                    descResult.second.stream().map(ColumnId::create).collect(Collectors.toList())));
        }
        return uniqueConstraints;
    }

    // result if a pair, the fist value is TableName(catalogName, dbName, tableName), the second value is columns
    public static Pair<TableName, List<String>> parseUniqueConstraintDesc(String defaultCatalogName, String defaultDbName,
                                                                          String defaultTableName, String constraintDesc) {
        String[] uniqueColumns = constraintDesc.split(",");
        List<String> columnNames = Arrays.stream(uniqueColumns).map(String::trim).collect(Collectors.toList());
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

        return Pair.create(new TableName(catalogName, dbName, tableName), uniqueConstraintColumns);
    }

    public void onTableRename(Table newTable, String oldTableName) {
        LOG.info("UniqueConstraint onTableRename: {} -> {}", oldTableName, newTable.getName());
        if (this.tableName.equals(oldTableName)) {
            this.tableName = newTable.getName();
        } else {
            LOG.warn("UniqueConstraint onTableRename: old table name not match, old: {}, new: {}",
                    oldTableName, newTable.getName());
        }
    }
}
