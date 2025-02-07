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
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

// Foreign-key constraint is used to guide optimizer rewrite for now, and is not enforced during ingestion.
// User should guarantee the foreign key property of data.
// A table may have multi-foreign-key constraints.
public class ForeignKeyConstraint extends Constraint {
    private static final Logger LOG = LogManager.getLogger(ForeignKeyConstraint.class);

    private static final String FOREIGN_KEY_REGEX = "((\\.?\\w+:?-?)*)\\s*\\(((,?\\s*\\w+\\s*)+)\\)\\s+((?i)REFERENCES)\\s+" +
            "((\\.?\\w+:?-?)+)\\s*\\(((,?\\s*\\w+\\s*)+)\\)";

    public static final Pattern FOREIGN_KEY_PATTERN = Pattern.compile(FOREIGN_KEY_REGEX);

    // table with primary key or unique key
    // if parent table is dropped, the foreign key is not dropped cascade now.
    private final BaseTableInfo parentTableInfo;

    // table with foreign key, it only used for materialized view.
    private final BaseTableInfo childTableInfo;

    // eg: [column1 -> column1', column2 -> column2']
    private final List<Pair<ColumnId, ColumnId>> columnRefPairs;

    public ForeignKeyConstraint(BaseTableInfo parentTableInfo,
                                BaseTableInfo childTableInfo,
                                List<Pair<ColumnId, ColumnId>> columnRefPairs) {
        super(ConstraintType.FOREIGN_KEY, TABLE_PROPERTY_CONSTRAINT);
        this.parentTableInfo = parentTableInfo;
        this.childTableInfo = childTableInfo;
        this.columnRefPairs = columnRefPairs;
    }

    public BaseTableInfo getParentTableInfo() {
        return parentTableInfo;
    }

    public BaseTableInfo getChildTableInfo() {
        return childTableInfo;
    }

    public List<Pair<ColumnId, ColumnId>> getColumnRefPairs() {
        return columnRefPairs;
    }

    public List<Pair<String, String>> getColumnNameRefPairs(Table defaultChildTable) {
        Table parentTable = getParentTable();
        Table childTable = defaultChildTable;
        if (childTableInfo != null) {
            childTable = getChildTable();
        }
        List<Pair<String, String>> result = new ArrayList<>(columnRefPairs.size());
        for (Pair<ColumnId, ColumnId> pair : columnRefPairs) {
            Column childColumn = childTable.getColumn(pair.first);
            Column parentColumn = parentTable.getColumn(pair.second);
            if (childColumn == null || parentColumn == null) {
                LOG.warn("can not find column by column id: {} in table: {}, the column may have been dropped",
                        pair.first, childTableInfo);
                continue;
            }
            result.add(Pair.create(childColumn.getName(), parentColumn.getName()));
        }

        return result;
    }

    private Table getParentTable() {
        if (parentTableInfo.isInternalCatalog()) {
            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(parentTableInfo.getDbId(), parentTableInfo.getTableId());
            if (table == null) {
                throw new SemanticException("Table %s is not found", parentTableInfo.getTableId());
            }
            return table;
        } else {
            Table table = GlobalStateMgr.getCurrentState().getMetadataMgr()
                    .getTable(parentTableInfo.getCatalogName(), parentTableInfo.getDbName(), parentTableInfo.getTableName());
            if (table == null) {
                throw new SemanticException("Table %s is not found", parentTableInfo.getTableName());
            }
            return table;
        }
    }

    private Table getChildTable() {
        if (childTableInfo.isInternalCatalog()) {
            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(childTableInfo.getDbId(), childTableInfo.getTableId());
            if (table == null) {
                throw new SemanticException("Table %s is not found", childTableInfo.getTableId());
            }
            return table;
        } else {
            Table table = GlobalStateMgr.getCurrentState().getMetadataMgr()
                    .getTable(childTableInfo.getCatalogName(), childTableInfo.getDbName(), childTableInfo.getTableName());
            if (table == null) {
                throw new SemanticException("Table %s is not found", childTableInfo.getTableName());
            }
            return table;
        }
    }

    // for olap table, the format is: (column1, column2) REFERENCES default_catalog.dbid.tableid(column1', column2')
    // for materialized view, the format is: catalog1.dbName1.tableName1(column1, column2) REFERENCES
    // catalog2.dbName2.tableName2(column1', column2')
    @Override
    public String toString() {
        if (parentTableInfo == null || columnRefPairs == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        if (childTableInfo != null) {
            sb.append(childTableInfo);
        }
        sb.append("(");
        String baseColumns = Joiner.on(",").join(columnRefPairs.stream().map(pair -> pair.first).collect(Collectors.toList()));
        sb.append(baseColumns);
        sb.append(")");
        sb.append(" REFERENCES ");
        sb.append(parentTableInfo);
        sb.append("(");
        String parentColumns = Joiner.on(",").join(columnRefPairs.stream().map(pair -> pair.second).collect(Collectors.toList()));
        sb.append(parentColumns);
        sb.append(")");
        return sb.toString();
    }

    // for default catalog, the format is: (column1, column2) REFERENCES default_catalog.dbid.tableid(column1', column2')
    // for other catalog, the format is: (column1, column2) REFERENCES default_catalog.dbname.tablename(column1', column2')
    public static List<ForeignKeyConstraint> parse(String foreignKeyConstraintDescs) {
        if (Strings.isNullOrEmpty(foreignKeyConstraintDescs)) {
            return null;
        }
        String[] constraintArray = foreignKeyConstraintDescs.split(";");
        List<ForeignKeyConstraint> foreignKeyConstraints = Lists.newArrayList();
        for (String constraintDesc : constraintArray) {
            if (Strings.isNullOrEmpty(constraintDesc)) {
                continue;
            }
            Matcher foreignKeyMatcher = FOREIGN_KEY_PATTERN.matcher(constraintDesc);
            if (!foreignKeyMatcher.find()) {
                LOG.warn("invalid constraint:{}", constraintDesc);
                continue;
            }

            String sourceTablePath = foreignKeyMatcher.group(1);
            String sourceColumns = foreignKeyMatcher.group(3);

            String targetTablePath = foreignKeyMatcher.group(6);
            String targetColumns = foreignKeyMatcher.group(8);

            List<ColumnId> childColumns = Arrays.stream(sourceColumns.split(",")).
                    map(colStr -> ColumnId.create(colStr.trim())).collect(Collectors.toList());
            List<ColumnId> parentColumns = Arrays.stream(targetColumns.split(",")).
                    map(colStr -> ColumnId.create(colStr.trim())).collect(Collectors.toList());

            String[] targetTableParts = targetTablePath.split("\\.");
            Preconditions.checkState(targetTableParts.length == 3);
            String targetCatalogName = targetTableParts[0];
            String targetDb = targetTableParts[1];
            String targetTable = targetTableParts[2];
            BaseTableInfo parentTableInfo;
            String[] targetTableSplits = targetTable.split(":");
            if (targetTableSplits.length != 2) {
                parentTableInfo = getTableBaseInfo(targetCatalogName, targetDb, targetTable, null);
            } else {
                parentTableInfo = getTableBaseInfo(targetCatalogName, targetDb, targetTableSplits[0], targetTable);
            }

            BaseTableInfo childTableInfo = null;
            if (!Strings.isNullOrEmpty(sourceTablePath)) {
                String[] sourceTableParts = sourceTablePath.split("\\.");
                Preconditions.checkState(sourceTableParts.length == 3);
                String sourceCatalogName = sourceTableParts[0];
                String sourceDb = sourceTableParts[1];
                String sourceTable = sourceTableParts[2];
                String[] sourceTableSplits = sourceTable.split(":");
                if (sourceTableSplits.length != 2) {
                    childTableInfo = getTableBaseInfo(sourceCatalogName, sourceDb, sourceTable, null);
                } else {
                    childTableInfo = getTableBaseInfo(sourceCatalogName, sourceDb, sourceTableSplits[0], sourceTable);
                }
            }

            List<Pair<ColumnId, ColumnId>> columnRefPairs =
                    Streams.zip(childColumns.stream(), parentColumns.stream(), Pair::create).collect(Collectors.toList());
            foreignKeyConstraints.add(new ForeignKeyConstraint(parentTableInfo, childTableInfo, columnRefPairs));
        }
        return foreignKeyConstraints;
    }

    private static BaseTableInfo getTableBaseInfo(String catalogName, String db, String table, String tableIdentifier) {
        BaseTableInfo baseTableInfo;
        if (catalogName.equals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)) {
            Preconditions.checkArgument(NumberUtils.isNumber(db),
                    "BaseTableInfo db %s should be dbId for internal catalog", db);
            Preconditions.checkArgument(NumberUtils.isNumber(table),
                    "BaseTableInfo table %s should be tableId for internal catalog", table);
            long dbId = Long.parseLong(db);
            long tableId = Long.parseLong(table);
            Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
            if (database == null) {
                throw new IllegalArgumentException(String.format("BaseInfo's db %s should not be null in the foreign key " +
                        "constraint, please drop foreign key constraints and retry", dbId));
            }
            Table baseTable = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getId(), tableId);
            if (baseTable == null) {
                throw new IllegalArgumentException(String.format("BaseInfo' base table %s should not be null in the foreign kee" +
                                " constraint, please drop foreign key constraints and retry",
                        tableId));
            }
            baseTableInfo = new BaseTableInfo(dbId, database.getFullName(), baseTable.getName(), tableId);
        } else {
            baseTableInfo = new BaseTableInfo(catalogName, db, table, tableIdentifier);
        }
        return baseTableInfo;
    }

    public static String getShowCreateTableConstraintDesc(OlapTable baseTable, List<ForeignKeyConstraint> constraints) {
        if (CollectionUtils.isEmpty(constraints)) {
            return "";
        }
        List<String> constraintStrs = Lists.newArrayList();
        for (ForeignKeyConstraint constraint : constraints) {
            BaseTableInfo parentTableInfo = constraint.getParentTableInfo();
            BaseTableInfo childTableInfo = constraint.getChildTableInfo();

            StringBuilder constraintSb = new StringBuilder();
            if (childTableInfo != null) {
                constraintSb.append(childTableInfo.getReadableString());
            }
            constraintSb.append("(");
            String baseColumns = Joiner.on(",").join(constraint.getColumnNameRefPairs(baseTable)
                    .stream().map(pair -> pair.first).collect(Collectors.toList()));
            constraintSb.append(baseColumns);
            constraintSb.append(")");
            constraintSb.append(" REFERENCES ");
            constraintSb.append(parentTableInfo.getReadableString());

            constraintSb.append("(");
            String parentColumns = Joiner.on(",").join(constraint.getColumnNameRefPairs(baseTable)
                    .stream().map(pair -> pair.second).collect(Collectors.toList()));
            constraintSb.append(parentColumns);
            constraintSb.append(")");
            constraintStrs.add(constraintSb.toString());
        }

        return Joiner.on(";").join(constraintStrs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnRefPairs, parentTableInfo, childTableInfo);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || !(obj instanceof ForeignKeyConstraint)) {
            return false;
        }
        ForeignKeyConstraint that = (ForeignKeyConstraint) obj;
        return Objects.equals(columnRefPairs, that.columnRefPairs) &&
                Objects.equals(parentTableInfo, that.parentTableInfo) &&
                Objects.equals(childTableInfo, that.childTableInfo);
    }

    public void onTableRename(Table table, String oldTableName) {
        LOG.info("ForeignKeyConstraint onTableRename, table: {}, oldTableName: {}, parentTableInfo: {}, childTableInfo: {}",
                table.getName(), oldTableName, parentTableInfo, childTableInfo);
        // parentTableInfo: how to know which other tables that ref for this table?
        GlobalConstraintManager globalConstraintManager = GlobalStateMgr.getCurrentState().getGlobalConstraintManager();
        Set<TableWithFKConstraint> refConstraints = globalConstraintManager.getRefConstraints(table);
        if (!CollectionUtils.isEmpty(refConstraints)) {
            for (TableWithFKConstraint tableWithFKConstraint : refConstraints) {
                Table childTable = tableWithFKConstraint.getChildTable();
                if (childTable == null) {
                    continue;
                }
                List<ForeignKeyConstraint> fks = childTable.getForeignKeyConstraints();
                if (!CollectionUtils.isEmpty(fks)) {
                    // refresh child table's foreign key constraints
                    List<ForeignKeyConstraint> newFKConstraints = Lists.newArrayList();
                    boolean isChanged = false;
                    for (ForeignKeyConstraint fk : fks) {
                        if (fk.parentTableInfo != null) {
                            fk.parentTableInfo.onTableRename(table, oldTableName);
                            isChanged = true;
                        }
                        newFKConstraints.add(fk);
                    }
                    if (isChanged) {
                        LOG.info("refresh child table's foreign key constraints, parent table, child table: {}, " +
                                "newFKConstraints: {}", table.getName(), childTable.getName(), newFKConstraints);
                        childTable.setForeignKeyConstraints(newFKConstraints);
                    }
                }
            }
        }

        // childTableInfo
        if (childTableInfo != null) {
            childTableInfo.onTableRename(table, oldTableName);
        }

        // columnRefPairs: no needs to change after table rename
    }
}
