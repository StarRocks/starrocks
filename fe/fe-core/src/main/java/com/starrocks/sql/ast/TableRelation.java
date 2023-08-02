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

package com.starrocks.sql.ast;

import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.parser.NodePosition;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableRelation extends Relation {

    public enum TableHint {
        _META_,
        _BINLOG_,
        _SYNC_MV_,
        _USE_PK_INDEX_,
    }

    private final TableName name;
    private Table table;
    private Map<Field, Column> columns;
    // Support temporary partition
    private PartitionNames partitionNames;
    private final List<Long> tabletIds;
    private final List<Long> replicaIds;
    private final Set<TableHint> tableHints = new HashSet<>();
    // optional temporal clause for external MySQL tables that support this syntax
    private String temporalClause;

    private Expr partitionPredicate;

    private Map<Expr, SlotRef> generatedExprToColumnRef = new HashMap<>();

    public TableRelation(TableName name) {
        super(name.getPos());
        this.name = name;
        this.partitionNames = null;
        this.tabletIds = Lists.newArrayList();
        this.replicaIds = Lists.newArrayList();
    }

    public TableRelation(TableName name, PartitionNames partitionNames, List<Long> tabletIds, List<Long> replicaIds) {
        this(name, partitionNames, tabletIds, replicaIds, NodePosition.ZERO);
    }

    public TableRelation(TableName name, PartitionNames partitionNames, List<Long> tabletIds, List<Long> replicaIds,
                         NodePosition pos) {
        super(pos);
        this.name = name;
        this.partitionNames = partitionNames;
        this.tabletIds = tabletIds;
        this.replicaIds = replicaIds;
    }

    public TableName getName() {
        return name;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public PartitionNames getPartitionNames() {
        return partitionNames;
    }

    public void setPartitionNames(PartitionNames partitionNames) {
        this.partitionNames = partitionNames;
    }

    // Check whether the table has some table hints, some rules should not be applied.
    public boolean hasTableHints() {
        return partitionNames != null || isSyncMVQuery() || (tabletIds != null && !tabletIds.isEmpty()) ||
                (replicaIds != null && !replicaIds.isEmpty());
    }

    public List<Long> getTabletIds() {
        return tabletIds;
    }

    public List<Long> getReplicaIds() {
        return replicaIds;
    }

    public Column getColumn(Field field) {
        return columns.get(field);
    }

    public void setColumns(Map<Field, Column> columns) {
        this.columns = columns;
    }

    public Map<Field, Column> getColumns() {
        return columns;
    }

    public Expr getPartitionPredicate() {
        return this.partitionPredicate;
    }

    public void setPartitionPredicate(Expr partitionPredicate) {
        this.partitionPredicate = partitionPredicate;
    }

    @Override
    public TableName getResolveTableName() {
        if (alias != null) {
            if (name.getDb() != null) {
                if (name.getCatalog() != null) {
                    return new TableName(name.getCatalog(), name.getDb(), alias.getTbl(), name.getPos());
                } else {
                    return new TableName(null, name.getDb(), alias.getTbl(), name.getPos());
                }
            } else {
                return alias;
            }
        } else {
            return name;
        }
    }

    // Return true if add the hint successfully, otherwise return false.
    // For example, if the hint name is not defined, false will be returned.
    public boolean addTableHint(String hintName) {
        try {
            TableHint hint = TableHint.valueOf(hintName);
            tableHints.add(hint);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    public Set<TableHint> getTableHints() {
        return tableHints;
    }

    public boolean isMetaQuery() {
        return tableHints.contains(TableHint._META_);
    }

    public boolean isBinlogQuery() {
        return tableHints.contains(TableHint._BINLOG_) && table.isOlapTable();
    }

    public boolean isSyncMVQuery() {
        return tableHints.contains(TableHint._SYNC_MV_);
    }

    public boolean isUsePkIndex() {
        return tableHints.contains(TableHint._USE_PK_INDEX_);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitTable(this, context);
    }

    @Override
    public String toString() {
        return name.toString();
    }

    public void setTemporalClause(String temporalClause) {
        this.temporalClause = temporalClause;
    }

    public String getTemporalClause() {
        return this.temporalClause;
    }

    public void setGeneratedExprToColumnRef(Map<Expr, SlotRef> generatedExprToColumnRef) {
        this.generatedExprToColumnRef = generatedExprToColumnRef;
    }

    public Map<Expr, SlotRef> getGeneratedExprToColumnRef() {
        return generatedExprToColumnRef;
    }
}