// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.ast;

import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.sql.analyzer.Field;

import java.util.List;
import java.util.Map;

public class TableRelation extends Relation {
    private final TableName name;
    private Table table;
    private Map<Field, Column> columns;
    // Support temporary partition
    private PartitionNames partitionNames;
    private final List<Long> tabletIds;
    private boolean isMetaQuery;

    // optional temporal clause for external MySQL tables that support this syntax
    private String temporalClause;

    private Expr partitionPredicate;

    public TableRelation(TableName name) {
        this.name = name;
        this.partitionNames = null;
        this.tabletIds = Lists.newArrayList();
    }

    public TableRelation(TableName name, PartitionNames partitionNames, List<Long> tabletIds) {
        this.name = name;
        this.partitionNames = partitionNames;
        this.tabletIds = tabletIds;
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

    public List<Long> getTabletIds() {
        return tabletIds;
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
                    return new TableName(name.getCatalog(), name.getDb(), alias.getTbl());
                } else {
                    return new TableName(name.getDb(), alias.getTbl());
                }
            } else {
                return alias;
            }
        } else {
            return name;
        }
    }

    public boolean isMetaQuery() {
        return isMetaQuery;
    }

    public void setMetaQuery(boolean metaQuery) {
        isMetaQuery = metaQuery;
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
}