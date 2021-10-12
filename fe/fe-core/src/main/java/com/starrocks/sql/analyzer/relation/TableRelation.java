// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.analyzer.relation;

import com.starrocks.analysis.PartitionNames;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.RelationFields;

import java.util.List;
import java.util.Map;

public class TableRelation extends Relation {
    private final TableName name;
    private final Table table;
    private final Map<Field, Column> columns;
    // Support temporary partition
    private final PartitionNames partitionNames;
    private final List<Long> tabletIds;
    private final boolean isMetaQuery;

    public TableRelation(TableName name, Table table,
                         Map<Field, Column> columns,
                         List<Field> relationFields,
                         PartitionNames partitionNames,
                         List<Long> tabletIds,
                         boolean isMetaQuery) {
        super(new RelationFields(relationFields));
        this.name = name;
        this.table = table;
        this.columns = columns;
        this.partitionNames = partitionNames;
        this.tabletIds = tabletIds;
        this.isMetaQuery = isMetaQuery;
    }

    public TableName getName() {
        return name;
    }

    public Table getTable() {
        return table;
    }

    public PartitionNames getPartitionNames() {
        return partitionNames;
    }

    public List<Long> getTabletIds() {
        return tabletIds;
    }

    public Column getColumn(Field field) {
        return columns.get(field);
    }

    public Map<Field, Column> getColumns() {
        return columns;
    }

    public boolean isMetaQuery() {
        return isMetaQuery;
    }

    public <R, C> R accept(RelationVisitor<R, C> visitor, C context) {
        return visitor.visitTable(this, context);
    }

    @Override
    public String toString() {
        return name.toString();
    }
}