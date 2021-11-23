// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.analyzer.relation;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;

import java.util.List;
import java.util.stream.Collectors;

public class InsertRelation extends Relation {
    private final QueryRelation queryRelation;
    private final Database database;
    private final Table targetTable;
    private final List<Long> targetPartitionIds;
    private final List<Column> targetColumn;
    private final List<String> targetColumnNames;

    public InsertRelation(QueryRelation queryRelation,
                          Database database, Table targetTable, List<Long> targetPartitionIds,
                          List<Column> targetColumn, List<String> targetColumnNames) {
        super(null);
        this.queryRelation = queryRelation;
        this.database = database;
        this.targetTable = targetTable;
        this.targetPartitionIds = targetPartitionIds;
        this.targetColumn = targetColumn;

        // StarRocks tables are not case-sensitive, so targetColumnNames are converted
        // to lowercase characters to facilitate subsequent matching.
        if (targetColumnNames != null) {
            this.targetColumnNames = targetColumnNames.stream().map(String::toLowerCase).collect(Collectors.toList());
        } else {
            this.targetColumnNames = null;
        }
    }

    public QueryRelation getQueryRelation() {
        return queryRelation;
    }

    public Database getDatabase() {
        return database;
    }

    public Table getTargetTable() {
        return targetTable;
    }

    public List<Long> getTargetPartitionIds() {
        return targetPartitionIds;
    }

    public List<Column> getTargetColumn() {
        return targetColumn;
    }

    public List<String> getTargetColumnNames() {
        return targetColumnNames;
    }

    @Override
    public <R, C> R accept(RelationVisitor<R, C> visitor, C context) {
        return visitor.visitInsert(this, context);
    }
}

