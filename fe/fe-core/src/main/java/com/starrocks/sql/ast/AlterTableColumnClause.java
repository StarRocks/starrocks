// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.ast;

import com.starrocks.alter.AlterOpType;
import com.starrocks.catalog.Column;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class AlterTableColumnClause extends AlterTableClause {
    // if rollupName is null, add to column to base index.
    protected String rollupName;
    protected Map<String, String> properties;

    // set in analyze
    // for AddColumnClause and ModifyColumnClause
    private Column column;
    // for AddColumnsClause
    private final List<Column> columns = new ArrayList<>();

    public AlterTableColumnClause(AlterOpType opType, String rollupName, Map<String, String> properties) {
        super(opType);
        this.rollupName = rollupName;
        this.properties = properties;
    }

    public String getRollupName() {
        return rollupName;
    }

    public void setRollupName(String rollupName) {
        this.rollupName = rollupName;
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }

    public Column getColumn() {
        return column;
    }

    public void setColumn(Column column) {
        this.column = column;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void addColumn(Column column) {
        this.columns.add(column);
    }
}
