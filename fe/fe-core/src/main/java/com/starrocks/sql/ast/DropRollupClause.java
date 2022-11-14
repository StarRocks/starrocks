// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.base.Strings;
import com.starrocks.alter.AlterOpType;
import com.starrocks.analysis.Analyzer;
import com.starrocks.common.AnalysisException;

import java.util.Map;

// Delete one rollup from table
public class DropRollupClause extends AlterTableClause {
    private final String rollupName;
    private Map<String, String> properties;

    public DropRollupClause(String rollupName, Map<String, String> properties) {
        super(AlterOpType.DROP_ROLLUP);
        this.rollupName = rollupName;
        this.properties = properties;
        this.needTableStable = false;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (Strings.isNullOrEmpty(rollupName)) {
            throw new AnalysisException("No rollup in delete rollup.");
        }
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("DROP ROLLUP ");
        stringBuilder.append("`").append(rollupName).append("`");
        return stringBuilder.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    public String getRollupName() {
        return rollupName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropRollupClause(this, context);
    }
}
