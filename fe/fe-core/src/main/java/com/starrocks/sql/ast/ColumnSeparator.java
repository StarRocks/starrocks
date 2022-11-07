// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.Delimiter;
import com.starrocks.analysis.ParseNode;

public class ColumnSeparator implements ParseNode {
    private final String oriSeparator;
    private final String separator;

    public ColumnSeparator(String separator) {
        this.oriSeparator = separator;
        this.separator = Delimiter.convertDelimiter(oriSeparator);
    }

    public String getColumnSeparator() {
        return separator;
    }

    public String toSql() {
        return "'" + oriSeparator + "'";
    }

    @Override
    public String toString() {
        return toSql();
    }
}
