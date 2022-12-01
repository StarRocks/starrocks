// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.Delimiter;
import com.starrocks.analysis.ParseNode;

public class RowDelimiter implements ParseNode {
    private final String oriDelimiter;
    private final String delimiter;

    public RowDelimiter(String delimiter) {
        this.oriDelimiter = delimiter;
        this.delimiter = Delimiter.convertDelimiter(oriDelimiter);
    }

    public String getRowDelimiter() {
        return delimiter;
    }

    public String toSql() {
        return "'" + oriDelimiter + "'";
    }

    @Override
    public String toString() {
        return toSql();
    }
}
