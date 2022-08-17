// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;

public class RowDelimiter implements ParseNode {
    private final String oriDelimiter;
    private String delimiter;

    public RowDelimiter(String delimiter) {
        this.oriDelimiter = delimiter;
        this.delimiter = null;
    }

    public String getRowDelimiter() {
        return delimiter;
    }

    public void analyze() throws AnalysisException {
        analyze(null);
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        this.delimiter = Delimiter.convertDelimiter(oriDelimiter);
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("'").append(oriDelimiter).append("'");
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
