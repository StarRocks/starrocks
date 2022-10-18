// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.RedirectStatus;

import java.util.List;

public class ImportColumnsStmt extends StatementBase {
    private final List<ImportColumnDesc> columns;

    public ImportColumnsStmt(List<ImportColumnDesc> columns) {
        this.columns = columns;
    }

    public List<ImportColumnDesc> getColumns() {
        return columns;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return null;
    }
}
