// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.base.Strings;
import com.starrocks.analysis.ParseNode;
import com.starrocks.common.FeNameFormat;

public class ColWithComment implements ParseNode {
    private final String colName;
    private final String comment;

    public ColWithComment(String colName, String comment) {
        this.colName = colName;
        this.comment = Strings.nullToEmpty(comment);
    }

    public String getColName() {
        return colName;
    }

    public String getComment() {
        return comment;
    }

    public void analyze() {
        FeNameFormat.checkColumnName(colName);
    }
}
