// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.UserDesc;

public class AlterUserStmt extends BaseCreateAlterUserStmt {

    public AlterUserStmt(UserDesc userDesc) {
        super(userDesc, "ALTER");
    }
}
