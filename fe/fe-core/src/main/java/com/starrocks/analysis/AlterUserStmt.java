// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.sql.ast.BaseCreateAlterUserStmt;

public class AlterUserStmt extends BaseCreateAlterUserStmt {

    public AlterUserStmt(UserDesc userDesc) {
        super(userDesc, "ALTER");
    }
}
