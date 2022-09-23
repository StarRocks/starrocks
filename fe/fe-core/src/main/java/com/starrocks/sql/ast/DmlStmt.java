// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.ast;

import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.TableName;

public abstract class DmlStmt extends StatementBase {
    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }

    public abstract TableName getTableName();

    private boolean isSupportNewPlanner() {
        return true;
    }
}
