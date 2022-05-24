// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.ast;

import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.TableName;

/**
 * 1.Support for modifying the way of refresh and the cycle of asynchronous refresh;
 * 2.Support for modifying the name of a materialized view;
 * 3.For the time being, the way of SYNC is not supported and the way of ASYNC is changed to SYNC
 */
public class AlterMaterializedViewStmt extends DdlStmt {

    private TableName mvName;
    private String newMvName;
    private RefreshSchemeDesc refreshSchemeDesc;

    public AlterMaterializedViewStmt(TableName mvName, String newMvName,
                                     RefreshSchemeDesc refreshSchemeDesc) {
        this.mvName = mvName;
        this.newMvName = newMvName;
        this.refreshSchemeDesc = refreshSchemeDesc;
    }

    public TableName getMvName() {
        return mvName;
    }

    public String getNewMvName() {
        return newMvName;
    }

    public RefreshSchemeDesc getRefreshSchemeDesc() {
        return refreshSchemeDesc;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterMaterializedViewStatement(this, context);
    }
}
