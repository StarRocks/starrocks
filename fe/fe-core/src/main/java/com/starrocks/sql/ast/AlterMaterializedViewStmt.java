// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.TableName;

/**
 * 1.Support for modifying the way of refresh and the cycle of asynchronous refresh;
 * 2.Support for modifying the name of a materialized view;
 * 3.SYNC is not supported and ASYNC is not allow changed to SYNC
 */
public class AlterMaterializedViewStmt extends DdlStmt {

    private final TableName mvName;
    private final String newMvName;
    private final RefreshSchemeDesc refreshSchemeDesc;

    private final ModifyTablePropertiesClause modifyTablePropertiesClause;

    public AlterMaterializedViewStmt(TableName mvName, String newMvName,
                                     RefreshSchemeDesc refreshSchemeDesc,
                                     ModifyTablePropertiesClause modifyTablePropertiesClause) {
        this.mvName = mvName;
        this.newMvName = newMvName;
        this.refreshSchemeDesc = refreshSchemeDesc;
        this.modifyTablePropertiesClause = modifyTablePropertiesClause;
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

    public ModifyTablePropertiesClause getModifyTablePropertiesClause() {
        return modifyTablePropertiesClause;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterMaterializedViewStatement(this, context);
    }
}
