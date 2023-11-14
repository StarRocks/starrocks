// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.collect.Sets;
import com.starrocks.analysis.TableName;

import java.util.Set;

/**
 * 1.Support for modifying the way of refresh and the cycle of asynchronous refresh;
 * 2.Support for modifying the name of a materialized view;
 * 3.SYNC is not supported and ASYNC is not allow changed to SYNC
 */
public class AlterMaterializedViewStmt extends DdlStmt {

    private final TableName mvName;
    private final String newMvName;
    private final RefreshSchemeDesc refreshSchemeDesc;
    private final String status;

    public static final String ACTIVE = "active";
    public static final String INACTIVE = "inactive";
    public static final Set<String> SUPPORTED_MV_STATUS = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);

    static {
        SUPPORTED_MV_STATUS.add(ACTIVE);
        SUPPORTED_MV_STATUS.add(INACTIVE);
    }
    private final ModifyTablePropertiesClause modifyTablePropertiesClause;

    public AlterMaterializedViewStmt(TableName mvName, String newMvName,
                                     RefreshSchemeDesc refreshSchemeDesc,
                                     ModifyTablePropertiesClause modifyTablePropertiesClause,
                                     String status) {
        this.mvName = mvName;
        this.newMvName = newMvName;
        this.refreshSchemeDesc = refreshSchemeDesc;
        this.modifyTablePropertiesClause = modifyTablePropertiesClause;
        this.status = status;
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

    public String getStatus() {
        return status;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterMaterializedViewStatement(this, context);
    }
}
