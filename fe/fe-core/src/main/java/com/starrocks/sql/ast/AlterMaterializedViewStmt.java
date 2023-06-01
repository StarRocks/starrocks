// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.sql.ast;

import com.google.common.collect.Sets;
import com.starrocks.analysis.TableName;
import com.starrocks.sql.parser.NodePosition;

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
    private final ModifyTablePropertiesClause modifyTablePropertiesClause;
    private final String status;
    private final SwapTableClause swapTable;

    public static final String ACTIVE = "active";
    public static final String INACTIVE = "inactive";
    public static final Set<String> SUPPORTED_MV_STATUS = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);

    static {
        SUPPORTED_MV_STATUS.add(ACTIVE);
        SUPPORTED_MV_STATUS.add(INACTIVE);
    }

    public AlterMaterializedViewStmt(TableName mvName, String newMvName,
                                     RefreshSchemeDesc refreshSchemeDesc,
                                     ModifyTablePropertiesClause modifyTablePropertiesClause,
                                     String status, SwapTableClause swapTable,
                                     NodePosition pos) {
        super(pos);
        this.mvName = mvName;
        this.newMvName = newMvName;
        this.refreshSchemeDesc = refreshSchemeDesc;
        this.modifyTablePropertiesClause = modifyTablePropertiesClause;
        this.status = status;
        this.swapTable = swapTable;
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

    public SwapTableClause getSwapTable() {
        return swapTable;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterMaterializedViewStatement(this, context);
    }
}
