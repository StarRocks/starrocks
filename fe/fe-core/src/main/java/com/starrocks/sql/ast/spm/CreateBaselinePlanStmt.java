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

package com.starrocks.sql.ast.spm;

import com.starrocks.analysis.RedirectStatus;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.parser.NodePosition;

public class CreateBaselinePlanStmt extends DdlStmt {
    private final boolean isGlobal;

    private final QueryRelation bindStmt;

    private final QueryRelation planStmt;

    public CreateBaselinePlanStmt(boolean isGlobal, QueryRelation bindStmt, QueryRelation planStmt, NodePosition pos) {
        super(pos);
        this.isGlobal = isGlobal;
        this.bindStmt = bindStmt;
        this.planStmt = planStmt;
    }

    public boolean isGlobal() {
        return isGlobal;
    }

    public QueryRelation getBindStmt() {
        return bindStmt;
    }

    public QueryRelation getPlanStmt() {
        return planStmt;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        // @todo: check is global
        return RedirectStatus.NO_FORWARD;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateBaselinePlanStatement(this, context);
    }
}
