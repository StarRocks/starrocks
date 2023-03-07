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

import com.starrocks.analysis.OutFileClause;
import com.starrocks.analysis.RedirectStatus;

public class QueryStatement extends StatementBase {
    private final QueryRelation queryRelation;

    // represent the "INTO OUTFILE" clause
    protected OutFileClause outFileClause;

    public QueryStatement(QueryRelation queryRelation) {
        super(queryRelation.getPos());
        this.queryRelation = queryRelation;
    }

    public QueryRelation getQueryRelation() {
        return queryRelation;
    }

    public void setOutFileClause(OutFileClause outFileClause) {
        this.outFileClause = outFileClause;
    }

    public OutFileClause getOutFileClause() {
        return outFileClause;
    }

    public boolean hasOutFileClause() {
        return outFileClause != null;
    }

    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitQueryStatement(this, context);
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }
}