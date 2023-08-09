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

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.Subquery;
import com.starrocks.sql.parser.NodePosition;

import java.util.ArrayList;

public class ImportWhereStmt extends StatementBase {
    private final Expr expr;

    public ImportWhereStmt(Expr expr) {
        this(expr, NodePosition.ZERO);
    }

    public ImportWhereStmt(Expr expr, NodePosition pos) {
        super(pos);
        this.expr = expr;
    }

    public Expr getExpr() {
        return expr;
    }

    public boolean isContainSubquery() {
        ArrayList<Expr> matched = new ArrayList<>();
        expr.collect(Subquery.class, matched);
        if (matched.size() != 0) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return null;
    }
}
