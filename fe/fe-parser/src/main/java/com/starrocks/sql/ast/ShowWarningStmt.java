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

import com.starrocks.sql.ast.expression.LimitElement;
import com.starrocks.sql.parser.NodePosition;

// Backs both `SHOW WARNINGS` and `SHOW ERRORS` (grammar rule `SHOW (WARNINGS | ERRORS) ...`).
public class ShowWarningStmt extends ShowStmt {
    private final boolean showErrors;

    public ShowWarningStmt(LimitElement limitElement, NodePosition pos) {
        this(limitElement, false, pos);
    }

    public ShowWarningStmt(LimitElement limitElement, boolean showErrors, NodePosition pos) {
        super(pos);
        setLimitElement(limitElement);
        this.showErrors = showErrors;
    }

    // True for `SHOW ERRORS` (returns only Error-level diagnostics), false for `SHOW WARNINGS`
    // (returns all diagnostics). Both keywords share this node, so the executor filters by level.
    public boolean isShowErrors() {
        return showErrors;
    }

    public long getLimitNum() {
        LimitElement limitElement = getLimitElement();
        if (limitElement != null && limitElement.hasLimit()) {
            return limitElement.getLimit();
        }
        return -1L;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowWarningStatement(this, context);
    }
}
