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

import com.starrocks.analysis.RedirectStatus;
import com.starrocks.sql.parser.NodePosition;

// EXECUTE AS XX WITH NO REVERT
public class ExecuteAsStmt extends StatementBase {
    protected UserIdentity toUser;
    protected boolean allowRevert;

    public ExecuteAsStmt(UserIdentity toUser, boolean allowRevert) {
        this(toUser, allowRevert, NodePosition.ZERO);
    }

    public ExecuteAsStmt(UserIdentity toUser, boolean allowRevert, NodePosition pos) {
        super(pos);
        this.toUser = toUser;
        this.allowRevert = allowRevert;
    }

    public UserIdentity getToUser() {
        return toUser;
    }

    public boolean isAllowRevert() {
        return allowRevert;
    }

    @Override
    public String toString() {
        String s = String.format("EXECUTE AS %s", this.toUser.toString());
        if (allowRevert) {
            return s;
        } else {
            return s + " WITH NO REVERT";
        }
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitExecuteAsStatement(this, context);
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }
}
