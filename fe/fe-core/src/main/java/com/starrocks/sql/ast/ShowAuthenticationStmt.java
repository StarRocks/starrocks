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

import com.starrocks.sql.parser.NodePosition;

public class ShowAuthenticationStmt extends ShowStmt {
    private final boolean isAll;
    private User user;

    // SHOW AUTHENTICATION -> (null, false)
    // SHOW ALL AUTHENTICATION -> (null, true)
    // SHOW AUTHENTICATION FOR xx -> (xx, false)
    public ShowAuthenticationStmt(User userIdent, boolean isAll) {
        this(userIdent, isAll, NodePosition.ZERO);
    }

    public ShowAuthenticationStmt(User userIdent, boolean isAll, NodePosition pos) {
        super(pos);
        this.user = userIdent;
        this.isAll = isAll;
    }

    public User getUser() {
        return user;
    }

    public boolean isAll() {
        return isAll;
    }

    public void setUser(User user) {
        this.user = user;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowAuthenticationStatement(this, context);
    }
}
