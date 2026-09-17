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

// SHOW CREATE USER xx           -> (xx)
// SHOW CREATE USER CURRENT_USER -> (null), filled with the current user by the analyzer
public class ShowCreateUserStmt extends ShowStmt {
    private UserRef user;

    public ShowCreateUserStmt(UserRef user) {
        this(user, NodePosition.ZERO);
    }

    public ShowCreateUserStmt(UserRef user, NodePosition pos) {
        super(pos);
        this.user = user;
    }

    public UserRef getUser() {
        return user;
    }

    public void setUser(UserRef user) {
        this.user = user;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowCreateUserStatement(this, context);
    }
}
