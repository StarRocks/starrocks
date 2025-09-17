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

public class ShowGrantsStmt extends ShowStmt {
    private final UserRef user;
    private final String groupOrRole;
    private final GrantType grantType;

    public ShowGrantsStmt(UserRef user, NodePosition pos) {
        super(pos);
        this.user = user;
        this.groupOrRole = null;
        grantType = GrantType.USER;
    }

    public ShowGrantsStmt(String groupOrRole, GrantType grantType, NodePosition pos) {
        super(pos);
        this.user = null;
        this.groupOrRole = groupOrRole;
        this.grantType = grantType;
    }

    public UserRef getUser() {
        return user;
    }

    public String getGroupOrRole() {
        return groupOrRole;
    }

    public GrantType getGrantType() {
        return grantType;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitShowGrantsStatement(this, context);
    }
}
