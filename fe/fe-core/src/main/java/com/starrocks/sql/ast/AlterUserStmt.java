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

import com.starrocks.analysis.UserDesc;
import com.starrocks.sql.parser.NodePosition;

import java.util.Collections;

public class AlterUserStmt extends BaseCreateAlterUserStmt {
    private final boolean ifExists;

    public AlterUserStmt(UserDesc userDesc, boolean ifExists) {
        this(userDesc, ifExists, NodePosition.ZERO);
    }

    public AlterUserStmt(UserDesc userDesc, boolean ifExists, NodePosition pos) {
        super(userDesc, null, Collections.emptyList(), pos);
        this.ifExists = ifExists;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterUserStatement(this, context);
    }
}
