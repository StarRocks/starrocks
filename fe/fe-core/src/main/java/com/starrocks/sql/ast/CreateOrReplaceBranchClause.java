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

import com.starrocks.alter.AlterOpType;
import com.starrocks.connector.BranchOptions;
import com.starrocks.sql.parser.NodePosition;

public class CreateOrReplaceBranchClause extends AlterTableClause {
    private final String branchName;
    private final BranchOptions branchOptions;
    private final boolean create;
    private final boolean replace;
    private final boolean ifNotExists;

    public CreateOrReplaceBranchClause(NodePosition pos, String branchName,
                                     BranchOptions branchOptions, boolean create, boolean replace, boolean ifNotExists) {
        super(AlterOpType.ALTER_BRANCH, pos);
        this.branchName = branchName;
        this.branchOptions = branchOptions;
        this.create = create;
        this.replace = replace;
        this.ifNotExists = ifNotExists;
    }

    public String getBranchName() {
        return branchName;
    }

    public BranchOptions getBranchOptions() {
        return branchOptions;
    }

    public boolean isCreate() {
        return create;
    }

    public boolean isReplace() {
        return replace;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateOrReplaceBranchClause(this, context);
    }
}
