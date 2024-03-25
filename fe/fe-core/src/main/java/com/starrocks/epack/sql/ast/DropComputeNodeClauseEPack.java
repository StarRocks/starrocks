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
package com.starrocks.epack.sql.ast;

import com.starrocks.analysis.ParseNode;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.DropComputeNodeClause;
import com.starrocks.sql.parser.NodePosition;

public class DropComputeNodeClauseEPack extends AlterClause implements ParseNode {
    private final DropComputeNodeClause dropComputeNodeClause;
    public String warehouse;

    public DropComputeNodeClauseEPack(DropComputeNodeClause dropComputeNodeClause, String warehouse) {
        super(dropComputeNodeClause.getOpType(), dropComputeNodeClause.getPos());
        this.dropComputeNodeClause = dropComputeNodeClause;
        this.warehouse = warehouse;
    }

    public DropComputeNodeClause getDropComputeNodeClause() {
        return dropComputeNodeClause;
    }

    public String getWarehouse() {
        return warehouse;
    }

    @Override
    public NodePosition getPos() {
        return dropComputeNodeClause.getPos();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        if (visitor instanceof AstVisitorEPack) {
            return ((AstVisitorEPack<R, C>) visitor).visitDropComputeNodeClause(this, context);
        } else {
            return null;
        }
    }
}

