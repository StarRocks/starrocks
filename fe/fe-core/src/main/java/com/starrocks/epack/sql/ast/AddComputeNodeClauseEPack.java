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
import com.starrocks.sql.ast.AddComputeNodeClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;

public class AddComputeNodeClauseEPack extends AlterClause implements ParseNode {
    private final AddComputeNodeClause addComputeNodeClause;
    public String warehouse;

    public AddComputeNodeClauseEPack(AddComputeNodeClause addComputeNodeClause, String warehouse) {
        super(addComputeNodeClause.getOpType(), addComputeNodeClause.getPos());

        this.addComputeNodeClause = addComputeNodeClause;
        this.warehouse = warehouse;
    }

    public AddComputeNodeClause getAddComputeNodeClause() {
        return addComputeNodeClause;
    }

    public String getWarehouse() {
        return warehouse;
    }

    @Override
    public NodePosition getPos() {
        return addComputeNodeClause.getPos();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        if (visitor instanceof AstVisitorEPack) {
            return ((AstVisitorEPack<R, C>) visitor).visitAddComputeNodeClause(this, context);
        } else {
            return null;
        }
    }
}
