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
import com.starrocks.sql.ast.AddBackendClause;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;

public class AddBackendClauseEPack implements ParseNode {
    private final AddBackendClause addBackendClause;
    public String warehouse;

    public AddBackendClauseEPack(AddBackendClause addBackendClause, String warehouse) {
        this.addBackendClause = addBackendClause;
        this.warehouse = warehouse;
    }

    public AddBackendClause getAddBackendClause() {
        return addBackendClause;
    }

    public String getWarehouse() {
        return warehouse;
    }

    @Override
    public NodePosition getPos() {
        return addBackendClause.getPos();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorEPack<R, C>) visitor).visitAddBackendClause(this, context);
    }
}
