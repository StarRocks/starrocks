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
import com.starrocks.analysis.IndexDef;
import com.starrocks.catalog.Index;
import com.starrocks.sql.parser.NodePosition;

public class CreateIndexClause extends AlterTableClause {
    // index definition class
    private final IndexDef indexDef;
    // index internal class
    private Index index;

    public CreateIndexClause(IndexDef indexDef) {
        this(indexDef, NodePosition.ZERO);
    }

    public CreateIndexClause(IndexDef indexDef, NodePosition pos) {
        super(AlterOpType.SCHEMA_CHANGE, pos);
        this.indexDef = indexDef;
    }

    public Index getIndex() {
        return index;
    }

    public void setIndex(Index index) {
        this.index = index;
    }

    public IndexDef getIndexDef() {
        return indexDef;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateIndexClause(this, context);
    }
}
