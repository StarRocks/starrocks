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

import java.util.HashMap;

// modify one column comment
public class ModifyColumnCommentClause extends AlterTableColumnClause {
    private final String columnName;
    private final String comment;

    public String getColumnName() {
        return columnName;
    }

    public String getComment() {
        return comment;
    }

    public ModifyColumnCommentClause(String columnName, String comment, NodePosition pos) {
        super(null, new HashMap<>(), pos);
        this.columnName = columnName;
        this.comment = comment;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitModifyColumnCommentClause(this, context);
    }
}
