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

import java.util.List;

// ALTER TABLE ... DISABLE DICTIONARY (col, ...)  -> forbid low-cardinality global dict on the columns
// ALTER TABLE ... ENABLE  DICTIONARY (col, ...)  -> re-allow it (remove from the forbid set)
public class AlterTableDictColumnsClause extends AlterTableClause {
    // true = ENABLE (remove from the no-dict set), false = DISABLE (add to the no-dict set)
    private final boolean enable;
    private final List<String> columns;

    public AlterTableDictColumnsClause(boolean enable, List<String> columns, NodePosition pos) {
        super(pos);
        this.enable = enable;
        this.columns = columns;
    }

    public boolean isEnable() {
        return enable;
    }

    public List<String> getColumns() {
        return columns;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterTableDictColumnsClause(this, context);
    }
}
