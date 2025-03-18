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

import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.TableName;
import com.starrocks.sql.parser.NodePosition;

public class DropStatsStmt extends StatementBase {
    private final TableName tbl;
    private boolean isExternal = false;
    private boolean isMultiColumn = false;

    public DropStatsStmt(TableName tbl) {
        this(tbl, false, NodePosition.ZERO);
    }

    public DropStatsStmt(TableName tbl, boolean isMultiColumn, NodePosition pos) {
        super(pos);
        this.tbl = tbl;
        this.isMultiColumn = isMultiColumn;
    }

    public TableName getTableName() {
        return tbl;
    }

    public boolean isExternal() {
        return isExternal;
    }

    public void setExternal(boolean isExternal) {
        this.isExternal = isExternal;
    }

    public boolean isMultiColumn() {
        return isMultiColumn;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropStatsStatement(this, context);
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }
}
