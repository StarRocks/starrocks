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

import java.util.List;

public class DropHistogramStmt extends StatementBase {
    private final TableName tbl;
    private final List<String> columnNames;

    public DropHistogramStmt(TableName tbl, List<String> columnNames) {
        this(tbl, columnNames, NodePosition.ZERO);
    }

    public DropHistogramStmt(TableName tbl, List<String> columnNames, NodePosition pos) {
        super(pos);
        this.tbl = tbl;
        this.columnNames = columnNames;
    }

    public TableName getTableName() {
        return tbl;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropHistogramStatement(this, context);
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }
}
