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

<<<<<<< HEAD
=======
import com.starrocks.analysis.Expr;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.TableName;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public class DropHistogramStmt extends StatementBase {
    private final TableName tbl;
<<<<<<< HEAD
    private final List<String> columnNames;

    public DropHistogramStmt(TableName tbl, List<String> columnNames) {
        this(tbl, columnNames, NodePosition.ZERO);
    }

    public DropHistogramStmt(TableName tbl, List<String> columnNames, NodePosition pos) {
        super(pos);
        this.tbl = tbl;
        this.columnNames = columnNames;
=======
    private List<String> columnNames;
    private final List<Expr> columns;

    private boolean isExternal = false;

    public DropHistogramStmt(TableName tbl, List<Expr> columns) {
        this(tbl, columns, NodePosition.ZERO);
    }

    public DropHistogramStmt(TableName tbl, List<Expr> columns, NodePosition pos) {
        super(pos);
        this.tbl = tbl;
        this.columns = columns;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    public TableName getTableName() {
        return tbl;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

<<<<<<< HEAD
=======
    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    public List<Expr> getColumns() {
        return columns;
    }

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropHistogramStatement(this, context);
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }
<<<<<<< HEAD
=======

    public boolean isExternal() {
        return isExternal;
    }

    public void setExternal(boolean isExternal) {
        this.isExternal = isExternal;
    }
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
}
