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

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public class ShowEnginesStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Engine", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Support", ScalarType.createVarchar(8)))
                    .addColumn(new Column("Comment", ScalarType.createVarchar(80)))
                    .addColumn(new Column("Transactions", ScalarType.createVarchar(3)))
                    .addColumn(new Column("XA", ScalarType.createVarchar(3)))
                    .addColumn(new Column("Savepoints", ScalarType.createVarchar(3)))
                    .build();

    public ShowEnginesStmt() {
        this(null, null, null, null, NodePosition.ZERO);
    }

    public ShowEnginesStmt(NodePosition pos) {
        this(null, null, null, null, pos);
    }

    public ShowEnginesStmt(String pattern, Expr where, List<OrderByElement> orderByElements,
                          LimitElement limitElement, NodePosition pos) {
        super(pos);
        this.pattern = pattern;
        this.predicate = where;
        this.orderByElements = orderByElements;
        this.limitElement = limitElement;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowEnginesStatement(this, context);
    }
}
