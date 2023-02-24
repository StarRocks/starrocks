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
import com.starrocks.sql.parser.NodePosition;

import java.util.ArrayList;
import java.util.List;

public class ValuesRelation extends QueryRelation {
    private final List<List<Expr>> rows;

    /*
        isNullValues means a statement without from or from dual, add a single row of null values here,
        so that the semantics are the same, and the processing of subsequent query logic can be simplified,
        such as select sum(1) or select sum(1) from dual, will be converted to select sum(1) from (values(null)) t.
        This can share the same logic as select sum(1) from table
    */
    private boolean isNullValues;

    public ValuesRelation(List<ArrayList<Expr>> rows, List<String> explicitColumnNames) {
        this(rows, explicitColumnNames, NodePosition.ZERO);
    }

    public ValuesRelation(List<ArrayList<Expr>> rows, List<String> explicitColumnNames, NodePosition pos) {
        super(pos);
        this.rows = new ArrayList<>(rows);
        this.explicitColumnNames = explicitColumnNames;
    }

    public void addRow(ArrayList<Expr> row) {
        this.rows.add(row);
    }

    public List<Expr> getRow(int rowIdx) {
        return rows.get(rowIdx);
    }

    public List<List<Expr>> getRows() {
        return rows;
    }

    @Override
    public List<Expr> getOutputExpression() {
        return rows.get(0);
    }

    public void setNullValues(boolean nullValues) {
        isNullValues = nullValues;
    }

    public boolean isNullValues() {
        return isNullValues;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitValues(this, context);
    }
}
