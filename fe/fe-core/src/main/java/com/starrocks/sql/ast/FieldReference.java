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

import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TableName;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.thrift.TExprNode;

import java.util.Objects;

/**
 * The special "SlotRef" does not store the column name.
 * Only store the offset. Used in the analysis of star
 * eg. "select * from (select count(*) from table) t"
 * will store field reference 0 in inner queryblock
 */
public class FieldReference extends Expr {
    private final int fieldIndex;
    private final TableName tblName;

    public FieldReference(int fieldIndex, TableName tableName) {
        this.fieldIndex = fieldIndex;
        this.tblName = tableName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitFieldReference(this, context);
    }

    public int getFieldIndex() {
        return fieldIndex;
    }

    public TableName getTblName() {
        return tblName;
    }

    @Override
    public boolean equalsWithoutChild(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FieldReference that = (FieldReference) o;
        return fieldIndex == that.fieldIndex && Objects.equals(tblName, that.tblName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fieldIndex, tblName);
    }

    @Override
    public Expr clone() {
        return new FieldReference(fieldIndex, tblName);
    }

    @Override
    protected String toSqlImpl() {
        return "FieldReference(" + fieldIndex + ")";
    }

    @Override
    protected void toThrift(TExprNode msg) {
        throw new StarRocksPlannerException("FieldReference not implement toThrift", ErrorType.INTERNAL_ERROR);
    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        throw new StarRocksPlannerException("FieldReference not implement toThrift", ErrorType.INTERNAL_ERROR);
    }
}
