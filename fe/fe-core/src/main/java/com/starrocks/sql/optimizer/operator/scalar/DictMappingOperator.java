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


package com.starrocks.sql.optimizer.operator.scalar;

import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class DictMappingOperator extends ScalarOperator {
<<<<<<< HEAD

    private ColumnRefOperator dictColumn;
    private ScalarOperator originScalaOperator;
=======
    // use dict id
    private ColumnRefOperator dictColumn;
    // dict expression
    private ScalarOperator originScalaOperator;
    // input string expression
    private ScalarOperator stringProvideOperator;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

    public DictMappingOperator(ColumnRefOperator dictColumn, ScalarOperator originScalaOperator, Type retType) {
        super(OperatorType.DICT_MAPPING, retType);
        this.dictColumn = dictColumn;
        this.originScalaOperator = originScalaOperator;
    }

<<<<<<< HEAD
=======
    public DictMappingOperator(Type type, ColumnRefOperator dictColumn, ScalarOperator originScalaOperator,
                               ScalarOperator stringScalarOperator) {
        super(OperatorType.DICT_MAPPING, type);
        this.dictColumn = dictColumn;
        this.originScalaOperator = originScalaOperator;
        this.stringProvideOperator = stringScalarOperator;
    }

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    public ColumnRefOperator getDictColumn() {
        return dictColumn;
    }

    public ScalarOperator getOriginScalaOperator() {
        return originScalaOperator;
    }

<<<<<<< HEAD
=======
    public ScalarOperator getStringProvideOperator() {
        return stringProvideOperator;
    }

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    @Override
    public boolean isNullable() {
        return originScalaOperator.isNullable();
    }

    @Override
    public List<ScalarOperator> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public ScalarOperator getChild(int index) {
        return null;
    }

    @Override
    public void setChild(int index, ScalarOperator child) {
    }

    @Override
    public String toString() {
<<<<<<< HEAD
        return "DictMapping(" + dictColumn + "{" + originScalaOperator + "}" + ")";
=======
        String stringOperator = stringProvideOperator == null ? "" : ", " + stringProvideOperator;
        return "DictMapping(" + dictColumn + ", " + originScalaOperator + stringOperator + ")";
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    @Override
    public int hashCode() {
        return Objects.hash(getType(), dictColumn, originScalaOperator);
    }

    @Override
<<<<<<< HEAD
    public boolean equals(Object other) {
        if (other == null) {
            return false;
        }
        if (this == other) {
            return true;
        }
        if (other instanceof DictMappingOperator) {
            final DictMappingOperator mapping = (DictMappingOperator) other;
            return mapping.getType().equals(getType()) && mapping.originScalaOperator.equals(originScalaOperator) &&
                    mapping.dictColumn.equals(dictColumn);
        }
        return false;
=======
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DictMappingOperator that = (DictMappingOperator) o;
        return Objects.equals(dictColumn, that.dictColumn) &&
                Objects.equals(originScalaOperator, that.originScalaOperator) &&
                Objects.equals(stringProvideOperator, that.stringProvideOperator);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return visitor.visitDictMappingOperator(this, context);
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        return dictColumn.getUsedColumns();
    }

    @Override
<<<<<<< HEAD
=======
    public void getColumnRefs(List<ColumnRefOperator> columns) {
        dictColumn.getColumnRefs(columns);
        if (stringProvideOperator != null) {
            stringProvideOperator.getColumnRefs(columns);
        } else {
            originScalaOperator.getColumnRefs(columns);
        }
    }

    @Override
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    public ScalarOperator clone() {
        DictMappingOperator clone = (DictMappingOperator) super.clone();
        clone.dictColumn = (ColumnRefOperator) this.dictColumn.clone();
        clone.originScalaOperator = this.originScalaOperator.clone();
<<<<<<< HEAD
=======
        if (this.stringProvideOperator != null) {
            clone.stringProvideOperator = this.stringProvideOperator.clone();
        }
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return clone;
    }
}
