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

package com.starrocks.planner.expression;

import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.type.Type;

import java.util.List;

/**
 * Execution-plan struct subfield access expression.
 */
public class ExecSubfield extends ExecExpr {
    private final List<String> fieldNames;
    private final boolean copyFlag;

    public ExecSubfield(Type type, List<String> fieldNames, boolean copyFlag, List<ExecExpr> children) {
        super(type, children);
        this.fieldNames = fieldNames;
        this.copyFlag = copyFlag;
    }

    private ExecSubfield(ExecSubfield other) {
        super(other.type, other.cloneChildren());
        this.fieldNames = other.fieldNames;
        this.copyFlag = other.copyFlag;
        this.originType = other.originType;
        this.isIndexOnlyFilter = other.isIndexOnlyFilter;
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public boolean isCopyFlag() {
        return copyFlag;
    }

    @Override
    public boolean isNullable() {
        return true;
    }

    @Override
    public TExprNodeType getNodeType() {
        return TExprNodeType.SUBFIELD_EXPR;
    }

    @Override
    public void toThrift(TExprNode node) {
        node.setUsed_subfield_names(fieldNames);
        node.setCopy_flag(copyFlag);
    }

    @Override
    public <R, C> R accept(ExecExprVisitor<R, C> visitor, C context) {
        return visitor.visitExecSubfield(this, context);
    }

    @Override
    public ExecSubfield clone() {
        return new ExecSubfield(this);
    }
}
