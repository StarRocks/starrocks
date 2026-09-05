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

package com.starrocks.sql.optimizer.operator.logical;

import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.RowOutputInfo;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.property.DomainProperty;

import java.util.List;

/**
 * A logical marker operator used by IVM rewrite to bind a subtree to a snapshot version.
 * It should be eliminated by IVM version rewrite rules before physical optimization.
 */
public class LogicalVersionOperator extends LogicalOperator {
    public enum VersionRefType {
        FROM_VERSION,
        TO_VERSION
    }

    private final VersionRefType versionRefType;

    public LogicalVersionOperator(VersionRefType versionRefType) {
        super(OperatorType.LOGICAL_VERSION);
        this.versionRefType = versionRefType;
    }

    public static LogicalVersionOperator fromVersion() {
        return new LogicalVersionOperator(VersionRefType.FROM_VERSION);
    }

    public static LogicalVersionOperator toVersion() {
        return new LogicalVersionOperator(VersionRefType.TO_VERSION);
    }

    public VersionRefType getVersionRefType() {
        return versionRefType;
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        return expressionContext.getChildLogicalProperty(0).getOutputColumns();
    }

    @Override
    public RowOutputInfo deriveRowOutputInfo(List<OptExpression> inputs) {
        return projectInputRow(inputs.get(0).getRowOutputInfo());
    }

    @Override
    public DomainProperty deriveDomainProperty(List<OptExpression> inputs) {
        return inputs.get(0).getDomainProperty();
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalVersion(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalVersion(optExpression, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!super.equals(o)) {
            return false;
        }
        LogicalVersionOperator that = (LogicalVersionOperator) o;
        return versionRefType == that.versionRefType;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(super.hashCode(), versionRefType);
    }
}
