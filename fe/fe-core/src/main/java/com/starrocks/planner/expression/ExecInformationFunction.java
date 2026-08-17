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

import com.starrocks.catalog.FunctionSet;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TInfoFunc;
import com.starrocks.type.Type;

/**
 * Execution-plan information function (database(), user(), current_user(), etc.).
 */
public class ExecInformationFunction extends ExecExpr {
    private final String funcName;
    private final String strValue;
    private final long intValue;

    public ExecInformationFunction(Type type, String funcName, String strValue, long intValue) {
        super(type);
        this.funcName = funcName;
        this.strValue = strValue;
        this.intValue = intValue;
    }

    private ExecInformationFunction(ExecInformationFunction other) {
        super(other.type);
        this.funcName = other.funcName;
        this.strValue = other.strValue;
        this.intValue = other.intValue;
        this.originType = other.originType;
        this.isIndexOnlyFilter = other.isIndexOnlyFilter;
    }

    public String getFuncName() {
        return funcName;
    }

    public String getStrValue() {
        return strValue;
    }

    public long getIntValue() {
        return intValue;
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public boolean isConstant() {
        return true;
    }

    @Override
    public TExprNodeType getNodeType() {
        return TExprNodeType.INFO_FUNC;
    }

    @Override
    public void toThrift(TExprNode node) {
        node.info_func = new TInfoFunc(intValue, strValue);
        if (FunctionSet.CURRENT_WAREHOUSE.equalsIgnoreCase(funcName) && strValue != null) {
            node.info_func.setStr_value(strValue);
        }
    }

    @Override
    public <R, C> R accept(ExecExprVisitor<R, C> visitor, C context) {
        return visitor.visitExecInformationFunction(this, context);
    }

    @Override
    public ExecInformationFunction clone() {
        return new ExecInformationFunction(this);
    }
}
