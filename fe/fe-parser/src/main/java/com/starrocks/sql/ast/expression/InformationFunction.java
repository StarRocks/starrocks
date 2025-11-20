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

package com.starrocks.sql.ast.expression;

import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;

public class InformationFunction extends Expr {
    private final String funcType;
    private long intValue;
    private String strValue;

    // First child is the comparison expr which should be in [lowerBound, upperBound].
    public InformationFunction(String funcType) {
        this(funcType, NodePosition.ZERO);
    }

    public InformationFunction(String funcType, NodePosition pos) {
        this(funcType, null, 0, pos);
    }

    public InformationFunction(String funcType, String strValue, long intValue) {
        this.funcType = funcType;
        this.strValue = strValue;
        this.intValue = intValue;
    }

    public InformationFunction(String funcType, String strValue, long intValue, NodePosition pos) {
        super(pos);
        this.funcType = funcType;
        this.strValue = strValue;
        this.intValue = intValue;
    }

    protected InformationFunction(InformationFunction other) {
        super(other);
        funcType = other.funcType;
        intValue = other.intValue;
        strValue = other.strValue;
    }

    @Override
    public Expr clone() {
        return new InformationFunction(this);
    }

    public String getFuncType() {
        return funcType;
    }

    public void setIntValue(long intValue) {
        this.intValue = intValue;
    }

    public long getIntValue() {
        return intValue;
    }

    public void setStrValue(String strValue) {
        this.strValue = strValue;
    }

    public String getStrValue() {
        return strValue;
    }


    /**
     * Below function is added by new analyzer
     */
    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitInformationFunction(this, context);
    }
}
