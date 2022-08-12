// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/SysVariableDesc.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.thrift.TBoolLiteral;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TFloatLiteral;
import com.starrocks.thrift.TIntLiteral;
import com.starrocks.thrift.TStringLiteral;

import java.util.Objects;

// System variable
// Converted to StringLiteral in analyze, if this variable is not exist, throw AnalysisException.
public class VariableExpr extends Expr {
    private final String name;
    private final SetType setType;
    private boolean isNull;
    private boolean boolValue;
    private long intValue;
    private double floatValue;
    private String strValue;

    public VariableExpr(String name) {
        this(name, SetType.SESSION);
    }

    public VariableExpr(String name, SetType setType) {
        this.name = name;
        this.setType = setType;
    }

    protected VariableExpr(VariableExpr other) {
        super(other);
        name = other.name;
        setType = other.setType;
        isNull = other.isNull;
        boolValue = other.boolValue;
        intValue = other.intValue;
        floatValue = other.floatValue;
        strValue = other.strValue;
    }

    @Override
    public Expr clone() {
        return new VariableExpr(this);
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    }

    public String getName() {
        return name;
    }

    public SetType getSetType() {
        return setType;
    }

    public void setIsNull() {
        isNull = true;
    }

    public boolean isNull() {
        return isNull;
    }

    public void setBoolValue(boolean value) {
        this.boolValue = value;
    }

    public boolean getBoolValue() {
        return this.boolValue;
    }

    public void setIntValue(long value) {
        this.intValue = value;
    }

    public long getIntValue() {
        return intValue;
    }

    public void setFloatValue(double value) {
        this.floatValue = value;
    }

    public double getFloatValue() {
        return floatValue;
    }

    public void setStringValue(String value) {
        this.strValue = value;
    }

    public String getStrValue() {
        return strValue;
    }

    @Override
    protected void toThrift(TExprNode msg) {
        switch (type.getPrimitiveType()) {
            case BOOLEAN:
                msg.node_type = TExprNodeType.BOOL_LITERAL;
                msg.bool_literal = new TBoolLiteral(boolValue);
                break;
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                msg.node_type = TExprNodeType.INT_LITERAL;
                msg.int_literal = new TIntLiteral(intValue);
                break;
            case FLOAT:
            case DOUBLE:
                msg.node_type = TExprNodeType.FLOAT_LITERAL;
                msg.float_literal = new TFloatLiteral(floatValue);
                break;
            default:
                msg.node_type = TExprNodeType.STRING_LITERAL;
                msg.string_literal = new TStringLiteral(strValue);
        }
    }

    @Override
    public String toSqlImpl() {
        StringBuilder sb = new StringBuilder();
        if (setType == SetType.USER) {
            sb.append("@");
        } else {
            sb.append("@@");
            if (setType == SetType.GLOBAL) {
                sb.append("GLOBAL.");
            } else {
                sb.append("SESSION.");
            }
        }
        sb.append(name);
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    /**
     * Below function is added by new analyzer
     */
    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitVariableExpr(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        VariableExpr that = (VariableExpr) o;
        return boolValue == that.boolValue && intValue == that.intValue &&
                Double.compare(that.floatValue, floatValue) == 0 && Objects.equals(name, that.name) &&
                setType == that.setType && Objects.equals(strValue, that.strValue) &&
                Objects.equals(isNull, that.isNull);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), name, setType, boolValue, intValue, floatValue, strValue, isNull);
    }
}
