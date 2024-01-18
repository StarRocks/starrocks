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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/TimestampArithmeticExpr.java

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

import com.google.common.base.Preconditions;
import com.starrocks.analysis.ArithmeticExpr.Operator;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;

import java.util.HashMap;
import java.util.Map;

/**
 * Describes the addition and subtraction of time units from timestamps.
 * Arithmetic expressions on timestamps are syntactic sugar.
 * They are executed as function call exprs in the BE.
 */
public class TimestampArithmeticExpr extends Expr {
    private static final Map<String, TimeUnit> TIME_UNITS_MAP = new HashMap<String, TimeUnit>();

    static {
        for (TimeUnit timeUnit : TimeUnit.values()) {
            TIME_UNITS_MAP.put(timeUnit.toString(), timeUnit);
        }
    }

    // Set for function call-like arithmetic.
    private final String funcName;
    // Keep the original string passed in the c'tor to resolve
    // ambiguities with other uses of IDENT during query parsing.
    private final String timeUnitIdent;
    // Indicates an expr where the interval comes first, e.g., 'interval b year + a'.
    private final boolean intervalFirst;
    private ArithmeticExpr.Operator op;
    private TimeUnit timeUnit;

    // C'tor for function-call like arithmetic, e.g., 'date_add(a, interval b year)'.
    public TimestampArithmeticExpr(String funcName, Expr e1, Expr e2, String timeUnitIdent) {
        this(funcName, e1, e2, timeUnitIdent, NodePosition.ZERO);
    }

    public TimestampArithmeticExpr(String funcName, Expr e1, Expr e2, String timeUnitIdent, NodePosition pos) {
        super(pos);
        this.funcName = funcName.toLowerCase();
        this.timeUnitIdent = timeUnitIdent;
        this.intervalFirst = false;
        children.add(e1);
        children.add(e2);
    }

    // C'tor for non-function-call like arithmetic, e.g., 'a + interval b year'.
    // e1 always refers to the timestamp to be added/subtracted from, and e2
    // to the time value (even in the interval-first case).
    public TimestampArithmeticExpr(ArithmeticExpr.Operator op, Expr e1, Expr e2,
                                   String timeUnitIdent, boolean intervalFirst) {
        this(op, e1, e2, timeUnitIdent, intervalFirst, NodePosition.ZERO);
    }

    public TimestampArithmeticExpr(ArithmeticExpr.Operator op, Expr e1, Expr e2,
                                   String timeUnitIdent, boolean intervalFirst, NodePosition pos) {
        super(pos);
        Preconditions.checkState(op == Operator.ADD || op == Operator.SUBTRACT);
        this.funcName = null;
        this.op = op;
        this.timeUnitIdent = timeUnitIdent;
        this.intervalFirst = intervalFirst;
        children.add(e1);
        children.add(e2);
    }

    protected TimestampArithmeticExpr(TimestampArithmeticExpr other) {
        super(other);
        funcName = other.funcName;
        op = other.op;
        timeUnitIdent = other.timeUnitIdent;
        timeUnit = other.timeUnit;
        intervalFirst = other.intervalFirst;
    }

    @Override
    public Expr clone() {
        return new TimestampArithmeticExpr(this);
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.COMPUTE_FUNCTION_CALL;
        msg.setOpcode(opcode);
    }

    public ArithmeticExpr.Operator getOp() {
        return op;
    }

    public String getFuncName() {
        return funcName;
    }

    public String getTimeUnitIdent() {
        return timeUnitIdent;
    }

    public boolean isIntervalFirst() {
        return intervalFirst;
    }

    @Override
    public String toSqlImpl() {
        StringBuilder strBuilder = new StringBuilder();
        if (funcName != null) {
            if (funcName.equalsIgnoreCase("TIMESTAMPDIFF") || funcName.equalsIgnoreCase("TIMESTAMPADD")) {
                strBuilder.append(funcName).append("(");
                strBuilder.append(timeUnitIdent).append(", ");
                strBuilder.append(getChild(1).toSql()).append(", ");
                strBuilder.append(getChild(0).toSql()).append(")");
                return strBuilder.toString();
            }
            // Function-call like version.
            strBuilder.append(funcName).append("(");
            strBuilder.append(getChild(0).toSql()).append(", ");
            strBuilder.append("INTERVAL ");
            strBuilder.append(getChild(1).toSql());
            strBuilder.append(" ").append(timeUnitIdent);
            strBuilder.append(")");
            return strBuilder.toString();
        }
        if (intervalFirst) {
            // Non-function-call like version with interval as first operand.
            strBuilder.append("INTERVAL ");
            strBuilder.append(getChild(1).toSql()).append(" ");
            strBuilder.append(timeUnitIdent);
            strBuilder.append(" ").append(op.toString()).append(" ");
            strBuilder.append(getChild(0).toSql());
        } else {
            // Non-function-call like version with interval as second operand.
            strBuilder.append(getChild(0).toSql());
            strBuilder.append(" ").append(op.toString()).append(" ");
            strBuilder.append("INTERVAL ");
            strBuilder.append(getChild(1).toSql()).append(" ");
            strBuilder.append(timeUnitIdent);
        }
        return strBuilder.toString();
    }

    // Time units supported in timestamp arithmetic.
    public enum TimeUnit {
        YEAR("YEAR"),                               // YEARS
        MONTH("MONTH"),                             // MONTHS
        WEEK("WEEK"),                               // WEEKS
        DAY("DAY"),                                 // DAYS
        HOUR("HOUR"),                               // HOURS
        MINUTE("MINUTE"),                           // MINUTES
        SECOND("SECOND"),                           // SECONDS
        MICROSECOND("MICROSECOND"),                 // MICROSECONDS
        SECOND_MICROSECOND("SECOND_MICROSECOND"),   // 'SECONDS.MICROSECONDS'
        MINUTE_MICROSECOND("MINUTE_MICROSECOND"),   // 'MINUTES:SECONDS.MICROSECONDS'
        MINUTE_SECOND("MINUTE_SECOND"),             // 'MINUTES:SECONDS'
        HOUR_MICROSECOND("HOUR_MICROSECOND"),       // 'HOURS:MINUTES:SECONDS.MICROSECONDS'
        HOUR_SECOND("HOUR_SECOND"),                 // 'HOURS:MINUTES:SECONDS'
        HOUR_MINUTE("HOUR_MINUTE"),                 // 'HOURS:MINUTES'
        DAY_MICROSECOND("DAY_MICROSECOND"),         // 'DAYS HOURS:MINUTES:SECONDS.MICROSECONDS'
        DAY_SECOND("DAY_SECOND"),                   // 'DAYS HOURS:MINUTES:SECONDS'
        DAY_MINUTE("DAY_MINUTE"),                   // 'DAYS HOURS:MINUTES'
        DAY_HOUR("DAY_HOUR"),                       // 'DAYS HOURS'
        YEAR_MONTH("YEAR_MONTH");                   // 'YEARS-MONTHS'

        private final String description;

        TimeUnit(String description) {
            this.description = description;
        }

        public static TimeUnit fromName(String timeUnitStr) {
            for (TimeUnit timeUnit : TimeUnit.values()) {
                if (timeUnit.name().equalsIgnoreCase(timeUnitStr)) {
                    return timeUnit;
                }
            }
            return null;
        }

        @Override
        public String toString() {
            return description;
        }
    }

    /**
     * Below function is added by new analyzer
     */
    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitTimestampArithmeticExpr(this, context);
    }
}
