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

import com.google.common.base.Preconditions;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.Subquery;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TResultSinkType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class UserVariable extends SetListItem {
    private final String variable;
    private Expr unevaluatedExpression;
    private Expr evaluatedExpression;

    private final boolean isFromHint;
    public UserVariable(String variable, Expr unevaluatedExpression, NodePosition pos) {
        this(variable, unevaluatedExpression, false, pos);
    }

    public UserVariable(String variable, Expr unevaluatedExpression, boolean isFromHint, NodePosition pos) {
        super(pos);
        this.variable = variable;
        this.unevaluatedExpression = unevaluatedExpression;
        if (unevaluatedExpression instanceof LiteralExpr) {
            this.evaluatedExpression = unevaluatedExpression;
        }
        this.isFromHint = isFromHint;
    }



    public String getVariable() {
        return variable;
    }

    public Expr getUnevaluatedExpression() {
        return unevaluatedExpression;
    }

    public void setUnevaluatedExpression(Expr unevaluatedExpression) {
        this.unevaluatedExpression = unevaluatedExpression;
    }

    public Expr getEvaluatedExpression() {
        return evaluatedExpression;
    }

    public void setEvaluatedExpression(Expr evaluatedExpression) {
        this.evaluatedExpression = evaluatedExpression;
    }

    public boolean isFromHint() {
        return isFromHint;
    }

    @Override
    public String toSql() {
        return AstToSQLBuilder.toSQL(unevaluatedExpression);
    }

    public void deriveUserVariableExpressionResult(ConnectContext ctx) {
        QueryStatement queryStatement = ((Subquery) unevaluatedExpression).getQueryStatement();
        ExecPlan execPlan = StatementPlanner.plan(queryStatement,
                ConnectContext.get(), TResultSinkType.MYSQL_PROTOCAL);
        StmtExecutor executor = StmtExecutor.newInternalExecutor(ctx, queryStatement);
        Pair<List<TResultBatch>, Status> sqlResult = executor.executeStmtWithExecPlan(ctx, execPlan);
        if (!sqlResult.second.ok()) {
            throw new SemanticException(sqlResult.second.getErrorMsg());
        } else {
            evaluatedExpression = decodeVariableData(sqlResult.first, unevaluatedExpression.getType());
        }

        if (ctx.getState().getStateType() == QueryState.MysqlStateType.ERR) {
            throw new SemanticException(ctx.getState().getErrorMessage());
        }
    }

    private static Expr decodeVariableData(List<TResultBatch> sqlResult, Type targetType) {
        Preconditions.checkState(sqlResult.size() == 1,
                "subquery in user variable must return at most 1 value");
        Preconditions.checkState(sqlResult.get(0).getRows().size() == 1,
                "subquery in user variable must return at most 1 value");
        ByteBuffer byteBuffer = sqlResult.get(0).getRows().get(0);
        byte[] bytes = new byte[byteBuffer.limit() - byteBuffer.position()];
        byteBuffer.get(bytes);

        int lengthOffset = getOffset(bytes);

        // process null value
        if (lengthOffset == -1) {
            return NullLiteral.create(Type.VARCHAR);
        }

        String value;
        try {
            value = new String(bytes, lengthOffset, bytes.length - lengthOffset, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new SemanticException("unable decode bytes to string. bytes offset: %s, bytes length: %s",
                    lengthOffset, bytes.length);
        }

        //JSON type will be stored as string type
        if (targetType.isJsonType()) {
            targetType = Type.VARCHAR;
        }

        if (targetType.isScalarType()) {
            try {
                return LiteralExpr.create(value, targetType);
            } catch (AnalysisException e) {
                throw new SemanticException("Unsupported string value: %s to type: %s", value, targetType);
            }
        } else if (targetType.isArrayType()) {
            //build a cast(string to array) expr
            return TypeManager.addCastExpr(new StringLiteral(removeEscapeCharacter(value)), targetType);
        } else {
            throw new SemanticException("Unsupported type: %s in user variable", targetType);
        }
    }

    /**
     * It's the same with MYSQL_PROTOCAL.
     * the value of the first byte meaning:
     * less 251 means the length of the following bytes
     * 251 means NULL
     * 252 means the length need the next 2 bytes to represent
     * 253 means the length need the next 3 bytes to represent
     * 254 means the length need the next 8 bytes to represent
     * @param bytes
     * @return
     */
    private static int getOffset(byte[] bytes) {
        int sw = bytes[0] & 0xff;
        switch (sw) {
            case 251:
                return -1;
            case 252:
                return 3;
            case 253:
                return 4;
            case 254:
                return 9;
            default:
                return 1;
        }
    }

    // remove escape character added in BE
    // like [\"a\"] -> ["a"]
    public static String removeEscapeCharacter(String strValue) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < strValue.length(); i++) {
            char c = strValue.charAt(i);
            if (c == '\\' && i < strValue.length() - 1 && (strValue.charAt(i + 1) == '"' || strValue.charAt(i + 1) == '\'')) {
                // just skip
            } else if (c == '\\' && i < strValue.length() - 1 && strValue.charAt(i + 1) == '\\') {
                i++;
                sb.append('\\');
            } else {
                sb.append(c);
            }

        }
        return sb.toString();
    }

}
