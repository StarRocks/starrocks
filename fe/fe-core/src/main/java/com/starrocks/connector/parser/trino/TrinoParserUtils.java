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

package com.starrocks.connector.parser.trino;

import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.common.util.DateUtils;
import com.starrocks.sql.ast.StatementBase;
import io.trino.sql.tree.Explain;
import io.trino.sql.tree.ExplainAnalyze;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.Statement;

import java.time.format.DateTimeParseException;
import java.util.HashSet;
import java.util.Set;

public class TrinoParserUtils {
    public static StatementBase toStatement(String query, long sqlMode) {
        String trimmedQuery = query.trim();
        Statement statement = TrinoParser.parse(trimmedQuery);
        if (statement instanceof Query || statement instanceof Explain || statement instanceof ExplainAnalyze) {
            return (StatementBase) statement.accept(new AstBuilder(sqlMode), new ParseTreeContext());
        } else {
            throw new UnsupportedOperationException("Unsupported statement type: " + statement.getClass().getName());
        }
    }

    /**
     * Align with Trino's type conversion rules for Dates/Timestamps. For Trino, if the input type is Date,
     * then the output type must also be Date:
     * date '2012-08-08' + interval '2' day -> 2012-08-10;
     * If the input type is Timestamp, then the output type should be Timestamp, as well:
     * timestamp '2012-10-31 01:00' + interval '1' month -> 2012-11-30 01:00:00.000
     * @param expr the input timestamp arithmetic expr which we need to determine the output type
     * @return the wrapped or original expr, after applying appropriate output type conversions
     */
    public static Expr alignWithInputDatetimeType(TimestampArithmeticExpr expr) {
        if (isDateType(expr.getChild(0))) {
            return new CastExpr(Type.DATE, expr);
        }
        return expr;
    }

    /**
     * Functions that return DATE as the result type
     */
    private static Set<String> DATE_RETURNING_FUNCTIONS = new HashSet<>();
    static {
        DATE_RETURNING_FUNCTIONS.add(FunctionSet.DATE);
        DATE_RETURNING_FUNCTIONS.add(FunctionSet.LAST_DAY);
        DATE_RETURNING_FUNCTIONS.add(FunctionSet.MAKEDATE);
        DATE_RETURNING_FUNCTIONS.add(FunctionSet.NEXT_DAY);
        DATE_RETURNING_FUNCTIONS.add(FunctionSet.PREVIOUS_DAY);
        DATE_RETURNING_FUNCTIONS.add(FunctionSet.TO_TERA_DATE);
        DATE_RETURNING_FUNCTIONS.add(FunctionSet.TO_DATE);
        DATE_RETURNING_FUNCTIONS.add(FunctionSet.CURDATE);
        DATE_RETURNING_FUNCTIONS.add(FunctionSet.CURRENT_DATE);
        DATE_RETURNING_FUNCTIONS.add(FunctionSet.DATE_SLICE);
        DATE_RETURNING_FUNCTIONS.add(FunctionSet.FROM_DAYS);
        DATE_RETURNING_FUNCTIONS.add(FunctionSet.STR2DATE);
    }
    private static boolean isDateType(Expr expr) {
        // type of expr could be Type.INVALID till now, hence we need to examine many other possible cases
        if (expr.getType().isDate()) {
            return true;
        } else if (expr instanceof FunctionCallExpr) {
            return DATE_RETURNING_FUNCTIONS.contains(((FunctionCallExpr) expr).getFnName().getFunction().toLowerCase());
        } else if (expr instanceof CastExpr) {
            return ((CastExpr) expr).getTargetTypeDef().getType().isDate();
        } else if (expr instanceof StringLiteral) {
            // Support the format: select '2023-12-10' - interval '1' day
            String literal = ((StringLiteral) expr).getStringValue();
            try {
                DateUtils.DATE_FORMATTER_UNIX.parse(literal);
            } catch (DateTimeParseException e) {
                return false;
            }
            return true;
        }
        return false;
    }
}
