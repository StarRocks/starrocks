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

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonPreProcessable;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprToSql;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.FunctionParams;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.starrocks.sql.analyzer.ColumnDefAnalyzer.validateComplexTypeDefaultValue;

public class DefaultExpr implements GsonPreProcessable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(DefaultExpr.class);
    
    @SerializedName("expr")
    private String expr;
    
    private boolean hasArguments;
    
    @SerializedName("complexExprSql")
    private String complexExprSql;
    
    private Expr complexExpr;

    public DefaultExpr(String expr, boolean hasArguments) {
        this.expr = expr;
        this.hasArguments = hasArguments;
        this.complexExpr = null;
        this.complexExprSql = null;
    }
    
    public DefaultExpr(Expr complexExpr) {
        this.expr = null;
        this.hasArguments = false;
        this.complexExpr = complexExpr;
    }

    public String getExpr() {
        return expr;
    }

    public void setExpr(String expr) {
        this.expr = expr;
    }

    public boolean hasArgs() {
        return hasArguments;
    }
    
    public boolean isComplexExpr() {
        return complexExpr != null || complexExprSql != null;
    }
    
    public Expr getComplexExpr() {
        return complexExpr;
    }
    
    public void setComplexExpr(Expr complexExpr) {
        this.complexExpr = complexExpr;
    }
    
    /**
     * Called before serialization (write to metadata)
     * Convert runtime Expr object to SQL string for persistence
     * Note: We preserve CastExpr to maintain type conversion information
     */
    @Override
    public void gsonPreProcess() throws IOException {
        if (complexExpr != null) {
            // Serialize the complete expression including CastExpr if present
            // This preserves type conversion information in metadata
            complexExprSql = ExprToSql.toSql(complexExpr);
            LOG.debug("DefaultExpr.gsonPreProcess: serialized complex expr to SQL: {}", complexExprSql);
        }
    }
    
    /**
     * Called after deserialization (read from metadata)
     * Restore Expr object from SQL string
     * Similar to Column.gsonPostProcess() for generatedColumnExpr
     */
    @Override
    public void gsonPostProcess() throws IOException {
        if (complexExprSql != null && !complexExprSql.isEmpty()) {
            try {
                // Parse SQL string back to runtime Expr object
                complexExpr = SqlParser.parseSqlToExpr(complexExprSql, SqlModeHelper.MODE_DEFAULT);
                LOG.debug("DefaultExpr.gsonPostProcess: restored complex expr from SQL: {}", complexExprSql);
            } catch (Exception e) {
                LOG.warn("Failed to restore complex default expression from SQL: {}", complexExprSql, e);
                // Keep complexExprSql for error reporting, complexExpr will be null
            }
        }
    }

    public static boolean isValidDefaultFunction(String expr) {
        String[] defaultfunctions = {
            "current_timestamp\\([0-6]?\\)",
            "now\\([0-6]?\\)",
            "uuid\\(\\)",
            "uuid_numeric\\(\\)"
        };

        String combinedPattern = String.format("^(%s)$", String.join("|", defaultfunctions));
        Pattern pattern = Pattern.compile(combinedPattern, Pattern.CASE_INSENSITIVE);

        Matcher matcher = pattern.matcher(expr.trim());
        return matcher.matches();
    }

    public static boolean isValidDefaultTimeFunction(String expr) {
        String[] defaultfunctions = {
            "current_timestamp\\([0-6]?\\)",
            "now\\([0-6]?\\)"
        };

        String combinedPattern = String.format("^(%s)$", String.join("|", defaultfunctions));
        Pattern pattern = Pattern.compile(combinedPattern, Pattern.CASE_INSENSITIVE);

        Matcher matcher = pattern.matcher(expr.trim());
        return matcher.matches();
    }

    public static boolean isEmptyDefaultTimeFunction(DefaultExpr expr) {
        return !expr.isComplexExpr() && isValidDefaultTimeFunction(expr.getExpr()) && !expr.hasArgs();
    }

    public Expr obtainExpr() {
        // If it's a complex expression, return it directly
        if (complexExpr != null) {
            return complexExpr;
        }
        
        // Legacy: handle simple function expressions like now()
        if (expr != null && isValidDefaultFunction(expr)) {
            String functionName = expr.replaceAll("\\(.*\\)", "");

            Pattern pattern = Pattern.compile("\\(([^)]+)\\)");
            Matcher matcher = pattern.matcher(expr);
            String parameter = null;

            if (matcher.find()) {
                parameter = matcher.group(1);
            }
            List<Expr> exprs = Lists.newArrayList();
            Type[] argumentTypes = new Type[] {};
            if (parameter != null) {
                exprs.add(new IntLiteral(Long.parseLong(parameter), IntegerType.INT));
                argumentTypes = exprs.stream().map(Expr::getType).toArray(Type[]::new);
            }
            FunctionCallExpr functionCallExpr =
                    new FunctionCallExpr(functionName, new FunctionParams(false, exprs));
            Function fn = ExprUtils.getBuiltinFunction(functionName, argumentTypes, Function.CompareMode.IS_IDENTICAL);
            functionCallExpr.setFn(fn);
            functionCallExpr.setType(fn.getReturnType());
            return functionCallExpr;
        }
        return null;
    }
    
    /**
     * Get the SQL representation of the default expression
     */
    public String toSql() {
        if (complexExpr != null) {
            return ExprToSql.toSql(complexExpr);
        }
        if (complexExprSql != null) {
            return complexExprSql;
        }
        return expr;
    }
}
