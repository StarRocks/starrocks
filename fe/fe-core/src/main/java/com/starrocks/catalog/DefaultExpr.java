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
import com.starrocks.sql.ast.expression.ArrayExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprToSql;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.FunctionParams;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.MapExpr;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DefaultExpr {
    private static final Logger LOG = LogManager.getLogger(DefaultExpr.class);
    @SerializedName("expr")
    private String expr;
    private boolean hasArguments;
    
    // For complex types (ARRAY, MAP, STRUCT), store the actual expression
    private Expr complexExpr;

    public DefaultExpr(String expr, boolean hasArguments) {
        this.expr = expr;
        this.hasArguments = hasArguments;
        this.complexExpr = null;
    }
    
    // Constructor for complex type expressions
    public DefaultExpr(Expr expr, boolean hasArguments) {
        this.complexExpr = expr;
        this.expr = ExprToSql.toSql(expr);
        this.hasArguments = hasArguments;
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
        return isValidDefaultTimeFunction(expr.getExpr()) && !expr.hasArgs();
    }

    public Expr obtainExpr() {
        // If this is a complex type expression, return it directly
        if (complexExpr != null) {
            return complexExpr;
        }
        
        if (isValidDefaultFunction(expr)) {
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
                    new FunctionCallExpr(new FunctionName(functionName), new FunctionParams(false, exprs));
            Function fn = ExprUtils.getBuiltinFunction(functionName, argumentTypes, Function.CompareMode.IS_IDENTICAL);
            functionCallExpr.setFn(fn);
            functionCallExpr.setType(fn.getReturnType());
            return functionCallExpr;
        }
        return null;
    }
    
    public boolean isComplexTypeExpr() {
        return complexExpr != null && 
               (complexExpr instanceof ArrayExpr || 
                complexExpr instanceof MapExpr || 
                (complexExpr instanceof FunctionCallExpr && 
                 (((FunctionCallExpr) complexExpr).getFnName().getFunction().equalsIgnoreCase("row") ||
                  ((FunctionCallExpr) complexExpr).getFnName().getFunction().equalsIgnoreCase("named_struct"))));
    }
}
