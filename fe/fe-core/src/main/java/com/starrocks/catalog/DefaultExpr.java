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
import com.starrocks.sql.ast.expression.ArrayExpr;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprToSql;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.FunctionParams;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.MapExpr;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DefaultExpr implements GsonPreProcessable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(DefaultExpr.class);

    @SerializedName("expr")
    private String expr;

    private final boolean hasArguments;

    @SerializedName("serializedExpr")
    private String serializedExpr;

    private Expr exprObject;

    public DefaultExpr(String expr, boolean hasArguments) {
        this.expr = expr;
        this.hasArguments = hasArguments;
        this.exprObject = null;
        this.serializedExpr = null;
    }

    public DefaultExpr(Expr exprObject) {
        this.expr = null;
        this.hasArguments = false;
        this.exprObject = exprObject;
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

    public boolean hasExprObject() {
        return exprObject != null || serializedExpr != null;
    }

    public Expr getExprObject() {
        return exprObject;
    }

    public void setExprObject(Expr exprObject) {
        this.exprObject = exprObject;
    }

    @Override
    public void gsonPreProcess() throws IOException {
        if (exprObject != null) {
            serializedExpr = ExprToSql.toSql(exprObject);
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
        if (serializedExpr != null && !serializedExpr.isEmpty()) {
            try {
                exprObject = SqlParser.parseSqlToExpr(serializedExpr, SqlModeHelper.MODE_DEFAULT);
            } catch (Exception e) {
                LOG.warn("Failed to restore expr object from SQL: {}", serializedExpr, e);
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
        return expr.getExprObject() == null && expr.getExpr() != null &&
                isValidDefaultTimeFunction(expr.getExpr()) && !expr.hasArgs();
    }

    public Expr obtainExpr() {
        if (exprObject != null) {
            return exprObject;
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

    private static Expr removeAllCastExprs(Expr expr) {
        if (expr instanceof CastExpr) {
            Expr unwrapped = expr.getChild(0);
            return removeAllCastExprs(unwrapped);
        }

        if (expr instanceof MapExpr mapExpr) {
            boolean anyChanged = false;
            List<Expr> newChildren = new ArrayList<>();
            for (Expr child : mapExpr.getChildren()) {
                Expr newChild = removeAllCastExprs(child);
                newChildren.add(newChild);
                if (newChild != child) {
                    anyChanged = true;
                }
            }
            if (!anyChanged) {
                return mapExpr;
            }
            return new MapExpr(mapExpr.getType(), newChildren, mapExpr.getPos());
        }

        if (expr instanceof ArrayExpr arrayExpr) {
            boolean anyChanged = false;
            List<Expr> newChildren = new ArrayList<>();
            for (Expr child : arrayExpr.getChildren()) {
                Expr newChild = removeAllCastExprs(child);
                newChildren.add(newChild);
                if (newChild != child) {
                    anyChanged = true;
                }
            }
            if (!anyChanged) {
                return arrayExpr;
            }
            return new ArrayExpr(arrayExpr.getType(), newChildren, arrayExpr.getPos());
        }

        if (expr instanceof FunctionCallExpr funcExpr) {
            String funcName = funcExpr.getFunctionName();
            if ("row".equalsIgnoreCase(funcName)) {
                boolean anyChanged = false;
                List<Expr> newChildren = new ArrayList<>();
                for (Expr child : funcExpr.getChildren()) {
                    Expr newChild = removeAllCastExprs(child);
                    newChildren.add(newChild);
                    if (newChild != child) {
                        anyChanged = true;
                    }
                }
                if (!anyChanged) {
                    return funcExpr;
                }
                FunctionCallExpr newFuncExpr = new FunctionCallExpr(funcName, newChildren);
                newFuncExpr.setType(funcExpr.getType());
                newFuncExpr.setFn(funcExpr.getFn());
                return newFuncExpr;
            }
        }

        return expr;
    }

    public String toSql() {
        if (exprObject != null) {
            Expr exprWithoutCast = removeAllCastExprs(exprObject);
            return ExprToSql.toSql(exprWithoutCast);
        }
        if (serializedExpr != null) {
            return serializedExpr;
        }
        return expr;
    }
}