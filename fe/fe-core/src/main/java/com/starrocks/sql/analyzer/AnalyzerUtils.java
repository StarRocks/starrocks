// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.starrocks.analysis.AnalyticExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.GroupingFunctionCallExpr;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.EsTable;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.HudiTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.MysqlTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.SchemaTable;
import com.starrocks.catalog.Table;

import java.util.List;

public class AnalyzerUtils {
    public static void verifyNoAggregateFunctions(Expr expression, String clause) {
        List<FunctionCallExpr> functions = Lists.newArrayList();
        expression.collectAll((Predicate<Expr>) arg -> arg instanceof FunctionCallExpr &&
                arg.getFn() instanceof AggregateFunction, functions);
        if (!functions.isEmpty()) {
            throw new SemanticException("%s clause cannot contain aggregations", clause);
        }
    }

    public static void verifyNoWindowFunctions(Expr expression, String clause) {
        List<AnalyticExpr> functions = Lists.newArrayList();
        expression.collectAll((Predicate<Expr>) arg -> arg instanceof AnalyticExpr, functions);
        if (!functions.isEmpty()) {
            throw new SemanticException("%s clause cannot contain window function", clause);
        }
    }

    public static void verifyNoGroupingFunctions(Expr expression, String clause) {
        List<GroupingFunctionCallExpr> calls = Lists.newArrayList();
        expression.collectAll((Predicate<Expr>) arg -> arg instanceof GroupingFunctionCallExpr, calls);
        if (!calls.isEmpty()) {
            throw new SemanticException("%s clause cannot contain grouping", clause);
        }
    }

    public static boolean isAggregate(List<FunctionCallExpr> aggregates, List<Expr> groupByExpressions) {
        return !aggregates.isEmpty() || !groupByExpressions.isEmpty();
    }

    public static boolean isSupportedTable(Table table) {
        return table instanceof OlapTable || table instanceof HiveTable || table instanceof SchemaTable ||
                table instanceof MysqlTable || table instanceof EsTable || table instanceof IcebergTable ||
                table instanceof HudiTable;
    }
}
