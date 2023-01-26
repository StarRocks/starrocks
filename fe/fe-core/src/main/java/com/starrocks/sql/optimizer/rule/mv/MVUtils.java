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


package com.starrocks.sql.optimizer.rule.mv;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.CaseExpr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;

import java.util.List;

public class MVUtils {
    public static boolean isEquivalencePredicate(ScalarOperator predicate) {
        if (predicate instanceof InPredicateOperator) {
            return true;
        }
        if (predicate instanceof BinaryPredicateOperator) {
            BinaryPredicateOperator binary = (BinaryPredicateOperator) predicate;
            return binary.getBinaryType().isEquivalence();
        }
        return false;
    }

    public static boolean isPredicateUsedForPrefixIndex(ScalarOperator predicate) {
        if (!(predicate instanceof InPredicateOperator)
                && !(predicate instanceof BinaryPredicateOperator)) {
            return false;
        }
        if (predicate instanceof InPredicateOperator) {
            return isInPredicateUsedForPrefixIndex((InPredicateOperator) predicate);
        } else {
            return isBinaryPredicateUsedForPrefixIndex((BinaryPredicateOperator) predicate);
        }
    }

    private static boolean isInPredicateUsedForPrefixIndex(InPredicateOperator predicate) {
        if (predicate.isNotIn()) {
            return false;
        }
        return isColumnRefNested(predicate.getChild(0)) && predicate.allValuesMatch(ScalarOperator::isConstant);
    }

    private static boolean isBinaryPredicateUsedForPrefixIndex(BinaryPredicateOperator predicate) {
        if (predicate.getBinaryType().isNotEqual()) {
            return false;
        }
        return (isColumnRefNested(predicate.getChild(0)) && predicate.getChild(1).isConstant())
                || (isColumnRefNested(predicate.getChild(1)) && predicate.getChild(0).isConstant());
    }

    private static boolean isColumnRefNested(ScalarOperator operator) {
        while (operator instanceof CastOperator) {
            operator = operator.getChild(0);
        }
        return operator.isColumnRef();
    }

    public static String getMVColumnName(Column mvColumn, String functionName, String queryColumn) {
        // Support count(column) MV
        // The origin MV design is bad !!!
        if (mvColumn.getDefineExpr() instanceof CaseExpr && functionName.equals(FunctionSet.COUNT)) {
            return "mv_" + FunctionSet.COUNT + "_" + queryColumn;
        }
        return "mv_" + mvColumn.getAggregationType().name().toLowerCase() + "_" + queryColumn;
    }

    /**
     * Build the maintenance plan for MV
     * NOTE: it's just a workaround, later we could persist the execution plan directly to avoid changed plan
     */
    public static ExecPlan buildPlanForMV(String viewDefinitionSql, ConnectContext session) {
        // TODO(murphy) rewrite the query to scan the MV itself
        List<StatementBase> stmts = SqlParser.parse(viewDefinitionSql, session.getSessionVariable());
        Preconditions.checkState(stmts.size() == 1);
        StatementBase mvStmt = stmts.get(0);
        Preconditions.checkState(mvStmt instanceof CreateMaterializedViewStatement,
                "statement should not be " + mvStmt);
        CreateMaterializedViewStatement createMVStmt = (CreateMaterializedViewStatement) mvStmt;
        boolean enableMVRewrite = session.getSessionVariable().isEnableMaterializedViewRewrite();
        try {
            session.getSessionVariable().setEnableMaterializedViewRewrite(false);
            StatementPlanner.plan(createMVStmt, session);
        } finally {
            session.getSessionVariable().setEnableMaterializedViewRewrite(enableMVRewrite);
        }
        return Preconditions.checkNotNull(createMVStmt.getMaintenancePlan());
    }
}
