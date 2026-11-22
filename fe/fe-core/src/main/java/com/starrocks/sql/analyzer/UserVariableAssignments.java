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

package com.starrocks.sql.analyzer;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.OrderByElement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.UserVariable;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.Subquery;
import com.starrocks.sql.ast.expression.UserVariableAssignExpr;
import com.starrocks.sql.ast.expression.UserVariableExpr;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.NullType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarcharType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The user variable assignments ({@code @v := expr}) of one select list.
 * <p>
 * A SELECT without FROM returns a single row, so its assignments run once, while the statement is analyzed. Whatever
 * is analyzed afterwards sees the new values, like after a SET.
 * <p>
 * With a FROM clause the select list runs once per row and a variable carries its value from one row to the next.
 * Such a block is planned with window functions (see UserVariableAssignPlanner), which needs every variable to keep
 * a single type. The type is derived here from the initial value and from all the values the block assigns.
 */
public class UserVariableAssignments {
    private static final int MAX_TYPE_INFERENCE_ROUNDS = 16;

    private final ConnectContext session;
    private final boolean perRow;
    // The variables assigned once per row. The type stays null until it has been inferred.
    private final Map<String, Type> types = new LinkedHashMap<>();
    private final Map<String, Expr> initialValues = new LinkedHashMap<>();
    private boolean typesChanged;

    private UserVariableAssignments(ConnectContext session, boolean perRow) {
        this.session = session;
        this.perRow = perRow;
    }

    /**
     * Returns null for a select list that does not assign user variables.
     */
    public static UserVariableAssignments of(SelectList selectList, Relation from, ConnectContext session) {
        List<UserVariableAssignExpr> assigns = collect(selectList);
        if (assigns.isEmpty()) {
            return null;
        }

        boolean hasFrom = !from.isDualRelation();
        UserVariableAssignments assignments = new UserVariableAssignments(session, hasFrom);
        if (hasFrom) {
            assigns.forEach(assign -> assignments.types.put(assign.getName(), null));
            return assignments;
        }

        // Nothing here evaluates CASE, IF, AND or OR lazily, so an assignment inside one would always run.
        for (SelectListItem item : selectList.getItems()) {
            if (item.isStar()) {
                continue;
            }
            Expr expr = item.getExpr();
            Expr inner = expr instanceof UserVariableAssignExpr ? ((UserVariableAssignExpr) expr).getValue() : expr;
            if (!find(inner, UserVariableAssignExpr.class).isEmpty()) {
                throw new SemanticException("In a SELECT without FROM, ':=' must be the outermost operator of a " +
                        "select item", expr.getPos());
            }
        }
        return assignments;
    }

    /**
     * MySQL evaluates a select list that assigns user variables in the order of its ORDER BY clause, or else in the
     * order in which the subquery in its FROM clause returns the rows. Bring these ORDER BY clauses back in case they
     * were dropped for being part of a subquery, where the order of the rows does not matter otherwise.
     */
    public static void restoreRowOrder(SelectRelation queryBlock) {
        if (queryBlock.getSelectList() == null || collect(queryBlock.getSelectList()).isEmpty()) {
            return;
        }
        queryBlock.restoreOrder();
        SubqueryRelation rowSource = rowSource(queryBlock.getRelation());
        if (rowSource != null) {
            rowSource.getQueryStatement().getQueryRelation().restoreOrder();
        }
    }

    /**
     * The subquery that a FROM clause takes its rows from, or null if it is not a single subquery. A join with a
     * SELECT without FROM, as in "FROM (SELECT ... ORDER BY ...) t, (SELECT @n := 0) init", does not change the
     * order of the rows.
     */
    public static SubqueryRelation rowSource(Relation from) {
        if (from instanceof JoinRelation join && (join.getJoinOp().isCrossJoin() || join.getJoinOp().isInnerJoin())) {
            if (isSelectWithoutFrom(join.getRight())) {
                return rowSource(join.getLeft());
            } else if (isSelectWithoutFrom(join.getLeft())) {
                return rowSource(join.getRight());
            }
        }
        return from instanceof SubqueryRelation subquery ? subquery : null;
    }

    private static boolean isSelectWithoutFrom(Relation relation) {
        return relation instanceof SubqueryRelation subquery
                && subquery.getQueryStatement().getQueryRelation() instanceof SelectRelation select
                && select.getRelation().isDualRelation();
    }

    public boolean isPerRow() {
        return perRow;
    }

    public Set<String> getPerRowVariables() {
        return types.keySet();
    }

    public boolean isAssignedPerRow(String name) {
        return types.containsKey(name);
    }

    public Type getType(String name) {
        return types.get(name);
    }

    /**
     * The value a variable assigned per row has before the first row.
     */
    public Expr getInitialValue(String name) {
        return initialValues.get(name);
    }

    /**
     * Gives every variable that is assigned per row its type. The select list must not be analyzed before that,
     * because the type of a variable decides the types of the expressions that read it.
     */
    public void inferTypes(SelectList selectList, AnalyzeState analyzeState, Scope scope) {
        if (!perRow) {
            return;
        }

        for (String name : types.keySet()) {
            Expr value = sessionValue(name);
            if (value != null) {
                types.put(name, normalize(value.getType()));
            }
        }

        List<UserVariableAssignExpr> assigns = collect(selectList);
        propagateTypes(assigns, analyzeState, scope);
        // What is left only ever depends on itself, like "@n := @n + 1" without an initial value. It starts as NULL.
        types.replaceAll((name, type) -> type == null ? NullType.NULL : type);
        propagateTypes(assigns, analyzeState, scope);
        types.replaceAll((name, type) -> type.isNull() ? VarcharType.VARCHAR : type);

        for (Map.Entry<String, Type> entry : types.entrySet()) {
            Expr value = sessionValue(entry.getKey());
            initialValues.put(entry.getKey(), value == null ? NullLiteral.create(entry.getValue())
                    : TypeManager.addCastExpr(value.clone(), entry.getValue()));
        }
    }

    private void propagateTypes(List<UserVariableAssignExpr> assigns, AnalyzeState analyzeState, Scope scope) {
        int round = 0;
        do {
            if (++round > MAX_TYPE_INFERENCE_ROUNDS) {
                throw new SemanticException("Cannot determine the types of the user variables assigned by the query");
            }
            typesChanged = false;
            for (UserVariableAssignExpr assign : assigns) {
                if (readsUntypedVariable(assign.getValue())) {
                    continue;
                }
                // Analyze a copy. The real expression is analyzed once the types are final.
                Expr value = assign.getValue().clone();
                new ExpressionAnalyzer(session).analyzeWithoutUpdateState(value, analyzeState, scope);
                widen(assign.getName(), value.getType());
            }
        } while (typesChanged);
    }

    private boolean readsUntypedVariable(Expr expr) {
        return find(expr, UserVariableExpr.class).stream()
                .anyMatch(read -> isAssignedPerRow(read.getName()) && getType(read.getName()) == null);
    }

    private void widen(String name, Type valueType) {
        Type current = types.get(name);
        Type assigned = normalize(valueType);
        Type widened = current == null ? assigned : TypeManager.getCommonSuperType(current, assigned);
        if (!widened.isValid()) {
            throw new SemanticException("User variable @%s is assigned values of the incompatible types %s and %s",
                    name, current.toSql(), assigned.toSql());
        }
        widened = normalize(widened);
        if (!widened.equals(current)) {
            types.put(name, widened);
            typesChanged = true;
        }
    }

    // MySQL only has integer, decimal, floating point and string variables. Using the widest type of each family
    // keeps a counter from overflowing the type of its initial value, and lets the inference settle quickly.
    private static Type normalize(Type type) {
        if (type.isIntegerType()) {
            return IntegerType.BIGINT;
        } else if (type.isFloat()) {
            return FloatType.DOUBLE;
        } else if (type.isDecimalV3() && !type.isDecimal256()) {
            return TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128,
                    PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL128),
                    ((ScalarType) type).getScalarScale());
        } else if (type.isStringType() || type.isDateType()) {
            return VarcharType.VARCHAR;
        }
        return type;
    }

    /**
     * Called by the expression analyzer for every assignment, after its value has been analyzed.
     */
    public void analyze(UserVariableAssignExpr assign) {
        if (perRow) {
            widen(assign.getName(), assign.getValue().getType());
            assign.setType(types.get(assign.getName()));
            return;
        }

        UserVariable variable = new UserVariable(assign.getName(), assign.getValue().clone(), assign.getPos());
        SetStmtAnalyzer.analyzeUserVariable(variable);
        SetStmtAnalyzer.calcuteUserVariable(variable);
        if (variable.getEvaluatedExpression() == null) {
            throw new SemanticException("In a SELECT without FROM, the value assigned to @" + assign.getName() +
                    " must be an expression that can be folded into a constant. Use SET for other values",
                    assign.getPos());
        }
        if (session.getUserVariablesCopyInWrite() != null) {
            session.modifyUserVariableCopyInWrite(variable);
        } else {
            session.modifyUserVariable(variable);
        }
        assign.setType(assign.getValue().getType());
    }

    /**
     * Only the select list of a plain SELECT ... FROM is evaluated row by row. Reject the clauses that would have
     * to see the variables change as well, rather than silently reading a stale value.
     */
    public void verify(AnalyzeState analyzeState, Expr where, List<OrderByElement> orderBy, boolean isAggregation) {
        if (!perRow) {
            return;
        }
        if (isAggregation || analyzeState.isDistinct()) {
            throw new SemanticException("User variable assignment with ':=' is not supported together with " +
                    "DISTINCT, GROUP BY or aggregate functions");
        }
        if (!analyzeState.getOutputAnalytic().isEmpty() || !analyzeState.getOrderByAnalytic().isEmpty()) {
            throw new SemanticException("User variable assignment with ':=' is not supported together with " +
                    "window functions");
        }
        if (where != null && isEvaluatedPerRow(where)) {
            throw new SemanticException("A user variable that the select list assigns with ':=' cannot be used in " +
                    "WHERE", where.getPos());
        }
        for (OrderByElement element : orderBy) {
            if (isEvaluatedPerRow(element.getExpr())) {
                throw new SemanticException("A user variable that the select list assigns with ':=' cannot be used " +
                        "in ORDER BY. Give the select item an alias and sort by the alias instead",
                        element.getExpr().getPos());
            }
        }
    }

    private Expr sessionValue(String name) {
        UserVariable variable = session.getUserVariablesCopyInWrite() != null
                ? session.getUserVariableCopyInWrite(name) : session.getUserVariable(name);
        if (variable == null || variable.getEvaluatedExpression() instanceof NullLiteral) {
            return null;
        }
        return variable.getEvaluatedExpression();
    }

    /**
     * True if evaluating the expression reads or assigns a variable that changes from row to row.
     */
    public boolean isEvaluatedPerRow(Expr expr) {
        return !find(expr, UserVariableAssignExpr.class).isEmpty()
                || find(expr, UserVariableExpr.class).stream().anyMatch(read -> isAssignedPerRow(read.getName()))
                || find(expr, Subquery.class).stream().anyMatch(this::readsPerRowVariable);
    }

    // A subquery is analyzed on its own. What it reads is the value from before the first row.
    private boolean readsPerRowVariable(Subquery subquery) {
        List<String> names = new ArrayList<>();
        new AstTraverser<Void, Void>() {
            @Override
            public Void visitUserVariableExpr(UserVariableExpr node, Void context) {
                names.add(node.getName());
                return null;
            }
        }.visit(subquery.getQueryStatement());
        return names.stream().anyMatch(this::isAssignedPerRow);
    }

    private static List<UserVariableAssignExpr> collect(SelectList selectList) {
        List<UserVariableAssignExpr> assigns = new ArrayList<>();
        for (SelectListItem item : selectList.getItems()) {
            if (!item.isStar()) {
                assigns.addAll(find(item.getExpr(), UserVariableAssignExpr.class));
            }
        }
        return assigns;
    }

    public static <T extends Expr> List<T> find(Expr expr, Class<T> nodeClass) {
        List<Expr> nodes = new ArrayList<>();
        expr.collectAll(nodeClass::isInstance, nodes);
        return nodes.stream().map(nodeClass::cast).collect(Collectors.toList());
    }
}
