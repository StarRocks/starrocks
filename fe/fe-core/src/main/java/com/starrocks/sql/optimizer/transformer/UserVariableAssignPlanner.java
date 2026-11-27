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

package com.starrocks.sql.optimizer.transformer;

import com.google.common.collect.Lists;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzeState;
import com.starrocks.sql.analyzer.ExpressionAnalyzer;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.ResolvedField;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.analyzer.UserVariableAssignments;
import com.starrocks.sql.ast.OrderByElement;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.expression.AnalyticExpr;
import com.starrocks.sql.ast.expression.AnalyticWindow;
import com.starrocks.sql.ast.expression.ArithmeticExpr;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.sql.ast.expression.CaseExpr;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FloatLiteral;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.IsNullPredicate;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.Subquery;
import com.starrocks.sql.ast.expression.UserVariableAssignExpr;
import com.starrocks.sql.ast.expression.UserVariableExpr;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.type.BooleanType;
import com.starrocks.type.FloatType;
import com.starrocks.type.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Plans a select list that assigns user variables with {@code :=}, for example the MySQL idiom for numbering the
 * rows of each group:
 * <pre>
 * SELECT CASE dept WHEN @dept THEN @n := @n + 1 ELSE @n := 1 AND @dept := dept END AS n, t.*
 * FROM t, (SELECT @n := 0, @dept := '') init
 * ORDER BY dept, salary
 * </pre>
 * MySQL evaluates such a select list one row after the other and carries the variables from row to row. StarRocks
 * has no execution mode that does this, so the row loop is expressed with window functions instead.
 * <p>
 * The select list is first evaluated symbolically for a single row. That yields, for every variable, how the row
 * changes it relative to the value the previous row left behind: keep it, add something to it, assign a value that
 * does not depend on it, or one of those depending on a condition. A variable that only ever changes in these ways
 * is a running sum that restarts whenever it is assigned, which window functions can compute:
 * <ul>
 * <li>ROW_NUMBER() fixes the order in which the rows are evaluated,</li>
 * <li>a running count of the assignments numbers the stretches of rows between two assignments,</li>
 * <li>within a stretch the variable is the assigned value plus the running sum of what was added since,</li>
 * <li>LAG() of that is the value a row starts with.</li>
 * </ul>
 * Anything else, say {@code @v := @v * 2}, cannot be written this way and is rejected.
 */
class UserVariableAssignPlanner {
    /**
     * What a row does to a variable, relative to the value the previous row left behind.
     */
    private interface Update {
    }

    private record Keep() implements Update {
    }

    private record Add(Expr delta) implements Update {
    }

    private record Assign(Expr value) implements Update {
    }

    private record Branch(Expr condition, Update whenTrue, Update otherwise) implements Update {
    }

    private record State(Map<String, Update> updates, Map<String, Expr> values) {
    }

    private static final String NUMERIC_PREFIX = "^\\s*([-+]?(?:\\d+\\.?\\d*|\\.\\d+)(?:[eE][-+]?\\d+)?)";

    private final QueryTransformer transformer;
    private final ConnectContext session;
    private final SelectRelation queryBlock;
    private final UserVariableAssignments assignments;
    private final AnalyzeState analyzeState = new AnalyzeState();

    // Stand for the value the previous row left in each variable.
    private final Map<String, UserVariableExpr> previousValues = new HashMap<>();
    // What the row has done to each variable so far, and what reading the variable returns at this point.
    private Map<String, Update> updates = new HashMap<>();
    private Map<String, Expr> values = new HashMap<>();

    private OptExprBuilder builder;
    private Scope sourceScope;
    private AnalyticExpr rowNumber;

    UserVariableAssignPlanner(QueryTransformer transformer, ConnectContext session, SelectRelation queryBlock) {
        this.transformer = transformer;
        this.session = session;
        this.queryBlock = queryBlock;
        this.assignments = queryBlock.getUserVariableAssignments();
        this.analyzeState.setUserVariableAssignments(assignments);
    }

    /**
     * Adds the window functions on top of the FROM and WHERE plan, and returns the select list rewritten to read
     * their results. The rewritten expressions take the place of the original ones in the rest of the plan.
     */
    List<Expr> plan(OptExprBuilder subOpt) {
        builder = subOpt;
        sourceScope = subOpt.getScope();
        for (String name : assignments.getPerRowVariables()) {
            UserVariableExpr previous = new UserVariableExpr(name, NodePosition.ZERO, true);
            previous.setType(assignments.getType(name));
            previousValues.put(name, previous);
            updates.put(name, new Keep());
            values.put(name, previous);
        }

        List<Expr> outputExpressions = new ArrayList<>();
        for (Expr output : queryBlock.getOutputExpression()) {
            if (assignments.isEvaluatedPerRow(output) && !UserVariableAssignments.find(output, Subquery.class).isEmpty()) {
                throw new SemanticException("A select item that reads or assigns a user variable assigned with ':=' " +
                        "cannot contain a subquery", output.getPos());
            }
            outputExpressions.add(evaluate(output));
        }
        outputExpressions.forEach(output -> planPreviousValues(output, new LinkedHashSet<>()));
        return outputExpressions;
    }

    OptExprBuilder getBuilder() {
        return builder;
    }

    private Expr evaluate(Expr expr) {
        if (!assignments.isEvaluatedPerRow(expr)) {
            return expr;
        }
        if (expr instanceof UserVariableExpr read) {
            return values.get(read.getName());
        }
        if (expr instanceof UserVariableAssignExpr assign) {
            return evaluateAssign(assign);
        }
        if (isTruthValueOfString(expr)) {
            return truthValue(evaluate(expr.getChild(0)));
        }

        // MySQL does not evaluate the branch of a CASE or IF that is not taken, nor the right side of an AND or OR
        // whose left side decides the result. That matters as soon as the part that may be skipped assigns.
        List<Expr> children = expr.getChildren();
        if (expr instanceof CaseExpr caseExpr) {
            int first = caseExpr.hasCaseExpr() ? 2 : 1;
            if (assigns(children.subList(first, children.size()))) {
                return evaluateCase(caseExpr);
            }
        } else if (expr instanceof CompoundPredicate predicate) {
            if (children.size() == 2 && assigns(children.subList(1, 2))) {
                return evaluateAndOr(predicate);
            }
        } else if (expr instanceof FunctionCallExpr call && assigns(children.subList(Math.min(1, children.size()),
                children.size()))) {
            String function = call.getFunctionName();
            if (FunctionSet.IF.equalsIgnoreCase(function)) {
                return evaluateIf(call);
            } else if (FunctionSet.IFNULL.equalsIgnoreCase(function) || FunctionSet.COALESCE.equalsIgnoreCase(function)) {
                throw new SemanticException("User variable assignment with ':=' is not supported in the arguments of " +
                        function.toUpperCase(), expr.getPos());
            }
        }

        List<Expr> evaluated = new ArrayList<>();
        children.forEach(child -> evaluated.add(evaluate(child)));
        return withChildren(expr, evaluated);
    }

    private boolean assigns(List<Expr> exprs) {
        return exprs.stream().anyMatch(expr -> !UserVariableAssignments.find(expr, UserVariableAssignExpr.class).isEmpty());
    }

    private static Expr withChildren(Expr expr, List<Expr> children) {
        Expr copy = expr.clone();
        for (int i = 0; i < children.size(); i++) {
            copy.setChild(i, children.get(i));
        }
        return copy;
    }

    private Expr evaluateAssign(UserVariableAssignExpr assign) {
        String name = assign.getName();
        Expr value = evaluate(assign.getValue());
        // Classify before the assignment takes effect, while values still holds what the value expression read.
        updates.put(name, updateOf(name, value));
        values.put(name, castTo(value, assignments.getType(name)));
        return values.get(name);
    }

    /**
     * Describes the assignment of value to the variable as an update of the variable.
     */
    private Update updateOf(String name, Expr value) {
        // What the value expression got when it read the variable.
        Expr current = values.get(name);
        if (!refersTo(value, current)) {
            return assign(name, value);
        }
        while (value != current && value instanceof CastExpr cast && cast.isImplicit()) {
            value = cast.getChild(0);
        }
        if (value == current) {
            return updates.get(name);
        }
        // Something added to an expression that is itself an update, as in "@v + a - b" or "IF(c, @v, 0) + a".
        if (value instanceof ArithmeticExpr arithmetic && arithmetic.getChildren().size() == 2) {
            Expr left = arithmetic.getChild(0);
            Expr right = arithmetic.getChild(1);
            if (arithmetic.getOp() == ArithmeticExpr.Operator.ADD && !refersTo(right, current)) {
                return add(name, updateOf(name, left), right);
            } else if (arithmetic.getOp() == ArithmeticExpr.Operator.ADD && !refersTo(left, current)) {
                return add(name, updateOf(name, right), left);
            } else if (arithmetic.getOp() == ArithmeticExpr.Operator.SUBTRACT && !refersTo(right, current)) {
                Expr negated = analyze(new ArithmeticExpr(ArithmeticExpr.Operator.SUBTRACT, new IntLiteral(0), right));
                return add(name, updateOf(name, left), negated);
            }
        }
        if (value instanceof FunctionCallExpr call && FunctionSet.IF.equalsIgnoreCase(call.getFunctionName())) {
            return choice(name, call.getChild(0), updateOf(name, call.getChild(1)), updateOf(name, call.getChild(2)));
        }
        if (value instanceof CaseExpr caseExpr) {
            return updateOfCase(name, caseExpr, caseExpr.hasCaseExpr() ? 1 : 0);
        }
        if (value instanceof FunctionCallExpr call && (FunctionSet.IFNULL.equalsIgnoreCase(call.getFunctionName())
                || FunctionSet.COALESCE.equalsIgnoreCase(call.getFunctionName()))) {
            return updateOfCoalesce(name, call, 0);
        }
        throw unsupported(name, value);
    }

    private static boolean refersTo(Expr expr, Expr target) {
        return expr == target || expr.getChildren().stream().anyMatch(child -> refersTo(child, target));
    }

    private Update updateOfCase(String name, CaseExpr caseExpr, int index) {
        int remaining = caseExpr.getChildren().size() - index;
        if (remaining == 0) {
            return assign(name, NullLiteral.create(assignments.getType(name)));
        } else if (remaining == 1) {
            return updateOf(name, caseExpr.getChild(index));
        }
        Expr condition = caseExpr.getChild(index);
        if (caseExpr.hasCaseExpr()) {
            condition = equal(caseExpr.getChild(0), condition);
        }
        return choice(name, condition, updateOf(name, caseExpr.getChild(index + 1)),
                updateOfCase(name, caseExpr, index + 2));
    }

    private Update updateOfCoalesce(String name, FunctionCallExpr call, int index) {
        Expr argument = call.getChild(index);
        if (index == call.getChildren().size() - 1) {
            return updateOf(name, argument);
        }
        return choice(name, analyze(new IsNullPredicate(argument, true)), updateOf(name, argument),
                updateOfCoalesce(name, call, index + 1));
    }

    private Update assign(String name, Expr value) {
        if (readsPreviousValue(value, name)) {
            throw unsupported(name, value);
        }
        Type type = assignments.getType(name);
        // "IF(c, date, @v)" has the type of @v. A date would become a number there, where MySQL keeps it as it is.
        if (stripImplicitCasts(value).getType().isDateType() && !type.isStringType()) {
            throw new SemanticException("User variable @" + name + " holds a number and cannot be assigned a date. " +
                    "Initialize it with NULL or with a string", value.getPos());
        }
        return new Assign(castTo(value, type));
    }

    private Update add(String name, Update update, Expr delta) {
        if (readsPreviousValue(delta, name)) {
            throw unsupported(name, delta);
        }
        if (update instanceof Keep) {
            return new Add(delta);
        } else if (update instanceof Add added) {
            return new Add(sum(added.delta(), delta));
        } else if (update instanceof Assign assigned) {
            return new Assign(castTo(sum(assigned.value(), delta), assignments.getType(name)));
        }
        Branch branch = (Branch) update;
        return new Branch(branch.condition(), add(name, branch.whenTrue(), delta), add(name, branch.otherwise(), delta));
    }

    private Update choice(String name, Expr condition, Update whenTrue, Update otherwise) {
        if (whenTrue.equals(otherwise)) {
            return whenTrue;
        }
        if (!readsPreviousValue(condition, name)) {
            return new Branch(condition, whenTrue, otherwise);
        }
        // "Keep the variable if it already equals x, otherwise assign x" always leaves x in the variable. The
        // idiom above tracks the current group this way.
        if (whenTrue instanceof Keep && otherwise instanceof Assign assigned
                && isEqualityWithPreviousValue(condition, name, assigned.value())) {
            return otherwise;
        }
        throw unsupported(name, condition);
    }

    private boolean isEqualityWithPreviousValue(Expr condition, String name, Expr value) {
        if (!(condition instanceof BinaryPredicate predicate) || predicate.getOp() != BinaryType.EQ) {
            return false;
        }
        Expr left = stripImplicitCasts(predicate.getChild(0));
        Expr right = stripImplicitCasts(predicate.getChild(1));
        Expr previous = previousValues.get(name);
        value = stripImplicitCasts(value);
        return (left == previous && right.equals(value)) || (right == previous && left.equals(value));
    }

    private boolean readsPreviousValue(Expr expr, String name) {
        return UserVariableAssignments.find(expr, UserVariableExpr.class).contains(previousValues.get(name));
    }

    private SemanticException unsupported(String name, Expr expr) {
        return new SemanticException("Unsupported assignment to user variable @" + name + ". Its new value can be " +
                "independent of the old one, or the old one with something added, optionally depending on a " +
                "condition that does not read @" + name, expr.getPos());
    }

    private Expr evaluateCase(CaseExpr caseExpr) {
        List<Expr> children = new ArrayList<>();
        Expr operand = null;
        if (caseExpr.hasCaseExpr()) {
            operand = evaluate(caseExpr.getChild(0));
            children.add(operand);
        }
        evaluateWhenClauses(caseExpr, children.size(), operand, children);
        return withChildren(caseExpr, children);
    }

    private void evaluateWhenClauses(CaseExpr caseExpr, int index, Expr operand, List<Expr> children) {
        int remaining = caseExpr.getChildren().size() - index;
        if (remaining == 0) {
            return;
        } else if (remaining == 1) {
            children.add(evaluate(caseExpr.getChild(index)));
            return;
        }
        Expr when = evaluate(caseExpr.getChild(index));
        children.add(when);
        State before = copyState();
        children.add(evaluate(caseExpr.getChild(index + 1)));
        State afterThen = copyState();
        restoreState(before);
        evaluateWhenClauses(caseExpr, index + 2, operand, children);
        merge(operand == null ? when : equal(operand, when), afterThen);
    }

    private Expr evaluateIf(FunctionCallExpr call) {
        Expr condition = evaluate(call.getChild(0));
        State before = copyState();
        Expr whenTrue = evaluate(call.getChild(1));
        State afterTrue = copyState();
        restoreState(before);
        Expr otherwise = evaluate(call.getChild(2));
        merge(condition, afterTrue);
        return withChildren(call, List.of(condition, whenTrue, otherwise));
    }

    private Expr evaluateAndOr(CompoundPredicate predicate) {
        Expr left = evaluate(predicate.getChild(0));
        if (!(stripImplicitCasts(left) instanceof LiteralExpr literal) || literal instanceof NullLiteral) {
            throw new SemanticException("User variable assignment with ':=' on the right side of AND or OR is only " +
                    "supported when the left side is a constant", predicate.getPos());
        }
        boolean leftIsTrue = literal instanceof BoolLiteral ? ((BoolLiteral) literal).getValue()
                : literal.getDoubleValue() != 0;
        boolean skipsRight = predicate.getOp() == CompoundPredicate.Operator.AND ? !leftIsTrue : leftIsTrue;
        return skipsRight ? left : withChildren(predicate, List.of(left, evaluate(predicate.getChild(1))));
    }

    private State copyState() {
        return new State(new HashMap<>(updates), new HashMap<>(values));
    }

    private void restoreState(State state) {
        updates = new HashMap<>(state.updates());
        values = new HashMap<>(state.values());
    }

    /**
     * Combines the state the row is in when the condition holds with the current state, which is the one for when
     * it does not.
     */
    private void merge(Expr condition, State whenTrue) {
        for (String name : previousValues.keySet()) {
            Update update = choice(name, condition, whenTrue.updates().get(name), updates.get(name));
            if (update.equals(updates.get(name))) {
                continue;
            }
            updates.put(name, update);
            values.put(name, update instanceof Assign assigned ? assigned.value() : castTo(
                    ifThenElse(condition, whenTrue.values().get(name), values.get(name)), assignments.getType(name)));
        }
    }

    private static boolean isTruthValueOfString(Expr expr) {
        return expr instanceof CastExpr cast && cast.isImplicit() && cast.getType().isBoolean()
                && cast.getChild(0) instanceof UserVariableAssignExpr && cast.getChild(0).getType().isStringType();
    }

    // MySQL reads a string that is used as a truth value as the number it starts with, or as 0 if it does not start
    // with one. Casting the string to BOOLEAN gives NULL instead, while "@n := 1 AND @dept := dept" relies on
    // getting 0 or 1.
    private Expr truthValue(Expr string) {
        Expr number = new FunctionCallExpr(FunctionSet.REGEXP_EXTRACT,
                Lists.newArrayList(string, new StringLiteral(NUMERIC_PREFIX), new IntLiteral(1)));
        Expr nonZero = new BinaryPredicate(BinaryType.NE, new CastExpr(FloatType.DOUBLE, number), new FloatLiteral(0.0));
        Expr truth = new FunctionCallExpr(FunctionSet.IFNULL, Lists.newArrayList(nonZero, new BoolLiteral(false)));
        return analyze(new FunctionCallExpr(FunctionSet.IF, Lists.newArrayList(new IsNullPredicate(string, false),
                NullLiteral.create(BooleanType.BOOLEAN), truth)));
    }

    private void planPreviousValues(Expr expr, Set<String> inProgress) {
        for (UserVariableExpr read : UserVariableAssignments.find(expr, UserVariableExpr.class)) {
            if (read == previousValues.get(read.getName())) {
                planPreviousValue(read.getName(), inProgress);
            }
        }
    }

    /**
     * Plans the column that holds the value the previous row left in the variable.
     */
    private void planPreviousValue(String name, Set<String> inProgress) {
        UserVariableExpr previous = previousValues.get(name);
        if (builder.getExpressionMapping().hasExpression(previous)) {
            return;
        }
        if (!inProgress.add(name)) {
            throw new SemanticException("Unsupported assignment to user variables: the new values of @" +
                    String.join(", @", inProgress) + " depend on each other's old values");
        }
        Update update = updates.get(name);
        collectInputs(update).forEach(input -> planPreviousValues(input, inProgress));

        Type type = assignments.getType(name);
        Expr initial = assignments.getInitialValue(name);
        Expr value;
        if (update instanceof Assign assigned) {
            value = assigned.value();
        } else {
            // The rows since the last assignment share the number of assignments made so far.
            List<Expr> stretch = new ArrayList<>();
            value = initial;
            if (contains(update, Assign.class)) {
                Expr assignCount = window(FunctionSet.SUM, List.of(isAssigned(update)), List.of(),
                        AnalyticWindow.DEFAULT_ROWS_WINDOW);
                Expr assignedValue = window(FunctionSet.FIRST_VALUE, List.of(assignedValue(update, type)),
                        List.of(assignCount), null);
                value = ifThenElse(equal(assignCount, new IntLiteral(0)), initial, assignedValue);
                stretch.add(assignCount);
            }
            if (contains(update, Add.class)) {
                Expr delta = delta(update);
                value = sum(value, window(FunctionSet.SUM, List.of(delta), stretch, AnalyticWindow.DEFAULT_ROWS_WINDOW));
                if (deltaMayBeNull(update)) {
                    // Adding NULL leaves NULL in the variable until the next assignment, but SUM() skips it.
                    Expr nullCount = window(FunctionSet.SUM,
                            List.of(ifThenElse(new IsNullPredicate(delta, false), new IntLiteral(1), new IntLiteral(0))),
                            stretch, AnalyticWindow.DEFAULT_ROWS_WINDOW);
                    value = ifThenElse(equal(nullCount, new IntLiteral(0)), value, NullLiteral.create(type));
                }
            }
        }

        AnalyticExpr lag = window(FunctionSet.LAG, List.of(castTo(value, type), new IntLiteral(1), initial), List.of(),
                null);
        builder.getExpressionMapping().put(previous, builder.getExpressionMapping().get(lag));
        inProgress.remove(name);
    }

    private static List<Expr> collectInputs(Update update) {
        if (update instanceof Add added) {
            return List.of(added.delta());
        } else if (update instanceof Assign assigned) {
            return List.of(assigned.value());
        } else if (update instanceof Branch branch) {
            List<Expr> inputs = Lists.newArrayList(branch.condition());
            inputs.addAll(collectInputs(branch.whenTrue()));
            inputs.addAll(collectInputs(branch.otherwise()));
            return inputs;
        }
        return List.of();
    }

    private static boolean contains(Update update, Class<? extends Update> kind) {
        if (update instanceof Branch branch) {
            return contains(branch.whenTrue(), kind) || contains(branch.otherwise(), kind);
        }
        return kind.isInstance(update);
    }

    private static boolean deltaMayBeNull(Update update) {
        if (update instanceof Branch branch) {
            return deltaMayBeNull(branch.whenTrue()) || deltaMayBeNull(branch.otherwise());
        }
        return update instanceof Add added && (!(stripImplicitCasts(added.delta()) instanceof LiteralExpr)
                || stripImplicitCasts(added.delta()) instanceof NullLiteral);
    }

    // 1 if the row assigns the variable, else 0.
    private Expr isAssigned(Update update) {
        if (update instanceof Branch branch) {
            return ifThenElse(branch.condition(), isAssigned(branch.whenTrue()), isAssigned(branch.otherwise()));
        }
        return new IntLiteral(update instanceof Assign ? 1 : 0);
    }

    private Expr assignedValue(Update update, Type type) {
        if (update instanceof Branch branch) {
            return ifThenElse(branch.condition(), assignedValue(branch.whenTrue(), type),
                    assignedValue(branch.otherwise(), type));
        }
        return update instanceof Assign assigned ? assigned.value() : NullLiteral.create(type);
    }

    private Expr delta(Update update) {
        if (update instanceof Branch branch) {
            return ifThenElse(branch.condition(), delta(branch.whenTrue()), delta(branch.otherwise()));
        }
        return update instanceof Add added ? added.delta() : new IntLiteral(0);
    }

    private AnalyticExpr window(String function, List<Expr> arguments, List<Expr> partitionBy, AnalyticWindow frame) {
        if (rowNumber == null) {
            rowNumber = planWindow(FunctionSet.ROW_NUMBER, List.of(), List.of(), rowOrder(), null);
        }
        return planWindow(function, arguments, partitionBy, List.of(new OrderByElement(rowNumber, true, true)), frame);
    }

    private AnalyticExpr planWindow(String function, List<Expr> arguments, List<Expr> partitionBy,
                                    List<OrderByElement> orderBy, AnalyticWindow frame) {
        FunctionCallExpr call = new FunctionCallExpr(function, Lists.newArrayList(arguments));
        call.setIsAnalyticFnCall(true);
        AnalyticExpr analytic = new AnalyticExpr(call, Lists.newArrayList(partitionBy), orderBy, frame, null);
        analyze(analytic);
        // Two variables that change in the same way share their window functions.
        if (!builder.getExpressionMapping().hasExpression(analytic)) {
            builder = transformer.window(builder, Lists.newArrayList(analytic));
        }
        return analytic;
    }

    /**
     * The order in which the select list is evaluated. As in MySQL, that is the ORDER BY of the query block if it
     * can be applied before the select list is computed. Otherwise the rows are taken as the FROM clause delivers
     * them, which is only a defined order for a subquery with an ORDER BY of its own.
     */
    private List<OrderByElement> rowOrder() {
        // MySQL does not evaluate the select list for the rows that OFFSET skips.
        if (queryBlock.hasLimit() && queryBlock.getLimit().getOffset() > 0) {
            throw new SemanticException("User variable assignment with ':=' is not supported together with OFFSET");
        }
        List<OrderByElement> orderBy = new ArrayList<>();
        for (OrderByElement element : queryBlock.getOrderBy()) {
            Expr key = sourceExpression(element.getExpr());
            if (key == null) {
                return subqueryOrder();
            }
            if (!key.isConstant()) {
                orderBy.add(new OrderByElement(key.clone(), element.getIsAsc(), element.getNullsFirstParam()));
            }
        }
        return orderBy.isEmpty() ? subqueryOrder() : orderBy;
    }

    // An ORDER BY expression in terms of the FROM clause, or null if it depends on the variables.
    private Expr sourceExpression(Expr orderByExpr) {
        Scope orderScope = queryBlock.getOrderScope();
        if (queryBlock.getOutputExpression().contains(orderByExpr)) {
            return sortKeyOf(orderByExpr);
        } else if (orderByExpr instanceof SlotRef slot) {
            Optional<ResolvedField> field = orderScope.tryResolveField(slot);
            boolean isSelectItem = field.isPresent() && field.get().getScope() == orderScope;
            return isSelectItem ? sortKeyOf(field.get().getField().getOriginExpression()) : orderByExpr;
        }

        List<Expr> children = new ArrayList<>();
        for (Expr child : orderByExpr.getChildren()) {
            Expr key = sourceExpression(child);
            if (key == null) {
                return null;
            }
            children.add(key);
        }
        return children.isEmpty() ? orderByExpr : withChildren(orderByExpr, children);
    }

    // What sorting by a select item means before the select list has been computed.
    private Expr sortKeyOf(Expr selectItem) {
        // Sorting by "@v := expr" is sorting by expr.
        if (selectItem instanceof UserVariableAssignExpr assign) {
            selectItem = castTo(assign.getValue(), assign.getType());
        }
        return selectItem == null || assignments.isEvaluatedPerRow(selectItem) ? null : selectItem;
    }

    private List<OrderByElement> subqueryOrder() {
        List<OrderByElement> orderBy = new ArrayList<>();
        SubqueryRelation subquery = UserVariableAssignments.rowSource(queryBlock.getRelation());
        if (subquery == null) {
            return orderBy;
        }
        QueryRelation query = subquery.getQueryStatement().getQueryRelation();
        if (!query.hasOrderByClause()) {
            return orderBy;
        }
        for (OrderByElement element : query.getOrderBy()) {
            int column = outputColumn(query, element.getExpr());
            if (column < 0) {
                throw new SemanticException("A subquery whose ORDER BY decides the order in which user variables " +
                        "are assigned must return the expressions it is sorted by", element.getExpr().getPos());
            }
            Field field = subquery.getRelationFields().getFieldByIndex(column);
            orderBy.add(new OrderByElement(new SlotRef(field.getRelationAlias(), field.getName()), element.getIsAsc(),
                    element.getNullsFirstParam()));
        }
        return orderBy;
    }

    // The position of the output column of a query that an ORDER BY expression of that query sorts by, or -1.
    private static int outputColumn(QueryRelation query, Expr orderByExpr) {
        int column = query.getOutputExpression().indexOf(orderByExpr);
        if (column >= 0 || !(orderByExpr instanceof SlotRef slot)) {
            return column;
        }
        List<String> names = query.getColumnOutputNames();
        for (int i = 0; i < names.size(); i++) {
            if (names.get(i).equalsIgnoreCase(slot.getColumnName())) {
                if (column >= 0) {
                    return -1;
                }
                column = i;
            }
        }
        return column;
    }

    private <T extends Expr> T analyze(T expr) {
        ExpressionAnalyzer.analyzeExpression(expr, analyzeState, sourceScope, session);
        return expr;
    }

    private Expr ifThenElse(Expr condition, Expr whenTrue, Expr otherwise) {
        return analyze(new FunctionCallExpr(FunctionSet.IF, Lists.newArrayList(condition, whenTrue, otherwise)));
    }

    private Expr equal(Expr left, Expr right) {
        return analyze(new BinaryPredicate(BinaryType.EQ, left, right));
    }

    private Expr sum(Expr left, Expr right) {
        return analyze(new ArithmeticExpr(ArithmeticExpr.Operator.ADD, left, right));
    }

    private static Expr castTo(Expr expr, Type type) {
        // Strings only differ in their length, which a variable does not have.
        return expr.getType().isStringType() && type.isStringType() ? expr : TypeManager.addCastExpr(expr, type);
    }

    private static Expr stripImplicitCasts(Expr expr) {
        while (expr instanceof CastExpr cast && cast.isImplicit()) {
            expr = cast.getChild(0);
        }
        return expr;
    }
}
