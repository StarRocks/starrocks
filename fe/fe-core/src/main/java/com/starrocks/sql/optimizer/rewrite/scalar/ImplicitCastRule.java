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


package com.starrocks.sql.optimizer.rewrite.scalar;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariableConstants;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.ArithmeticExpr;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.LargeInPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.MapOperator;
import com.starrocks.sql.optimizer.operator.scalar.MultiInPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.type.BooleanType;
import com.starrocks.type.MapType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

//
// Add cast function when children's type different with parent required type
//
// example:
//        Binary(+)
//        /      \
//  a(String)   b(int)
//
// After rule:
//             Binary(+)
//             /     \
//   cast(bigint)   cast(bigint)
//          /           \
//   a(String)          b(int)
//
public class ImplicitCastRule extends TopDownScalarOperatorRewriteRule {
    @Override
    public ScalarOperator visitCall(CallOperator call, ScalarOperatorRewriteContext context) {
        Function fn = call.getFunction();
        if (fn == null) {
            for (int i = 0; i < call.getChildren().size(); ++i) {
                Type type = call.getType();
                if (!type.matchesType(call.getChild(i).getType())) {
                    addCastChild(type, call, i);
                }
            }
        } else {
            // functions with various data types do not need to implicit cast, such as following functions.
            if (fn.functionName().equals(FunctionSet.ARRAY_MAP) ||
                    fn.functionName().equals(FunctionSet.EXCHANGE_BYTES) ||
                    fn.functionName().equals(FunctionSet.EXCHANGE_SPEED) ||
                    fn.functionName().equals(FunctionSet.ARRAY_SORTBY)) {
                return call;
            }

            if (fn.functionName().equals(FunctionSet.ARRAY_SORT_LAMBDA)) {
                LambdaFunctionOperator lambdaOp = (LambdaFunctionOperator) call.getChild(1);
                ScalarOperator lambdaBodyOp = lambdaOp.getChild(0);

                Map<Boolean, List<ColumnRefOperator>> argumentGroups = lambdaBodyOp.asStream()
                        .filter(ScalarOperator::isColumnRef)
                        .map(op -> (ColumnRefOperator) op)
                        .collect(Collectors.partitioningBy(colRef -> colRef.getOpType().equals(
                                OperatorType.LAMBDA_ARGUMENT)));

                boolean isLegal = argumentGroups.get(true).stream().distinct().count() == 2 &&
                        argumentGroups.get(false).isEmpty() && !Utils.hasNonDeterministicFunc(lambdaBodyOp);

                if (!isLegal) {
                    throw new SemanticException("Lambda function in sort_array should only depend on both two" +
                            " arguments and contain no non-deterministic functions");
                }

                if (lambdaBodyOp.getType().isBoolean()) {
                    return visit(call, context);
                } else if (lambdaBodyOp.getType().isNumericType()) {
                    ScalarOperator zeroValue =
                            new CastOperator(lambdaBodyOp.getType(), ConstantOperator.createTinyInt((byte) 0));
                    ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
                    zeroValue = rewriter.rewrite(zeroValue, List.of(new FoldConstantsRule()));
                    lambdaBodyOp = new BinaryPredicateOperator(BinaryType.LT, lambdaBodyOp, zeroValue);
                } else {
                    throw new SemanticException(
                            "Return type of lambda function in array_sort must be boolean|numeric types");
                }
                lambdaOp.setChild(0, lambdaBodyOp);
                return call;
            }
            if (!call.isAggregate() || FunctionSet.AVG.equalsIgnoreCase(fn.functionName())) {
                Preconditions.checkArgument(Arrays.stream(fn.getArgs()).noneMatch(Type::isWildcardDecimal),
                        String.format("Resolved function %s has wildcard decimal as argument type", fn.functionName()));
            }

            boolean needAdjustScale = ArithmeticExpr.DECIMAL_SCALE_ADJUST_OPERATOR_SET
                    .contains(fn.getFunctionName().getFunction());
            for (int i = 0; i < fn.getNumArgs(); i++) {
                Type type = fn.getArgs()[i];
                ScalarOperator child = call.getChild(i);

                // For compatibility, decimal ArithmeticExpr(+-*/%) use Type::equals instead of Type::matchesType to
                // determine whether to cast child of the ArithmeticExpr
                if (needAdjustScale && type.isDecimalOfAnyVersion() && !type.equals(child.getType())) {
                    addCastChild(type, call, i);
                    continue;
                }

                if (!type.matchesType(child.getType())) {
                    addCastChild(type, call, i);
                }
            }

            // variable args
            if (fn.hasVarArgs() && call.getChildren().size() > fn.getNumArgs()) {
                Type type = fn.getVarArgsType();
                for (int i = fn.getNumArgs(); i < call.getChildren().size(); i++) {
                    ScalarOperator child = call.getChild(i);
                    if (!type.matchesType(child.getType())) {
                        addCastChild(type, call, i);
                    }
                }
            }
        }

        return call;
    }

    @Override
    public ScalarOperator visitBetweenPredicate(BetweenPredicateOperator predicate,
                                                ScalarOperatorRewriteContext context) {
        return castForBetweenAndIn(predicate, true);
    }

    @Override
    public ScalarOperator visitMap(MapOperator map, ScalarOperatorRewriteContext context) {
        MapType mapType = (MapType) map.getType();
        Type[] kvType = {mapType.getKeyType(), mapType.getValueType()};
        for (int i = 0; i < map.getChildren().size(); i++) {
            if (!map.getChildren().get(i).getType().matchesType(kvType[i % 2])) {
                addCastChild(kvType[i % 2], map, i);
            }
        }
        return map;
    }

    @Override
    public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator predicate,
                                               ScalarOperatorRewriteContext context) {
        ScalarOperator leftChild = predicate.getChild(0);
        ScalarOperator rightChild = predicate.getChild(1);
        Type type1 = leftChild.getType();
        Type type2 = rightChild.getType();

        // For a query like: select 'a' <=> NULL, we should cast Constant Null to the type of the other side
        if (predicate.getBinaryType() == BinaryType.EQ_FOR_NULL &&
                (leftChild.isConstantNull() || rightChild.isConstantNull())) {
            if (leftChild.isConstantNull()) {
                predicate.setChild(0, ConstantOperator.createNull(type2));
            }
            if (rightChild.isConstantNull()) {
                predicate.setChild(1, ConstantOperator.createNull(type1));
            }
            return predicate;
        }

        if (type1.matchesType(type2)) {
            return predicate;
        }

        // we will try cast const operator to variable operator
        if ((rightChild.isVariable() && leftChild.isConstantRef()) ||
                (leftChild.isVariable() && rightChild.isConstantRef())) {
            int constant = leftChild.isVariable() ? 1 : 0;
            int variable = 1 - constant;
            Optional<BinaryPredicateOperator> optional = optimizeConstantAndVariable(predicate, constant, variable);
            if (optional.isPresent()) {
                return optional.get();
            }
        }

        Type compatibleType =
                TypeManager.getCompatibleTypeForBinary(predicate.getBinaryType().isRange(), type1, type2);

        if (!type1.matchesType(compatibleType)) {
            addCastChild(compatibleType, predicate, 0);
        }
        if (!type2.matchesType(compatibleType)) {
            addCastChild(compatibleType, predicate, 1);
        }
        return predicate;
    }

    private Optional<BinaryPredicateOperator> optimizeConstantAndVariable(BinaryPredicateOperator predicate,
                                                                          int constantIndex, int variableIndex) {
        ScalarOperator constant = predicate.getChild(constantIndex);
        ScalarOperator variable = predicate.getChild(variableIndex);
        Type typeConstant = constant.getType();
        Type typeVariable = variable.getType();

        boolean checkStringCastToNumber = false;
        if (typeVariable.isNumericType() && typeConstant.isStringType()) {
            if (predicate.getBinaryType().isNotRangeComparison()) {
                String baseType = ConnectContext.get() != null ?
                        ConnectContext.get().getSessionVariable().getCboEqBaseType() : SessionVariableConstants.VARCHAR;
                checkStringCastToNumber = SessionVariableConstants.DECIMAL.equals(baseType) ||
                        SessionVariableConstants.DOUBLE.equals(baseType);
            } else {
                // range compare, base type must be double
                checkStringCastToNumber = true;
            }
        }

        // strict check, only support white check
        if ((typeVariable.isNumericType() && typeConstant.isNumericType()) ||
                (typeVariable.isNumericType() && typeConstant.isBoolean()) ||
                (typeVariable.isDateType() && typeConstant.isNumericType()) ||
                (typeVariable.isDateType() && typeConstant.isStringType()) ||
                (typeVariable.isFixedPointType() && typeConstant.isStringType() &&
                        predicate.getBinaryType() == BinaryType.EQ) ||
                (typeVariable.isBoolean() && typeConstant.isStringType()) ||
                checkStringCastToNumber) {

            // Folding the constant directly into the variable's type is an
            // implicit coercion even though no CastOperator is produced, so
            // the strict-mode guard must run here too.
            checkImplicitCastAllowed(typeConstant, typeVariable);

            Optional<ScalarOperator> op = Utils.tryCastConstant(constant, variable.getType());
            if (op.isPresent()) {
                predicate.getChildren().set(constantIndex, op.get());
                return Optional.of(predicate);
            } else if (variable.getType().isDateType() && TypeManager.canCastTo(constant.getType(), variable.getType())) {
                // For like MySQL, convert to date type as much as possible
                addCastChild(variable.getType(), predicate, constantIndex);
                return Optional.of(predicate);
            }
        }

        return Optional.empty();
    }

    @Override
    public ScalarOperator visitCompoundPredicate(CompoundPredicateOperator predicate,
                                                 ScalarOperatorRewriteContext context) {
        for (int i = 0; i < predicate.getChildren().size(); i++) {
            ScalarOperator child = predicate.getChild(i);

            if (!BooleanType.BOOLEAN.matchesType(child.getType())) {
                addCastChild(BooleanType.BOOLEAN, predicate, i);
            }
        }

        return predicate;
    }

    @Override
    public ScalarOperator visitInPredicate(InPredicateOperator predicate, ScalarOperatorRewriteContext context) {
        return castForBetweenAndIn(predicate, false);
    }

    @Override
    public ScalarOperator visitLargeInPredicate(LargeInPredicateOperator predicate, ScalarOperatorRewriteContext context) {
        return predicate;
    }

    @Override
    public ScalarOperator visitMultiInPredicate(MultiInPredicateOperator predicate, ScalarOperatorRewriteContext c) {
        throw new StarRocksPlannerException("Implicit casting of multi-column IN predicate is not supported.",
                ErrorType.INTERNAL_ERROR);
    }

    @Override
    public ScalarOperator visitLikePredicateOperator(LikePredicateOperator predicate,
                                                     ScalarOperatorRewriteContext context) {
        Type type1 = predicate.getChild(0).getType();
        Type type2 = predicate.getChild(1).getType();

        if (!type1.isStringType()) {
            addCastChild(VarcharType.VARCHAR, predicate, 0);
        }

        if (!type2.isStringType()) {
            addCastChild(VarcharType.VARCHAR, predicate, 1);
        }

        return predicate;
    }

    @Override
    public ScalarOperator visitCaseWhenOperator(CaseWhenOperator operator, ScalarOperatorRewriteContext context) {
        if (operator.hasElse() && !operator.getType().matchesType(operator.getElseClause().getType())) {
            operator.setElseClause(makeImplicitCast(operator.getType(), operator.getElseClause()));
        }

        for (int i = 0; i < operator.getWhenClauseSize(); i++) {
            if (!operator.getType().matchesType(operator.getThenClause(i).getType())) {
                operator.setThenClause(i, makeImplicitCast(operator.getType(), operator.getThenClause(i)));
            }
        }

        Type compatibleType = BooleanType.BOOLEAN;
        if (operator.hasCase()) {
            List<Type> whenTypes = Lists.newArrayList();
            whenTypes.add(operator.getCaseClause().getType());
            for (int i = 0; i < operator.getWhenClauseSize(); i++) {
                whenTypes.add(operator.getWhenClause(i).getType());
            }

            compatibleType = TypeManager.getCompatibleTypeForCaseWhen(whenTypes);

            if (!compatibleType.matchesType(operator.getCaseClause().getType())) {
                operator.setCaseClause(makeImplicitCast(compatibleType, operator.getCaseClause()));
            }
        }

        for (int i = 0; i < operator.getWhenClauseSize(); i++) {
            if (!compatibleType.matchesType(operator.getWhenClause(i).getType())) {
                operator.setWhenClause(i, makeImplicitCast(compatibleType, operator.getWhenClause(i)));
            }
        }

        return operator;
    }

    private ScalarOperator castForBetweenAndIn(ScalarOperator predicate, boolean isBetween) {
        Type firstType = predicate.getChildren().get(0).getType();
        if (predicate.getChildren().stream().skip(1).allMatch(o -> firstType.matchesType(o.getType()))) {
            return predicate;
        }

        List<Type> types = predicate.getChildren().stream().map(ScalarOperator::getType).collect(Collectors.toList());
        if (predicate.getChild(0).isVariable() && predicate.getChildren().stream().skip(1)
                .allMatch(ScalarOperator::isConstantRef)) {
            // Same strict-mode concern as optimizeConstantAndVariable: each
            // constant will be folded into firstType without producing a
            // CastOperator, so validate before we start the fold.
            for (int i = 1; i < types.size(); i++) {
                checkImplicitCastAllowed(types.get(i), firstType);
            }
            List<ScalarOperator> newChild = Lists.newArrayList();
            newChild.add(predicate.getChild(0));
            for (int i = 1; i < types.size(); i++) {
                Optional<ScalarOperator> op = Utils.tryCastConstant(predicate.getChild(i), firstType);
                op.ifPresent(newChild::add);
            }

            if (newChild.size() == predicate.getChildren().size()) {
                predicate.getChildren().clear();
                predicate.getChildren().addAll(newChild);
                return predicate;
            }
        }

        Type compatibleType = TypeManager.getCompatibleTypeForBetweenAndIn(types, isBetween);
        for (int i = 0; i < predicate.getChildren().size(); i++) {
            Type childType = predicate.getChild(i).getType();

            if (!childType.matchesType(compatibleType)) {
                addCastChild(compatibleType, predicate, i);
            }
        }
        return predicate;
    }

    private void addCastChild(Type returnType, ScalarOperator node, int index) {
        ScalarOperator child = node.getChild(index);
        checkImplicitCastAllowed(child.getType(), returnType);
        node.getChildren().set(index, new CastOperator(returnType, child, true));
    }

    private CastOperator makeImplicitCast(Type returnType, ScalarOperator child) {
        checkImplicitCastAllowed(child.getType(), returnType);
        return new CastOperator(returnType, child, true);
    }

    // When MODE_FORBID_INVALID_IMPLICIT_CAST is set, reject implicit casts that
    // Trino would not allow: cross-family casts (e.g. varchar<->int) and
    // narrowing casts (e.g. bigint->int, varchar(10)->varchar(5)). Only
    // widening coercions within the same type family pass.
    private static void checkImplicitCastAllowed(Type from, Type to) {
        if (ConnectContext.get() == null) {
            return;
        }
        if (!SqlModeHelper.check(ConnectContext.get().getSessionVariable().getSqlMode(),
                SqlModeHelper.MODE_FORBID_INVALID_IMPLICIT_CAST)) {
            return;
        }
        if (isImplicitCastSafe(from, to)) {
            return;
        }
        throw new SemanticException(String.format(
                "Implicit cast from %s to %s is not allowed under FORBID_INVALID_IMPLICIT_CAST sql_mode. "
                        + "Use an explicit CAST.",
                from, to));
    }

    private static boolean isImplicitCastSafe(Type from, Type to) {
        if (from == null || to == null) {
            return true;
        }
        if (from.isNull() || from.isInvalid() || to.isInvalid()) {
            return true;
        }
        if (from.matchesType(to)) {
            return true;
        }
        if (from.isNumericType() && to.isNumericType()) {
            return isNumericWidening(from, to);
        }
        if (from.isStringType() && to.isStringType()) {
            // VARCHAR/CHAR length is a declared maximum in StarRocks and is
            // not treated as a strict type bound, so we allow any implicit
            // cast within the string family regardless of length.
            return true;
        }
        if (from.isDateType() && to.isDateType()) {
            // DATE -> DATETIME is widening; DATETIME -> DATE is narrowing.
            return from.isDate() && to.isDatetime();
        }
        // Complex types (array/map/struct) fall back to their element-level
        // coercions, which re-enter this rule. Skip the family check here.
        if (from.isComplexType() || to.isComplexType()) {
            return true;
        }
        return false;
    }

    // Trino-like numeric widening rank:
    //   TINYINT < SMALLINT < INT < BIGINT < LARGEINT < DECIMAL < FLOAT < DOUBLE
    // A cast is widening iff fromRank <= toRank; DECIMAL -> DECIMAL additionally
    // requires non-decreasing precision and scale.
    private static boolean isNumericWidening(Type from, Type to) {
        int fromRank = numericRank(from);
        int toRank = numericRank(to);
        if (fromRank < 0 || toRank < 0) {
            return false;
        }
        if (fromRank > toRank) {
            return false;
        }
        if (from.isDecimalOfAnyVersion() && to.isDecimalOfAnyVersion()) {
            Integer fromPrecision = from.getPrecision();
            Integer toPrecision = to.getPrecision();
            int fromScale = from instanceof ScalarType ? ((ScalarType) from).getScalarScale() : 0;
            int toScale = to instanceof ScalarType ? ((ScalarType) to).getScalarScale() : 0;
            if (fromPrecision == null || toPrecision == null) {
                return true;
            }
            int fromIntegralDigits = fromPrecision - fromScale;
            int toIntegralDigits = toPrecision - toScale;
            return toScale >= fromScale && toIntegralDigits >= fromIntegralDigits;
        }
        return true;
    }

    private static int numericRank(Type t) {
        if (t.isTinyint()) {
            return 1;
        }
        if (t.isSmallint()) {
            return 2;
        }
        if (t.isInt()) {
            return 3;
        }
        if (t.isBigint()) {
            return 4;
        }
        if (t.isLargeIntType()) {
            return 5;
        }
        if (t.isDecimalOfAnyVersion()) {
            return 6;
        }
        if (t.isFloat()) {
            return 7;
        }
        if (t.isDouble()) {
            return 8;
        }
        return -1;
    }

}
