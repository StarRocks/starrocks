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

package com.starrocks.sql.ast.expression;

import com.google.common.collect.Lists;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.SlotId;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.UnitIdentifier;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TExprOpcode;
import com.starrocks.thrift.TInPredicate;
import com.starrocks.thrift.TInfoFunc;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class ExprToThriftVisitorTest {

    @Test
    public void testExprToThriftCoverage() {
        for (ExprCase exprCase : ExprCaseFactory.ALL_CASES) {
            if (exprCase.expectedException != null) {
                Assertions.assertThrows(exprCase.expectedException,
                        () -> convert(exprCase.factory.get()),
                        () -> "Expected exception for " + exprCase.name);
                continue;
            }

            TExprNode node = Assertions.assertDoesNotThrow(
                    () -> convert(exprCase.factory.get()),
                    () -> "Conversion failed for " + exprCase.name);

            if (exprCase.expectedNodeType != null) {
                Assertions.assertEquals(exprCase.expectedNodeType, node.getNode_type(),
                        () -> "Unexpected node type for " + exprCase.name);
            }

            if (exprCase.afterCheck != null) {
                exprCase.afterCheck.accept(node);
            }
        }
    }

    private static TExprNode convert(Expr expr) {
        TExpr thrift = expr.treeToThrift();
        Assertions.assertFalse(thrift.getNodes().isEmpty(), "No nodes produced");
        return thrift.getNodes().get(0);
    }

    private record ExprCase(String name,
                            Supplier<Expr> factory,
                            TExprNodeType expectedNodeType,
                            Class<? extends Throwable> expectedException,
                            java.util.function.Consumer<TExprNode> afterCheck) {
    }

    private static final class ExprCaseFactory {
        private static final List<ExprCase> ALL_CASES = buildCases();

        private static List<ExprCase> buildCases() {
            List<ExprCase> cases = new ArrayList<>();

            // Literals
            cases.add(nodeCase("IntLiteral", () -> withType(new IntLiteral(7), Type.INT), TExprNodeType.INT_LITERAL));
            cases.add(nodeCase("FloatLiteral", ExprCaseFactory::buildFloatLiteral,
                    TExprNodeType.FLOAT_LITERAL));
            cases.add(nodeCase("DecimalLiteral", ExprCaseFactory::buildDecimalLiteral,
                    TExprNodeType.DECIMAL_LITERAL));
            cases.add(nodeCase("StringLiteral", () -> withType(new StringLiteral("starrocks"),
                    ScalarType.createVarchar(16)), TExprNodeType.STRING_LITERAL));
            cases.add(nodeCase("BoolLiteral", () -> withType(new BoolLiteral(true), Type.BOOLEAN),
                    TExprNodeType.BOOL_LITERAL));
            cases.add(nodeCase("VarBinaryLiteral", () -> withType(new VarBinaryLiteral(new byte[] {1, 2}),
                    Type.VARBINARY), TExprNodeType.BINARY_LITERAL));
            cases.add(nodeCase("DateLiteral", ExprCaseFactory::buildDateLiteral, TExprNodeType.DATE_LITERAL));
            cases.add(nodeCase("NullLiteral", () -> NullLiteral.create(Type.INT), TExprNodeType.NULL_LITERAL));
            cases.add(nodeCase("LargeIntLiteral", ExprCaseFactory::buildLargeIntLiteral,
                    TExprNodeType.LARGE_INT_LITERAL));
            cases.add(nodeCase("MaxLiteral", ExprCaseFactory::buildMaxLiteral, null,
                    node -> Assertions.assertFalse(node.isSetNode_type())));

            // Collection & JSON related
            cases.add(nodeCase("ArrayExpr", ExprCaseFactory::buildArrayExpr, TExprNodeType.ARRAY_EXPR));
            cases.add(nodeCase("CollectionElementArray", ExprCaseFactory::buildArrayElementExpr,
                    TExprNodeType.ARRAY_ELEMENT_EXPR));
            cases.add(nodeCase("CollectionElementMap", ExprCaseFactory::buildMapElementExpr,
                    TExprNodeType.MAP_ELEMENT_EXPR));
            cases.add(nodeCase("ArraySliceExpr", ExprCaseFactory::buildArraySliceExpr,
                    TExprNodeType.ARRAY_SLICE_EXPR));
            cases.add(nodeCase("MapExpr", ExprCaseFactory::buildMapExpr, TExprNodeType.MAP_EXPR));
            cases.add(nodeCase("SubfieldExpr", ExprCaseFactory::buildSubfieldExpr, TExprNodeType.SUBFIELD_EXPR));
            cases.add(nodeCase("CloneExpr", ExprCaseFactory::buildCloneExpr, TExprNodeType.CLONE_EXPR));
            cases.add(nodeCase("DictMappingExpr", ExprCaseFactory::buildDictMappingExpr, TExprNodeType.DICT_EXPR));

            // Functions and operators
            cases.add(nodeCase("ArithmeticExpr", ExprCaseFactory::buildArithmeticExpr,
                    TExprNodeType.ARITHMETIC_EXPR));
            cases.add(nodeCase("CaseExpr", ExprCaseFactory::buildCaseExpr, TExprNodeType.CASE_EXPR));
            cases.add(nodeCase("FunctionCall", ExprCaseFactory::buildScalarFunctionCall,
                    TExprNodeType.FUNCTION_CALL));
            cases.add(nodeCase("AggregateFunctionCall", ExprCaseFactory::buildAggregateFunctionCall,
                    TExprNodeType.AGG_EXPR));
            cases.add(nodeCase("AnalyticExpr", ExprCaseFactory::buildAnalyticExpr, null, node -> {
                // AnalyticExpr historically leaves node type unset
                Assertions.assertFalse(node.isSetNode_type());
            }));
            cases.add(nodeCase("InformationFunction", ExprCaseFactory::buildInformationFunction,
                    TExprNodeType.INFO_FUNC, node -> Assertions.assertEquals(new TInfoFunc(11, "cluster"),
                    node.getInfo_func())));
            cases.add(nodeCase("TimestampArithmeticExpr", ExprCaseFactory::buildTimestampArithmeticExpr,
                    TExprNodeType.COMPUTE_FUNCTION_CALL));
            cases.add(nodeCase("LambdaFunctionExpr", ExprCaseFactory::buildLambdaFunctionExpr,
                    TExprNodeType.LAMBDA_FUNCTION_EXPR, node -> Assertions.assertEquals(1, node.getOutput_column())));

            // Predicates
            cases.add(nodeCase("BinaryPredicate", ExprCaseFactory::buildBinaryPredicate, TExprNodeType.BINARY_PRED));
            cases.add(nodeCase("CompoundPredicate", ExprCaseFactory::buildCompoundPredicate,
                    TExprNodeType.COMPOUND_PRED));
            cases.add(nodeCase("InPredicate", ExprCaseFactory::buildInPredicate, TExprNodeType.IN_PRED,
                    node -> Assertions.assertEquals(new TInPredicate(false), node.getIn_predicate())));
            cases.add(nodeCase("LikePredicate", ExprCaseFactory::buildLikePredicate, TExprNodeType.FUNCTION_CALL));
            cases.add(nodeCase("MatchExpr", ExprCaseFactory::buildMatchExpr, TExprNodeType.MATCH_EXPR));
            cases.add(nodeCase("IsNullPredicate", ExprCaseFactory::buildIsNullPredicate, TExprNodeType.FUNCTION_CALL));

            // Exception expected predicates
            cases.add(exceptionCase("BetweenPredicate", ExprCaseFactory::buildBetweenPredicate,
                    IllegalStateException.class));
            cases.add(exceptionCase("ExistsPredicate", ExprCaseFactory::buildExistsPredicate,
                    IllegalStateException.class));
            cases.add(exceptionCase("LargeInPredicate", ExprCaseFactory::buildLargeInPredicate,
                    UnsupportedOperationException.class));

            // Dictionary functions
            cases.add(nodeCase("DictionaryGetExpr", ExprCaseFactory::buildDictionaryGetExpr,
                    TExprNodeType.DICTIONARY_GET_EXPR));
            cases.add(nodeCase("DictQueryExpr", ExprCaseFactory::buildDictQueryExpr, TExprNodeType.DICT_QUERY_EXPR));

            // References
            cases.add(nodeCase("SlotRef", ExprCaseFactory::buildSlotRef, TExprNodeType.SLOT_REF,
                    node -> Assertions.assertEquals(9, node.getOutput_column())));
            cases.add(nodeCase("PlaceHolderExpr", ExprCaseFactory::buildPlaceHolderExpr,
                    TExprNodeType.PLACEHOLDER_EXPR));
            cases.add(exceptionCase("FieldReference", ExprCaseFactory::buildFieldReference,
                    StarRocksPlannerException.class));
            cases.add(exceptionCase("LambdaArgument", ExprCaseFactory::buildLambdaArgument,
                    StarRocksPlannerException.class));
            cases.add(exceptionCase("ArrowExpr", ExprCaseFactory::buildArrowExpr,
                    StarRocksPlannerException.class));

            // Miscellaneous
            cases.add(nodeCase("Subquery", ExprCaseFactory::buildSubquery, null,
                    node -> Assertions.assertFalse(node.isSetNode_type())));
            cases.add(nodeCase("DefaultValueExpr", ExprCaseFactory::buildDefaultValueExpr, null,
                    node -> Assertions.assertFalse(node.isSetNode_type())));
            cases.add(exceptionCase("IntervalLiteral", ExprCaseFactory::buildIntervalLiteral,
                    StarRocksPlannerException.class));

            return cases;
        }

        private static ExprCase nodeCase(String name, Supplier<Expr> factory, TExprNodeType nodeType) {
            return nodeCase(name, factory, nodeType, null);
        }

        private static ExprCase nodeCase(String name, Supplier<Expr> factory, TExprNodeType nodeType,
                                         java.util.function.Consumer<TExprNode> afterCheck) {
            return new ExprCase(name, factory, nodeType, null, afterCheck);
        }

        private static ExprCase exceptionCase(String name, Supplier<Expr> factory,
                                              Class<? extends Throwable> expectedException) {
            return new ExprCase(name, factory, null, expectedException, null);
        }

        private static Expr buildFloatLiteral() {
            try {
                FloatLiteral literal = new FloatLiteral(3.14);
                literal.setType(Type.DOUBLE);
                literal.setOriginType(Type.DOUBLE);
                return literal;
            } catch (AnalysisException e) {
                throw new RuntimeException(e);
            }
        }

        private static Expr buildDecimalLiteral() {
            try {
                DecimalLiteral literal = new DecimalLiteral("1.23");
                ScalarType type = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 6, 2);
                literal.setType(type);
                literal.setOriginType(type);
                return literal;
            } catch (AnalysisException e) {
                throw new RuntimeException(e);
            }
        }

        private static Expr buildDateLiteral() {
            try {
                DateLiteral literal = new DateLiteral("2024-01-02", Type.DATE);
                literal.setOriginType(Type.DATE);
                return literal;
            } catch (AnalysisException e) {
                throw new RuntimeException(e);
            }
        }

        private static Expr buildLargeIntLiteral() {
            LargeIntLiteral literal = LargeIntLiteral.createMaxValue();
            literal.setOriginType(Type.LARGEINT);
            return literal;
        }

        private static Expr buildMaxLiteral() {
            MaxLiteral max = MaxLiteral.MAX_VALUE;
            max.setType(Type.INT);
            max.setOriginType(Type.INT);
            return max;
        }

        private static Expr buildArrayExpr() {
            ArrayExpr arrayExpr = new ArrayExpr(new ArrayType(Type.INT),
                    Lists.newArrayList(new IntLiteral(1), new IntLiteral(2)));
            arrayExpr.setOriginType(arrayExpr.getType());
            return arrayExpr;
        }

        private static Expr buildArrayElementExpr() {
            ArrayExpr arrayExpr = (ArrayExpr) buildArrayExpr();
            CollectionElementExpr expr =
                    new CollectionElementExpr(Type.INT, arrayExpr, new IntLiteral(0), false);
            expr.setOriginType(Type.INT);
            return expr;
        }

        private static Expr buildMapExpr() {
            List<Expr> children = List.of(
                    withType(new StringLiteral("k"), ScalarType.createVarchar(8)),
                    withType(new IntLiteral(1), Type.INT),
                    withType(new StringLiteral("k2"), ScalarType.createVarchar(8)),
                    withType(new IntLiteral(2), Type.INT));
            MapExpr mapExpr = new MapExpr(new MapType(ScalarType.createVarchar(8), Type.INT), children);
            mapExpr.setOriginType(mapExpr.getType());
            return mapExpr;
        }

        private static Expr buildMapElementExpr() {
            MapExpr mapExpr = (MapExpr) buildMapExpr();
            CollectionElementExpr expr =
                    new CollectionElementExpr(Type.INT, mapExpr, withType(new StringLiteral("k"),
                            ScalarType.createVarchar(8)), true);
            expr.setOriginType(Type.INT);
            return expr;
        }

        private static Expr buildArraySliceExpr() {
            ArrayExpr arrayExpr = (ArrayExpr) buildArrayExpr();
            ArraySliceExpr sliceExpr =
                    new ArraySliceExpr(arrayExpr, new IntLiteral(0), new IntLiteral(1));
            sliceExpr.setType(arrayExpr.getType());
            sliceExpr.setOriginType(arrayExpr.getType());
            return sliceExpr;
        }

        private static Expr buildSubfieldExpr() {
            SlotRef structRef = buildSlotRef();
            SubfieldExpr expr = new SubfieldExpr(structRef, List.of("f1", "f2"));
            expr.setType(Type.INT);
            expr.setOriginType(Type.INT);
            return expr;
        }

        private static Expr buildCloneExpr() {
            CloneExpr expr = new CloneExpr(new IntLiteral(5));
            expr.setType(Type.INT);
            expr.setOriginType(Type.INT);
            return expr;
        }

        private static Expr buildDictMappingExpr() {
            DictMappingExpr expr = new DictMappingExpr(buildSlotRef(), withType(new StringLiteral("expr"),
                    ScalarType.createVarchar(16)));
            expr.setType(ScalarType.createVarchar(16));
            expr.setOriginType(expr.getType());
            return expr;
        }

        private static Expr buildArithmeticExpr() {
            ArithmeticExpr expr = new ArithmeticExpr(ArithmeticExpr.Operator.ADD,
                    new IntLiteral(1), new IntLiteral(2));
            expr.setType(Type.INT);
            expr.setOriginType(Type.INT);
            return expr;
        }

        private static Expr buildCaseExpr() {
            CaseExpr expr = new CaseExpr(new IntLiteral(1),
                    List.of(new CaseWhenClause(new IntLiteral(1), new StringLiteral("x"))),
                    new StringLiteral("y"));
            expr.setType(ScalarType.createVarchar(8));
            expr.setOriginType(expr.getType());
            return expr;
        }

        private static Expr buildScalarFunctionCall() {
            FunctionCallExpr call = new FunctionCallExpr("abs", List.of(new IntLiteral(-3)));
            ScalarFunction fn = ScalarFunction.createBuiltinOperator("abs", Lists.newArrayList(Type.INT), Type.INT);
            call.setFn(fn);
            call.setType(Type.INT);
            call.setOriginType(Type.INT);
            return call;
        }

        private static Expr buildAggregateFunctionCall() {
            FunctionCallExpr call = new FunctionCallExpr("sum", List.of(new IntLiteral(3)));
            AggregateFunction fn = AggregateFunction.createBuiltin("sum", List.of(Type.INT), Type.INT, Type.INT,
                    false, false, false, false);
            call.setFn(fn);
            call.setType(Type.INT);
            call.setOriginType(Type.INT);
            return call;
        }

        private static Expr buildAnalyticExpr() {
            FunctionCallExpr call = new FunctionCallExpr("row_number", Collections.emptyList());
            AggregateFunction fn = AggregateFunction.createBuiltin("row_number", Collections.emptyList(),
                    Type.BIGINT, Type.BIGINT, false, false, true, true);
            call.setFn(fn);
            call.setType(Type.BIGINT);
            call.setOriginType(Type.BIGINT);
            call.setIsAnalyticFnCall(true);
            AnalyticExpr analyticExpr = new AnalyticExpr(call, List.of(new IntLiteral(1)),
                    Collections.emptyList(), null, null);
            analyticExpr.setType(Type.BIGINT);
            analyticExpr.setOriginType(Type.BIGINT);
            return analyticExpr;
        }

        private static Expr buildInformationFunction() {
            InformationFunction infoFunction = new InformationFunction("CURRENT_CATALOG", "cluster", 11);
            infoFunction.setType(ScalarType.createVarchar(16));
            infoFunction.setOriginType(infoFunction.getType());
            return infoFunction;
        }

        private static Expr buildTimestampArithmeticExpr() {
            try {
                DateLiteral dateLiteral = new DateLiteral("2024-01-01", Type.DATE);
                TimestampArithmeticExpr expr =
                        new TimestampArithmeticExpr(ArithmeticExpr.Operator.ADD, dateLiteral,
                                new IntLiteral(1), "DAY", false);
                expr.setType(Type.DATE);
                expr.setOriginType(Type.DATE);
                expr.opcode = TExprOpcode.ADD;
                return expr;
            } catch (AnalysisException e) {
                throw new RuntimeException(e);
            }
        }

        private static Expr buildLambdaFunctionExpr() {
            SlotRef argSlot = buildSlotRef();
            LambdaFunctionExpr lambda =
                    new LambdaFunctionExpr(List.of(new IntLiteral(1), argSlot),
                            Map.of(argSlot, new IntLiteral(2)));
            lambda.setType(Type.INT);
            lambda.setOriginType(Type.INT);
            return lambda;
        }

        private static Expr buildBinaryPredicate() {
            BinaryPredicate predicate =
                    new BinaryPredicate(BinaryType.EQ, new IntLiteral(1), new IntLiteral(1));
            predicate.setType(Type.BOOLEAN);
            predicate.setOriginType(Type.BOOLEAN);
            return predicate;
        }

        private static Expr buildCompoundPredicate() {
            CompoundPredicate predicate =
                    new CompoundPredicate(CompoundPredicate.Operator.AND, new BoolLiteral(true),
                            new BoolLiteral(false));
            predicate.setType(Type.BOOLEAN);
            predicate.setOriginType(Type.BOOLEAN);
            return predicate;
        }

        private static Expr buildInPredicate() {
            InPredicate predicate =
                    new InPredicate(new IntLiteral(1), Lists.newArrayList(new IntLiteral(2), new IntLiteral(3)), false);
            predicate.setType(Type.BOOLEAN);
            predicate.setOriginType(Type.BOOLEAN);
            return predicate;
        }

        private static Expr buildLikePredicate() {
            LikePredicate predicate =
                    new LikePredicate(LikePredicate.Operator.LIKE, new StringLiteral("abc"),
                            new StringLiteral("a%"));
            predicate.setType(Type.BOOLEAN);
            predicate.setOriginType(Type.BOOLEAN);
            return predicate;
        }

        private static Expr buildMatchExpr() {
            MatchExpr expr = new MatchExpr(new StringLiteral("field"), new StringLiteral("value"));
            expr.setType(Type.BOOLEAN);
            expr.setOriginType(Type.BOOLEAN);
            return expr;
        }

        private static Expr buildIsNullPredicate() {
            IsNullPredicate predicate = new IsNullPredicate(new IntLiteral(1), false);
            predicate.setType(Type.BOOLEAN);
            predicate.setOriginType(Type.BOOLEAN);
            return predicate;
        }

        private static Expr buildBetweenPredicate() {
            BetweenPredicate predicate =
                    new BetweenPredicate(new IntLiteral(1), new IntLiteral(0), new IntLiteral(2), false);
            predicate.setType(Type.BOOLEAN);
            predicate.setOriginType(Type.BOOLEAN);
            return predicate;
        }

        private static Expr buildExistsPredicate() {
            ExistsPredicate predicate = new ExistsPredicate(buildSubquery(), false);
            predicate.setType(Type.BOOLEAN);
            predicate.setOriginType(Type.BOOLEAN);
            return predicate;
        }

        private static Expr buildLargeInPredicate() {
            LargeInPredicate predicate = new LargeInPredicate(new IntLiteral(1), "(1,2,3)",
                    List.of(1, 2, 3), 3, false, List.of(new IntLiteral(1)), NodePosition.ZERO);
            predicate.setType(Type.BOOLEAN);
            predicate.setOriginType(Type.BOOLEAN);
            return predicate;
        }

        private static Expr buildDictionaryGetExpr() {
            DictionaryGetExpr expr = new DictionaryGetExpr(List.of(new IntLiteral(1)));
            expr.setDictionaryId(99L);
            expr.setDictionaryTxnId(100L);
            expr.setKeySize(10);
            expr.setNullIfNotExist(true);
            expr.setType(Type.INT);
            expr.setOriginType(Type.INT);
            return expr;
        }

        private static Expr buildDictQueryExpr() {
            DictQueryExpr expr = new DictQueryExpr(List.of(new StringLiteral("dict")));
            expr.setDictQueryExpr(new com.starrocks.thrift.TDictQueryExpr());
            expr.setType(Type.INT);
            expr.setOriginType(Type.INT);
            return expr;
        }

        private static SlotRef buildSlotRef() {
            SlotDescriptor descriptor = new SlotDescriptor(new SlotId(3), "col", Type.INT, true);
            SlotRef slotRef = new SlotRef(descriptor);
            slotRef.setType(Type.INT);
            slotRef.setOriginType(Type.INT);
            slotRef.outputColumn = 9;
            return slotRef;
        }

        private static Expr buildPlaceHolderExpr() {
            PlaceHolderExpr expr = new PlaceHolderExpr(5, true, Type.INT);
            expr.setOriginType(Type.INT);
            return expr;
        }

        private static Expr buildFieldReference() {
            FieldReference ref = new FieldReference(1, new TableName("db", "tbl"));
            ref.setType(Type.INT);
            ref.setOriginType(Type.INT);
            return ref;
        }

        private static Expr buildLambdaArgument() {
            LambdaArgument arg = new LambdaArgument("arg");
            arg.setType(Type.INT);
            arg.setOriginType(Type.INT);
            return arg;
        }

        private static Expr buildArrowExpr() {
            ArrowExpr arrowExpr = new ArrowExpr(new StringLiteral("k"), new StringLiteral("v"));
            ScalarType varchar = ScalarType.createVarchar(8);
            arrowExpr.setType(varchar);
            arrowExpr.setOriginType(varchar);
            return arrowExpr;
        }

        private static Subquery buildSubquery() {
            DummyRelation relation = new DummyRelation();
            QueryStatement statement = new QueryStatement(relation);
            Subquery subquery = new Subquery(statement);
            subquery.setType(Type.INT);
            subquery.setOriginType(Type.INT);
            return subquery;
        }

        private static Expr buildDefaultValueExpr() {
            DefaultValueExpr expr = new DefaultValueExpr(NodePosition.ZERO);
            expr.setType(Type.INT);
            expr.setOriginType(Type.INT);
            return expr;
        }

        private static Expr buildIntervalLiteral() {
            IntervalLiteral intervalLiteral = new IntervalLiteral(new IntLiteral(1), new UnitIdentifier("DAY"));
            intervalLiteral.setType(Type.INT);
            intervalLiteral.setOriginType(Type.INT);
            return intervalLiteral;
        }

        private static <T extends Expr> T withType(T expr, Type type) {
            expr.setType(type);
            expr.setOriginType(type);
            return expr;
        }
    }

    private static final class DummyRelation extends QueryRelation {
        DummyRelation() {
            super(NodePosition.ZERO);
        }

        @Override
        public List<Expr> getOutputExpression() {
            return List.of(new IntLiteral(1));
        }
    }
}
