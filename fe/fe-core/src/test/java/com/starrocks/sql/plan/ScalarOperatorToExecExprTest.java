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

package com.starrocks.sql.plan;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionName;
import com.starrocks.common.FeConstants;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.SlotId;
import com.starrocks.planner.expression.ExecArithmetic;
import com.starrocks.planner.expression.ExecArrayExpr;
import com.starrocks.planner.expression.ExecBetweenPredicate;
import com.starrocks.planner.expression.ExecBinaryPredicate;
import com.starrocks.planner.expression.ExecCaseWhen;
import com.starrocks.planner.expression.ExecCast;
import com.starrocks.planner.expression.ExecCompoundPredicate;
import com.starrocks.planner.expression.ExecExpr;
import com.starrocks.planner.expression.ExecFunctionCall;
import com.starrocks.planner.expression.ExecInPredicate;
import com.starrocks.planner.expression.ExecInformationFunction;
import com.starrocks.planner.expression.ExecIsNullPredicate;
import com.starrocks.planner.expression.ExecLambdaFunction;
import com.starrocks.planner.expression.ExecLikePredicate;
import com.starrocks.planner.expression.ExecLiteral;
import com.starrocks.planner.expression.ExecMapExpr;
import com.starrocks.planner.expression.ExecSlotRef;
import com.starrocks.planner.expression.ExecSubfield;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.ArithmeticExpr;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.MapOperator;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.type.ArrayType;
import com.starrocks.type.BooleanType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.MapType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ScalarOperatorToExecExprTest {

    @BeforeAll
    public static void setUp() {
        FeConstants.runningUnitTest = true;
    }

    @AfterAll
    public static void tearDown() {
        FeConstants.runningUnitTest = false;
    }

    // ---- Helper methods ----

    private static ScalarOperatorToExecExpr.FormatterContext makeContext(
            Map<ColumnRefOperator, ExecExpr> colRefToExecExpr) {
        return new ScalarOperatorToExecExpr.FormatterContext(colRefToExecExpr);
    }

    private static ScalarOperatorToExecExpr.FormatterContext emptyContext() {
        return makeContext(new HashMap<>());
    }

    private static ExecSlotRef makeSlotRef(int id, String name, Type type, boolean nullable) {
        SlotDescriptor desc = new SlotDescriptor(new SlotId(id), name, type, nullable);
        return new ExecSlotRef(desc);
    }

    private static Function makeScalarFunction(String name, Type[] argTypes, Type retType) {
        Function fn = new Function(new FunctionName(name), argTypes, retType, false);
        fn.setBinaryType(TFunctionBinaryType.BUILTIN);
        return fn;
    }

    // ---- Tests ----

    @Test
    public void testVisitVariableReference() {
        ColumnRefOperator colRef = new ColumnRefOperator(1, IntegerType.INT, "col1", true);
        ExecSlotRef slotRef = makeSlotRef(1, "col1", IntegerType.INT, true);

        Map<ColumnRefOperator, ExecExpr> map = new HashMap<>();
        map.put(colRef, slotRef);
        ScalarOperatorToExecExpr.FormatterContext ctx = makeContext(map);

        ExecExpr result = ScalarOperatorToExecExpr.build(colRef, ctx);
        assertInstanceOf(ExecSlotRef.class, result);
        assertEquals(IntegerType.INT, result.getType());
    }

    @Test
    public void testVisitVariableReferenceNotFound() {
        ColumnRefOperator colRef = new ColumnRefOperator(1, IntegerType.INT, "col1", true);
        ScalarOperatorToExecExpr.FormatterContext ctx = emptyContext();

        SemanticException ex = assertThrows(SemanticException.class, () -> {
            ScalarOperatorToExecExpr.build(colRef, ctx);
        });
        assertTrue(ex.getMessage().contains("Cannot convert ColumnRefOperator"));
    }

    @Test
    public void testVisitVariableReferenceWithProjectMap() {
        ColumnRefOperator colRef = new ColumnRefOperator(1, IntegerType.INT, "col1", true);
        // The projected expression is a constant
        ConstantOperator projected = ConstantOperator.createInt(42);

        Map<ColumnRefOperator, ExecExpr> colRefToExecExpr = new HashMap<>();
        Map<ColumnRefOperator, com.starrocks.sql.optimizer.operator.scalar.ScalarOperator> projectMap =
                new HashMap<>();
        projectMap.put(colRef, projected);

        ScalarOperatorToExecExpr.FormatterContext ctx =
                new ScalarOperatorToExecExpr.FormatterContext(colRefToExecExpr, projectMap);

        ExecExpr result = ScalarOperatorToExecExpr.build(colRef, ctx);
        assertInstanceOf(ExecLiteral.class, result);
        assertEquals(42, ((ExecLiteral) result).getValue().getInt());
    }

    @Test
    public void testVisitConstantInt() {
        ConstantOperator intConst = ConstantOperator.createInt(100);
        ExecExpr result = ScalarOperatorToExecExpr.build(intConst, emptyContext());

        assertInstanceOf(ExecLiteral.class, result);
        ExecLiteral literal = (ExecLiteral) result;
        assertEquals(IntegerType.INT, literal.getType());
        assertEquals(100, literal.getValue().getInt());
        assertFalse(literal.isNullable());
    }

    @Test
    public void testVisitConstantVarchar() {
        ConstantOperator strConst = ConstantOperator.createVarchar("hello");
        ExecExpr result = ScalarOperatorToExecExpr.build(strConst, emptyContext());

        assertInstanceOf(ExecLiteral.class, result);
        ExecLiteral literal = (ExecLiteral) result;
        assertEquals("hello", literal.getValue().getVarchar());
    }

    @Test
    public void testVisitConstantBoolean() {
        ConstantOperator boolConst = ConstantOperator.createBoolean(true);
        ExecExpr result = ScalarOperatorToExecExpr.build(boolConst, emptyContext());

        assertInstanceOf(ExecLiteral.class, result);
        ExecLiteral literal = (ExecLiteral) result;
        assertEquals(BooleanType.BOOLEAN, literal.getType());
        assertTrue(literal.getValue().getBoolean());
    }

    @Test
    public void testVisitConstantNull() {
        ConstantOperator nullConst = ConstantOperator.createNull(IntegerType.INT);
        ExecExpr result = ScalarOperatorToExecExpr.build(nullConst, emptyContext());

        assertInstanceOf(ExecLiteral.class, result);
        ExecLiteral literal = (ExecLiteral) result;
        assertTrue(literal.isNullable());
        assertTrue(literal.getValue().isNull());
    }

    @Test
    public void testVisitCallOperatorDefault() {
        // Test a generic function call: upper(varchar) -> varchar
        Function fn = makeScalarFunction("upper",
                new Type[] {VarcharType.VARCHAR}, VarcharType.VARCHAR);
        CallOperator call = new CallOperator("upper", VarcharType.VARCHAR,
                List.of(ConstantOperator.createVarchar("hello")), fn);

        ExecExpr result = ScalarOperatorToExecExpr.build(call, emptyContext());

        assertInstanceOf(ExecFunctionCall.class, result);
        ExecFunctionCall funcCall = (ExecFunctionCall) result;
        assertEquals("upper", funcCall.getFnName());
        assertEquals(fn, funcCall.getFn());
        assertEquals(1, funcCall.getNumChildren());
        assertInstanceOf(ExecLiteral.class, funcCall.getChild(0));
    }

    @Test
    public void testVisitCallOperatorArithmeticAdd() {
        Function fn = makeScalarFunction("add",
                new Type[] {IntegerType.INT, IntegerType.INT}, IntegerType.INT);
        CallOperator call = new CallOperator("add", IntegerType.INT,
                List.of(ConstantOperator.createInt(1), ConstantOperator.createInt(2)), fn);

        ExecExpr result = ScalarOperatorToExecExpr.build(call, emptyContext());

        assertInstanceOf(ExecArithmetic.class, result);
        ExecArithmetic arith = (ExecArithmetic) result;
        assertEquals(ArithmeticExpr.Operator.ADD, arith.getOp());
        assertEquals(2, arith.getNumChildren());
    }

    @Test
    public void testVisitCallOperatorArithmeticSubtract() {
        Function fn = makeScalarFunction("subtract",
                new Type[] {IntegerType.INT, IntegerType.INT}, IntegerType.INT);
        CallOperator call = new CallOperator("subtract", IntegerType.INT,
                List.of(ConstantOperator.createInt(5), ConstantOperator.createInt(3)), fn);

        ExecExpr result = ScalarOperatorToExecExpr.build(call, emptyContext());

        assertInstanceOf(ExecArithmetic.class, result);
        ExecArithmetic arith = (ExecArithmetic) result;
        assertEquals(ArithmeticExpr.Operator.SUBTRACT, arith.getOp());
    }

    @Test
    public void testVisitCallOperatorInformationFunction() {
        // database() takes a ConstantOperator(varchar) child with the current db name
        Function fn = makeScalarFunction("database",
                new Type[] {VarcharType.VARCHAR}, VarcharType.VARCHAR);
        CallOperator call = new CallOperator("database", VarcharType.VARCHAR,
                List.of(ConstantOperator.createVarchar("test_db")), fn);

        ExecExpr result = ScalarOperatorToExecExpr.build(call, emptyContext());

        assertInstanceOf(ExecInformationFunction.class, result);
        ExecInformationFunction infoFn = (ExecInformationFunction) result;
        assertEquals("database", infoFn.getFuncName());
        assertEquals("test_db", infoFn.getStrValue());
        assertEquals(0, infoFn.getIntValue());
    }

    @Test
    public void testVisitCallOperatorConnectionId() {
        Function fn = makeScalarFunction("connection_id",
                new Type[] {IntegerType.BIGINT}, IntegerType.BIGINT);
        CallOperator call = new CallOperator("connection_id", IntegerType.BIGINT,
                List.of(ConstantOperator.createBigint(12345L)), fn);

        ExecExpr result = ScalarOperatorToExecExpr.build(call, emptyContext());

        assertInstanceOf(ExecInformationFunction.class, result);
        ExecInformationFunction infoFn = (ExecInformationFunction) result;
        assertEquals("connection_id", infoFn.getFuncName());
        assertEquals(12345L, infoFn.getIntValue());
    }

    @Test
    public void testVisitCastOperator() {
        // CAST(INT to BIGINT)
        ConstantOperator child = ConstantOperator.createInt(42);
        CastOperator castOp = new CastOperator(IntegerType.BIGINT, child);

        ExecExpr result = ScalarOperatorToExecExpr.build(castOp, emptyContext());

        assertInstanceOf(ExecCast.class, result);
        ExecCast cast = (ExecCast) result;
        assertEquals(IntegerType.BIGINT, cast.getType());
        assertFalse(cast.isImplicit());
        assertEquals(1, cast.getNumChildren());
        assertInstanceOf(ExecLiteral.class, cast.getChild(0));
    }

    @Test
    public void testVisitCastOperatorImplicit() {
        ConstantOperator child = ConstantOperator.createInt(42);
        CastOperator castOp = new CastOperator(IntegerType.BIGINT, child, true);

        ExecExpr result = ScalarOperatorToExecExpr.build(castOp, emptyContext());

        assertInstanceOf(ExecCast.class, result);
        ExecCast cast = (ExecCast) result;
        assertTrue(cast.isImplicit());
    }

    @Test
    public void testVisitBinaryPredicateEQ() {
        ConstantOperator left = ConstantOperator.createInt(1);
        ConstantOperator right = ConstantOperator.createInt(1);
        BinaryPredicateOperator binPred = new BinaryPredicateOperator(BinaryType.EQ, left, right);

        ExecExpr result = ScalarOperatorToExecExpr.build(binPred, emptyContext());

        assertInstanceOf(ExecBinaryPredicate.class, result);
        ExecBinaryPredicate pred = (ExecBinaryPredicate) result;
        assertEquals(BinaryType.EQ, pred.getOp());
        assertEquals(2, pred.getNumChildren());
    }

    @Test
    public void testVisitBinaryPredicateLT() {
        ConstantOperator left = ConstantOperator.createInt(1);
        ConstantOperator right = ConstantOperator.createInt(5);
        BinaryPredicateOperator binPred = new BinaryPredicateOperator(BinaryType.LT, left, right);

        ExecExpr result = ScalarOperatorToExecExpr.build(binPred, emptyContext());

        assertInstanceOf(ExecBinaryPredicate.class, result);
        ExecBinaryPredicate pred = (ExecBinaryPredicate) result;
        assertEquals(BinaryType.LT, pred.getOp());
    }

    @Test
    public void testVisitBinaryPredicateEqForNull() {
        ConstantOperator left = ConstantOperator.createInt(1);
        ConstantOperator right = ConstantOperator.createNull(IntegerType.INT);
        BinaryPredicateOperator binPred = new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL, left, right);

        ExecExpr result = ScalarOperatorToExecExpr.build(binPred, emptyContext());

        assertInstanceOf(ExecBinaryPredicate.class, result);
        ExecBinaryPredicate pred = (ExecBinaryPredicate) result;
        assertEquals(BinaryType.EQ_FOR_NULL, pred.getOp());
        // EQ_FOR_NULL should not be nullable
        assertFalse(pred.isNullable());
    }

    @Test
    public void testVisitCompoundPredicateAND() {
        ConstantOperator t = ConstantOperator.createBoolean(true);
        ConstantOperator f = ConstantOperator.createBoolean(false);
        CompoundPredicateOperator andPred =
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, t, f);

        ExecExpr result = ScalarOperatorToExecExpr.build(andPred, emptyContext());

        assertInstanceOf(ExecCompoundPredicate.class, result);
        ExecCompoundPredicate pred = (ExecCompoundPredicate) result;
        assertEquals(CompoundPredicate.Operator.AND, pred.getCompoundType());
        assertEquals(2, pred.getNumChildren());
    }

    @Test
    public void testVisitCompoundPredicateOR() {
        ConstantOperator t = ConstantOperator.createBoolean(true);
        ConstantOperator f = ConstantOperator.createBoolean(false);
        CompoundPredicateOperator orPred =
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, t, f);

        ExecExpr result = ScalarOperatorToExecExpr.build(orPred, emptyContext());

        assertInstanceOf(ExecCompoundPredicate.class, result);
        ExecCompoundPredicate pred = (ExecCompoundPredicate) result;
        assertEquals(CompoundPredicate.Operator.OR, pred.getCompoundType());
    }

    @Test
    public void testVisitCompoundPredicateNOT() {
        ConstantOperator t = ConstantOperator.createBoolean(true);
        CompoundPredicateOperator notPred =
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, t);

        ExecExpr result = ScalarOperatorToExecExpr.build(notPred, emptyContext());

        assertInstanceOf(ExecCompoundPredicate.class, result);
        ExecCompoundPredicate pred = (ExecCompoundPredicate) result;
        assertEquals(CompoundPredicate.Operator.NOT, pred.getCompoundType());
        assertEquals(1, pred.getNumChildren());
    }

    @Test
    public void testVisitInPredicate() {
        ConstantOperator col = ConstantOperator.createInt(1);
        ConstantOperator val1 = ConstantOperator.createInt(1);
        ConstantOperator val2 = ConstantOperator.createInt(2);
        ConstantOperator val3 = ConstantOperator.createInt(3);
        InPredicateOperator inPred = new InPredicateOperator(false, col, val1, val2, val3);

        ExecExpr result = ScalarOperatorToExecExpr.build(inPred, emptyContext());

        assertInstanceOf(ExecInPredicate.class, result);
        ExecInPredicate pred = (ExecInPredicate) result;
        assertFalse(pred.isNotIn());
        assertEquals(4, pred.getNumChildren()); // 1 subject + 3 values
    }

    @Test
    public void testVisitInPredicateNotIn() {
        ConstantOperator col = ConstantOperator.createInt(1);
        ConstantOperator val1 = ConstantOperator.createInt(1);
        InPredicateOperator inPred = new InPredicateOperator(true, col, val1);

        ExecExpr result = ScalarOperatorToExecExpr.build(inPred, emptyContext());

        assertInstanceOf(ExecInPredicate.class, result);
        ExecInPredicate pred = (ExecInPredicate) result;
        assertTrue(pred.isNotIn());
    }

    @Test
    public void testVisitIsNullPredicate() {
        ConstantOperator child = ConstantOperator.createInt(1);
        IsNullPredicateOperator isNullOp = new IsNullPredicateOperator(child);

        ExecExpr result = ScalarOperatorToExecExpr.build(isNullOp, emptyContext());

        assertInstanceOf(ExecIsNullPredicate.class, result);
        ExecIsNullPredicate pred = (ExecIsNullPredicate) result;
        assertFalse(pred.isNotNull());
        assertEquals(1, pred.getNumChildren());
    }

    @Test
    public void testVisitIsNotNullPredicate() {
        ConstantOperator child = ConstantOperator.createInt(1);
        IsNullPredicateOperator isNotNullOp = new IsNullPredicateOperator(true, child);

        ExecExpr result = ScalarOperatorToExecExpr.build(isNotNullOp, emptyContext());

        assertInstanceOf(ExecIsNullPredicate.class, result);
        ExecIsNullPredicate pred = (ExecIsNullPredicate) result;
        assertTrue(pred.isNotNull());
    }

    @Test
    public void testVisitLikePredicateOperator() {
        ConstantOperator col = ConstantOperator.createVarchar("hello");
        ConstantOperator pattern = ConstantOperator.createVarchar("%ell%");
        LikePredicateOperator likeOp = new LikePredicateOperator(
                LikePredicateOperator.LikeType.LIKE, col, pattern);

        ExecExpr result = ScalarOperatorToExecExpr.build(likeOp, emptyContext());

        assertInstanceOf(ExecLikePredicate.class, result);
        ExecLikePredicate pred = (ExecLikePredicate) result;
        assertFalse(pred.isRegexp());
        assertEquals(2, pred.getNumChildren());
    }

    @Test
    public void testVisitLikePredicateOperatorRegexp() {
        ConstantOperator col = ConstantOperator.createVarchar("hello");
        ConstantOperator pattern = ConstantOperator.createVarchar(".*ell.*");
        LikePredicateOperator regexpOp = new LikePredicateOperator(
                LikePredicateOperator.LikeType.REGEXP, col, pattern);

        ExecExpr result = ScalarOperatorToExecExpr.build(regexpOp, emptyContext());

        assertInstanceOf(ExecLikePredicate.class, result);
        ExecLikePredicate pred = (ExecLikePredicate) result;
        assertTrue(pred.isRegexp());
    }

    @Test
    public void testVisitCaseWhenOperatorSimple() {
        // CASE WHEN true THEN 1 ELSE 0 END
        ConstantOperator whenClause = ConstantOperator.createBoolean(true);
        ConstantOperator thenClause = ConstantOperator.createInt(1);
        ConstantOperator elseClause = ConstantOperator.createInt(0);
        List<com.starrocks.sql.optimizer.operator.scalar.ScalarOperator> whenThen =
                List.of(whenClause, thenClause);
        CaseWhenOperator caseWhen = new CaseWhenOperator(IntegerType.INT, null, elseClause, whenThen);

        ExecExpr result = ScalarOperatorToExecExpr.build(caseWhen, emptyContext());

        assertInstanceOf(ExecCaseWhen.class, result);
        ExecCaseWhen cw = (ExecCaseWhen) result;
        assertFalse(cw.hasCase());
        assertTrue(cw.hasElse());
        // Children: [whenExpr, thenExpr, elseExpr] = 3
        assertEquals(3, cw.getNumChildren());
    }

    @Test
    public void testVisitCaseWhenOperatorWithCase() {
        // CASE col WHEN 1 THEN 'a' WHEN 2 THEN 'b' END
        ConstantOperator caseClause = ConstantOperator.createInt(1);
        ConstantOperator when1 = ConstantOperator.createInt(1);
        ConstantOperator then1 = ConstantOperator.createVarchar("a");
        ConstantOperator when2 = ConstantOperator.createInt(2);
        ConstantOperator then2 = ConstantOperator.createVarchar("b");
        List<com.starrocks.sql.optimizer.operator.scalar.ScalarOperator> whenThen =
                List.of(when1, then1, when2, then2);
        CaseWhenOperator caseWhen = new CaseWhenOperator(VarcharType.VARCHAR, caseClause, null, whenThen);

        ExecExpr result = ScalarOperatorToExecExpr.build(caseWhen, emptyContext());

        assertInstanceOf(ExecCaseWhen.class, result);
        ExecCaseWhen cw = (ExecCaseWhen) result;
        assertTrue(cw.hasCase());
        assertFalse(cw.hasElse());
        // Children: [caseExpr, when1, then1, when2, then2] = 5
        assertEquals(5, cw.getNumChildren());
    }

    @Test
    public void testVisitArrayOperator() {
        ArrayType arrayType = new ArrayType(IntegerType.INT);
        ArrayOperator arrayOp = new ArrayOperator(arrayType, true,
                List.of(ConstantOperator.createInt(1), ConstantOperator.createInt(2)));

        ExecExpr result = ScalarOperatorToExecExpr.build(arrayOp, emptyContext());

        assertInstanceOf(ExecArrayExpr.class, result);
        ExecArrayExpr arr = (ExecArrayExpr) result;
        assertEquals(2, arr.getNumChildren());
        assertTrue(arr.getType() instanceof ArrayType);
    }

    @Test
    public void testVisitMapOperator() {
        MapType mapType = new MapType(IntegerType.INT, VarcharType.VARCHAR);
        MapOperator mapOp = new MapOperator(mapType,
                List.of(ConstantOperator.createInt(1), ConstantOperator.createVarchar("v1")));

        ExecExpr result = ScalarOperatorToExecExpr.build(mapOp, emptyContext());

        assertInstanceOf(ExecMapExpr.class, result);
        ExecMapExpr map = (ExecMapExpr) result;
        assertEquals(2, map.getNumChildren());
        assertTrue(map.getType() instanceof MapType);
    }

    @Test
    public void testVisitLambdaFunctionOperator() {
        // Lambda: x -> x + 1
        ColumnRefOperator lambdaArg = new ColumnRefOperator(100, IntegerType.INT, "x", true, true);

        Function addFn = makeScalarFunction("add",
                new Type[] {IntegerType.INT, IntegerType.INT}, IntegerType.INT);
        CallOperator addCall = new CallOperator("add", IntegerType.INT,
                List.of(lambdaArg, ConstantOperator.createInt(1)), addFn);

        LambdaFunctionOperator lambdaOp = new LambdaFunctionOperator(
                List.of(lambdaArg), addCall, IntegerType.INT);

        ExecExpr result = ScalarOperatorToExecExpr.build(lambdaOp, emptyContext());

        assertInstanceOf(ExecLambdaFunction.class, result);
        ExecLambdaFunction lambda = (ExecLambdaFunction) result;
        assertEquals(0, lambda.getCommonSubOperatorNum());
        assertFalse(lambda.isNondeterministic());
        // Children: [lambdaBody, arg] = 2
        assertTrue(lambda.getNumChildren() >= 2);
    }

    @Test
    public void testVisitSubfieldOperator() {
        // Access struct.field_a
        StructType structType = new StructType(
                new java.util.ArrayList<>(List.of(new StructField("field_a", IntegerType.INT))));
        ColumnRefOperator colRef = new ColumnRefOperator(1, structType, "struct_col", true);
        ExecSlotRef slotRef = makeSlotRef(1, "struct_col", structType, true);

        Map<ColumnRefOperator, ExecExpr> map = new HashMap<>();
        map.put(colRef, slotRef);
        ScalarOperatorToExecExpr.FormatterContext ctx = makeContext(map);

        SubfieldOperator subfieldOp = new SubfieldOperator(colRef, IntegerType.INT,
                ImmutableList.of("field_a"));

        ExecExpr result = ScalarOperatorToExecExpr.build(subfieldOp, ctx);

        assertInstanceOf(ExecSubfield.class, result);
        ExecSubfield subfield = (ExecSubfield) result;
        assertEquals(IntegerType.INT, subfield.getType());
        assertEquals(List.of("field_a"), subfield.getFieldNames());
        assertTrue(subfield.isCopyFlag());
        assertEquals(1, subfield.getNumChildren());
    }

    @Test
    public void testVisitBetweenPredicate() {
        ConstantOperator col = ConstantOperator.createInt(5);
        ConstantOperator low = ConstantOperator.createInt(1);
        ConstantOperator high = ConstantOperator.createInt(10);
        BetweenPredicateOperator between = new BetweenPredicateOperator(false, col, low, high);

        ExecExpr result = ScalarOperatorToExecExpr.build(between, emptyContext());

        assertInstanceOf(ExecBetweenPredicate.class, result);
        ExecBetweenPredicate pred = (ExecBetweenPredicate) result;
        assertFalse(pred.isNotBetween());
        assertEquals(3, pred.getNumChildren());
    }

    @Test
    public void testVisitBetweenPredicateNotBetween() {
        ConstantOperator col = ConstantOperator.createInt(5);
        ConstantOperator low = ConstantOperator.createInt(1);
        ConstantOperator high = ConstantOperator.createInt(10);
        BetweenPredicateOperator notBetween = new BetweenPredicateOperator(true, col, low, high);

        ExecExpr result = ScalarOperatorToExecExpr.build(notBetween, emptyContext());

        assertInstanceOf(ExecBetweenPredicate.class, result);
        ExecBetweenPredicate pred = (ExecBetweenPredicate) result;
        assertTrue(pred.isNotBetween());
    }

    @Test
    public void testBuildIgnoreSlot() {
        // buildIgnoreSlot should create ExecSlotRef directly from ColumnRefOperator
        ColumnRefOperator colRef = new ColumnRefOperator(5, IntegerType.INT, "col_x", false);

        ExecExpr result = ScalarOperatorToExecExpr.buildIgnoreSlot(colRef, emptyContext());

        assertInstanceOf(ExecSlotRef.class, result);
        ExecSlotRef slotRef = (ExecSlotRef) result;
        assertEquals(5, slotRef.getSlotId().asInt());
        assertEquals(IntegerType.INT, slotRef.getType());
    }

    @Test
    public void testVisitBinaryPredicateIndexOnlyFilter() {
        ConstantOperator left = ConstantOperator.createInt(1);
        ConstantOperator right = ConstantOperator.createInt(2);
        BinaryPredicateOperator binPred = new BinaryPredicateOperator(BinaryType.EQ, left, right);
        binPred.setIndexOnlyFilter(true);

        ExecExpr result = ScalarOperatorToExecExpr.build(binPred, emptyContext());

        assertInstanceOf(ExecBinaryPredicate.class, result);
        assertTrue(result.isIndexOnlyFilter());
    }

    @Test
    public void testVisitCompoundPredicateIndexOnlyFilter() {
        ConstantOperator t = ConstantOperator.createBoolean(true);
        ConstantOperator f = ConstantOperator.createBoolean(false);
        CompoundPredicateOperator andPred =
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, t, f);
        andPred.setIndexOnlyFilter(true);

        ExecExpr result = ScalarOperatorToExecExpr.build(andPred, emptyContext());

        assertTrue(result.isIndexOnlyFilter());
    }

    @Test
    public void testVisitCallOperatorMultipleArithmeticOps() {
        // Test multiply, divide, mod
        for (String opName : List.of("multiply", "divide", "int_divide", "mod",
                "bitand", "bitor", "bitxor", "bit_shift_left", "bit_shift_right",
                "bit_shift_right_logical")) {
            Function fn = makeScalarFunction(opName,
                    new Type[] {IntegerType.INT, IntegerType.INT}, IntegerType.INT);
            CallOperator call = new CallOperator(opName, IntegerType.INT,
                    List.of(ConstantOperator.createInt(10), ConstantOperator.createInt(3)), fn);

            ExecExpr result = ScalarOperatorToExecExpr.build(call, emptyContext());
            assertInstanceOf(ExecArithmetic.class, result,
                    "Expected ExecArithmetic for op: " + opName);
        }
    }

    @Test
    public void testVisitCallOperatorBitnot() {
        Function fn = makeScalarFunction("bitnot",
                new Type[] {IntegerType.INT}, IntegerType.INT);
        CallOperator call = new CallOperator("bitnot", IntegerType.INT,
                List.of(ConstantOperator.createInt(42)), fn);

        ExecExpr result = ScalarOperatorToExecExpr.build(call, emptyContext());

        assertInstanceOf(ExecArithmetic.class, result);
        ExecArithmetic arith = (ExecArithmetic) result;
        assertEquals(ArithmeticExpr.Operator.BITNOT, arith.getOp());
        assertEquals(1, arith.getNumChildren());
    }

    @Test
    public void testVisitConstantNullReplacesNullType() {
        // NULL_TYPE should be replaced with BOOLEAN
        ConstantOperator nullConst = ConstantOperator.createNull(
                com.starrocks.type.NullType.NULL);

        ExecExpr result = ScalarOperatorToExecExpr.build(nullConst, emptyContext());

        assertInstanceOf(ExecLiteral.class, result);
        ExecLiteral literal = (ExecLiteral) result;
        // NULL_TYPE gets replaced with BOOLEAN
        assertEquals(BooleanType.BOOLEAN, literal.getType());
    }
}
