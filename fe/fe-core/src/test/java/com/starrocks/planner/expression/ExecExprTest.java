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

package com.starrocks.planner.expression;

import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.SlotId;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.planner.TupleId;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.expression.ArithmeticExpr;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.MatchExpr;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.thrift.TDictQueryExpr;
import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TExprOpcode;
import com.starrocks.thrift.TKeysType;
import com.starrocks.type.ArrayType;
import com.starrocks.type.BooleanType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.MapType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive unit tests for the ExecExpr hierarchy, ExecExprVisitor,
 * ExecExprSerializer, ExecExprExplain, ThriftEnumConverter, and ExprOpcodeRegistry.
 */
public class ExecExprTest {

    // ======================================================================
    // Helper methods for constructing test objects
    // ======================================================================

    /**
     * Create a SlotDescriptor attached to a TupleDescriptor, simulating a resolved slot.
     */
    private static SlotDescriptor makeSlotDescriptor(int slotId, int tupleId, Type type, boolean nullable) {
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(tupleId));
        SlotDescriptor slotDesc = new SlotDescriptor(new SlotId(slotId), tupleDesc);
        slotDesc.setType(type);
        slotDesc.setIsNullable(nullable);
        return slotDesc;
    }

    /**
     * Create an ExecSlotRef from a fresh SlotDescriptor.
     */
    private static ExecSlotRef makeSlotRef(int slotId, int tupleId, Type type, boolean nullable) {
        SlotDescriptor desc = makeSlotDescriptor(slotId, tupleId, type, nullable);
        return new ExecSlotRef(desc);
    }

    /**
     * Create an ExecLiteral wrapping an integer constant.
     */
    private static ExecLiteral makeIntLiteral(int value) {
        ConstantOperator constOp = ConstantOperator.createInt(value);
        return new ExecLiteral(constOp, IntegerType.INT);
    }

    /**
     * Create a varchar ExecLiteral.
     */
    private static ExecLiteral makeVarcharLiteral(String value) {
        ConstantOperator constOp = ConstantOperator.createVarchar(value);
        return new ExecLiteral(constOp, VarcharType.VARCHAR);
    }

    /**
     * Create a null ExecLiteral.
     */
    private static ExecLiteral makeNullLiteral() {
        ConstantOperator constOp = ConstantOperator.createNull(IntegerType.INT);
        return new ExecLiteral(constOp, IntegerType.INT);
    }

    /**
     * Create a simple ScalarFunction for testing.
     */
    private static ScalarFunction makeScalarFunction(String name, Type[] argTypes, Type retType) {
        return new ScalarFunction(new FunctionName(name), argTypes, retType, false);
    }

    /**
     * Create an ExecFunctionCall with the given function name and children.
     */
    private static ExecFunctionCall makeFunctionCall(String fnName, Type retType, ScalarFunction fn,
                                                     List<ExecExpr> children) {
        return new ExecFunctionCall(retType, fn, fnName, children,
                false, false, false, false);
    }

    // ======================================================================
    // 1. ExecExpr type construction and properties
    // ======================================================================

    @Test
    public void testExecSlotRefConstruction() {
        SlotDescriptor desc = makeSlotDescriptor(5, 2, IntegerType.INT, true);
        ExecSlotRef slotRef = new ExecSlotRef(desc);

        assertEquals(5, slotRef.getSlotId().asInt());
        assertEquals(2, slotRef.getTupleId().asInt());
        assertEquals(IntegerType.INT, slotRef.getType());
        assertTrue(slotRef.isNullable());
        assertEquals(TExprNodeType.SLOT_REF, slotRef.getNodeType());
    }

    @Test
    public void testExecSlotRefNonNullable() {
        SlotDescriptor desc = makeSlotDescriptor(3, 1, IntegerType.BIGINT, false);
        ExecSlotRef slotRef = new ExecSlotRef(desc);

        assertEquals(3, slotRef.getSlotId().asInt());
        assertFalse(slotRef.isNullable());
        assertEquals(IntegerType.BIGINT, slotRef.getType());
    }

    @Test
    public void testExecSlotRefIsNotConstant() {
        ExecSlotRef slotRef = makeSlotRef(1, 0, IntegerType.INT, false);
        assertFalse(slotRef.isConstant());
    }

    @Test
    public void testExecSlotRefIsSelfMonotonic() {
        ExecSlotRef slotRef = makeSlotRef(1, 0, IntegerType.INT, false);
        assertTrue(slotRef.isSelfMonotonic());
        assertTrue(slotRef.isMonotonic());
    }

    @Test
    public void testExecSlotRefWithLabel() {
        SlotDescriptor desc = makeSlotDescriptor(7, 3, IntegerType.INT, true);
        ExecSlotRef slotRef = new ExecSlotRef("my_column", desc);

        assertEquals("my_column", slotRef.getLabel());
        assertEquals(7, slotRef.getSlotId().asInt());
    }

    @Test
    public void testExecSlotRefClone() {
        ExecSlotRef original = makeSlotRef(5, 2, IntegerType.INT, true);
        ExecSlotRef cloned = original.clone();

        assertEquals(original.getSlotId().asInt(), cloned.getSlotId().asInt());
        assertEquals(original.getType(), cloned.getType());
        assertEquals(original.isNullable(), cloned.isNullable());
    }

    @Test
    public void testExecLiteralConstruction() {
        ExecLiteral literal = makeIntLiteral(42);

        assertEquals(IntegerType.INT, literal.getType());
        assertTrue(literal.isConstant());
        assertFalse(literal.isNullable());
        assertEquals(TExprNodeType.INT_LITERAL, literal.getNodeType());

        ConstantOperator value = literal.getValue();
        assertNotNull(value);
        assertEquals(42, value.getInt());
    }

    @Test
    public void testExecLiteralNull() {
        ExecLiteral nullLiteral = makeNullLiteral();

        assertTrue(nullLiteral.isNullable());
        assertTrue(nullLiteral.isConstant());
        assertEquals(TExprNodeType.NULL_LITERAL, nullLiteral.getNodeType());
    }

    @Test
    public void testExecLiteralVarchar() {
        ExecLiteral literal = makeVarcharLiteral("hello");

        assertEquals(VarcharType.VARCHAR, literal.getType());
        assertEquals(TExprNodeType.STRING_LITERAL, literal.getNodeType());
        assertFalse(literal.isNullable());
        assertEquals("hello", literal.getValue().getVarchar());
    }

    @Test
    public void testExecLiteralBoolean() {
        ConstantOperator constOp = ConstantOperator.createBoolean(true);
        ExecLiteral literal = new ExecLiteral(constOp, BooleanType.BOOLEAN);

        assertEquals(TExprNodeType.BOOL_LITERAL, literal.getNodeType());
        assertTrue(literal.getValue().getBoolean());
    }

    @Test
    public void testExecLiteralIsSelfMonotonic() {
        ExecLiteral literal = makeIntLiteral(10);
        assertTrue(literal.isSelfMonotonic());
    }

    @Test
    public void testExecLiteralClone() {
        ExecLiteral original = makeIntLiteral(99);
        ExecLiteral cloned = original.clone();

        assertEquals(original.getValue().getInt(), cloned.getValue().getInt());
        assertEquals(original.getType(), cloned.getType());
    }

    @Test
    public void testExecFunctionCallConstruction() {
        ExecSlotRef child1 = makeSlotRef(1, 0, IntegerType.INT, true);
        ExecLiteral child2 = makeIntLiteral(10);
        List<ExecExpr> children = List.of(child1, child2);

        ScalarFunction fn = makeScalarFunction("add", new Type[]{IntegerType.INT, IntegerType.INT}, IntegerType.INT);
        ExecFunctionCall funcCall = makeFunctionCall("add", IntegerType.INT, fn, children);

        assertNotNull(funcCall.getFn());
        assertEquals("add", funcCall.getFnName());
        assertFalse(funcCall.isDistinct());
        assertEquals(2, funcCall.getNumChildren());
        assertEquals(TExprNodeType.FUNCTION_CALL, funcCall.getNodeType());
    }

    @Test
    public void testExecFunctionCallAggregateNodeType() {
        List<ExecExpr> children = List.of(makeSlotRef(1, 0, IntegerType.INT, true));
        ScalarFunction fn = makeScalarFunction("sum", new Type[]{IntegerType.INT}, IntegerType.BIGINT);

        ExecFunctionCall aggCall = new ExecFunctionCall(
                IntegerType.BIGINT, fn, "sum", children,
                false, false, true, false);

        assertEquals(TExprNodeType.AGG_EXPR, aggCall.getNodeType());
        assertTrue(aggCall.isAggregateOrAnalytic());
    }

    @Test
    public void testExecFunctionCallDistinct() {
        List<ExecExpr> children = List.of(makeSlotRef(1, 0, IntegerType.INT, true));
        ScalarFunction fn = makeScalarFunction("count", new Type[]{IntegerType.INT}, IntegerType.BIGINT);

        ExecFunctionCall funcCall = new ExecFunctionCall(
                IntegerType.BIGINT, fn, "count", children,
                true, false, true, false);

        assertTrue(funcCall.isDistinct());
    }

    @Test
    public void testExecFunctionCallClone() {
        List<ExecExpr> children = new ArrayList<>(List.of(makeSlotRef(1, 0, IntegerType.INT, true)));
        ScalarFunction fn = makeScalarFunction("abs", new Type[]{IntegerType.INT}, IntegerType.INT);
        ExecFunctionCall original = makeFunctionCall("abs", IntegerType.INT, fn, children);

        ExecFunctionCall cloned = original.clone();
        assertEquals(original.getFnName(), cloned.getFnName());
        assertEquals(original.getNumChildren(), cloned.getNumChildren());
    }

    @Test
    public void testExecFunctionCallCountStar() {
        ScalarFunction fn = makeScalarFunction("count", new Type[]{}, IntegerType.BIGINT);
        ExecFunctionCall countStar = new ExecFunctionCall(
                IntegerType.BIGINT, fn, "count", List.of(),
                false, false, true, false, true);

        assertTrue(countStar.isCountStar());
    }

    @Test
    public void testExecCastConstruction() {
        ExecLiteral child = makeIntLiteral(42);
        ExecCast cast = new ExecCast(IntegerType.BIGINT, child, false);

        assertEquals(IntegerType.BIGINT, cast.getType());
        assertEquals(1, cast.getNumChildren());
        assertEquals(TExprNodeType.CAST_EXPR, cast.getNodeType());
        assertFalse(cast.isImplicit());
    }

    @Test
    public void testExecCastImplicit() {
        ExecLiteral child = makeIntLiteral(42);
        ExecCast cast = new ExecCast(IntegerType.BIGINT, child, true);

        assertTrue(cast.isImplicit());
    }

    @Test
    public void testExecCastNullability() {
        // Cast of non-nullable child should be non-nullable
        ExecLiteral nonNull = makeIntLiteral(42);
        ExecCast castNonNull = new ExecCast(IntegerType.BIGINT, nonNull, false);
        assertFalse(castNonNull.isNullable());

        // Cast of nullable child should be nullable
        ExecSlotRef nullable = makeSlotRef(1, 0, IntegerType.INT, true);
        ExecCast castNullable = new ExecCast(IntegerType.BIGINT, nullable, false);
        assertTrue(castNullable.isNullable());
    }

    @Test
    public void testExecCastIsSelfMonotonic() {
        ExecLiteral child = makeIntLiteral(1);
        ExecCast cast = new ExecCast(IntegerType.BIGINT, child, false);
        assertTrue(cast.isSelfMonotonic());
    }

    @Test
    public void testExecCastClone() {
        ExecLiteral child = makeIntLiteral(42);
        ExecCast original = new ExecCast(IntegerType.BIGINT, child, false);
        ExecCast cloned = original.clone();

        assertEquals(original.getType(), cloned.getType());
        assertEquals(original.getNumChildren(), cloned.getNumChildren());
        assertEquals(original.isImplicit(), cloned.isImplicit());
    }

    @Test
    public void testExecBinaryPredicateConstruction() {
        ExecSlotRef left = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLiteral right = makeIntLiteral(10);

        ExecBinaryPredicate pred = new ExecBinaryPredicate(BinaryType.EQ, left, right);

        assertEquals(BinaryType.EQ, pred.getOp());
        assertEquals(BooleanType.BOOLEAN, pred.getType());
        assertEquals(2, pred.getNumChildren());
        assertEquals(TExprNodeType.BINARY_PRED, pred.getNodeType());
    }

    @Test
    public void testExecBinaryPredicateNullability() {
        ExecSlotRef nullableSlot = makeSlotRef(1, 0, IntegerType.INT, true);
        ExecLiteral literal = makeIntLiteral(5);

        // EQ with nullable child -> nullable
        ExecBinaryPredicate eqPred = new ExecBinaryPredicate(BinaryType.EQ, nullableSlot, literal);
        assertTrue(eqPred.isNullable());

        // EQ_FOR_NULL -> always non-nullable
        ExecBinaryPredicate eqNullPred = new ExecBinaryPredicate(BinaryType.EQ_FOR_NULL, nullableSlot, literal);
        assertFalse(eqNullPred.isNullable());
    }

    @Test
    public void testExecBinaryPredicateAllOps() {
        ExecSlotRef left = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLiteral right = makeIntLiteral(10);

        for (BinaryType op : BinaryType.values()) {
            ExecBinaryPredicate pred = new ExecBinaryPredicate(op, left, right);
            assertEquals(op, pred.getOp());
            assertEquals(TExprNodeType.BINARY_PRED, pred.getNodeType());
        }
    }

    @Test
    public void testExecBinaryPredicateClone() {
        ExecSlotRef left = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLiteral right = makeIntLiteral(10);
        ExecBinaryPredicate original = new ExecBinaryPredicate(BinaryType.LT, left, right);

        ExecBinaryPredicate cloned = original.clone();
        assertEquals(original.getOp(), cloned.getOp());
        assertEquals(original.getNumChildren(), cloned.getNumChildren());
    }

    // ======================================================================
    // ExecExpr base class properties
    // ======================================================================

    @Test
    public void testExecExprChildManipulation() {
        ExecSlotRef slotRef = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLiteral literal = makeIntLiteral(42);

        List<ExecExpr> children = new ArrayList<>(List.of(slotRef, literal));
        ScalarFunction fn = makeScalarFunction("add", new Type[]{IntegerType.INT, IntegerType.INT}, IntegerType.INT);
        ExecFunctionCall funcCall = makeFunctionCall("add", IntegerType.INT, fn, children);

        assertEquals(2, funcCall.getNumChildren());
        assertEquals(slotRef, funcCall.getChild(0));
        assertEquals(literal, funcCall.getChild(1));

        // Replace a child
        ExecLiteral newChild = makeIntLiteral(99);
        funcCall.setChild(1, newChild);
        assertEquals(newChild, funcCall.getChild(1));
    }

    @Test
    public void testExecExprAddChild() {
        ExecSlotRef slotRef = makeSlotRef(1, 0, IntegerType.INT, false);
        // Start with no children
        assertEquals(0, slotRef.getNumChildren());

        // addChild should work even when children starts as empty list
        slotRef.addChild(makeIntLiteral(1));
        assertEquals(1, slotRef.getNumChildren());
    }

    @Test
    public void testExecExprOriginType() {
        ExecLiteral literal = makeIntLiteral(42);
        // When originType is null, getOriginType returns type
        assertEquals(IntegerType.INT, literal.getOriginType());

        // After setting originType, it returns the explicitly set value
        literal.setOriginType(IntegerType.BIGINT);
        assertEquals(IntegerType.BIGINT, literal.getOriginType());
    }

    @Test
    public void testExecExprIsConstantVacuouslyTrue() {
        // A leaf expression with no children should be constant (vacuously true)
        // ExecLiteral overrides to return true
        ExecLiteral literal = makeIntLiteral(42);
        assertTrue(literal.isConstant());
    }

    @Test
    public void testExecExprIsConstantWithNonConstantChild() {
        // FunctionCall with a slotRef child should not be constant
        ExecSlotRef slot = makeSlotRef(1, 0, IntegerType.INT, false);
        List<ExecExpr> children = new ArrayList<>(List.of(slot));
        ScalarFunction fn = makeScalarFunction("abs", new Type[]{IntegerType.INT}, IntegerType.INT);
        ExecFunctionCall funcCall = makeFunctionCall("abs", IntegerType.INT, fn, children);

        assertFalse(funcCall.isConstant());
    }

    @Test
    public void testExecExprIsConstantWithAllConstantChildren() {
        // FunctionCall with all literal children should be constant
        List<ExecExpr> children = new ArrayList<>(List.of(makeIntLiteral(1), makeIntLiteral(2)));
        ScalarFunction fn = makeScalarFunction("add", new Type[]{IntegerType.INT, IntegerType.INT}, IntegerType.INT);
        ExecFunctionCall funcCall = makeFunctionCall("add", IntegerType.INT, fn, children);

        assertTrue(funcCall.isConstant());
    }

    @Test
    public void testExecExprHasNullableChild() {
        ExecSlotRef nullableSlot = makeSlotRef(1, 0, IntegerType.INT, true);
        ExecLiteral literal = makeIntLiteral(10);
        List<ExecExpr> children = new ArrayList<>(List.of(nullableSlot, literal));
        ScalarFunction fn = makeScalarFunction("add", new Type[]{IntegerType.INT, IntegerType.INT}, IntegerType.INT);
        ExecFunctionCall funcCall = makeFunctionCall("add", IntegerType.INT, fn, children);

        assertTrue(funcCall.hasNullableChild());
    }

    @Test
    public void testExecExprIndexOnlyFilter() {
        ExecLiteral literal = makeIntLiteral(42);
        assertFalse(literal.isIndexOnlyFilter());

        literal.setIsIndexOnlyFilter(true);
        assertTrue(literal.isIndexOnlyFilter());
    }

    // ======================================================================
    // 2. ExecExprVisitor
    // ======================================================================

    /**
     * A simple counting visitor that counts nodes in the tree by visiting each node
     * and recursing into children.
     */
    private static class NodeCountingVisitor implements ExecExprVisitor<Integer, Void> {
        @Override
        public Integer visitExecExpr(ExecExpr expr, Void context) {
            int count = 1;
            for (ExecExpr child : expr.getChildren()) {
                count += child.accept(this, context);
            }
            return count;
        }
    }

    @Test
    public void testVisitorCountsThreeNodes() {
        // Tree: FunctionCall(SlotRef, Literal) -- 3 nodes
        ExecSlotRef slotRef = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLiteral literal = makeIntLiteral(42);
        List<ExecExpr> children = new ArrayList<>(List.of(slotRef, literal));
        ScalarFunction fn = makeScalarFunction("add", new Type[]{IntegerType.INT, IntegerType.INT}, IntegerType.INT);
        ExecFunctionCall funcCall = makeFunctionCall("add", IntegerType.INT, fn, children);

        NodeCountingVisitor counter = new NodeCountingVisitor();
        int count = funcCall.accept(counter, null);
        assertEquals(3, count);
    }

    @Test
    public void testVisitorCountsSingleNode() {
        ExecLiteral literal = makeIntLiteral(42);
        NodeCountingVisitor counter = new NodeCountingVisitor();
        assertEquals(1, literal.accept(counter, null));
    }

    @Test
    public void testVisitorCountsNestedTree() {
        // Tree: Cast(BinaryPredicate(SlotRef, Literal)) -- 4 nodes
        ExecSlotRef slot = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLiteral literal = makeIntLiteral(10);
        ExecBinaryPredicate pred = new ExecBinaryPredicate(BinaryType.EQ, slot, literal);
        ExecCast cast = new ExecCast(IntegerType.INT, pred, false);

        NodeCountingVisitor counter = new NodeCountingVisitor();
        assertEquals(4, cast.accept(counter, null));
    }

    /**
     * A visitor that tracks which visit methods are dispatched.
     */
    private static class DispatchTrackingVisitor implements ExecExprVisitor<String, Void> {
        @Override
        public String visitExecExpr(ExecExpr expr, Void context) {
            return "ExecExpr";
        }

        @Override
        public String visitExecSlotRef(ExecSlotRef expr, Void context) {
            return "SlotRef";
        }

        @Override
        public String visitExecLiteral(ExecLiteral expr, Void context) {
            return "Literal";
        }

        @Override
        public String visitExecFunctionCall(ExecFunctionCall expr, Void context) {
            return "FunctionCall";
        }

        @Override
        public String visitExecCast(ExecCast expr, Void context) {
            return "Cast";
        }

        @Override
        public String visitExecBinaryPredicate(ExecBinaryPredicate expr, Void context) {
            return "BinaryPredicate";
        }
    }

    @Test
    public void testVisitorDispatchSlotRef() {
        ExecSlotRef slotRef = makeSlotRef(1, 0, IntegerType.INT, false);
        assertEquals("SlotRef", slotRef.accept(new DispatchTrackingVisitor(), null));
    }

    @Test
    public void testVisitorDispatchLiteral() {
        ExecLiteral literal = makeIntLiteral(42);
        assertEquals("Literal", literal.accept(new DispatchTrackingVisitor(), null));
    }

    @Test
    public void testVisitorDispatchFunctionCall() {
        ScalarFunction fn = makeScalarFunction("abs", new Type[]{IntegerType.INT}, IntegerType.INT);
        ExecFunctionCall funcCall = makeFunctionCall("abs", IntegerType.INT, fn, new ArrayList<>());
        assertEquals("FunctionCall", funcCall.accept(new DispatchTrackingVisitor(), null));
    }

    @Test
    public void testVisitorDispatchCast() {
        ExecCast cast = new ExecCast(IntegerType.BIGINT, makeIntLiteral(42), false);
        assertEquals("Cast", cast.accept(new DispatchTrackingVisitor(), null));
    }

    @Test
    public void testVisitorDispatchBinaryPredicate() {
        ExecBinaryPredicate pred = new ExecBinaryPredicate(BinaryType.EQ,
                makeSlotRef(1, 0, IntegerType.INT, false), makeIntLiteral(10));
        assertEquals("BinaryPredicate", pred.accept(new DispatchTrackingVisitor(), null));
    }

    @Test
    public void testVisitorDefaultFallback() {
        // An unoverridden visit method falls back to visitExecExpr
        ExecExprVisitor<String, Void> minimalVisitor = new ExecExprVisitor<>() {
            @Override
            public String visitExecExpr(ExecExpr expr, Void context) {
                return "default";
            }
        };

        ExecSlotRef slotRef = makeSlotRef(1, 0, IntegerType.INT, false);
        assertEquals("default", slotRef.accept(minimalVisitor, null));

        ExecLiteral literal = makeIntLiteral(42);
        assertEquals("default", literal.accept(minimalVisitor, null));
    }

    // ======================================================================
    // 3. ExecExprSerializer
    // ======================================================================

    @Test
    public void testSerializeIntLiteral() {
        ExecLiteral literal = makeIntLiteral(42);
        TExpr texpr = ExecExprSerializer.serialize(literal);

        assertNotNull(texpr);
        assertNotNull(texpr.getNodes());
        assertEquals(1, texpr.getNodes().size());

        TExprNode node = texpr.getNodes().get(0);
        assertEquals(TExprNodeType.INT_LITERAL, node.node_type);
        assertNotNull(node.int_literal);
        assertEquals(42, node.int_literal.value);
        assertEquals(0, node.num_children);
    }

    @Test
    public void testSerializeBoolLiteral() {
        ConstantOperator constOp = ConstantOperator.createBoolean(true);
        ExecLiteral literal = new ExecLiteral(constOp, BooleanType.BOOLEAN);
        TExpr texpr = ExecExprSerializer.serialize(literal);

        assertEquals(1, texpr.getNodes().size());
        TExprNode node = texpr.getNodes().get(0);
        assertEquals(TExprNodeType.BOOL_LITERAL, node.node_type);
        assertNotNull(node.bool_literal);
        assertTrue(node.bool_literal.value);
    }

    @Test
    public void testSerializeVarcharLiteral() {
        ExecLiteral literal = makeVarcharLiteral("hello");
        TExpr texpr = ExecExprSerializer.serialize(literal);

        assertEquals(1, texpr.getNodes().size());
        TExprNode node = texpr.getNodes().get(0);
        assertEquals(TExprNodeType.STRING_LITERAL, node.node_type);
        assertNotNull(node.string_literal);
        assertEquals("hello", node.string_literal.value);
    }

    @Test
    public void testSerializeNullLiteral() {
        ExecLiteral nullLiteral = makeNullLiteral();
        TExpr texpr = ExecExprSerializer.serialize(nullLiteral);

        assertEquals(1, texpr.getNodes().size());
        TExprNode node = texpr.getNodes().get(0);
        assertEquals(TExprNodeType.NULL_LITERAL, node.node_type);
    }

    @Test
    public void testSerializeSlotRef() {
        ExecSlotRef slotRef = makeSlotRef(5, 2, IntegerType.INT, true);
        TExpr texpr = ExecExprSerializer.serialize(slotRef);

        assertNotNull(texpr);
        assertEquals(1, texpr.getNodes().size());

        TExprNode node = texpr.getNodes().get(0);
        assertEquals(TExprNodeType.SLOT_REF, node.node_type);
        assertNotNull(node.slot_ref);
        assertEquals(5, node.slot_ref.slot_id);
        assertEquals(2, node.slot_ref.tuple_id);
        assertTrue(node.is_nullable);
        assertEquals(0, node.num_children);
    }

    @Test
    public void testSerializeSlotRefNonNullable() {
        ExecSlotRef slotRef = makeSlotRef(3, 1, IntegerType.INT, false);
        TExpr texpr = ExecExprSerializer.serialize(slotRef);

        TExprNode node = texpr.getNodes().get(0);
        assertFalse(node.is_nullable);
    }

    @Test
    public void testSerializeFunctionCallWithChildren() {
        ExecSlotRef child1 = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLiteral child2 = makeIntLiteral(10);
        List<ExecExpr> children = new ArrayList<>(List.of(child1, child2));

        ScalarFunction fn = makeScalarFunction("add", new Type[]{IntegerType.INT, IntegerType.INT}, IntegerType.INT);
        ExecFunctionCall funcCall = makeFunctionCall("add", IntegerType.INT, fn, children);

        TExpr texpr = ExecExprSerializer.serialize(funcCall);

        assertNotNull(texpr);
        // Should have 3 nodes: funcCall, slotRef, literal (pre-order DFS)
        assertEquals(3, texpr.getNodes().size());

        TExprNode funcNode = texpr.getNodes().get(0);
        assertEquals(TExprNodeType.FUNCTION_CALL, funcNode.node_type);
        assertEquals(2, funcNode.num_children);

        TExprNode slotNode = texpr.getNodes().get(1);
        assertEquals(TExprNodeType.SLOT_REF, slotNode.node_type);

        TExprNode literalNode = texpr.getNodes().get(2);
        assertEquals(TExprNodeType.INT_LITERAL, literalNode.node_type);
    }

    @Test
    public void testSerializeAggFunctionCall() {
        List<ExecExpr> children = new ArrayList<>(List.of(makeSlotRef(1, 0, IntegerType.INT, true)));
        ScalarFunction fn = makeScalarFunction("sum", new Type[]{IntegerType.INT}, IntegerType.BIGINT);
        ExecFunctionCall aggCall = new ExecFunctionCall(
                IntegerType.BIGINT, fn, "sum", children,
                false, false, true, false);

        TExpr texpr = ExecExprSerializer.serialize(aggCall);
        TExprNode aggNode = texpr.getNodes().get(0);
        assertEquals(TExprNodeType.AGG_EXPR, aggNode.node_type);
        assertNotNull(aggNode.agg_expr);
    }

    @Test
    public void testSerializeCastWithChild() {
        ExecLiteral child = makeIntLiteral(42);
        ExecCast cast = new ExecCast(IntegerType.BIGINT, child, false);

        TExpr texpr = ExecExprSerializer.serialize(cast);

        assertNotNull(texpr);
        // Should have 2 nodes: cast, literal
        assertEquals(2, texpr.getNodes().size());

        TExprNode castNode = texpr.getNodes().get(0);
        assertEquals(TExprNodeType.CAST_EXPR, castNode.node_type);
        assertEquals(1, castNode.num_children);
        assertEquals(TExprOpcode.CAST, castNode.opcode);

        TExprNode literalNode = texpr.getNodes().get(1);
        assertEquals(TExprNodeType.INT_LITERAL, literalNode.node_type);
    }

    @Test
    public void testSerializeBinaryPredicate() {
        ExecSlotRef left = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLiteral right = makeIntLiteral(10);
        ExecBinaryPredicate pred = new ExecBinaryPredicate(BinaryType.EQ, left, right);

        TExpr texpr = ExecExprSerializer.serialize(pred);

        assertEquals(3, texpr.getNodes().size());

        TExprNode predNode = texpr.getNodes().get(0);
        assertEquals(TExprNodeType.BINARY_PRED, predNode.node_type);
        assertEquals(2, predNode.num_children);
        assertEquals(TExprOpcode.EQ, predNode.opcode);
    }

    @Test
    public void testSerializeCommonFields() {
        // Verify common fields are populated: type, is_nullable, is_monotonic, output_scale, is_index_only_filter
        ExecSlotRef slotRef = makeSlotRef(1, 0, IntegerType.INT, true);
        slotRef.setIsIndexOnlyFilter(true);

        TExpr texpr = ExecExprSerializer.serialize(slotRef);
        TExprNode node = texpr.getNodes().get(0);

        assertNotNull(node.type);
        assertTrue(node.is_nullable);
        assertTrue(node.is_monotonic); // SlotRef is self-monotonic
        assertEquals(-1, node.output_scale);
        assertTrue(node.is_index_only_filter);
    }

    @Test
    public void testSerializeList() {
        ExecLiteral lit1 = makeIntLiteral(1);
        ExecLiteral lit2 = makeIntLiteral(2);
        ExecLiteral lit3 = makeIntLiteral(3);

        List<TExpr> results = ExecExprSerializer.serializeList(List.of(lit1, lit2, lit3));
        assertEquals(3, results.size());
        for (TExpr texpr : results) {
            assertEquals(1, texpr.getNodes().size());
            assertEquals(TExprNodeType.INT_LITERAL, texpr.getNodes().get(0).node_type);
        }
    }

    @Test
    public void testSerializeDeepTree() {
        // Build a deeper tree: BinaryPred(Cast(SlotRef), FuncCall(Literal))
        ExecSlotRef slot = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecCast cast = new ExecCast(IntegerType.BIGINT, slot, false);
        ExecLiteral literal = makeIntLiteral(10);
        ScalarFunction fn = makeScalarFunction("abs", new Type[]{IntegerType.INT}, IntegerType.INT);
        ExecFunctionCall funcCall = makeFunctionCall("abs", IntegerType.INT, fn, new ArrayList<>(List.of(literal)));
        ExecBinaryPredicate pred = new ExecBinaryPredicate(BinaryType.GT, cast, funcCall);

        TExpr texpr = ExecExprSerializer.serialize(pred);
        // 5 nodes: pred, cast, slot, funcCall, literal
        assertEquals(5, texpr.getNodes().size());

        assertEquals(TExprNodeType.BINARY_PRED, texpr.getNodes().get(0).node_type);
        assertEquals(TExprNodeType.CAST_EXPR, texpr.getNodes().get(1).node_type);
        assertEquals(TExprNodeType.SLOT_REF, texpr.getNodes().get(2).node_type);
        assertEquals(TExprNodeType.FUNCTION_CALL, texpr.getNodes().get(3).node_type);
        assertEquals(TExprNodeType.INT_LITERAL, texpr.getNodes().get(4).node_type);
    }

    // ======================================================================
    // 4. ExecExprExplain
    // ======================================================================

    @Test
    public void testExplainSlotRefWithoutLabel() {
        ExecSlotRef slotRef = makeSlotRef(5, 0, IntegerType.INT, false);
        String explain = ExecExprExplain.explain(slotRef);

        // Format: "<slot N>" where N is the slot ID
        assertEquals("<slot 5>", explain);
    }

    @Test
    public void testExplainSlotRefWithLabel() {
        SlotDescriptor desc = makeSlotDescriptor(5, 0, IntegerType.INT, false);
        ExecSlotRef slotRef = new ExecSlotRef("my_col", desc);
        String explain = ExecExprExplain.explain(slotRef);

        assertEquals("my_col", explain);
    }

    @Test
    public void testExplainIntLiteral() {
        ExecLiteral literal = makeIntLiteral(42);
        String explain = ExecExprExplain.explain(literal);

        assertEquals("42", explain);
    }

    @Test
    public void testExplainBigintLiteral() {
        ConstantOperator constOp = ConstantOperator.createBigint(123456789L);
        ExecLiteral literal = new ExecLiteral(constOp, IntegerType.BIGINT);
        String explain = ExecExprExplain.explain(literal);

        assertEquals("123456789", explain);
    }

    @Test
    public void testExplainBoolLiteral() {
        ConstantOperator trueOp = ConstantOperator.createBoolean(true);
        ExecLiteral trueLiteral = new ExecLiteral(trueOp, BooleanType.BOOLEAN);
        assertEquals("TRUE", ExecExprExplain.explain(trueLiteral));

        ConstantOperator falseOp = ConstantOperator.createBoolean(false);
        ExecLiteral falseLiteral = new ExecLiteral(falseOp, BooleanType.BOOLEAN);
        assertEquals("FALSE", ExecExprExplain.explain(falseLiteral));
    }

    @Test
    public void testExplainVarcharLiteral() {
        ExecLiteral literal = makeVarcharLiteral("hello world");
        String explain = ExecExprExplain.explain(literal);

        assertEquals("'hello world'", explain);
    }

    @Test
    public void testExplainVarcharLiteralWithEscapes() {
        ExecLiteral literal = makeVarcharLiteral("it's a \"test\"");
        String explain = ExecExprExplain.explain(literal);

        assertEquals("'it\\'s a \"test\"'", explain);
    }

    @Test
    public void testExplainNullLiteral() {
        ExecLiteral nullLiteral = makeNullLiteral();
        assertEquals("NULL", ExecExprExplain.explain(nullLiteral));
    }

    @Test
    public void testExplainFunctionCall() {
        ExecSlotRef slot = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLiteral literal = makeIntLiteral(10);
        List<ExecExpr> children = new ArrayList<>(List.of(slot, literal));
        ScalarFunction fn = makeScalarFunction("add", new Type[]{IntegerType.INT, IntegerType.INT}, IntegerType.INT);
        ExecFunctionCall funcCall = makeFunctionCall("add", IntegerType.INT, fn, children);

        String explain = ExecExprExplain.explain(funcCall);
        // Format: "fnName(child1, child2)"
        assertEquals("add(<slot 1>, 10)", explain);
    }

    @Test
    public void testExplainFunctionCallDistinct() {
        ExecSlotRef slot = makeSlotRef(1, 0, IntegerType.INT, false);
        List<ExecExpr> children = new ArrayList<>(List.of(slot));
        ScalarFunction fn = makeScalarFunction("count", new Type[]{IntegerType.INT}, IntegerType.BIGINT);
        ExecFunctionCall funcCall = new ExecFunctionCall(
                IntegerType.BIGINT, fn, "count", children,
                true, false, true, false);

        String explain = ExecExprExplain.explain(funcCall);
        assertTrue(explain.contains("DISTINCT"));
    }

    @Test
    public void testExplainFunctionCallCountStar() {
        ScalarFunction fn = makeScalarFunction("count", new Type[]{}, IntegerType.BIGINT);
        ExecFunctionCall countStar = new ExecFunctionCall(
                IntegerType.BIGINT, fn, "count", List.of(),
                false, false, true, false, true);

        String explain = ExecExprExplain.explain(countStar);
        assertEquals("count(*)", explain);
    }

    @Test
    public void testExplainCast() {
        ExecLiteral child = makeIntLiteral(42);
        ExecCast cast = new ExecCast(IntegerType.BIGINT, child, false);
        String explain = ExecExprExplain.explain(cast);

        assertEquals("CAST(42 AS BIGINT)", explain);
    }

    @Test
    public void testExplainBinaryPredicate() {
        ExecSlotRef left = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLiteral right = makeIntLiteral(10);

        assertEquals("<slot 1> = 10",
                ExecExprExplain.explain(new ExecBinaryPredicate(BinaryType.EQ, left, right)));
        assertEquals("<slot 1> != 10",
                ExecExprExplain.explain(new ExecBinaryPredicate(BinaryType.NE, left, right)));
        assertEquals("<slot 1> < 10",
                ExecExprExplain.explain(new ExecBinaryPredicate(BinaryType.LT, left, right)));
        assertEquals("<slot 1> <= 10",
                ExecExprExplain.explain(new ExecBinaryPredicate(BinaryType.LE, left, right)));
        assertEquals("<slot 1> > 10",
                ExecExprExplain.explain(new ExecBinaryPredicate(BinaryType.GT, left, right)));
        assertEquals("<slot 1> >= 10",
                ExecExprExplain.explain(new ExecBinaryPredicate(BinaryType.GE, left, right)));
        assertEquals("<slot 1> <=> 10",
                ExecExprExplain.explain(new ExecBinaryPredicate(BinaryType.EQ_FOR_NULL, left, right)));
    }

    @Test
    public void testExplainNestedExpression() {
        // CAST(add(<slot 1>, 10) AS BIGINT)
        ExecSlotRef slot = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLiteral literal = makeIntLiteral(10);
        List<ExecExpr> children = new ArrayList<>(List.of(slot, literal));
        ScalarFunction fn = makeScalarFunction("add", new Type[]{IntegerType.INT, IntegerType.INT}, IntegerType.INT);
        ExecFunctionCall funcCall = makeFunctionCall("add", IntegerType.INT, fn, children);
        ExecCast cast = new ExecCast(IntegerType.BIGINT, funcCall, false);

        String explain = ExecExprExplain.explain(cast);
        assertEquals("CAST(add(<slot 1>, 10) AS BIGINT)", explain);
    }

    @Test
    public void testExplainList() {
        ExecLiteral lit1 = makeIntLiteral(1);
        ExecLiteral lit2 = makeIntLiteral(2);
        ExecLiteral lit3 = makeIntLiteral(3);

        String result = ExecExprExplain.explainList(List.of(lit1, lit2, lit3));
        assertEquals("1, 2, 3", result);
    }

    @Test
    public void testVerboseExplainSlotRef() {
        ExecSlotRef slotRef = makeSlotRef(5, 0, IntegerType.INT, true);
        String explain = ExecExprExplain.verboseExplain(slotRef);

        // Verbose format: [slotId, TYPE, nullable]
        assertEquals("[5, INT, true]", explain);
    }

    @Test
    public void testVerboseExplainSlotRefWithLabel() {
        SlotDescriptor desc = makeSlotDescriptor(5, 0, IntegerType.INT, false);
        ExecSlotRef slotRef = new ExecSlotRef("my_col", desc);
        String explain = ExecExprExplain.verboseExplain(slotRef);

        assertEquals("[my_col, INT, false]", explain);
    }

    @Test
    public void testVerboseExplainCast() {
        ExecSlotRef slot = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecCast cast = new ExecCast(IntegerType.BIGINT, slot, false);
        String explain = ExecExprExplain.verboseExplain(cast);

        assertTrue(explain.contains("cast("));
        assertTrue(explain.contains("BIGINT"));
    }

    @Test
    public void testToSqlVarchar() {
        ExecLiteral literal = makeVarcharLiteral("hello");
        String sql = ExecExprExplain.toSql(literal);
        assertEquals("'hello'", sql);
    }

    // ======================================================================
    // 5. ThriftEnumConverter
    // ======================================================================

    @Test
    public void testKeysTypeToThriftDupKeys() {
        assertEquals(TKeysType.DUP_KEYS, ThriftEnumConverter.keysTypeToThrift(KeysType.DUP_KEYS));
    }

    @Test
    public void testKeysTypeToThriftAggKeys() {
        assertEquals(TKeysType.AGG_KEYS, ThriftEnumConverter.keysTypeToThrift(KeysType.AGG_KEYS));
    }

    @Test
    public void testKeysTypeToThriftUniqueKeys() {
        assertEquals(TKeysType.UNIQUE_KEYS, ThriftEnumConverter.keysTypeToThrift(KeysType.UNIQUE_KEYS));
    }

    @Test
    public void testKeysTypeToThriftPrimaryKeys() {
        assertEquals(TKeysType.PRIMARY_KEYS, ThriftEnumConverter.keysTypeToThrift(KeysType.PRIMARY_KEYS));
    }

    @Test
    public void testKeysTypeToThriftAllValues() {
        // Verify every KeysType value has a mapping
        for (KeysType keysType : KeysType.values()) {
            TKeysType result = ThriftEnumConverter.keysTypeToThrift(keysType);
            assertNotNull(result);
        }
    }

    @Test
    public void testCompoundPredicateOperatorToThrift() {
        assertEquals(TExprOpcode.COMPOUND_AND,
                ThriftEnumConverter.compoundPredicateOperatorToThrift(
                        com.starrocks.sql.ast.expression.CompoundPredicate.Operator.AND));
        assertEquals(TExprOpcode.COMPOUND_OR,
                ThriftEnumConverter.compoundPredicateOperatorToThrift(
                        com.starrocks.sql.ast.expression.CompoundPredicate.Operator.OR));
        assertEquals(TExprOpcode.COMPOUND_NOT,
                ThriftEnumConverter.compoundPredicateOperatorToThrift(
                        com.starrocks.sql.ast.expression.CompoundPredicate.Operator.NOT));
    }

    @Test
    public void testSetTypeConversionRoundTrip() {
        // Test GLOBAL round-trip
        assertEquals(com.starrocks.sql.ast.SetType.GLOBAL,
                ThriftEnumConverter.setTypeFromThrift(
                        ThriftEnumConverter.setTypeToThrift(com.starrocks.sql.ast.SetType.GLOBAL)));

        // Test SESSION round-trip
        assertEquals(com.starrocks.sql.ast.SetType.SESSION,
                ThriftEnumConverter.setTypeFromThrift(
                        ThriftEnumConverter.setTypeToThrift(com.starrocks.sql.ast.SetType.SESSION)));

        // Test VERBOSE round-trip
        assertEquals(com.starrocks.sql.ast.SetType.VERBOSE,
                ThriftEnumConverter.setTypeFromThrift(
                        ThriftEnumConverter.setTypeToThrift(com.starrocks.sql.ast.SetType.VERBOSE)));
    }

    // ======================================================================
    // 6. ExprOpcodeRegistry
    // ======================================================================

    @Test
    public void testBinaryOpcodeEQ() {
        assertEquals(TExprOpcode.EQ, ExprOpcodeRegistry.getBinaryOpcode(BinaryType.EQ));
    }

    @Test
    public void testBinaryOpcodeLT() {
        assertEquals(TExprOpcode.LT, ExprOpcodeRegistry.getBinaryOpcode(BinaryType.LT));
    }

    @Test
    public void testBinaryOpcodeGT() {
        assertEquals(TExprOpcode.GT, ExprOpcodeRegistry.getBinaryOpcode(BinaryType.GT));
    }

    @Test
    public void testBinaryOpcodeLE() {
        assertEquals(TExprOpcode.LE, ExprOpcodeRegistry.getBinaryOpcode(BinaryType.LE));
    }

    @Test
    public void testBinaryOpcodeGE() {
        assertEquals(TExprOpcode.GE, ExprOpcodeRegistry.getBinaryOpcode(BinaryType.GE));
    }

    @Test
    public void testBinaryOpcodeNE() {
        assertEquals(TExprOpcode.NE, ExprOpcodeRegistry.getBinaryOpcode(BinaryType.NE));
    }

    @Test
    public void testBinaryOpcodeEqForNull() {
        assertEquals(TExprOpcode.EQ_FOR_NULL, ExprOpcodeRegistry.getBinaryOpcode(BinaryType.EQ_FOR_NULL));
    }

    @Test
    public void testAllBinaryOpcodes() {
        // Verify every BinaryType has a mapped opcode (not INVALID_OPCODE)
        for (BinaryType bt : BinaryType.values()) {
            TExprOpcode opcode = ExprOpcodeRegistry.getBinaryOpcode(bt);
            assertNotNull(opcode);
            assertTrue(opcode != TExprOpcode.INVALID_OPCODE,
                    "BinaryType " + bt + " should have a valid opcode mapping");
        }
    }

    @Test
    public void testArithmeticOpcodeAdd() {
        assertEquals(TExprOpcode.ADD, ExprOpcodeRegistry.getArithmeticOpcode(ArithmeticExpr.Operator.ADD));
    }

    @Test
    public void testArithmeticOpcodeSubtract() {
        assertEquals(TExprOpcode.SUBTRACT, ExprOpcodeRegistry.getArithmeticOpcode(ArithmeticExpr.Operator.SUBTRACT));
    }

    @Test
    public void testArithmeticOpcodeMultiply() {
        assertEquals(TExprOpcode.MULTIPLY, ExprOpcodeRegistry.getArithmeticOpcode(ArithmeticExpr.Operator.MULTIPLY));
    }

    @Test
    public void testArithmeticOpcodeDivide() {
        assertEquals(TExprOpcode.DIVIDE, ExprOpcodeRegistry.getArithmeticOpcode(ArithmeticExpr.Operator.DIVIDE));
    }

    @Test
    public void testArithmeticOpcodeMod() {
        assertEquals(TExprOpcode.MOD, ExprOpcodeRegistry.getArithmeticOpcode(ArithmeticExpr.Operator.MOD));
    }

    @Test
    public void testArithmeticOpcodeIntDivide() {
        assertEquals(TExprOpcode.INT_DIVIDE,
                ExprOpcodeRegistry.getArithmeticOpcode(ArithmeticExpr.Operator.INT_DIVIDE));
    }

    @Test
    public void testArithmeticOpcodeBitAnd() {
        assertEquals(TExprOpcode.BITAND, ExprOpcodeRegistry.getArithmeticOpcode(ArithmeticExpr.Operator.BITAND));
    }

    @Test
    public void testArithmeticOpcodeBitOr() {
        assertEquals(TExprOpcode.BITOR, ExprOpcodeRegistry.getArithmeticOpcode(ArithmeticExpr.Operator.BITOR));
    }

    @Test
    public void testArithmeticOpcodeBitXor() {
        assertEquals(TExprOpcode.BITXOR, ExprOpcodeRegistry.getArithmeticOpcode(ArithmeticExpr.Operator.BITXOR));
    }

    @Test
    public void testArithmeticOpcodeBitNot() {
        assertEquals(TExprOpcode.BITNOT, ExprOpcodeRegistry.getArithmeticOpcode(ArithmeticExpr.Operator.BITNOT));
    }

    @Test
    public void testArithmeticOpcodeFactorial() {
        assertEquals(TExprOpcode.FACTORIAL,
                ExprOpcodeRegistry.getArithmeticOpcode(ArithmeticExpr.Operator.FACTORIAL));
    }

    @Test
    public void testArithmeticOpcodeBitShiftLeft() {
        assertEquals(TExprOpcode.BIT_SHIFT_LEFT,
                ExprOpcodeRegistry.getArithmeticOpcode(ArithmeticExpr.Operator.BIT_SHIFT_LEFT));
    }

    @Test
    public void testArithmeticOpcodeBitShiftRight() {
        assertEquals(TExprOpcode.BIT_SHIFT_RIGHT,
                ExprOpcodeRegistry.getArithmeticOpcode(ArithmeticExpr.Operator.BIT_SHIFT_RIGHT));
    }

    @Test
    public void testArithmeticOpcodeBitShiftRightLogical() {
        assertEquals(TExprOpcode.BIT_SHIFT_RIGHT_LOGICAL,
                ExprOpcodeRegistry.getArithmeticOpcode(ArithmeticExpr.Operator.BIT_SHIFT_RIGHT_LOGICAL));
    }

    @Test
    public void testAllArithmeticOpcodes() {
        // Verify every ArithmeticExpr.Operator has a mapped opcode
        for (ArithmeticExpr.Operator op : ArithmeticExpr.Operator.values()) {
            TExprOpcode opcode = ExprOpcodeRegistry.getArithmeticOpcode(op);
            assertNotNull(opcode);
            assertTrue(opcode != TExprOpcode.INVALID_OPCODE,
                    "ArithmeticExpr.Operator " + op + " should have a valid opcode mapping");
        }
    }

    @Test
    public void testGetCastOpcode() {
        assertEquals(TExprOpcode.CAST, ExprOpcodeRegistry.getCastOpcode());
    }

    @Test
    public void testGetInPredicateOpcode() {
        assertEquals(TExprOpcode.FILTER_IN, ExprOpcodeRegistry.getInPredicateOpcode(false));
        assertEquals(TExprOpcode.FILTER_NOT_IN, ExprOpcodeRegistry.getInPredicateOpcode(true));
    }

    @Test
    public void testGetMatchOpcode() {
        assertEquals(TExprOpcode.MATCH,
                ExprOpcodeRegistry.getMatchOpcode(com.starrocks.sql.ast.expression.MatchExpr.MatchOperator.MATCH));
        assertEquals(TExprOpcode.MATCH_ANY,
                ExprOpcodeRegistry.getMatchOpcode(
                        com.starrocks.sql.ast.expression.MatchExpr.MatchOperator.MATCH_ANY));
        assertEquals(TExprOpcode.MATCH_ALL,
                ExprOpcodeRegistry.getMatchOpcode(
                        com.starrocks.sql.ast.expression.MatchExpr.MatchOperator.MATCH_ALL));
    }

    // ======================================================================
    // 7. ExecCompoundPredicate
    // ======================================================================

    @Test
    public void testExecCompoundPredicateAndConstruction() {
        ExecSlotRef left = makeSlotRef(1, 0, BooleanType.BOOLEAN, false);
        ExecSlotRef right = makeSlotRef(2, 0, BooleanType.BOOLEAN, false);

        ExecCompoundPredicate pred = new ExecCompoundPredicate(CompoundPredicate.Operator.AND, left, right);

        assertEquals(CompoundPredicate.Operator.AND, pred.getCompoundType());
        assertEquals(BooleanType.BOOLEAN, pred.getType());
        assertEquals(2, pred.getNumChildren());
        assertEquals(TExprNodeType.COMPOUND_PRED, pred.getNodeType());
    }

    @Test
    public void testExecCompoundPredicateOrConstruction() {
        ExecSlotRef left = makeSlotRef(1, 0, BooleanType.BOOLEAN, false);
        ExecSlotRef right = makeSlotRef(2, 0, BooleanType.BOOLEAN, false);

        ExecCompoundPredicate pred = new ExecCompoundPredicate(CompoundPredicate.Operator.OR, left, right);
        assertEquals(CompoundPredicate.Operator.OR, pred.getCompoundType());
    }

    @Test
    public void testExecCompoundPredicateNotConstruction() {
        ExecSlotRef child = makeSlotRef(1, 0, BooleanType.BOOLEAN, false);
        ExecCompoundPredicate pred = new ExecCompoundPredicate(
                CompoundPredicate.Operator.NOT, new ArrayList<>(List.of(child)));

        assertEquals(CompoundPredicate.Operator.NOT, pred.getCompoundType());
        assertEquals(1, pred.getNumChildren());
    }

    @Test
    public void testExecCompoundPredicateNullability() {
        ExecSlotRef nullable = makeSlotRef(1, 0, BooleanType.BOOLEAN, true);
        ExecSlotRef nonNullable = makeSlotRef(2, 0, BooleanType.BOOLEAN, false);

        ExecCompoundPredicate withNullable = new ExecCompoundPredicate(
                CompoundPredicate.Operator.AND, nullable, nonNullable);
        assertTrue(withNullable.isNullable());

        ExecCompoundPredicate withoutNullable = new ExecCompoundPredicate(
                CompoundPredicate.Operator.AND, nonNullable, nonNullable.clone());
        assertFalse(withoutNullable.isNullable());
    }

    @Test
    public void testExecCompoundPredicateToThrift() {
        ExecSlotRef left = makeSlotRef(1, 0, BooleanType.BOOLEAN, false);
        ExecSlotRef right = makeSlotRef(2, 0, BooleanType.BOOLEAN, false);

        ExecCompoundPredicate andPred = new ExecCompoundPredicate(CompoundPredicate.Operator.AND, left, right);
        TExprNode node = new TExprNode();
        andPred.toThrift(node);
        assertEquals(TExprOpcode.COMPOUND_AND, node.opcode);

        ExecCompoundPredicate orPred = new ExecCompoundPredicate(CompoundPredicate.Operator.OR, left, right);
        TExprNode node2 = new TExprNode();
        orPred.toThrift(node2);
        assertEquals(TExprOpcode.COMPOUND_OR, node2.opcode);
    }

    @Test
    public void testExecCompoundPredicateVisitorDispatch() {
        ExecSlotRef left = makeSlotRef(1, 0, BooleanType.BOOLEAN, false);
        ExecSlotRef right = makeSlotRef(2, 0, BooleanType.BOOLEAN, false);
        ExecCompoundPredicate pred = new ExecCompoundPredicate(CompoundPredicate.Operator.AND, left, right);

        ExecExprVisitor<String, Void> visitor = new ExecExprVisitor<>() {
            @Override
            public String visitExecExpr(ExecExpr expr, Void context) {
                return "default";
            }

            @Override
            public String visitExecCompoundPredicate(ExecCompoundPredicate expr, Void context) {
                return "CompoundPredicate";
            }
        };
        assertEquals("CompoundPredicate", pred.accept(visitor, null));
    }

    @Test
    public void testExecCompoundPredicateClone() {
        ExecSlotRef left = makeSlotRef(1, 0, BooleanType.BOOLEAN, false);
        ExecSlotRef right = makeSlotRef(2, 0, BooleanType.BOOLEAN, false);
        ExecCompoundPredicate original = new ExecCompoundPredicate(CompoundPredicate.Operator.AND, left, right);
        original.setIsIndexOnlyFilter(true);

        ExecCompoundPredicate cloned = original.clone();
        assertEquals(original.getCompoundType(), cloned.getCompoundType());
        assertEquals(original.getNumChildren(), cloned.getNumChildren());
        assertTrue(cloned.isIndexOnlyFilter());
    }

    // ======================================================================
    // 8. ExecLikePredicate
    // ======================================================================

    @Test
    public void testExecLikePredicateConstruction() {
        ExecSlotRef col = makeSlotRef(1, 0, VarcharType.VARCHAR, false);
        ExecLiteral pattern = makeVarcharLiteral("%test%");
        ScalarFunction fn = makeScalarFunction("like", new Type[]{VarcharType.VARCHAR, VarcharType.VARCHAR},
                BooleanType.BOOLEAN);

        ExecLikePredicate pred = new ExecLikePredicate(false, fn, new ArrayList<>(List.of(col, pattern)));

        assertFalse(pred.isRegexp());
        assertEquals(fn, pred.getFn());
        assertEquals(BooleanType.BOOLEAN, pred.getType());
        assertEquals(2, pred.getNumChildren());
        assertEquals(TExprNodeType.FUNCTION_CALL, pred.getNodeType());
    }

    @Test
    public void testExecLikePredicateRegexp() {
        ExecSlotRef col = makeSlotRef(1, 0, VarcharType.VARCHAR, false);
        ExecLiteral pattern = makeVarcharLiteral("^test.*$");
        ScalarFunction fn = makeScalarFunction("regexp", new Type[]{VarcharType.VARCHAR, VarcharType.VARCHAR},
                BooleanType.BOOLEAN);

        ExecLikePredicate pred = new ExecLikePredicate(true, fn, new ArrayList<>(List.of(col, pattern)));
        assertTrue(pred.isRegexp());
    }

    @Test
    public void testExecLikePredicateNullability() {
        ExecSlotRef nullable = makeSlotRef(1, 0, VarcharType.VARCHAR, true);
        ExecLiteral pattern = makeVarcharLiteral("%test%");
        ScalarFunction fn = makeScalarFunction("like", new Type[]{VarcharType.VARCHAR, VarcharType.VARCHAR},
                BooleanType.BOOLEAN);

        ExecLikePredicate pred = new ExecLikePredicate(false, fn, new ArrayList<>(List.of(nullable, pattern)));
        assertTrue(pred.isNullable());
    }

    @Test
    public void testExecLikePredicateToThrift() {
        ExecSlotRef col = makeSlotRef(1, 0, VarcharType.VARCHAR, false);
        ExecLiteral pattern = makeVarcharLiteral("%test%");
        ScalarFunction fn = makeScalarFunction("like", new Type[]{VarcharType.VARCHAR, VarcharType.VARCHAR},
                BooleanType.BOOLEAN);

        ExecLikePredicate pred = new ExecLikePredicate(false, fn, new ArrayList<>(List.of(col, pattern)));
        TExprNode node = new TExprNode();
        pred.toThrift(node);
        assertNotNull(node.getFn());
    }

    @Test
    public void testExecLikePredicateToThriftNullFn() {
        ExecSlotRef col = makeSlotRef(1, 0, VarcharType.VARCHAR, false);
        ExecLiteral pattern = makeVarcharLiteral("%test%");

        ExecLikePredicate pred = new ExecLikePredicate(false, null, new ArrayList<>(List.of(col, pattern)));
        TExprNode node = new TExprNode();
        pred.toThrift(node);
        // Should not throw; fn is null so no function is set
        assertNull(node.getFn());
    }

    @Test
    public void testExecLikePredicateVisitorDispatch() {
        ExecSlotRef col = makeSlotRef(1, 0, VarcharType.VARCHAR, false);
        ExecLiteral pattern = makeVarcharLiteral("%test%");
        ScalarFunction fn = makeScalarFunction("like", new Type[]{VarcharType.VARCHAR, VarcharType.VARCHAR},
                BooleanType.BOOLEAN);
        ExecLikePredicate pred = new ExecLikePredicate(false, fn, new ArrayList<>(List.of(col, pattern)));

        ExecExprVisitor<String, Void> visitor = new ExecExprVisitor<>() {
            @Override
            public String visitExecExpr(ExecExpr expr, Void context) {
                return "default";
            }

            @Override
            public String visitExecLikePredicate(ExecLikePredicate expr, Void context) {
                return "LikePredicate";
            }
        };
        assertEquals("LikePredicate", pred.accept(visitor, null));
    }

    @Test
    public void testExecLikePredicateClone() {
        ExecSlotRef col = makeSlotRef(1, 0, VarcharType.VARCHAR, false);
        ExecLiteral pattern = makeVarcharLiteral("%test%");
        ScalarFunction fn = makeScalarFunction("like", new Type[]{VarcharType.VARCHAR, VarcharType.VARCHAR},
                BooleanType.BOOLEAN);
        ExecLikePredicate original = new ExecLikePredicate(false, fn, new ArrayList<>(List.of(col, pattern)));

        ExecLikePredicate cloned = original.clone();
        assertEquals(original.isRegexp(), cloned.isRegexp());
        assertEquals(original.getFn(), cloned.getFn());
        assertEquals(original.getNumChildren(), cloned.getNumChildren());
    }

    // ======================================================================
    // 9. ExecInPredicate
    // ======================================================================

    @Test
    public void testExecInPredicateConstruction() {
        ExecSlotRef col = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLiteral v1 = makeIntLiteral(1);
        ExecLiteral v2 = makeIntLiteral(2);
        ExecLiteral v3 = makeIntLiteral(3);

        ExecInPredicate pred = new ExecInPredicate(false, new ArrayList<>(List.of(col, v1, v2, v3)));

        assertFalse(pred.isNotIn());
        assertEquals(BooleanType.BOOLEAN, pred.getType());
        assertEquals(4, pred.getNumChildren());
        assertEquals(TExprNodeType.IN_PRED, pred.getNodeType());
    }

    @Test
    public void testExecInPredicateNotIn() {
        ExecSlotRef col = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLiteral v1 = makeIntLiteral(1);

        ExecInPredicate pred = new ExecInPredicate(true, new ArrayList<>(List.of(col, v1)));
        assertTrue(pred.isNotIn());
    }

    @Test
    public void testExecInPredicateNullability() {
        ExecSlotRef nullable = makeSlotRef(1, 0, IntegerType.INT, true);
        ExecLiteral v1 = makeIntLiteral(1);

        ExecInPredicate pred = new ExecInPredicate(false, new ArrayList<>(List.of(nullable, v1)));
        assertTrue(pred.isNullable());

        ExecSlotRef nonNullable = makeSlotRef(2, 0, IntegerType.INT, false);
        ExecInPredicate pred2 = new ExecInPredicate(false, new ArrayList<>(List.of(nonNullable, v1)));
        assertFalse(pred2.isNullable());
    }

    @Test
    public void testExecInPredicateToThrift() {
        ExecSlotRef col = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLiteral v1 = makeIntLiteral(1);
        ExecLiteral v2 = makeIntLiteral(2);

        ExecInPredicate pred = new ExecInPredicate(false, new ArrayList<>(List.of(col, v1, v2)));
        TExprNode node = new TExprNode();
        pred.toThrift(node);

        assertNotNull(node.in_predicate);
        assertFalse(node.in_predicate.is_not_in);
        assertEquals(TExprOpcode.FILTER_IN, node.opcode);
    }

    @Test
    public void testExecInPredicateToThriftNotIn() {
        ExecSlotRef col = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLiteral v1 = makeIntLiteral(1);

        ExecInPredicate pred = new ExecInPredicate(true, new ArrayList<>(List.of(col, v1)));
        TExprNode node = new TExprNode();
        pred.toThrift(node);

        assertTrue(node.in_predicate.is_not_in);
        assertEquals(TExprOpcode.FILTER_NOT_IN, node.opcode);
    }

    @Test
    public void testExecInPredicateVisitorDispatch() {
        ExecSlotRef col = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLiteral v1 = makeIntLiteral(1);
        ExecInPredicate pred = new ExecInPredicate(false, new ArrayList<>(List.of(col, v1)));

        ExecExprVisitor<String, Void> visitor = new ExecExprVisitor<>() {
            @Override
            public String visitExecExpr(ExecExpr expr, Void context) {
                return "default";
            }

            @Override
            public String visitExecInPredicate(ExecInPredicate expr, Void context) {
                return "InPredicate";
            }
        };
        assertEquals("InPredicate", pred.accept(visitor, null));
    }

    @Test
    public void testExecInPredicateClone() {
        ExecSlotRef col = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLiteral v1 = makeIntLiteral(1);
        ExecInPredicate original = new ExecInPredicate(true, new ArrayList<>(List.of(col, v1)));

        ExecInPredicate cloned = original.clone();
        assertEquals(original.isNotIn(), cloned.isNotIn());
        assertEquals(original.getNumChildren(), cloned.getNumChildren());
    }

    // ======================================================================
    // 10. ExecIsNullPredicate
    // ======================================================================

    @Test
    public void testExecIsNullPredicateIsNull() {
        ExecSlotRef col = makeSlotRef(1, 0, IntegerType.INT, true);
        ExecIsNullPredicate pred = new ExecIsNullPredicate(false, col);

        assertFalse(pred.isNotNull());
        assertEquals(BooleanType.BOOLEAN, pred.getType());
        assertEquals(1, pred.getNumChildren());
        assertEquals(TExprNodeType.FUNCTION_CALL, pred.getNodeType());
        // IS NULL predicate is never nullable itself
        assertFalse(pred.isNullable());
    }

    @Test
    public void testExecIsNullPredicateIsNotNull() {
        ExecSlotRef col = makeSlotRef(1, 0, IntegerType.INT, true);
        ExecIsNullPredicate pred = new ExecIsNullPredicate(true, col);

        assertTrue(pred.isNotNull());
        assertFalse(pred.isNullable());
    }

    @Test
    public void testExecIsNullPredicateToThriftIsNull() {
        ExecSlotRef col = makeSlotRef(1, 0, IntegerType.INT, true);
        ExecIsNullPredicate pred = new ExecIsNullPredicate(false, col);

        TExprNode node = new TExprNode();
        pred.toThrift(node);
        assertNotNull(node.getFn());
        assertEquals("is_null_pred", node.getFn().getName().getFunction_name());
    }

    @Test
    public void testExecIsNullPredicateToThriftIsNotNull() {
        ExecSlotRef col = makeSlotRef(1, 0, IntegerType.INT, true);
        ExecIsNullPredicate pred = new ExecIsNullPredicate(true, col);

        TExprNode node = new TExprNode();
        pred.toThrift(node);
        assertNotNull(node.getFn());
        assertEquals("is_not_null_pred", node.getFn().getName().getFunction_name());
    }

    @Test
    public void testExecIsNullPredicateVisitorDispatch() {
        ExecSlotRef col = makeSlotRef(1, 0, IntegerType.INT, true);
        ExecIsNullPredicate pred = new ExecIsNullPredicate(false, col);

        ExecExprVisitor<String, Void> visitor = new ExecExprVisitor<>() {
            @Override
            public String visitExecExpr(ExecExpr expr, Void context) {
                return "default";
            }

            @Override
            public String visitExecIsNullPredicate(ExecIsNullPredicate expr, Void context) {
                return "IsNullPredicate";
            }
        };
        assertEquals("IsNullPredicate", pred.accept(visitor, null));
    }

    @Test
    public void testExecIsNullPredicateClone() {
        ExecSlotRef col = makeSlotRef(1, 0, IntegerType.INT, true);
        ExecIsNullPredicate original = new ExecIsNullPredicate(true, col);

        ExecIsNullPredicate cloned = original.clone();
        assertEquals(original.isNotNull(), cloned.isNotNull());
        assertEquals(original.getNumChildren(), cloned.getNumChildren());
    }

    // ======================================================================
    // 11. ExecMatchExpr
    // ======================================================================

    @Test
    public void testExecMatchExprConstruction() {
        ExecSlotRef col = makeSlotRef(1, 0, VarcharType.VARCHAR, false);
        ExecLiteral pattern = makeVarcharLiteral("test");
        ExecMatchExpr expr = new ExecMatchExpr(MatchExpr.MatchOperator.MATCH,
                new ArrayList<>(List.of(col, pattern)));

        assertEquals(MatchExpr.MatchOperator.MATCH, expr.getMatchOp());
        assertEquals(BooleanType.BOOLEAN, expr.getType());
        assertEquals(2, expr.getNumChildren());
        assertEquals(TExprNodeType.MATCH_EXPR, expr.getNodeType());
    }

    @Test
    public void testExecMatchExprMatchAny() {
        ExecSlotRef col = makeSlotRef(1, 0, VarcharType.VARCHAR, false);
        ExecLiteral pattern = makeVarcharLiteral("test");
        ExecMatchExpr expr = new ExecMatchExpr(MatchExpr.MatchOperator.MATCH_ANY,
                new ArrayList<>(List.of(col, pattern)));

        assertEquals(MatchExpr.MatchOperator.MATCH_ANY, expr.getMatchOp());
    }

    @Test
    public void testExecMatchExprNullability() {
        ExecSlotRef nullable = makeSlotRef(1, 0, VarcharType.VARCHAR, true);
        ExecLiteral pattern = makeVarcharLiteral("test");
        ExecMatchExpr expr = new ExecMatchExpr(MatchExpr.MatchOperator.MATCH,
                new ArrayList<>(List.of(nullable, pattern)));
        assertTrue(expr.isNullable());
    }

    @Test
    public void testExecMatchExprToThrift() {
        ExecSlotRef col = makeSlotRef(1, 0, VarcharType.VARCHAR, false);
        ExecLiteral pattern = makeVarcharLiteral("test");
        ExecMatchExpr expr = new ExecMatchExpr(MatchExpr.MatchOperator.MATCH,
                new ArrayList<>(List.of(col, pattern)));

        TExprNode node = new TExprNode();
        expr.toThrift(node);
        assertEquals(TExprOpcode.MATCH, node.opcode);
    }

    @Test
    public void testExecMatchExprToThriftMatchAll() {
        ExecSlotRef col = makeSlotRef(1, 0, VarcharType.VARCHAR, false);
        ExecLiteral pattern = makeVarcharLiteral("test");
        ExecMatchExpr expr = new ExecMatchExpr(MatchExpr.MatchOperator.MATCH_ALL,
                new ArrayList<>(List.of(col, pattern)));

        TExprNode node = new TExprNode();
        expr.toThrift(node);
        assertEquals(TExprOpcode.MATCH_ALL, node.opcode);
    }

    @Test
    public void testExecMatchExprVisitorDispatch() {
        ExecSlotRef col = makeSlotRef(1, 0, VarcharType.VARCHAR, false);
        ExecLiteral pattern = makeVarcharLiteral("test");
        ExecMatchExpr expr = new ExecMatchExpr(MatchExpr.MatchOperator.MATCH,
                new ArrayList<>(List.of(col, pattern)));

        ExecExprVisitor<String, Void> visitor = new ExecExprVisitor<>() {
            @Override
            public String visitExecExpr(ExecExpr expr2, Void context) {
                return "default";
            }

            @Override
            public String visitExecMatchExpr(ExecMatchExpr expr2, Void context) {
                return "MatchExpr";
            }
        };
        assertEquals("MatchExpr", expr.accept(visitor, null));
    }

    @Test
    public void testExecMatchExprClone() {
        ExecSlotRef col = makeSlotRef(1, 0, VarcharType.VARCHAR, false);
        ExecLiteral pattern = makeVarcharLiteral("test");
        ExecMatchExpr original = new ExecMatchExpr(MatchExpr.MatchOperator.MATCH,
                new ArrayList<>(List.of(col, pattern)));

        ExecMatchExpr cloned = original.clone();
        assertEquals(original.getMatchOp(), cloned.getMatchOp());
        assertEquals(original.getNumChildren(), cloned.getNumChildren());
    }

    // ======================================================================
    // 12. ExecArithmetic
    // ======================================================================

    @Test
    public void testExecArithmeticConstruction() {
        ExecSlotRef left = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLiteral right = makeIntLiteral(10);

        ExecArithmetic expr = new ExecArithmetic(IntegerType.INT, ArithmeticExpr.Operator.ADD,
                new ArrayList<>(List.of(left, right)));

        assertEquals(ArithmeticExpr.Operator.ADD, expr.getOp());
        assertEquals(IntegerType.INT, expr.getType());
        assertEquals(2, expr.getNumChildren());
        assertEquals(TExprNodeType.ARITHMETIC_EXPR, expr.getNodeType());
    }

    @Test
    public void testExecArithmeticNullabilityDivide() {
        ExecSlotRef left = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLiteral right = makeIntLiteral(10);

        // DIVIDE is always nullable (division by zero)
        ExecArithmetic divide = new ExecArithmetic(IntegerType.INT, ArithmeticExpr.Operator.DIVIDE,
                new ArrayList<>(List.of(left, right)));
        assertTrue(divide.isNullable());

        // INT_DIVIDE is always nullable
        ExecArithmetic intDivide = new ExecArithmetic(IntegerType.INT, ArithmeticExpr.Operator.INT_DIVIDE,
                new ArrayList<>(List.of(left, right)));
        assertTrue(intDivide.isNullable());

        // MOD is always nullable
        ExecArithmetic mod = new ExecArithmetic(IntegerType.INT, ArithmeticExpr.Operator.MOD,
                new ArrayList<>(List.of(left, right)));
        assertTrue(mod.isNullable());
    }

    @Test
    public void testExecArithmeticNullabilityAdd() {
        ExecSlotRef nonNullable = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLiteral literal = makeIntLiteral(10);

        // ADD with non-nullable children -> not nullable
        ExecArithmetic add = new ExecArithmetic(IntegerType.INT, ArithmeticExpr.Operator.ADD,
                new ArrayList<>(List.of(nonNullable, literal)));
        assertFalse(add.isNullable());

        // ADD with nullable child -> nullable
        ExecSlotRef nullable = makeSlotRef(2, 0, IntegerType.INT, true);
        ExecArithmetic addNullable = new ExecArithmetic(IntegerType.INT, ArithmeticExpr.Operator.ADD,
                new ArrayList<>(List.of(nullable, literal)));
        assertTrue(addNullable.isNullable());
    }

    @Test
    public void testExecArithmeticSelfMonotonic() {
        ExecSlotRef left = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLiteral right = makeIntLiteral(10);

        ExecArithmetic add = new ExecArithmetic(IntegerType.INT, ArithmeticExpr.Operator.ADD,
                new ArrayList<>(List.of(left, right)));
        assertTrue(add.isSelfMonotonic());
    }

    @Test
    public void testExecArithmeticToThrift() {
        ExecSlotRef left = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLiteral right = makeIntLiteral(10);

        ExecArithmetic add = new ExecArithmetic(IntegerType.INT, ArithmeticExpr.Operator.ADD,
                new ArrayList<>(List.of(left, right)));
        TExprNode node = new TExprNode();
        add.toThrift(node);
        assertEquals(TExprOpcode.ADD, node.opcode);

        ExecArithmetic sub = new ExecArithmetic(IntegerType.INT, ArithmeticExpr.Operator.SUBTRACT,
                new ArrayList<>(List.of(left, right)));
        TExprNode node2 = new TExprNode();
        sub.toThrift(node2);
        assertEquals(TExprOpcode.SUBTRACT, node2.opcode);
    }

    @Test
    public void testExecArithmeticVisitorDispatch() {
        ExecSlotRef left = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLiteral right = makeIntLiteral(10);
        ExecArithmetic expr = new ExecArithmetic(IntegerType.INT, ArithmeticExpr.Operator.ADD,
                new ArrayList<>(List.of(left, right)));

        ExecExprVisitor<String, Void> visitor = new ExecExprVisitor<>() {
            @Override
            public String visitExecExpr(ExecExpr e, Void context) {
                return "default";
            }

            @Override
            public String visitExecArithmetic(ExecArithmetic e, Void context) {
                return "Arithmetic";
            }
        };
        assertEquals("Arithmetic", expr.accept(visitor, null));
    }

    @Test
    public void testExecArithmeticClone() {
        ExecSlotRef left = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLiteral right = makeIntLiteral(10);
        ExecArithmetic original = new ExecArithmetic(IntegerType.INT, ArithmeticExpr.Operator.MULTIPLY,
                new ArrayList<>(List.of(left, right)));

        ExecArithmetic cloned = original.clone();
        assertEquals(original.getOp(), cloned.getOp());
        assertEquals(original.getType(), cloned.getType());
        assertEquals(original.getNumChildren(), cloned.getNumChildren());
    }

    // ======================================================================
    // 13. ExecCaseWhen
    // ======================================================================

    @Test
    public void testExecCaseWhenConstruction() {
        ExecSlotRef cond = makeSlotRef(1, 0, BooleanType.BOOLEAN, false);
        ExecLiteral thenVal = makeIntLiteral(1);
        ExecLiteral elseVal = makeIntLiteral(0);

        ExecCaseWhen expr = new ExecCaseWhen(IntegerType.INT, false, true,
                new ArrayList<>(List.of(cond, thenVal, elseVal)));

        assertFalse(expr.hasCase());
        assertTrue(expr.hasElse());
        assertEquals(IntegerType.INT, expr.getType());
        assertEquals(3, expr.getNumChildren());
        assertEquals(TExprNodeType.CASE_EXPR, expr.getNodeType());
    }

    @Test
    public void testExecCaseWhenWithCaseExpr() {
        ExecSlotRef caseExpr = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLiteral when1 = makeIntLiteral(1);
        ExecLiteral then1 = makeIntLiteral(100);

        ExecCaseWhen expr = new ExecCaseWhen(IntegerType.INT, true, false,
                new ArrayList<>(List.of(caseExpr, when1, then1)));

        assertTrue(expr.hasCase());
        assertFalse(expr.hasElse());
    }

    @Test
    public void testExecCaseWhenAlwaysNullable() {
        ExecSlotRef cond = makeSlotRef(1, 0, BooleanType.BOOLEAN, false);
        ExecLiteral thenVal = makeIntLiteral(1);
        ExecCaseWhen expr = new ExecCaseWhen(IntegerType.INT, false, false,
                new ArrayList<>(List.of(cond, thenVal)));

        assertTrue(expr.isNullable());
    }

    @Test
    public void testExecCaseWhenToThrift() {
        ExecSlotRef cond = makeSlotRef(1, 0, BooleanType.BOOLEAN, false);
        ExecLiteral thenVal = makeIntLiteral(1);
        ExecLiteral elseVal = makeIntLiteral(0);

        ExecCaseWhen expr = new ExecCaseWhen(IntegerType.INT, true, true,
                new ArrayList<>(List.of(cond, thenVal, elseVal)));
        TExprNode node = new TExprNode();
        expr.toThrift(node);

        assertNotNull(node.case_expr);
        assertTrue(node.case_expr.has_case_expr);
        assertTrue(node.case_expr.has_else_expr);
    }

    @Test
    public void testExecCaseWhenVisitorDispatch() {
        ExecSlotRef cond = makeSlotRef(1, 0, BooleanType.BOOLEAN, false);
        ExecLiteral thenVal = makeIntLiteral(1);
        ExecCaseWhen expr = new ExecCaseWhen(IntegerType.INT, false, false,
                new ArrayList<>(List.of(cond, thenVal)));

        ExecExprVisitor<String, Void> visitor = new ExecExprVisitor<>() {
            @Override
            public String visitExecExpr(ExecExpr e, Void context) {
                return "default";
            }

            @Override
            public String visitExecCaseWhen(ExecCaseWhen e, Void context) {
                return "CaseWhen";
            }
        };
        assertEquals("CaseWhen", expr.accept(visitor, null));
    }

    @Test
    public void testExecCaseWhenClone() {
        ExecSlotRef cond = makeSlotRef(1, 0, BooleanType.BOOLEAN, false);
        ExecLiteral thenVal = makeIntLiteral(1);
        ExecCaseWhen original = new ExecCaseWhen(IntegerType.INT, true, true,
                new ArrayList<>(List.of(cond, thenVal)));

        ExecCaseWhen cloned = original.clone();
        assertEquals(original.hasCase(), cloned.hasCase());
        assertEquals(original.hasElse(), cloned.hasElse());
        assertEquals(original.getNumChildren(), cloned.getNumChildren());
    }

    // ======================================================================
    // 14. ExecBetweenPredicate
    // ======================================================================

    @Test
    public void testExecBetweenPredicateConstruction() {
        ExecSlotRef col = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLiteral low = makeIntLiteral(1);
        ExecLiteral high = makeIntLiteral(10);

        ExecBetweenPredicate pred = new ExecBetweenPredicate(false,
                new ArrayList<>(List.of(col, low, high)));

        assertFalse(pred.isNotBetween());
        assertEquals(BooleanType.BOOLEAN, pred.getType());
        assertEquals(3, pred.getNumChildren());
    }

    @Test
    public void testExecBetweenPredicateNotBetween() {
        ExecSlotRef col = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLiteral low = makeIntLiteral(1);
        ExecLiteral high = makeIntLiteral(10);

        ExecBetweenPredicate pred = new ExecBetweenPredicate(true,
                new ArrayList<>(List.of(col, low, high)));
        assertTrue(pred.isNotBetween());
    }

    @Test
    public void testExecBetweenPredicateNullability() {
        ExecSlotRef nullable = makeSlotRef(1, 0, IntegerType.INT, true);
        ExecLiteral low = makeIntLiteral(1);
        ExecLiteral high = makeIntLiteral(10);

        ExecBetweenPredicate pred = new ExecBetweenPredicate(false,
                new ArrayList<>(List.of(nullable, low, high)));
        assertTrue(pred.isNullable());
    }

    @Test
    public void testExecBetweenPredicateThrowsOnGetNodeType() {
        ExecSlotRef col = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLiteral low = makeIntLiteral(1);
        ExecLiteral high = makeIntLiteral(10);
        ExecBetweenPredicate pred = new ExecBetweenPredicate(false,
                new ArrayList<>(List.of(col, low, high)));

        assertThrows(IllegalStateException.class, pred::getNodeType);
    }

    @Test
    public void testExecBetweenPredicateThrowsOnToThrift() {
        ExecSlotRef col = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLiteral low = makeIntLiteral(1);
        ExecLiteral high = makeIntLiteral(10);
        ExecBetweenPredicate pred = new ExecBetweenPredicate(false,
                new ArrayList<>(List.of(col, low, high)));

        assertThrows(IllegalStateException.class, () -> pred.toThrift(new TExprNode()));
    }

    @Test
    public void testExecBetweenPredicateVisitorDispatch() {
        ExecSlotRef col = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLiteral low = makeIntLiteral(1);
        ExecLiteral high = makeIntLiteral(10);
        ExecBetweenPredicate pred = new ExecBetweenPredicate(false,
                new ArrayList<>(List.of(col, low, high)));

        ExecExprVisitor<String, Void> visitor = new ExecExprVisitor<>() {
            @Override
            public String visitExecExpr(ExecExpr e, Void context) {
                return "default";
            }

            @Override
            public String visitExecBetweenPredicate(ExecBetweenPredicate e, Void context) {
                return "BetweenPredicate";
            }
        };
        assertEquals("BetweenPredicate", pred.accept(visitor, null));
    }

    @Test
    public void testExecBetweenPredicateClone() {
        ExecSlotRef col = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLiteral low = makeIntLiteral(1);
        ExecLiteral high = makeIntLiteral(10);
        ExecBetweenPredicate original = new ExecBetweenPredicate(true,
                new ArrayList<>(List.of(col, low, high)));

        ExecBetweenPredicate cloned = original.clone();
        assertEquals(original.isNotBetween(), cloned.isNotBetween());
        assertEquals(original.getNumChildren(), cloned.getNumChildren());
    }

    // ======================================================================
    // 15. ExecArrayExpr
    // ======================================================================

    @Test
    public void testExecArrayExprConstruction() {
        ArrayType arrayType = new ArrayType(IntegerType.INT);
        ExecLiteral e1 = makeIntLiteral(1);
        ExecLiteral e2 = makeIntLiteral(2);

        ExecArrayExpr expr = new ExecArrayExpr(arrayType, new ArrayList<>(List.of(e1, e2)));

        assertEquals(arrayType, expr.getType());
        assertEquals(2, expr.getNumChildren());
        assertEquals(TExprNodeType.ARRAY_EXPR, expr.getNodeType());
        assertTrue(expr.isNullable());
    }

    @Test
    public void testExecArrayExprToThrift() {
        ArrayType arrayType = new ArrayType(IntegerType.INT);
        ExecLiteral e1 = makeIntLiteral(1);
        ExecArrayExpr expr = new ExecArrayExpr(arrayType, new ArrayList<>(List.of(e1)));

        TExprNode node = new TExprNode();
        expr.toThrift(node);
        // ARRAY_EXPR has no extra fields -- just verify it doesn't throw
    }

    @Test
    public void testExecArrayExprVisitorDispatch() {
        ArrayType arrayType = new ArrayType(IntegerType.INT);
        ExecArrayExpr expr = new ExecArrayExpr(arrayType, new ArrayList<>());

        ExecExprVisitor<String, Void> visitor = new ExecExprVisitor<>() {
            @Override
            public String visitExecExpr(ExecExpr e, Void context) {
                return "default";
            }

            @Override
            public String visitExecArrayExpr(ExecArrayExpr e, Void context) {
                return "ArrayExpr";
            }
        };
        assertEquals("ArrayExpr", expr.accept(visitor, null));
    }

    @Test
    public void testExecArrayExprClone() {
        ArrayType arrayType = new ArrayType(IntegerType.INT);
        ExecLiteral e1 = makeIntLiteral(1);
        ExecArrayExpr original = new ExecArrayExpr(arrayType, new ArrayList<>(List.of(e1)));

        ExecArrayExpr cloned = original.clone();
        assertEquals(original.getType(), cloned.getType());
        assertEquals(original.getNumChildren(), cloned.getNumChildren());
    }

    // ======================================================================
    // 16. ExecArraySlice
    // ======================================================================

    @Test
    public void testExecArraySliceConstruction() {
        ArrayType arrayType = new ArrayType(IntegerType.INT);
        ExecSlotRef arr = makeSlotRef(1, 0, arrayType, false);
        ExecLiteral offset = makeIntLiteral(1);
        ExecLiteral length = makeIntLiteral(3);

        ExecArraySlice expr = new ExecArraySlice(arrayType, new ArrayList<>(List.of(arr, offset, length)));

        assertEquals(arrayType, expr.getType());
        assertEquals(3, expr.getNumChildren());
        assertEquals(TExprNodeType.ARRAY_SLICE_EXPR, expr.getNodeType());
    }

    @Test
    public void testExecArraySliceNullability() {
        ArrayType arrayType = new ArrayType(IntegerType.INT);
        ExecSlotRef nullableArr = makeSlotRef(1, 0, arrayType, true);
        ExecLiteral offset = makeIntLiteral(1);

        ExecArraySlice expr = new ExecArraySlice(arrayType, new ArrayList<>(List.of(nullableArr, offset)));
        assertTrue(expr.isNullable());
    }

    @Test
    public void testExecArraySliceToThrift() {
        ArrayType arrayType = new ArrayType(IntegerType.INT);
        ExecSlotRef arr = makeSlotRef(1, 0, arrayType, false);
        ExecLiteral offset = makeIntLiteral(1);
        ExecArraySlice expr = new ExecArraySlice(arrayType, new ArrayList<>(List.of(arr, offset)));

        TExprNode node = new TExprNode();
        expr.toThrift(node);
        // No extra fields -- just verify it doesn't throw
    }

    @Test
    public void testExecArraySliceVisitorDispatch() {
        ArrayType arrayType = new ArrayType(IntegerType.INT);
        ExecArraySlice expr = new ExecArraySlice(arrayType, new ArrayList<>());

        ExecExprVisitor<String, Void> visitor = new ExecExprVisitor<>() {
            @Override
            public String visitExecExpr(ExecExpr e, Void context) {
                return "default";
            }

            @Override
            public String visitExecArraySlice(ExecArraySlice e, Void context) {
                return "ArraySlice";
            }
        };
        assertEquals("ArraySlice", expr.accept(visitor, null));
    }

    @Test
    public void testExecArraySliceClone() {
        ArrayType arrayType = new ArrayType(IntegerType.INT);
        ExecSlotRef arr = makeSlotRef(1, 0, arrayType, false);
        ExecLiteral offset = makeIntLiteral(1);
        ExecArraySlice original = new ExecArraySlice(arrayType, new ArrayList<>(List.of(arr, offset)));

        ExecArraySlice cloned = original.clone();
        assertEquals(original.getType(), cloned.getType());
        assertEquals(original.getNumChildren(), cloned.getNumChildren());
    }

    // ======================================================================
    // 17. ExecMapExpr
    // ======================================================================

    @Test
    public void testExecMapExprConstruction() {
        MapType mapType = new MapType(VarcharType.VARCHAR, IntegerType.INT);
        ExecLiteral key = makeVarcharLiteral("key1");
        ExecLiteral val = makeIntLiteral(1);

        ExecMapExpr expr = new ExecMapExpr(mapType, new ArrayList<>(List.of(key, val)));

        assertEquals(mapType, expr.getType());
        assertEquals(2, expr.getNumChildren());
        assertEquals(TExprNodeType.MAP_EXPR, expr.getNodeType());
        assertFalse(expr.isNullable());
    }

    @Test
    public void testExecMapExprToThrift() {
        MapType mapType = new MapType(VarcharType.VARCHAR, IntegerType.INT);
        ExecMapExpr expr = new ExecMapExpr(mapType, new ArrayList<>());

        TExprNode node = new TExprNode();
        expr.toThrift(node);
        // No extra fields
    }

    @Test
    public void testExecMapExprVisitorDispatch() {
        MapType mapType = new MapType(VarcharType.VARCHAR, IntegerType.INT);
        ExecMapExpr expr = new ExecMapExpr(mapType, new ArrayList<>());

        ExecExprVisitor<String, Void> visitor = new ExecExprVisitor<>() {
            @Override
            public String visitExecExpr(ExecExpr e, Void context) {
                return "default";
            }

            @Override
            public String visitExecMapExpr(ExecMapExpr e, Void context) {
                return "MapExpr";
            }
        };
        assertEquals("MapExpr", expr.accept(visitor, null));
    }

    @Test
    public void testExecMapExprClone() {
        MapType mapType = new MapType(VarcharType.VARCHAR, IntegerType.INT);
        ExecLiteral key = makeVarcharLiteral("key1");
        ExecLiteral val = makeIntLiteral(1);
        ExecMapExpr original = new ExecMapExpr(mapType, new ArrayList<>(List.of(key, val)));

        ExecMapExpr cloned = original.clone();
        assertEquals(original.getType(), cloned.getType());
        assertEquals(original.getNumChildren(), cloned.getNumChildren());
    }

    // ======================================================================
    // 18. ExecClone
    // ======================================================================

    @Test
    public void testExecCloneConstruction() {
        ExecSlotRef child = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecClone expr = new ExecClone(IntegerType.INT, child);

        assertEquals(IntegerType.INT, expr.getType());
        assertEquals(1, expr.getNumChildren());
        assertEquals(TExprNodeType.CLONE_EXPR, expr.getNodeType());
    }

    @Test
    public void testExecCloneNullability() {
        ExecSlotRef nullable = makeSlotRef(1, 0, IntegerType.INT, true);
        ExecClone cloneNullable = new ExecClone(IntegerType.INT, nullable);
        assertTrue(cloneNullable.isNullable());

        ExecSlotRef nonNullable = makeSlotRef(2, 0, IntegerType.INT, false);
        ExecClone cloneNonNullable = new ExecClone(IntegerType.INT, nonNullable);
        assertFalse(cloneNonNullable.isNullable());
    }

    @Test
    public void testExecCloneToThrift() {
        ExecSlotRef child = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecClone expr = new ExecClone(IntegerType.INT, child);

        TExprNode node = new TExprNode();
        expr.toThrift(node);
        // No extra fields
    }

    @Test
    public void testExecCloneVisitorDispatch() {
        ExecSlotRef child = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecClone expr = new ExecClone(IntegerType.INT, child);

        ExecExprVisitor<String, Void> visitor = new ExecExprVisitor<>() {
            @Override
            public String visitExecExpr(ExecExpr e, Void context) {
                return "default";
            }

            @Override
            public String visitExecClone(ExecClone e, Void context) {
                return "Clone";
            }
        };
        assertEquals("Clone", expr.accept(visitor, null));
    }

    @Test
    public void testExecCloneClone() {
        ExecSlotRef child = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecClone original = new ExecClone(IntegerType.INT, child);

        ExecClone cloned = original.clone();
        assertEquals(original.getType(), cloned.getType());
        assertEquals(original.getNumChildren(), cloned.getNumChildren());
    }

    // ======================================================================
    // 19. ExecDictMapping
    // ======================================================================

    @Test
    public void testExecDictMappingConstruction() {
        ExecSlotRef child = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecDictMapping expr = new ExecDictMapping(VarcharType.VARCHAR, new ArrayList<>(List.of(child)));

        assertEquals(VarcharType.VARCHAR, expr.getType());
        assertEquals(1, expr.getNumChildren());
        assertEquals(TExprNodeType.DICT_EXPR, expr.getNodeType());
        assertTrue(expr.isNullable());
    }

    @Test
    public void testExecDictMappingToThrift() {
        ExecSlotRef child = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecDictMapping expr = new ExecDictMapping(VarcharType.VARCHAR, new ArrayList<>(List.of(child)));

        TExprNode node = new TExprNode();
        expr.toThrift(node);
        // No extra fields
    }

    @Test
    public void testExecDictMappingVisitorDispatch() {
        ExecDictMapping expr = new ExecDictMapping(VarcharType.VARCHAR, new ArrayList<>());

        ExecExprVisitor<String, Void> visitor = new ExecExprVisitor<>() {
            @Override
            public String visitExecExpr(ExecExpr e, Void context) {
                return "default";
            }

            @Override
            public String visitExecDictMapping(ExecDictMapping e, Void context) {
                return "DictMapping";
            }
        };
        assertEquals("DictMapping", expr.accept(visitor, null));
    }

    @Test
    public void testExecDictMappingClone() {
        ExecSlotRef child = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecDictMapping original = new ExecDictMapping(VarcharType.VARCHAR, new ArrayList<>(List.of(child)));

        ExecDictMapping cloned = original.clone();
        assertEquals(original.getType(), cloned.getType());
        assertEquals(original.getNumChildren(), cloned.getNumChildren());
    }

    // ======================================================================
    // 20. ExecDictQuery
    // ======================================================================

    @Test
    public void testExecDictQueryConstruction() {
        TDictQueryExpr tDictQueryExpr = new TDictQueryExpr();
        ExecSlotRef child = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecDictQuery expr = new ExecDictQuery(VarcharType.VARCHAR, tDictQueryExpr,
                new ArrayList<>(List.of(child)));

        assertEquals(VarcharType.VARCHAR, expr.getType());
        assertEquals(tDictQueryExpr, expr.getDictQueryExpr());
        assertEquals(1, expr.getNumChildren());
        assertEquals(TExprNodeType.DICT_QUERY_EXPR, expr.getNodeType());
        assertTrue(expr.isNullable());
    }

    @Test
    public void testExecDictQueryToThrift() {
        TDictQueryExpr tDictQueryExpr = new TDictQueryExpr();
        ExecSlotRef child = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecDictQuery expr = new ExecDictQuery(VarcharType.VARCHAR, tDictQueryExpr,
                new ArrayList<>(List.of(child)));

        TExprNode node = new TExprNode();
        expr.toThrift(node);
        assertEquals(tDictQueryExpr, node.getDict_query_expr());
    }

    @Test
    public void testExecDictQueryVisitorDispatch() {
        TDictQueryExpr tDictQueryExpr = new TDictQueryExpr();
        ExecDictQuery expr = new ExecDictQuery(VarcharType.VARCHAR, tDictQueryExpr, new ArrayList<>());

        ExecExprVisitor<String, Void> visitor = new ExecExprVisitor<>() {
            @Override
            public String visitExecExpr(ExecExpr e, Void context) {
                return "default";
            }

            @Override
            public String visitExecDictQuery(ExecDictQuery e, Void context) {
                return "DictQuery";
            }
        };
        assertEquals("DictQuery", expr.accept(visitor, null));
    }

    @Test
    public void testExecDictQueryClone() {
        TDictQueryExpr tDictQueryExpr = new TDictQueryExpr();
        ExecSlotRef child = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecDictQuery original = new ExecDictQuery(VarcharType.VARCHAR, tDictQueryExpr,
                new ArrayList<>(List.of(child)));

        ExecDictQuery cloned = original.clone();
        assertEquals(original.getDictQueryExpr(), cloned.getDictQueryExpr());
        assertEquals(original.getNumChildren(), cloned.getNumChildren());
    }

    // ======================================================================
    // 21. ExecDictionaryGet
    // ======================================================================

    @Test
    public void testExecDictionaryGetConstruction() {
        ExecSlotRef key = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecDictionaryGet expr = new ExecDictionaryGet(VarcharType.VARCHAR,
                100L, 200L, 1, true, new ArrayList<>(List.of(key)));

        assertEquals(VarcharType.VARCHAR, expr.getType());
        assertEquals(100L, expr.getDictionaryId());
        assertEquals(200L, expr.getTxnId());
        assertEquals(1, expr.getKeySize());
        assertTrue(expr.isNullIfNotExist());
        assertEquals(1, expr.getNumChildren());
        assertEquals(TExprNodeType.DICTIONARY_GET_EXPR, expr.getNodeType());
        assertTrue(expr.isNullable());
    }

    @Test
    public void testExecDictionaryGetNotNullIfNotExist() {
        ExecSlotRef key = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecDictionaryGet expr = new ExecDictionaryGet(VarcharType.VARCHAR,
                50L, 60L, 2, false, new ArrayList<>(List.of(key)));

        assertFalse(expr.isNullIfNotExist());
        assertEquals(50L, expr.getDictionaryId());
        assertEquals(60L, expr.getTxnId());
        assertEquals(2, expr.getKeySize());
    }

    @Test
    public void testExecDictionaryGetToThrift() {
        ExecSlotRef key = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecDictionaryGet expr = new ExecDictionaryGet(VarcharType.VARCHAR,
                100L, 200L, 1, true, new ArrayList<>(List.of(key)));

        TExprNode node = new TExprNode();
        expr.toThrift(node);

        assertNotNull(node.getDictionary_get_expr());
        assertEquals(100L, node.getDictionary_get_expr().getDict_id());
        assertEquals(200L, node.getDictionary_get_expr().getTxn_id());
        assertEquals(1, node.getDictionary_get_expr().getKey_size());
        assertTrue(node.getDictionary_get_expr().isNull_if_not_exist());
    }

    @Test
    public void testExecDictionaryGetVisitorDispatch() {
        ExecDictionaryGet expr = new ExecDictionaryGet(VarcharType.VARCHAR,
                1L, 2L, 1, false, new ArrayList<>());

        ExecExprVisitor<String, Void> visitor = new ExecExprVisitor<>() {
            @Override
            public String visitExecExpr(ExecExpr e, Void context) {
                return "default";
            }

            @Override
            public String visitExecDictionaryGet(ExecDictionaryGet e, Void context) {
                return "DictionaryGet";
            }
        };
        assertEquals("DictionaryGet", expr.accept(visitor, null));
    }

    @Test
    public void testExecDictionaryGetClone() {
        ExecSlotRef key = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecDictionaryGet original = new ExecDictionaryGet(VarcharType.VARCHAR,
                100L, 200L, 1, true, new ArrayList<>(List.of(key)));

        ExecDictionaryGet cloned = original.clone();
        assertEquals(original.getDictionaryId(), cloned.getDictionaryId());
        assertEquals(original.getTxnId(), cloned.getTxnId());
        assertEquals(original.getKeySize(), cloned.getKeySize());
        assertEquals(original.isNullIfNotExist(), cloned.isNullIfNotExist());
        assertEquals(original.getNumChildren(), cloned.getNumChildren());
    }

    // ======================================================================
    // 22. ExecSubfield
    // ======================================================================

    @Test
    public void testExecSubfieldConstruction() {
        ExecSlotRef structCol = makeSlotRef(1, 0, IntegerType.INT, false);
        List<String> fieldNames = List.of("a", "b", "c");

        ExecSubfield expr = new ExecSubfield(IntegerType.INT, fieldNames, false,
                new ArrayList<>(List.of(structCol)));

        assertEquals(IntegerType.INT, expr.getType());
        assertEquals(fieldNames, expr.getFieldNames());
        assertFalse(expr.isCopyFlag());
        assertEquals(1, expr.getNumChildren());
        assertEquals(TExprNodeType.SUBFIELD_EXPR, expr.getNodeType());
        assertTrue(expr.isNullable());
    }

    @Test
    public void testExecSubfieldWithCopyFlag() {
        ExecSlotRef structCol = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecSubfield expr = new ExecSubfield(IntegerType.INT, List.of("x"), true,
                new ArrayList<>(List.of(structCol)));

        assertTrue(expr.isCopyFlag());
    }

    @Test
    public void testExecSubfieldToThrift() {
        ExecSlotRef structCol = makeSlotRef(1, 0, IntegerType.INT, false);
        List<String> fieldNames = List.of("field1", "field2");
        ExecSubfield expr = new ExecSubfield(IntegerType.INT, fieldNames, true,
                new ArrayList<>(List.of(structCol)));

        TExprNode node = new TExprNode();
        expr.toThrift(node);

        assertEquals(fieldNames, node.getUsed_subfield_names());
        assertTrue(node.isCopy_flag());
    }

    @Test
    public void testExecSubfieldVisitorDispatch() {
        ExecSlotRef structCol = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecSubfield expr = new ExecSubfield(IntegerType.INT, List.of("a"), false,
                new ArrayList<>(List.of(structCol)));

        ExecExprVisitor<String, Void> visitor = new ExecExprVisitor<>() {
            @Override
            public String visitExecExpr(ExecExpr e, Void context) {
                return "default";
            }

            @Override
            public String visitExecSubfield(ExecSubfield e, Void context) {
                return "Subfield";
            }
        };
        assertEquals("Subfield", expr.accept(visitor, null));
    }

    @Test
    public void testExecSubfieldClone() {
        ExecSlotRef structCol = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecSubfield original = new ExecSubfield(IntegerType.INT, List.of("a", "b"), true,
                new ArrayList<>(List.of(structCol)));

        ExecSubfield cloned = original.clone();
        assertEquals(original.getFieldNames(), cloned.getFieldNames());
        assertEquals(original.isCopyFlag(), cloned.isCopyFlag());
        assertEquals(original.getNumChildren(), cloned.getNumChildren());
    }

    // ======================================================================
    // 23. ExecLambdaFunction
    // ======================================================================

    @Test
    public void testExecLambdaFunctionConstruction() {
        ExecSlotRef body = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecSlotRef arg = makeSlotRef(2, 0, IntegerType.INT, false);

        ExecLambdaFunction expr = new ExecLambdaFunction(IntegerType.INT, 3, false,
                new ArrayList<>(List.of(body, arg)));

        assertEquals(IntegerType.INT, expr.getType());
        assertEquals(3, expr.getCommonSubOperatorNum());
        assertFalse(expr.isNondeterministic());
        assertEquals(2, expr.getNumChildren());
        assertEquals(TExprNodeType.LAMBDA_FUNCTION_EXPR, expr.getNodeType());
        assertTrue(expr.isNullable());
    }

    @Test
    public void testExecLambdaFunctionNondeterministic() {
        ExecSlotRef body = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLambdaFunction expr = new ExecLambdaFunction(IntegerType.INT, 0, true,
                new ArrayList<>(List.of(body)));

        assertTrue(expr.isNondeterministic());
    }

    @Test
    public void testExecLambdaFunctionToThrift() {
        ExecSlotRef body = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLambdaFunction expr = new ExecLambdaFunction(IntegerType.INT, 5, true,
                new ArrayList<>(List.of(body)));

        TExprNode node = new TExprNode();
        expr.toThrift(node);

        assertEquals(5, node.getOutput_column());
        assertTrue(node.isIs_nondeterministic());
    }

    @Test
    public void testExecLambdaFunctionVisitorDispatch() {
        ExecSlotRef body = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLambdaFunction expr = new ExecLambdaFunction(IntegerType.INT, 0, false,
                new ArrayList<>(List.of(body)));

        ExecExprVisitor<String, Void> visitor = new ExecExprVisitor<>() {
            @Override
            public String visitExecExpr(ExecExpr e, Void context) {
                return "default";
            }

            @Override
            public String visitExecLambdaFunction(ExecLambdaFunction e, Void context) {
                return "LambdaFunction";
            }
        };
        assertEquals("LambdaFunction", expr.accept(visitor, null));
    }

    @Test
    public void testExecLambdaFunctionClone() {
        ExecSlotRef body = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLambdaFunction original = new ExecLambdaFunction(IntegerType.INT, 3, true,
                new ArrayList<>(List.of(body)));

        ExecLambdaFunction cloned = original.clone();
        assertEquals(original.getCommonSubOperatorNum(), cloned.getCommonSubOperatorNum());
        assertEquals(original.isNondeterministic(), cloned.isNondeterministic());
        assertEquals(original.getNumChildren(), cloned.getNumChildren());
    }

    // ======================================================================
    // 24. ExecPlaceHolder
    // ======================================================================

    @Test
    public void testExecPlaceHolderConstruction() {
        ExecPlaceHolder expr = new ExecPlaceHolder(42, true, IntegerType.INT);

        assertEquals(42, expr.getSlotId());
        assertTrue(expr.isNullable());
        assertEquals(IntegerType.INT, expr.getType());
        assertEquals(0, expr.getNumChildren());
        assertEquals(TExprNodeType.PLACEHOLDER_EXPR, expr.getNodeType());
    }

    @Test
    public void testExecPlaceHolderNonNullable() {
        ExecPlaceHolder expr = new ExecPlaceHolder(10, false, VarcharType.VARCHAR);

        assertFalse(expr.isNullable());
        assertEquals(10, expr.getSlotId());
    }

    @Test
    public void testExecPlaceHolderIsNotConstant() {
        ExecPlaceHolder expr = new ExecPlaceHolder(1, false, IntegerType.INT);
        assertFalse(expr.isConstant());
    }

    @Test
    public void testExecPlaceHolderToThrift() {
        ExecPlaceHolder expr = new ExecPlaceHolder(42, true, IntegerType.INT);

        TExprNode node = new TExprNode();
        expr.toThrift(node);

        assertNotNull(node.getVslot_ref());
        assertEquals(42, node.getVslot_ref().getSlot_id());
        assertTrue(node.getVslot_ref().isNullable());
    }

    @Test
    public void testExecPlaceHolderToThriftNonNullable() {
        ExecPlaceHolder expr = new ExecPlaceHolder(7, false, IntegerType.INT);

        TExprNode node = new TExprNode();
        expr.toThrift(node);

        assertEquals(7, node.getVslot_ref().getSlot_id());
        assertFalse(node.getVslot_ref().isNullable());
    }

    @Test
    public void testExecPlaceHolderVisitorDispatch() {
        ExecPlaceHolder expr = new ExecPlaceHolder(1, false, IntegerType.INT);

        ExecExprVisitor<String, Void> visitor = new ExecExprVisitor<>() {
            @Override
            public String visitExecExpr(ExecExpr e, Void context) {
                return "default";
            }

            @Override
            public String visitExecPlaceHolder(ExecPlaceHolder e, Void context) {
                return "PlaceHolder";
            }
        };
        assertEquals("PlaceHolder", expr.accept(visitor, null));
    }

    @Test
    public void testExecPlaceHolderClone() {
        ExecPlaceHolder original = new ExecPlaceHolder(42, true, IntegerType.INT);

        ExecPlaceHolder cloned = original.clone();
        assertEquals(original.getSlotId(), cloned.getSlotId());
        assertEquals(original.isNullable(), cloned.isNullable());
        assertEquals(original.getType(), cloned.getType());
    }

    // ======================================================================
    // 25. ExecInformationFunction
    // ======================================================================

    @Test
    public void testExecInformationFunctionConstruction() {
        ExecInformationFunction expr = new ExecInformationFunction(
                VarcharType.VARCHAR, "database", "mydb", 0L);

        assertEquals(VarcharType.VARCHAR, expr.getType());
        assertEquals("database", expr.getFuncName());
        assertEquals("mydb", expr.getStrValue());
        assertEquals(0L, expr.getIntValue());
        assertEquals(0, expr.getNumChildren());
        assertEquals(TExprNodeType.INFO_FUNC, expr.getNodeType());
        assertFalse(expr.isNullable());
        assertTrue(expr.isConstant());
    }

    @Test
    public void testExecInformationFunctionWithIntValue() {
        ExecInformationFunction expr = new ExecInformationFunction(
                IntegerType.BIGINT, "connection_id", "", 12345L);

        assertEquals("connection_id", expr.getFuncName());
        assertEquals(12345L, expr.getIntValue());
    }

    @Test
    public void testExecInformationFunctionToThrift() {
        ExecInformationFunction expr = new ExecInformationFunction(
                VarcharType.VARCHAR, "database", "testdb", 0L);

        TExprNode node = new TExprNode();
        expr.toThrift(node);

        assertNotNull(node.info_func);
        assertEquals("testdb", node.info_func.getStr_value());
        assertEquals(0L, node.info_func.getInt_value());
    }

    @Test
    public void testExecInformationFunctionToThriftCurrentWarehouse() {
        ExecInformationFunction expr = new ExecInformationFunction(
                VarcharType.VARCHAR, "current_warehouse", "wh1", 0L);

        TExprNode node = new TExprNode();
        expr.toThrift(node);

        assertNotNull(node.info_func);
        assertEquals("wh1", node.info_func.getStr_value());
    }

    @Test
    public void testExecInformationFunctionVisitorDispatch() {
        ExecInformationFunction expr = new ExecInformationFunction(
                VarcharType.VARCHAR, "user", "root", 0L);

        ExecExprVisitor<String, Void> visitor = new ExecExprVisitor<>() {
            @Override
            public String visitExecExpr(ExecExpr e, Void context) {
                return "default";
            }

            @Override
            public String visitExecInformationFunction(ExecInformationFunction e, Void context) {
                return "InformationFunction";
            }
        };
        assertEquals("InformationFunction", expr.accept(visitor, null));
    }

    @Test
    public void testExecInformationFunctionClone() {
        ExecInformationFunction original = new ExecInformationFunction(
                VarcharType.VARCHAR, "database", "mydb", 0L);

        ExecInformationFunction cloned = original.clone();
        assertEquals(original.getFuncName(), cloned.getFuncName());
        assertEquals(original.getStrValue(), cloned.getStrValue());
        assertEquals(original.getIntValue(), cloned.getIntValue());
        assertEquals(original.getType(), cloned.getType());
    }

    // ======================================================================
    // 26. ExecExprUtils
    // ======================================================================

    @Test
    public void testExecExprUtilsCollectSlotRefs() {
        ExecSlotRef slot1 = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecSlotRef slot2 = makeSlotRef(2, 0, IntegerType.INT, false);
        ExecLiteral literal = makeIntLiteral(10);

        ScalarFunction fn = makeScalarFunction("add", new Type[]{IntegerType.INT, IntegerType.INT}, IntegerType.INT);
        ExecFunctionCall funcCall = makeFunctionCall("add", IntegerType.INT, fn,
                new ArrayList<>(List.of(slot1, literal)));

        ExecBinaryPredicate pred = new ExecBinaryPredicate(BinaryType.EQ, funcCall, slot2);

        List<ExecSlotRef> slotRefs = ExecExprUtils.collectSlotRefs(pred);
        assertEquals(2, slotRefs.size());
    }

    @Test
    public void testExecExprUtilsCollectSlotRefsNoSlots() {
        ExecLiteral literal = makeIntLiteral(42);
        List<ExecSlotRef> slotRefs = ExecExprUtils.collectSlotRefs(literal);
        assertTrue(slotRefs.isEmpty());
    }

    @Test
    public void testExecExprUtilsGetUsedSlotIdsSingleExpr() {
        ExecSlotRef slot1 = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecSlotRef slot2 = makeSlotRef(2, 0, IntegerType.INT, false);
        ExecBinaryPredicate pred = new ExecBinaryPredicate(BinaryType.EQ, slot1, slot2);

        Set<SlotId> slotIds = ExecExprUtils.getUsedSlotIds(pred);
        assertEquals(2, slotIds.size());
        assertTrue(slotIds.contains(new SlotId(1)));
        assertTrue(slotIds.contains(new SlotId(2)));
    }

    @Test
    public void testExecExprUtilsGetUsedSlotIdsList() {
        ExecSlotRef slot1 = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecSlotRef slot2 = makeSlotRef(2, 0, IntegerType.INT, false);
        ExecSlotRef slot3 = makeSlotRef(3, 0, IntegerType.INT, false);

        Set<SlotId> slotIds = ExecExprUtils.getUsedSlotIds(List.of(slot1, slot2, slot3));
        assertEquals(3, slotIds.size());
    }

    @Test
    public void testExecExprUtilsIsBoundByTupleIds() {
        ExecSlotRef slot = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLiteral literal = makeIntLiteral(10);
        ExecBinaryPredicate pred = new ExecBinaryPredicate(BinaryType.EQ, slot, literal);

        assertTrue(ExecExprUtils.isBoundByTupleIds(pred, List.of(new TupleId(0))));
        assertFalse(ExecExprUtils.isBoundByTupleIds(pred, List.of(new TupleId(1))));
    }

    @Test
    public void testExecExprUtilsIsBoundByTupleIdsLiteralOnly() {
        ExecLiteral literal = makeIntLiteral(42);
        // A literal is bound by any tuple
        assertTrue(ExecExprUtils.isBoundByTupleIds(literal, List.of(new TupleId(999))));
    }

    @Test
    public void testExecExprUtilsCloneList() {
        ExecLiteral lit1 = makeIntLiteral(1);
        ExecLiteral lit2 = makeIntLiteral(2);
        ExecLiteral lit3 = makeIntLiteral(3);

        List<ExecLiteral> originals = List.of(lit1, lit2, lit3);
        List<ExecLiteral> clones = ExecExprUtils.cloneList(originals);

        assertEquals(3, clones.size());
        for (int i = 0; i < originals.size(); i++) {
            assertEquals(originals.get(i).getValue().getInt(), clones.get(i).getValue().getInt());
            assertTrue(originals.get(i) != clones.get(i)); // Different objects
        }
    }

    @Test
    public void testExecExprUtilsCompoundAndEmpty() {
        assertNull(ExecExprUtils.compoundAnd(List.of()));
        assertNull(ExecExprUtils.compoundAnd(null));
    }

    @Test
    public void testExecExprUtilsCompoundAndSingle() {
        ExecSlotRef slot = makeSlotRef(1, 0, BooleanType.BOOLEAN, false);
        ExecExpr result = ExecExprUtils.compoundAnd(List.of(slot));

        // Single element should be returned as-is
        assertEquals(slot, result);
    }

    @Test
    public void testExecExprUtilsCompoundAndMultiple() {
        ExecSlotRef slot1 = makeSlotRef(1, 0, BooleanType.BOOLEAN, false);
        ExecSlotRef slot2 = makeSlotRef(2, 0, BooleanType.BOOLEAN, false);
        ExecSlotRef slot3 = makeSlotRef(3, 0, BooleanType.BOOLEAN, false);

        ExecExpr result = ExecExprUtils.compoundAnd(List.of(slot1, slot2, slot3));

        // Should be a left-deep AND tree: AND(AND(slot1, slot2), slot3)
        assertTrue(result instanceof ExecCompoundPredicate);
        ExecCompoundPredicate outerAnd = (ExecCompoundPredicate) result;
        assertEquals(CompoundPredicate.Operator.AND, outerAnd.getCompoundType());
        assertTrue(outerAnd.getChild(0) instanceof ExecCompoundPredicate);
    }

    @Test
    public void testExecExprUtilsContainsDictMappingExprFalse() {
        ExecSlotRef slot = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecLiteral literal = makeIntLiteral(10);
        ExecBinaryPredicate pred = new ExecBinaryPredicate(BinaryType.EQ, slot, literal);

        assertFalse(ExecExprUtils.containsDictMappingExpr(pred));
        assertFalse(ExecExprUtils.containsDictMappingExpr(null));
    }

    @Test
    public void testExecExprUtilsContainsDictMappingExprTrue() {
        ExecDictMapping dictMapping = new ExecDictMapping(VarcharType.VARCHAR,
                new ArrayList<>(List.of(makeSlotRef(1, 0, IntegerType.INT, false))));
        ExecBinaryPredicate pred = new ExecBinaryPredicate(BinaryType.EQ, dictMapping, makeIntLiteral(1));

        assertTrue(ExecExprUtils.containsDictMappingExpr(pred));
    }

    @Test
    public void testExecExprUtilsContainsDictMappingExprNested() {
        ExecDictMapping dictMapping = new ExecDictMapping(VarcharType.VARCHAR,
                new ArrayList<>(List.of(makeSlotRef(1, 0, IntegerType.INT, false))));
        ExecCast cast = new ExecCast(IntegerType.INT, dictMapping, false);
        ExecBinaryPredicate pred = new ExecBinaryPredicate(BinaryType.EQ, cast, makeIntLiteral(1));

        assertTrue(ExecExprUtils.containsDictMappingExpr(pred));
    }

    @Test
    public void testExecExprUtilsUnwrapSlotRefDirect() {
        ExecSlotRef slot = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecSlotRef unwrapped = ExecExprUtils.unwrapSlotRef(slot);
        assertEquals(slot, unwrapped);
    }

    @Test
    public void testExecExprUtilsUnwrapSlotRefThroughCast() {
        ExecSlotRef slot = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecCast cast = new ExecCast(IntegerType.BIGINT, slot, false);

        ExecSlotRef unwrapped = ExecExprUtils.unwrapSlotRef(cast);
        assertNotNull(unwrapped);
        assertEquals(1, unwrapped.getSlotId().asInt());
    }

    @Test
    public void testExecExprUtilsUnwrapSlotRefNestedCasts() {
        ExecSlotRef slot = makeSlotRef(1, 0, IntegerType.INT, false);
        ExecCast cast1 = new ExecCast(IntegerType.BIGINT, slot, false);
        ExecCast cast2 = new ExecCast(VarcharType.VARCHAR, cast1, false);

        ExecSlotRef unwrapped = ExecExprUtils.unwrapSlotRef(cast2);
        assertNotNull(unwrapped);
        assertEquals(1, unwrapped.getSlotId().asInt());
    }

    @Test
    public void testExecExprUtilsUnwrapSlotRefNonSlot() {
        ExecLiteral literal = makeIntLiteral(42);
        assertNull(ExecExprUtils.unwrapSlotRef(literal));
    }

    @Test
    public void testExecExprUtilsUnwrapSlotRefCastOfNonSlot() {
        ExecLiteral literal = makeIntLiteral(42);
        ExecCast cast = new ExecCast(IntegerType.BIGINT, literal, false);

        assertNull(ExecExprUtils.unwrapSlotRef(cast));
    }
}
