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
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TExprOpcode;
import com.starrocks.thrift.TKeysType;
import com.starrocks.type.BooleanType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
}
