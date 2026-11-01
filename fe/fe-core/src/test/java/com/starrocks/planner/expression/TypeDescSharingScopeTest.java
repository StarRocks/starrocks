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

import com.google.common.collect.Lists;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.SlotId;
import com.starrocks.rpc.ConfigurableSerDesFactory;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.InPredicate;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TDataPartition;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TPlanFragment;
import com.starrocks.thrift.TTypeDesc;
import com.starrocks.thrift.TTypeNodeType;
import com.starrocks.type.ArrayType;
import com.starrocks.type.BooleanType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.MapType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeSerializer;
import com.starrocks.type.VarcharType;
import mockit.Mock;
import mockit.MockUp;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TSerializer;
import org.apache.thrift.TUnion;
import org.apache.thrift.meta_data.FieldMetaData;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collection;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TypeDescSharingScopeTest extends PlanTestBase {
    private static final int STRUCT_WIDTH = 200;
    private static final int NUM_ACCESSED_FIELDS = 50;

    // Reference the wide struct column as a whole, through many subfields, in a predicate and in a comparison of
    // complex values (which also carries the child type), so that its type is carried by many expr nodes.
    private static final String WIDE_STRUCT_SQL = "select s, " +
            IntStream.range(0, NUM_ACCESSED_FIELDS).mapToObj(i -> "s.f" + i).collect(Collectors.joining(", ")) +
            " from t_wide_struct where s.f0 > 0 and s = s2";

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        String structType = "struct<" +
                IntStream.range(0, STRUCT_WIDTH).mapToObj(i -> "f" + i + " int").collect(Collectors.joining(", ")) +
                ">";
        starRocksAssert.withTable("create table t_wide_struct(k int, s " + structType + ", s2 " + structType + ") " +
                "duplicate key(k) distributed by hash(k) buckets 1 properties('replication_num'='1');");
    }

    private static StructType newStructType(String... fieldNames) {
        List<StructField> fields = Lists.newArrayList();
        for (String fieldName : fieldNames) {
            fields.add(new StructField(fieldName, IntegerType.INT));
        }
        return new StructType(fields, true);
    }

    @Test
    public void testNoSharingWithoutScope() {
        StructType type = newStructType("a", "b");
        TTypeDesc first = TypeDescSharingScope.toThrift(type);
        TTypeDesc second = TypeDescSharingScope.toThrift(type);
        assertNotSame(first, second);
        assertEquals(first, second);
        assertEquals(TypeSerializer.toThrift(type), first);
    }

    @Test
    public void testShareComplexTypeObjectInScope() {
        List<Type> types = List.of(newStructType("a", "b"), new ArrayType(VarcharType.VARCHAR),
                new MapType(IntegerType.INT, newStructType("c")));
        try (TypeDescSharingScope ignored = TypeDescSharingScope.open()) {
            for (Type type : types) {
                TTypeDesc first = TypeDescSharingScope.toThrift(type);
                assertSame(first, TypeDescSharingScope.toThrift(type));
                assertEquals(TypeSerializer.toThrift(type), first);
            }
        }
    }

    @Test
    public void testNotShareScalarType() {
        try (TypeDescSharingScope ignored = TypeDescSharingScope.open()) {
            assertNotSame(TypeDescSharingScope.toThrift(IntegerType.INT), TypeDescSharingScope.toThrift(IntegerType.INT));
        }
    }

    @Test
    public void testShareTypeObjectsWithSameThriftForm() {
        try (TypeDescSharingScope ignored = TypeDescSharingScope.open()) {
            assertSame(TypeDescSharingScope.toThrift(newStructType("a", "b")),
                    TypeDescSharingScope.toThrift(newStructType("a", "b")));
        }
    }

    private static Stream<Arguments> equalTypesWithDifferentThriftForms() {
        // StructType.equals ignores the case of the field names, the field comments and whether the struct is named,
        // which are all serialized.
        return Stream.of(
                Arguments.of(newStructType("a", "b"), newStructType("A", "B")),
                Arguments.of(new StructType(List.of(new StructField("a", IntegerType.INT, "comment 1")), true),
                        new StructType(List.of(new StructField("a", IntegerType.INT, "comment 2")), true)),
                Arguments.of(new StructType(List.of(new StructField("a", IntegerType.INT)), true),
                        new StructType(List.of(new StructField("a", IntegerType.INT)), false)));
    }

    @ParameterizedTest
    @MethodSource("equalTypesWithDifferentThriftForms")
    public void testNotShareEqualTypeObjectsWithDifferentThriftForms(Type first, Type second) {
        assertEquals(first, second);
        try (TypeDescSharingScope ignored = TypeDescSharingScope.open()) {
            TTypeDesc firstDesc = TypeDescSharingScope.toThrift(first);
            TTypeDesc secondDesc = TypeDescSharingScope.toThrift(second);
            assertNotEquals(firstDesc, secondDesc);
            assertEquals(TypeSerializer.toThrift(first), firstDesc);
            assertEquals(TypeSerializer.toThrift(second), secondDesc);
        }
    }

    @Test
    public void testScopeIsPerThread() throws Exception {
        StructType type = newStructType("a");
        try (TypeDescSharingScope ignored = TypeDescSharingScope.open()) {
            TTypeDesc desc = TypeDescSharingScope.toThrift(type);
            List<TTypeDesc> otherThreadDescs = Lists.newArrayList();
            Thread otherThread = new Thread(() -> {
                otherThreadDescs.add(TypeDescSharingScope.toThrift(type));
                otherThreadDescs.add(TypeDescSharingScope.toThrift(type));
            });
            otherThread.start();
            otherThread.join();
            assertNotSame(otherThreadDescs.get(0), otherThreadDescs.get(1));
            assertNotSame(desc, otherThreadDescs.get(0));
            assertSame(desc, TypeDescSharingScope.toThrift(type));
        }
    }

    @Test
    public void testScopeClosedWhenFragmentConversionFails() throws Exception {
        PlanFragment fragment = getExecPlan(WIDE_STRUCT_SQL).getFragments().get(0);
        // Fail after the expr nodes of the plan are converted, until the end of this test.
        new MockUp<DataPartition>() {
            @Mock
            public TDataPartition toThrift() {
                throw new IllegalStateException("injected failure");
            }
        };
        assertThrows(IllegalStateException.class, fragment::toThrift);
        // A scope left open would share the types with the next conversion in this thread.
        StructType type = newStructType("a");
        assertNotSame(TypeDescSharingScope.toThrift(type), TypeDescSharingScope.toThrift(type));
    }

    private static SlotRef newSlotRef(int slotId, Type type) {
        return new SlotRef(new SlotDescriptor(new SlotId(slotId), "c" + slotId, type, true));
    }

    @Test
    public void testShareChildTypeDesc() {
        StructType type = newStructType("a", "b");
        Expr cast = new CastExpr(newStructType("x", "y"), newSlotRef(1, type));
        Expr binaryPredicate = new BinaryPredicate(BinaryType.EQ, newSlotRef(1, type), newSlotRef(2, type));
        binaryPredicate.setType(BooleanType.BOOLEAN);
        Expr inPredicate = new InPredicate(newSlotRef(1, type), List.of(newSlotRef(2, type)), false);
        inPredicate.setType(BooleanType.BOOLEAN);
        try (TypeDescSharingScope ignored = TypeDescSharingScope.open()) {
            TTypeDesc desc = TypeDescSharingScope.toThrift(type);
            for (Expr expr : List.of(cast, binaryPredicate, inPredicate)) {
                assertChildrenShareTypeDesc(desc, ExprToThrift.treeToThrift(expr).getNodes());
            }
        }
    }

    private static ExecSlotRef newExecSlotRef(int slotId, Type type) {
        return new ExecSlotRef(new SlotDescriptor(new SlotId(slotId), "c" + slotId, type, true));
    }

    @Test
    public void testShareTypeDescOfExecExprs() {
        StructType type = newStructType("a", "b");
        ExecExpr cast = new ExecCast(newStructType("x", "y"), newExecSlotRef(1, type), false);
        ExecExpr binaryPredicate = new ExecBinaryPredicate(BinaryType.EQ, newExecSlotRef(1, type),
                newExecSlotRef(2, type));
        ExecExpr inPredicate = new ExecInPredicate(false, List.of(newExecSlotRef(1, type), newExecSlotRef(2, type)));
        try (TypeDescSharingScope ignored = TypeDescSharingScope.open()) {
            TTypeDesc desc = TypeDescSharingScope.toThrift(type);
            for (ExecExpr expr : List.of(cast, binaryPredicate, inPredicate)) {
                assertChildrenShareTypeDesc(desc, ExecExprSerializer.serialize(expr).getNodes());
            }
        }
    }

    // The root carries the type of its children as child_type_desc, and every child carries it as its type.
    private static void assertChildrenShareTypeDesc(TTypeDesc desc, List<TExprNode> nodes) {
        assertSame(desc, nodes.get(0).getChild_type_desc());
        for (TExprNode child : nodes.subList(1, nodes.size())) {
            assertSame(desc, child.getType());
        }
    }

    @Test
    public void testNestedScopeReusesOuterScope() {
        StructType type = newStructType("a");
        try (TypeDescSharingScope outer = TypeDescSharingScope.open()) {
            TTypeDesc outerDesc = TypeDescSharingScope.toThrift(type);
            try (TypeDescSharingScope inner = TypeDescSharingScope.open()) {
                assertSame(outerDesc, TypeDescSharingScope.toThrift(type));
            }
            // Closing the nested scope keeps the outer one.
            assertSame(outerDesc, TypeDescSharingScope.toThrift(type));
        }
        // Closing the outermost scope clears the cache.
        assertNotSame(TypeDescSharingScope.toThrift(type), TypeDescSharingScope.toThrift(type));
    }

    @Test
    public void testFragmentSharesTypeDescOfWideStruct() throws Exception {
        ExecPlan execPlan = getExecPlan(WIDE_STRUCT_SQL);
        List<Map<TTypeDesc, Integer>> sharedRefCounts = Lists.newArrayList();
        for (PlanFragment fragment : execPlan.getFragments()) {
            sharedRefCounts.add(wideStructTypeDescRefCounts(fragment.toThrift()));
        }

        // Build every type the way it was built before sharing, until the end of this test.
        new MockUp<TypeDescSharingScope>() {
            @Mock
            public TTypeDesc toThrift(Type type) {
                return TypeSerializer.toThrift(type);
            }
        };
        int numRefs = 0;
        int numDistinct = 0;
        for (int i = 0; i < execPlan.getFragments().size(); i++) {
            Map<TTypeDesc, Integer> regularRefCounts =
                    wideStructTypeDescRefCounts(execPlan.getFragments().get(i).toThrift());
            Map<TTypeDesc, Integer> refCounts = sharedRefCounts.get(i);
            // Every reference is kept, and the references with the same thrift form share one object.
            assertEquals(regularRefCounts.size(), refCounts.values().stream().mapToInt(Integer::intValue).sum());
            assertEquals(new HashSet<>(regularRefCounts.keySet()).size(), refCounts.size());
            numRefs += regularRefCounts.size();
            numDistinct += refCounts.size();
        }
        assertTrue(numRefs > NUM_ACCESSED_FIELDS, "wide struct type is referenced " + numRefs + " times");
        assertTrue(numDistinct * 10 <= numRefs,
                "wide struct type is built " + numDistinct + " times for " + numRefs + " references");
    }

    @ParameterizedTest
    @ValueSource(strings = {"binary", "compact", "json"})
    public void testSameBytesAsWithoutSharing(String protocol) throws Exception {
        ExecPlan execPlan = getExecPlan(WIDE_STRUCT_SQL);
        List<TPlanFragment> sharedFragments = Lists.newArrayList();
        for (PlanFragment fragment : execPlan.getFragments()) {
            sharedFragments.add(fragment.toThrift());
        }

        // Build every type the way it was built before sharing, until the end of this test.
        new MockUp<TypeDescSharingScope>() {
            @Mock
            public TTypeDesc toThrift(Type type) {
                return TypeSerializer.toThrift(type);
            }
        };
        TSerializer serializer = ConfigurableSerDesFactory.getTSerializer(protocol);
        for (int i = 0; i < execPlan.getFragments().size(); i++) {
            TPlanFragment regular = execPlan.getFragments().get(i).toThrift();
            assertTrue(wideStructTypeDescRefCounts(regular).values().stream().allMatch(count -> count == 1));
            TPlanFragment shared = sharedFragments.get(i);
            assertEquals(regular, shared);
            assertArrayEquals(serializer.serialize(regular), serializer.serialize(shared));
        }
    }

    // Count the references from the thrift expr nodes of the fragment to each thrift type of a wide struct, by identity.
    // The other thrift types, such as the column schemas of a scan node, are built once per column and not shared.
    private static Map<TTypeDesc, Integer> wideStructTypeDescRefCounts(TPlanFragment fragment) {
        Map<TTypeDesc, Integer> refCounts = new IdentityHashMap<>();
        collectExprTypeDescs(fragment, refCounts);
        refCounts.keySet().removeIf(desc -> desc.getTypes().get(0).getType() != TTypeNodeType.STRUCT ||
                desc.getTypes().get(0).getStruct_fields().size() != STRUCT_WIDTH);
        return refCounts;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void collectExprTypeDescs(Object value, Map<TTypeDesc, Integer> refCounts) {
        if (value instanceof TTypeDesc) {
            return;
        }
        if (value instanceof TExprNode) {
            TExprNode node = (TExprNode) value;
            refCounts.merge(node.getType(), 1, Integer::sum);
            if (node.isSetChild_type_desc()) {
                refCounts.merge(node.getChild_type_desc(), 1, Integer::sum);
            }
        }
        if (value instanceof TUnion) {
            collectExprTypeDescs(((TUnion) value).getFieldValue(), refCounts);
        } else if (value instanceof TBase) {
            TBase struct = (TBase) value;
            Map<? extends TFieldIdEnum, FieldMetaData> fields =
                    FieldMetaData.getStructMetaDataMap((Class) struct.getClass());
            for (TFieldIdEnum field : fields.keySet()) {
                collectExprTypeDescs(struct.getFieldValue(field), refCounts);
            }
        } else if (value instanceof Collection) {
            for (Object element : (Collection<?>) value) {
                collectExprTypeDescs(element, refCounts);
            }
        } else if (value instanceof Map) {
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                collectExprTypeDescs(entry.getKey(), refCounts);
                collectExprTypeDescs(entry.getValue(), refCounts);
            }
        }
    }
}
