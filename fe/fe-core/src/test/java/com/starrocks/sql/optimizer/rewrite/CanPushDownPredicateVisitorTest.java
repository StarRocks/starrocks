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

package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.catalog.JDBCTable.ProtocolType;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Per-dialect gating truth table for {@link CanPushDownPredicateVisitor}. Each operator below is a
 * pushdown atom whose acceptance differs by dialect; the asserted matrix mirrors section 5 of the
 * JDBC pushdown test design (the dialect capability table). UNKNOWN is the dialect SQL Server
 * resolves to (jdbc:sqlserver: has no dedicated ProtocolType).
 */
public class CanPushDownPredicateVisitorTest {

    private static final ColumnRefOperator COL = new ColumnRefOperator(1, IntegerType.INT, "c", true);

    private void assertPush(ScalarOperator op, ProtocolType dialect, boolean expected) {
        Assertions.assertEquals(expected, CanPushDownPredicateVisitor.canPushDown(op, dialect),
                dialect + " push-down of " + op);
    }

    // col <op> const wrapped in a comparison so the whole thing is a realistic predicate.
    private ScalarOperator infixPredicate(String fnName) {
        CallOperator call = new CallOperator(fnName, IntegerType.INT,
                List.of(COL, ConstantOperator.createInt(2)));
        return new BinaryPredicateOperator(BinaryType.GT, call, ConstantOperator.createInt(1));
    }

    @Test
    public void testDivideGate() {
        // divide: only Oracle (base behavior) and ClickHouse push it; MySQL/PG/UNKNOWN keep it local
        // because their renderers diverge from StarRocks' DOUBLE division semantics.
        ScalarOperator div = infixPredicate("divide");
        assertPush(div, ProtocolType.MYSQL, false);
        assertPush(div, ProtocolType.MARIADB, false);
        assertPush(div, ProtocolType.POSTGRES, false);
        assertPush(div, ProtocolType.ORACLE, true);
        assertPush(div, ProtocolType.CLICKHOUSE, true);
        assertPush(div, ProtocolType.UNKNOWN, false);
    }

    @Test
    public void testModGate() {
        // mod (%): MySQL/PG/ClickHouse push it; Oracle and UNKNOWN do not (no % operator).
        ScalarOperator mod = infixPredicate("mod");
        assertPush(mod, ProtocolType.MYSQL, true);
        assertPush(mod, ProtocolType.POSTGRES, true);
        assertPush(mod, ProtocolType.CLICKHOUSE, true);
        assertPush(mod, ProtocolType.ORACLE, false);
        assertPush(mod, ProtocolType.UNKNOWN, false);
    }

    @Test
    public void testConcatGate() {
        // concat: only the MySQL-compatible renderer emits CONCAT(...); every other dialect keeps it local.
        ScalarOperator concat = new CallOperator("concat", VarcharType.VARCHAR,
                List.of(COL, ConstantOperator.createVarchar("x")));
        assertPush(concat, ProtocolType.MYSQL, true);
        assertPush(concat, ProtocolType.MARIADB, true);
        assertPush(concat, ProtocolType.POSTGRES, false);
        assertPush(concat, ProtocolType.ORACLE, false);
        assertPush(concat, ProtocolType.UNKNOWN, false);
    }

    @Test
    public void testBooleanConstantGate() {
        // A boolean constant is unsupported on Oracle (no SQL boolean) and UNKNOWN; fine elsewhere.
        ScalarOperator boolConst = ConstantOperator.createBoolean(true);
        assertPush(boolConst, ProtocolType.MYSQL, true);
        assertPush(boolConst, ProtocolType.POSTGRES, true);
        assertPush(boolConst, ProtocolType.CLICKHOUSE, true);
        assertPush(boolConst, ProtocolType.ORACLE, false);
        assertPush(boolConst, ProtocolType.UNKNOWN, false);
    }

    @Test
    public void testEqForNullGate() {
        // <=> (EQ_FOR_NULL): MySQL/PG/ClickHouse accept it; Oracle and UNKNOWN do not.
        ScalarOperator eqForNull = new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL, COL, COL);
        assertPush(eqForNull, ProtocolType.MYSQL, true);
        assertPush(eqForNull, ProtocolType.POSTGRES, true);
        assertPush(eqForNull, ProtocolType.CLICKHOUSE, true);
        assertPush(eqForNull, ProtocolType.ORACLE, false);
        assertPush(eqForNull, ProtocolType.UNKNOWN, false);
    }

    private InPredicateOperator inList(int n) {
        List<ScalarOperator> children = new ArrayList<>();
        children.add(COL);
        for (int i = 0; i < n; i++) {
            children.add(ConstantOperator.createInt(i));
        }
        return new InPredicateOperator(false, children);
    }

    @Test
    public void testInListSizeCap() {
        // jdbc_predicate_pushdown_max_in_list_size: -1 unlimited, 0 never, N cap-at-N.
        InPredicateOperator in4 = inList(4);
        InPredicateOperator in2 = inList(2);
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(in4, ProtocolType.MYSQL, -1));
        Assertions.assertFalse(CanPushDownPredicateVisitor.canPushDown(in4, ProtocolType.MYSQL, 0));
        Assertions.assertFalse(CanPushDownPredicateVisitor.canPushDown(in4, ProtocolType.MYSQL, 3));
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(in2, ProtocolType.MYSQL, 3));
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(in2, ProtocolType.MYSQL, 2));
    }

    @Test
    public void testOracleInListNoHardCap() {
        // Oracle has no built-in IN-list floor: size is governed purely by the session cap, like
        // every dialect (ORA-01795's limit is Oracle-version-specific, so it is not hardcoded).
        InPredicateOperator in1001 = inList(1001);
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(in1001, ProtocolType.ORACLE, -1));
        Assertions.assertTrue(CanPushDownPredicateVisitor.canPushDown(in1001, ProtocolType.MYSQL, -1));
        // A cap set below the list size keeps it local — on Oracle just like the other dialects.
        Assertions.assertFalse(CanPushDownPredicateVisitor.canPushDown(in1001, ProtocolType.ORACLE, 1000));
    }
}
