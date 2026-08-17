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

package com.starrocks.authorization;

import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.load.rejected.RejectedRecordsTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Focused unit tests for the {@code _statistics_.rejected_records} row
 * access policy. The path that enumerates user-visible tables needs a
 * live cluster and is covered by cluster-integration tests elsewhere;
 * here we exercise the parts that can run against plain classes:
 * {@code matches()} guard, the admin bypass, the fail-closed branches
 * (null context, null user), and the OR-chain shape produced by
 * {@code buildOrChain()}.
 */
public class NativeAccessControllerRejectedRecordsPolicyTest {

    private static TableName rejectedRecordsTableName() {
        return new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                RejectedRecordsTable.DATABASE_NAME, RejectedRecordsTable.TABLE_NAME);
    }

    private static ConnectContext ctxFor(UserIdentity user) {
        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(user);
        return ctx;
    }

    @Test
    public void testUnrelatedTableReturnsNullForAdmin() {
        NativeAccessController controller = new NativeAccessController();
        ConnectContext ctx = ctxFor(UserIdentity.ROOT);
        TableName other = new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, "db1", "orders");
        Assertions.assertNull(controller.getRowAccessPolicy(ctx, other));
    }

    @Test
    public void testUnrelatedTableReturnsNullForNonAdmin() {
        NativeAccessController controller = new NativeAccessController();
        ConnectContext ctx = ctxFor(UserIdentity.createAnalyzedUserIdentWithIp("alice", "%"));
        TableName other = new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, "db1", "orders");
        Assertions.assertNull(controller.getRowAccessPolicy(ctx, other));
    }

    @Test
    public void testRejectedRecordsVisibleToAdmin() {
        // Admin queries get no policy attached -- everything visible.
        NativeAccessController controller = new NativeAccessController();
        ConnectContext ctx = ctxFor(UserIdentity.ROOT);
        Assertions.assertNull(controller.getRowAccessPolicy(ctx, rejectedRecordsTableName()));
    }

    @Test
    public void testRejectedRecordsHiddenWhenNoUserIdentitySet() {
        // Fail-closed: null user identity -> empty result set.
        NativeAccessController controller = new NativeAccessController();
        ConnectContext ctx = new ConnectContext();
        Expr policy = controller.getRowAccessPolicy(ctx, rejectedRecordsTableName());
        Assertions.assertInstanceOf(BoolLiteral.class, policy);
        Assertions.assertFalse(((BoolLiteral) policy).getValue());
    }

    @Test
    public void testRejectedRecordsHiddenWhenContextIsNull() {
        NativeAccessController controller = new NativeAccessController();
        Expr policy = controller.getRowAccessPolicy(null, rejectedRecordsTableName());
        Assertions.assertInstanceOf(BoolLiteral.class, policy);
        Assertions.assertFalse(((BoolLiteral) policy).getValue());
    }

    @Test
    public void testNonDefaultCatalogBypassesPolicy() {
        // A hive-catalog table that happens to share the name must NOT
        // be touched by this policy.
        NativeAccessController controller = new NativeAccessController();
        ConnectContext ctx = ctxFor(UserIdentity.createAnalyzedUserIdentWithIp("alice", "%"));
        TableName hive = new TableName("hive_catalog", RejectedRecordsTable.DATABASE_NAME,
                RejectedRecordsTable.TABLE_NAME);
        Assertions.assertNull(controller.getRowAccessPolicy(ctx, hive));
    }

    @Test
    public void testDifferentDatabaseInDefaultCatalogBypassesPolicy() {
        NativeAccessController controller = new NativeAccessController();
        ConnectContext ctx = ctxFor(UserIdentity.createAnalyzedUserIdentWithIp("alice", "%"));
        TableName other = new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, "not_statistics",
                RejectedRecordsTable.TABLE_NAME);
        Assertions.assertNull(controller.getRowAccessPolicy(ctx, other));
    }

    @Test
    public void testNullTableNameReturnsNull() {
        NativeAccessController controller = new NativeAccessController();
        ConnectContext ctx = ctxFor(UserIdentity.ROOT);
        Assertions.assertNull(controller.getRowAccessPolicy(ctx, null));
    }

    @Test
    public void testMatchesRecognisesDefaultCatalogTable() {
        Assertions.assertTrue(RejectedRecordsRowAccessPolicy.matches(rejectedRecordsTableName()));
        // Null catalog defaults to default_catalog at the analyzer layer
        // so the guard must accept it too.
        Assertions.assertTrue(RejectedRecordsRowAccessPolicy.matches(
                new TableName(null, RejectedRecordsTable.DATABASE_NAME, RejectedRecordsTable.TABLE_NAME)));
    }

    @Test
    public void testMatchesRejectsUnrelatedTables() {
        Assertions.assertFalse(RejectedRecordsRowAccessPolicy.matches(null));
        Assertions.assertFalse(RejectedRecordsRowAccessPolicy.matches(
                new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, "db1", "orders")));
        Assertions.assertFalse(RejectedRecordsRowAccessPolicy.matches(
                new TableName("hive_catalog", RejectedRecordsTable.DATABASE_NAME, RejectedRecordsTable.TABLE_NAME)));
    }

    @Test
    public void testBuildOrChainSingleTableProducesSingleAndConjunct() {
        TableName subject = rejectedRecordsTableName();
        List<TableName> accessible = new ArrayList<>();
        accessible.add(new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, "mydb", "orders"));

        Expr built = RejectedRecordsRowAccessPolicy.buildOrChain(subject, accessible);

        Assertions.assertInstanceOf(CompoundPredicate.class, built);
        CompoundPredicate and = (CompoundPredicate) built;
        Assertions.assertEquals(CompoundPredicate.Operator.AND, and.getOp());

        BinaryPredicate left = (BinaryPredicate) and.getChild(0);
        Assertions.assertEquals(BinaryType.EQ, left.getOp());
        Assertions.assertInstanceOf(SlotRef.class, left.getChild(0));
        Assertions.assertEquals("target_database", ((SlotRef) left.getChild(0)).getColumnName());
        Assertions.assertEquals("mydb", ((StringLiteral) left.getChild(1)).getValue());

        BinaryPredicate right = (BinaryPredicate) and.getChild(1);
        Assertions.assertEquals("target_table", ((SlotRef) right.getChild(0)).getColumnName());
        Assertions.assertEquals("orders", ((StringLiteral) right.getChild(1)).getValue());
    }

    @Test
    public void testBuildOrChainMultipleTablesProducesLeftAssociativeOr() {
        TableName subject = rejectedRecordsTableName();
        List<TableName> accessible = Arrays.asList(
                new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, "db1", "t1"),
                new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, "db1", "t2"),
                new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, "db2", "orders"));

        Expr built = RejectedRecordsRowAccessPolicy.buildOrChain(subject, accessible);

        Assertions.assertInstanceOf(CompoundPredicate.class, built);
        CompoundPredicate outerOr = (CompoundPredicate) built;
        Assertions.assertEquals(CompoundPredicate.Operator.OR, outerOr.getOp());
        // The outermost OR's right child is the LAST pair (db2.orders),
        // left child is an inner OR of the first two. Walking left
        // recovers all three leaves in the order they were supplied.
        List<String> collected = new ArrayList<>();
        collectTableNames(built, collected);
        Assertions.assertEquals(Arrays.asList("db1.t1", "db1.t2", "db2.orders"), collected);
    }

    @Test
    public void testBuildOrChainReturnsNullForEmptyTableList() {
        Expr built = RejectedRecordsRowAccessPolicy.buildOrChain(rejectedRecordsTableName(), new ArrayList<>());
        Assertions.assertNull(built, "caller is expected to translate null into BoolLiteral(false)");
    }

    // Walk an (OR ... (AND db-eq tbl-eq)) tree and append "db.tbl"
    // strings to `out` in the order they were constructed, so tests can
    // assert on the shape of the generated predicate without recursion
    // boilerplate at every call site.
    private static void collectTableNames(Expr node, List<String> out) {
        if (node instanceof CompoundPredicate) {
            CompoundPredicate cp = (CompoundPredicate) node;
            if (cp.getOp() == CompoundPredicate.Operator.OR) {
                collectTableNames(cp.getChild(0), out);
                collectTableNames(cp.getChild(1), out);
                return;
            }
            if (cp.getOp() == CompoundPredicate.Operator.AND) {
                String db = ((StringLiteral) ((BinaryPredicate) cp.getChild(0)).getChild(1)).getValue();
                String tbl = ((StringLiteral) ((BinaryPredicate) cp.getChild(1)).getChild(1)).getValue();
                out.add(db + "." + tbl);
                return;
            }
        }
        Assertions.fail("Unexpected node in predicate tree: " + node);
    }
}
