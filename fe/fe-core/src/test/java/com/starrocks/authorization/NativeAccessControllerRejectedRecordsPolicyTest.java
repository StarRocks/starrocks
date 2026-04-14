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
import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.sql.ast.expression.Expr;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Focused unit tests for the built-in row access policy on
 * {@code _statistics_.rejected_records}. These tests construct a
 * {@link NativeAccessController} directly so they do not require a live
 * cluster -- the method under test is pure and only reads from the passed
 * {@link ConnectContext} and {@link TableName}.
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
    public void testRejectedRecordsHiddenFromNonAdmin() {
        // Non-admin: policy returns BoolLiteral(false) so the injected
        // WHERE clause is always false, yielding an empty result set.
        NativeAccessController controller = new NativeAccessController();
        ConnectContext ctx = ctxFor(UserIdentity.createAnalyzedUserIdentWithIp("alice", "%"));
        Expr policy = controller.getRowAccessPolicy(ctx, rejectedRecordsTableName());
        Assertions.assertInstanceOf(BoolLiteral.class, policy);
        Assertions.assertFalse(((BoolLiteral) policy).getValue(),
                "non-admin policy must be BoolLiteral(false) to empty the result set");
    }

    @Test
    public void testRejectedRecordsHiddenWhenNoUserIdentitySet() {
        // Fail-closed: if the ConnectContext has no user, treat as non-admin.
        NativeAccessController controller = new NativeAccessController();
        ConnectContext ctx = new ConnectContext();
        // Intentionally do NOT call setCurrentUserIdentity.
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
        // A hive-catalog table that happens to share the `_statistics_` /
        // `rejected_records` names must NOT be touched by this policy.
        NativeAccessController controller = new NativeAccessController();
        ConnectContext ctx = ctxFor(UserIdentity.createAnalyzedUserIdentWithIp("alice", "%"));
        TableName hive = new TableName("hive_catalog", RejectedRecordsTable.DATABASE_NAME,
                RejectedRecordsTable.TABLE_NAME);
        Assertions.assertNull(controller.getRowAccessPolicy(ctx, hive));
    }

    @Test
    public void testDifferentDatabaseInDefaultCatalogBypassesPolicy() {
        // Same table name, different database: this is an unrelated table.
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
}
