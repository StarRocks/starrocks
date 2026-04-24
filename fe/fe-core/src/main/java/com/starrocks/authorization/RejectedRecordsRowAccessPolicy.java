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
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Shared row-access-policy logic for the {@code _statistics_.rejected_records}
 * system table, consumed by every concrete {@link AccessController}
 * implementation (native, Ranger-StarRocks, Ranger-Hive).
 *
 * <p>Policy semantics:
 * <ul>
 *   <li>{@code root} sees every row (returns {@code null}, i.e. no predicate).</li>
 *   <li>Any other user sees only rows whose
 *       {@code (target_database, target_table)} pair they already have
 *       {@code SELECT} on in the default internal catalog. The policy is
 *       rendered as a compound {@code OR} of per-pair equality conjuncts.</li>
 *   <li>A user with {@code SELECT} on zero tables, or a query with no
 *       {@link ConnectContext} / no current user, gets a
 *       {@code BoolLiteral(false)} -- fail-closed so a forgotten identity
 *       cannot leak bad data.</li>
 * </ul>
 *
 * <p>This helper is the single source of truth for the policy. When a
 * future commit introduces a {@code has_table_privilege()} scalar SQL
 * function, only the body of {@link #buildPolicy} changes; callers still
 * get the same {@link Expr} shape back.
 *
 * <p><b>Performance note:</b> {@link #buildPolicy} enumerates every
 * table in every database in the default internal catalog and runs an
 * {@link Authorizer#checkTableAction} per candidate. Cost is therefore
 * O(#databases &times; #tables) per query against
 * {@code _statistics_.rejected_records}, and the resulting predicate
 * is a left-linear chain of {@code (db = 'x' AND table = 'y') OR ...}
 * whose depth scales with the number of SELECT-privileged tables.
 * Clusters with tens of thousands of user-owned tables should use a
 * dedicated reporting role whose grants are narrowed to only the
 * target tables, or else expect multi-second latency on the
 * rejected-records query and watch for expression-analysis stack usage
 * on the hot path. A cached enumeration + balanced OR tree is tracked
 * as follow-up work.
 */
public final class RejectedRecordsRowAccessPolicy {

    private static final Logger LOG = LogManager.getLogger(RejectedRecordsRowAccessPolicy.class);

    private RejectedRecordsRowAccessPolicy() {
    }

    /**
     * @return {@code true} iff {@code tableName} refers to the
     *         {@code _statistics_.rejected_records} system table in the
     *         default internal catalog. Any other table (including Hive
     *         tables that happen to share the name) returns {@code false}
     *         so the access controller falls through to its own policy
     *         logic.
     */
    public static boolean matches(TableName tableName) {
        if (tableName == null) {
            return false;
        }
        String catalog = tableName.getCatalog();
        if (catalog != null && !catalog.equals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)) {
            return false;
        }
        return RejectedRecordsTable.DATABASE_NAME.equals(tableName.getDb())
                && RejectedRecordsTable.TABLE_NAME.equals(tableName.getTbl());
    }

    /**
     * Build the row access predicate to inject when a user queries
     * {@code _statistics_.rejected_records}.
     *
     * @param context the connect context running the query
     * @param policyTableName the {@code TableName} the policy rewrites;
     *                        used to qualify every {@link SlotRef} so the
     *                        outer {@link SecurityPolicyRewriteRule} wrap
     *                        references the correct subject table.
     * @return an {@link Expr} to use as a {@code WHERE} predicate, or
     *         {@code null} to apply no filter (admin-only case).
     */
    public static Expr buildPolicy(ConnectContext context, TableName policyTableName) {
        UserIdentity user = context != null ? context.getCurrentUserIdentity() : null;
        if (user != null && user.equals(UserIdentity.ROOT)) {
            return null;
        }
        if (context == null || user == null) {
            return new BoolLiteral(false);
        }

        List<TableName> accessible = enumerateAccessibleTables(context);
        if (accessible.isEmpty()) {
            // Logging here is intentional -- a non-admin operator hitting
            // the table with zero SELECT-able targets almost always means
            // they forgot to grant privileges rather than that the result
            // set is genuinely empty.
            LOG.info("RejectedRecordsRowAccessPolicy: user {} has SELECT on 0 tables; "
                    + "returning BoolLiteral(false) for _statistics_.rejected_records query",
                    user);
            return new BoolLiteral(false);
        }
        return buildOrChain(policyTableName, accessible);
    }

    // Visible for tests; do not call in production.
    static Expr buildOrChain(TableName policyTableName, List<TableName> accessibleTables) {
        Expr combined = null;
        for (TableName target : accessibleTables) {
            Expr dbEq = new BinaryPredicate(BinaryType.EQ,
                    new SlotRef(policyTableName, "target_database"),
                    new StringLiteral(target.getDb()));
            Expr tblEq = new BinaryPredicate(BinaryType.EQ,
                    new SlotRef(policyTableName, "target_table"),
                    new StringLiteral(target.getTbl()));
            Expr pair = new CompoundPredicate(CompoundPredicate.Operator.AND, dbEq, tblEq);
            combined = (combined == null) ? pair
                    : new CompoundPredicate(CompoundPredicate.Operator.OR, combined, pair);
        }
        return combined;
    }

    /**
     * Iterate the default internal catalog and return every
     * {@code (db, table)} the calling user has {@code SELECT} on. Uses
     * the same catch-the-denial idiom as {@code ViewsSystemTable} so a
     * cluster with many tables only pays the AuthorizationMgr's
     * LoadingCache lookup per table -- a linear in-memory scan.
     */
    private static List<TableName> enumerateAccessibleTables(ConnectContext context) {
        List<TableName> result = new ArrayList<>();
        String catalog = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
        List<String> dbNames;
        try {
            dbNames = GlobalStateMgr.getCurrentState().getMetadataMgr().listDbNames(context, catalog);
        } catch (Exception e) {
            LOG.warn("RejectedRecordsRowAccessPolicy: failed to list databases: {}", e.getMessage());
            return result;
        }
        for (String dbName : dbNames) {
            // Skip the system database itself so the policy never self-references.
            if (RejectedRecordsTable.DATABASE_NAME.equals(dbName)) {
                continue;
            }
            List<String> tableNames;
            try {
                tableNames = GlobalStateMgr.getCurrentState().getMetadataMgr().listTableNames(context, catalog, dbName);
            } catch (Exception e) {
                // A single unreadable database must not abort the whole
                // enumeration -- the user may still have SELECT on tables
                // in other databases.
                LOG.debug("RejectedRecordsRowAccessPolicy: listTableNames({}) failed: {}", dbName, e.getMessage());
                continue;
            }
            for (String tblName : tableNames) {
                TableName candidate = new TableName(catalog, dbName, tblName);
                try {
                    Authorizer.checkTableAction(context, candidate, PrivilegeType.SELECT);
                } catch (AccessDeniedException denied) {
                    continue;
                } catch (Exception e) {
                    LOG.debug("RejectedRecordsRowAccessPolicy: checkTableAction({}.{}) failed: {}",
                            dbName, tblName, e.getMessage());
                    continue;
                }
                result.add(candidate);
            }
        }
        return result;
    }
}
