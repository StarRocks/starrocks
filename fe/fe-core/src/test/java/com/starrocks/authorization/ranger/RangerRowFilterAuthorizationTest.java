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

package com.starrocks.authorization.ranger;

import com.starrocks.authorization.AccessControlProvider;
import com.starrocks.authorization.ColumnPrivilege;
import com.starrocks.authorization.ranger.hive.RangerHiveAccessController;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import mockit.Mock;
import mockit.MockUp;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/**
 * A Ranger row filter is applied by rewriting the table into a subquery before the privilege check; the check then
 * plans the statement once, rule-based, to learn which columns each scan reads. A filter that is always false used to
 * let that planning replace the scan with an empty Values, so the table had no column to check and no SELECT check
 * (and no audit event) was sent at all. The policy lookups themselves were evaluated without the plugin's audit
 * handler, so a row filter or masking hit was never audited either.
 */
public class RangerRowFilterAuthorizationTest extends ConnectorPlanTestBase {
    private static final String CATALOG = "hive0";
    private static final String TABLE = CATALOG + ".tpch.region";

    /** What one run sends to the Ranger plugin. */
    private static class Captured {
        final List<RangerAccessRequest> accessChecks = new ArrayList<>();
        final List<RangerAccessResultProcessor> rowFilterHandlers = new ArrayList<>();
        final List<RangerAccessResultProcessor> maskHandlers = new ArrayList<>();
        RangerAccessResultProcessor pluginHandler;

        List<String> checkedColumns() {
            return accessChecks.stream()
                    .map(r -> r.getResource().getValue("table") + "/" + r.getResource().getValue("column"))
                    .toList();
        }
    }

    @BeforeAll
    public static void beforeClass() throws Exception {
        new MockUp<RangerBasePlugin>() {
            @Mock
            void init() {
            }
        };
        ConnectorPlanTestBase.beforeClass();
    }

    private static RangerAccessResult allowed() {
        RangerAccessResult result =
                new RangerAccessResult(1, "hive", new RangerServiceDef(), new RangerAccessRequestImpl());
        result.setIsAllowed(true);
        return result;
    }

    private static Captured mockPlugin(String rowFilter) {
        Captured captured = new Captured();
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
                captured.accessChecks.add(request);
                return allowed();
            }

            @Mock
            RangerAccessResult evalDataMaskPolicies(RangerAccessRequest request, RangerAccessResultProcessor handler) {
                captured.maskHandlers.add(handler);
                return null;
            }

            @Mock
            RangerAccessResult evalRowFilterPolicies(RangerAccessRequest request, RangerAccessResultProcessor handler) {
                captured.rowFilterHandlers.add(handler);
                if (rowFilter == null) {
                    return null;
                }
                RangerAccessResult result = allowed();
                result.setFilterExpr(rowFilter);
                return result;
            }
        };
        return captured;
    }

    private static ConnectContext newContext() {
        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(new UserIdentity("ranger_user", "%"));
        ctx.setCurrentRoleIds(new HashSet<>());
        ctx.setGroups(new HashSet<>());
        ctx.setThreadLocalInfo();
        return ctx;
    }

    /** Analyzes and authorizes {@code sql} as a query a client submitted, with {@code rowFilter} on every table. */
    private static Captured check(String sql, String rowFilter) {
        Captured captured = mockPlugin(rowFilter);
        AccessControlProvider provider = Authorizer.getInstance();
        RangerHiveAccessController controller = new RangerHiveAccessController("hive-service", false);
        captured.pluginHandler = controller.getRangerPlugin().getResultProcessor();
        provider.setAccessControl(CATALOG, controller);
        try {
            ConnectContext ctx = newContext();
            StatementBase stmt = SqlParser.parse(sql, ctx.getSessionVariable().getSqlMode()).get(0);
            new AstTraverser<Void, Void>() {
                @Override
                public Void visitRelation(Relation relation, Void context) {
                    relation.setNeedRewrittenByPolicy(true);
                    return null;
                }
            }.visit(stmt);
            Analyzer.analyze(stmt, ctx);
            Authorizer.check(stmt, ctx);
        } finally {
            provider.removeAccessControl(CATALOG);
        }
        return captured;
    }

    @Test
    public void testQueryThatReadsNothingIsStillCheckedOnTheColumnsItNames() {
        // count(*) over a Hive scan is rewritten to a scan of the ___count___ placeholder column
        // (RewriteSimpleAggToHDFSScanRule), so that is the column Ranger has always been asked about,
        // whatever the predicate; a query that names a column is checked on it and on the filter's column.
        String[][] cases = {
                {"select count(*) from " + TABLE, null, "___count___"},
                {"select count(*) from " + TABLE, "0 = 1", "___count___"},
                {"select count(*) from " + TABLE + " limit 0", "0 = 1", "___count___"},
                {"select count(*) from " + TABLE + " where r_regionkey = 1", "0 = 1", "___count___"},
                {"select r_name from " + TABLE, "0 = 1", "r_name"},
                {"select r_name from " + TABLE, "NULL", "r_name"},
                {"select r_name from " + TABLE, "r_regionkey = 1", "r_name,r_regionkey"},
                {"select r_name from " + TABLE + " where 1 = 0", null, "r_name"},
                {"select r_name from " + TABLE + " limit 0", null, "r_name"},
                {"select r_name from " + TABLE + " where 1 = 0 union all select r_name from " + TABLE, null, "r_name"},
        };
        for (String[] c : cases) {
            Captured captured = check(c[0], c[1]);
            List<String> expected =
                    java.util.Arrays.stream(c[2].split(",")).map(col -> "region/" + col).sorted().toList();
            Assertions.assertEquals(expected, captured.checkedColumns().stream().sorted().toList(),
                    "access checks for `" + c[0] + "` with row filter " + c[1]);
        }
    }

    @Test
    public void testPolicyLookupsOfTheQueryAreAudited() {
        Captured captured = check("select r_name from " + TABLE, "0 = 1");
        Assertions.assertNotNull(captured.pluginHandler);
        Assertions.assertFalse(captured.rowFilterHandlers.isEmpty());
        Assertions.assertFalse(captured.maskHandlers.isEmpty());
        Assertions.assertTrue(captured.rowFilterHandlers.stream().allMatch(h -> h == captured.pluginHandler),
                "row filter evaluation must carry the plugin's audit handler");
        Assertions.assertTrue(captured.maskHandlers.stream().allMatch(h -> h == captured.pluginHandler),
                "masking evaluation must carry the plugin's audit handler");
    }

    @Test
    public void testLookupWithoutAuditCarriesNoHandler() {
        Captured captured = mockPlugin("0 = 1");
        RangerHiveAccessController controller = new RangerHiveAccessController("hive-service", false);
        ConnectContext ctx = newContext();
        TableName table = new TableName(CATALOG, "tpch", "region");
        Assertions.assertNotNull(controller.getRowAccessPolicyWithoutAudit(ctx, table));
        controller.getColumnMaskingPolicyWithoutAudit(ctx, table, GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getTable(ctx, CATALOG, "tpch", "region").getBaseSchema());
        Assertions.assertFalse(captured.rowFilterHandlers.isEmpty());
        Assertions.assertFalse(captured.maskHandlers.isEmpty());
        Assertions.assertTrue(captured.rowFilterHandlers.stream().allMatch(Objects::isNull));
        Assertions.assertTrue(captured.maskHandlers.stream().allMatch(Objects::isNull));
    }

    @Test
    public void testTableIsCheckedWhenNoScanColumnIsLeft() {
        // Pretend some rewrite removed the scan: the collector then reports no column for the table.
        new MockUp<ColumnPrivilege.ScanColumnCollector>() {
            @Mock
            Void visitLogicalTableScan(OptExpression node, Void context) {
                return null;
            }
        };
        Captured captured = check("select r_name from " + TABLE, null);
        Assertions.assertEquals(List.of("region/null"), captured.checkedColumns());
    }
}
