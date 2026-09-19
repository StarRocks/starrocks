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

import com.google.common.collect.Lists;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.authorization.ranger.starrocks.RangerStarRocksAccessController;
import com.starrocks.authorization.ranger.starrocks.RangerStarRocksResource;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.ArithmeticExpr;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.type.IntegerType;
import mockit.Mock;
import mockit.MockUp;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests validating that the StarRocks Ranger plugin correctly handles tag-based
 * policy evaluation results from the Ranger SDK.
 *
 * Tag-based policies in Ranger work as follows:
 * 1. Ranger Admin associates a Tag Service with the StarRocks resource service
 * 2. Resources are tagged in Ranger (via Atlas or manual API)
 * 3. Tag-based policies are created (e.g., "PII tag -> mask for analysts")
 * 4. RangerBasePlugin.isAccessAllowed() evaluates both resource and tag policies
 * 5. The combined result is returned to StarRocks
 *
 * These tests mock the RangerBasePlugin methods to simulate tag-based policy
 * evaluation results and verify StarRocks handles them correctly. The actual
 * tag-resource matching and policy combination logic is internal to the Ranger SDK.
 */
public class RangerTagBasedPolicyTest {

    @BeforeAll
    public static void beforeAll() {
        new MockUp<RangerBasePlugin>() {
            @Mock
            void init() {
            }

            @Mock
            RangerAccessResult evalDataMaskPolicies(RangerAccessRequest request,
                                                     RangerAccessResultProcessor resultProcessor) {
                return null;
            }
        };
    }

    /**
     * Simulates a tag-based access denial. When a resource is tagged (e.g., "PII")
     * and a tag-based policy denies access, the Ranger SDK returns isAllowed=false.
     * The StarRocks plugin must correctly propagate this denial.
     */
    @Test
    public void testTagBasedAccessDenied() {
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
                // Simulate: tag-based policy denies access to a PII-tagged table
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setIsAllowed(false);
                return result;
            }
        };

        RangerStarRocksAccessController controller = new RangerStarRocksAccessController();
        UserIdentity analyst = new UserIdentity("analyst_user", "%");

        // Access to a PII-tagged table should be denied
        Assertions.assertThrows(AccessDeniedException.class, () ->
                controller.hasPermission(
                        RangerStarRocksResource.builder()
                                .setCatalog("default_catalog")
                                .setDatabase("customer_db")
                                .setTable("customer_pii")
                                .build(),
                        analyst, Set.of("analysts"), PrivilegeType.SELECT));
    }

    /**
     * Simulates a tag-based access grant. When a resource is tagged and the user
     * has tag-based permissions, the Ranger SDK returns isAllowed=true.
     */
    @Test
    public void testTagBasedAccessAllowed() {
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
                // Simulate: tag-based policy allows access for data_engineers group
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setIsAllowed(true);
                return result;
            }
        };

        RangerStarRocksAccessController controller = new RangerStarRocksAccessController();
        UserIdentity engineer = new UserIdentity("data_engineer", "%");

        // Data engineer should have access to PII-tagged table via tag policy
        Assertions.assertDoesNotThrow(() ->
                controller.hasPermission(
                        RangerStarRocksResource.builder()
                                .setCatalog("default_catalog")
                                .setDatabase("customer_db")
                                .setTable("customer_pii")
                                .build(),
                        engineer, Set.of("data_engineers"), PrivilegeType.SELECT));
    }

    /**
     * Simulates tag-based column masking. A tag-based policy (e.g., "PII -> MASK_NULL")
     * causes the Ranger SDK to return a masking result from evalDataMaskPolicies().
     * The StarRocks plugin must correctly apply the masking expression.
     */
    @Test
    public void testTagBasedColumnMaskingNull() {
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult evalDataMaskPolicies(RangerAccessRequest request,
                                                     RangerAccessResultProcessor resultProcessor) {
                // Simulate: tag-based masking policy nullifies PII columns
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setMaskType(RangerPolicy.MASK_TYPE_NULL);
                return result;
            }
        };

        RangerStarRocksAccessController controller = new RangerStarRocksAccessController();

        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(new UserIdentity("analyst_user", "%"));
        TableName tableName = new TableName("default_catalog", "customer_db", "customers");
        List<Column> columns = Lists.newArrayList(new Column("ssn", IntegerType.INT));

        Map<String, Expr> masks = controller.getColumnMaskingPolicy(ctx, tableName, columns);
        Assertions.assertFalse(masks.isEmpty());
        Assertions.assertTrue(masks.get("ssn") instanceof NullLiteral);
    }

    /**
     * Simulates tag-based custom masking. A tag policy applies a custom masking
     * expression (e.g., showing only last 4 digits of SSN).
     */
    @Test
    public void testTagBasedColumnMaskingCustom() {
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult evalDataMaskPolicies(RangerAccessRequest request,
                                                     RangerAccessResultProcessor resultProcessor) {
                // Simulate: tag-based policy with custom mask expression
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setMaskType(RangerPolicy.MASK_TYPE_CUSTOM);
                result.setMaskedValue("ssn + 0");
                return result;
            }
        };

        RangerStarRocksAccessController controller = new RangerStarRocksAccessController();

        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(new UserIdentity("analyst_user", "%"));
        TableName tableName = new TableName("default_catalog", "customer_db", "customers");
        List<Column> columns = Lists.newArrayList(new Column("ssn", IntegerType.INT));

        Map<String, Expr> masks = controller.getColumnMaskingPolicy(ctx, tableName, columns);
        Assertions.assertFalse(masks.isEmpty());
        Assertions.assertTrue(masks.get("ssn") instanceof ArithmeticExpr);
    }

    /**
     * Simulates tag-based row filtering. A tag-based policy (e.g., "CONFIDENTIAL -> filter")
     * causes the Ranger SDK to return a row filter expression from evalRowFilterPolicies().
     */
    @Test
    public void testTagBasedRowFiltering() {
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult evalRowFilterPolicies(RangerAccessRequest request,
                                                      RangerAccessResultProcessor resultProcessor) {
                // Simulate: tag-based row filter restricting to non-sensitive records
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setFilterExpr("sensitivity_level = 1");
                return result;
            }
        };

        RangerStarRocksAccessController controller = new RangerStarRocksAccessController();

        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(new UserIdentity("analyst_user", "%"));
        TableName tableName = new TableName("default_catalog", "customer_db", "customers");

        Expr rowFilter = controller.getRowAccessPolicy(ctx, tableName);
        Assertions.assertNotNull(rowFilter);
        Assertions.assertTrue(rowFilter instanceof BinaryPredicate);
    }

    /**
     * Simulates combined tag + resource policy evaluation. When both tag-based
     * and resource-based policies exist, the Ranger SDK combines results internally.
     * The StarRocks plugin receives the final combined result.
     *
     * In this scenario: tag-based policy allows access, which is the combined result
     * returned by the Ranger SDK after evaluating both policy types.
     */
    @Test
    public void testCombinedTagAndResourcePolicyAllowed() {
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
                // Ranger SDK returns the combined result of tag + resource policies
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setIsAllowed(true);
                return result;
            }
        };

        RangerStarRocksAccessController controller = new RangerStarRocksAccessController();
        UserIdentity user = new UserIdentity("data_analyst", "%");

        Assertions.assertDoesNotThrow(() ->
                controller.hasPermission(
                        RangerStarRocksResource.builder()
                                .setCatalog("default_catalog")
                                .setDatabase("analytics_db")
                                .setTable("revenue_data")
                                .build(),
                        user, Set.of("analysts", "finance"), PrivilegeType.SELECT));
    }

    /**
     * Simulates tag-based policy denying access to a column-level resource.
     * Tags can be applied at column granularity (e.g., "PII" on specific columns).
     */
    @Test
    public void testTagBasedColumnAccessDenied() {
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
                // Simulate: tag-based policy denies access to PII-tagged column
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setIsAllowed(false);
                return result;
            }
        };

        RangerStarRocksAccessController controller = new RangerStarRocksAccessController();
        UserIdentity analyst = new UserIdentity("junior_analyst", "%");

        Assertions.assertThrows(AccessDeniedException.class, () ->
                controller.hasPermission(
                        RangerStarRocksResource.builder()
                                .setCatalog("default_catalog")
                                .setDatabase("customer_db")
                                .setTable("customers")
                                .setColumn("social_security_number")
                                .build(),
                        analyst, Set.of("junior_analysts"), PrivilegeType.SELECT));
    }

    /**
     * Verifies that root user bypasses tag-based policies, consistent with
     * the existing resource-based policy bypass behavior.
     */
    @Test
    public void testRootUserBypassesTagBasedPolicies() {
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
                // Ranger would deny via tag policy, but root should bypass
                RangerAccessResult result = new RangerAccessResult(1, "starrocks",
                        new RangerServiceDef(), new RangerAccessRequestImpl());
                result.setIsAllowed(false);
                return result;
            }
        };

        RangerStarRocksAccessController controller = new RangerStarRocksAccessController();

        // root user bypasses all Ranger policies including tag-based ones
        Assertions.assertDoesNotThrow(() ->
                controller.hasPermission(
                        RangerStarRocksResource.builder()
                                .setCatalog("default_catalog")
                                .setDatabase("customer_db")
                                .setTable("customer_pii")
                                .build(),
                        UserIdentity.ROOT, Set.of(), PrivilegeType.SELECT));
    }

    /**
     * Verifies that when no masking policy applies (neither tag-based nor
     * resource-based), the result is empty, leaving data unmasked.
     */
    @Test
    public void testNoTagOrResourceMaskingPolicy() {
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult evalDataMaskPolicies(RangerAccessRequest request,
                                                     RangerAccessResultProcessor resultProcessor) {
                // No masking policy applies
                return null;
            }
        };

        RangerStarRocksAccessController controller = new RangerStarRocksAccessController();

        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(new UserIdentity("admin_user", "%"));
        TableName tableName = new TableName("default_catalog", "public_db", "public_table");
        List<Column> columns = Lists.newArrayList(new Column("name", IntegerType.INT));

        Map<String, Expr> masks = controller.getColumnMaskingPolicy(ctx, tableName, columns);
        Assertions.assertTrue(masks.isEmpty());
    }

    /**
     * Verifies that when no row filter policy applies, no filter expression is returned.
     */
    @Test
    public void testNoTagOrResourceRowFilterPolicy() {
        new MockUp<RangerBasePlugin>() {
            @Mock
            RangerAccessResult evalRowFilterPolicies(RangerAccessRequest request,
                                                      RangerAccessResultProcessor resultProcessor) {
                return null;
            }
        };

        RangerStarRocksAccessController controller = new RangerStarRocksAccessController();

        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(new UserIdentity("admin_user", "%"));
        TableName tableName = new TableName("default_catalog", "public_db", "public_table");

        Expr rowFilter = controller.getRowAccessPolicy(ctx, tableName);
        Assertions.assertNull(rowFilter);
    }
}
