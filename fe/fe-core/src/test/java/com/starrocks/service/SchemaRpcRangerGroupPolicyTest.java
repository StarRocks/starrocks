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

package com.starrocks.service;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.starrocks.authentication.UserIdentityUtils;
import com.starrocks.authorization.AccessControlProvider;
import com.starrocks.authorization.AccessController;
import com.starrocks.authorization.ranger.hive.RangerHiveAccessController;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.connector.hive.MockedHiveMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.thrift.TColumnDef;
import com.starrocks.thrift.TDescribeTableParams;
import com.starrocks.thrift.TGetTablesParams;
import com.starrocks.thrift.TUserIdentity;
import mockit.Mock;
import mockit.MockUp;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * When the BE scans information_schema.columns or tables, it asks the FE through describeTable and getTableNames
 * on behalf of the session's user. A Ranger policy written for a group must decide there as it does in the
 * session, so the caller's groups have to arrive with the request: an LDAP user of a security integration
 * typically gets its Hive access only through a group policy.
 */
public class SchemaRpcRangerGroupPolicyTest extends ConnectorPlanTestBase {
    private static final String HIVE = MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME;
    private static final String DB = MockedHiveMetadata.MOCKED_TPCH_DB_NAME;
    private static final String GROUP = "rpc_hive_readers";

    private AccessController previous;

    @BeforeEach
    public void installRangerForHive() throws Exception {
        new MockUp<RangerBasePlugin>() {
            @Mock
            void init() {
            }
        };
        RangerHiveAccessController ranger = new RangerHiveAccessController("hive_rpc_test", false);
        ranger.getRangerPlugin().setPolicies(selectOnRegionForGroup());
        previous = Authorizer.getInstance().catalogToAccessControl.put(HIVE, ranger);
    }

    @AfterEach
    public void restoreAccessControl() {
        AccessControlProvider provider = Authorizer.getInstance();
        if (previous == null) {
            provider.catalogToAccessControl.remove(HIVE);
        } else {
            provider.catalogToAccessControl.put(HIVE, previous);
        }
    }

    @Test
    public void testGroupPolicyAppliesToColumnsRpc() throws Exception {
        List<TColumnDef> columns = new FrontendServiceImpl(null).describeTable(describe(caller(Set.of(GROUP))))
                .getColumns();
        Assertions.assertFalse(columns.isEmpty(), "the group policy grants SELECT on " + DB + ".region");

        // Without the group nothing grants access: the policy, not the user name, is what lets it through.
        Assertions.assertTrue(new FrontendServiceImpl(null).describeTable(describe(caller(Set.of()))).getColumns()
                .isEmpty());
    }

    @Test
    public void testGroupPolicyAppliesToTableNamesRpc() throws Exception {
        List<String> tables = new FrontendServiceImpl(null).getTableNames(tables(caller(Set.of(GROUP)))).getTables();
        Assertions.assertEquals(List.of("region"), tables, "only region is covered by the group policy");

        Assertions.assertTrue(new FrontendServiceImpl(null).getTableNames(tables(caller(Set.of()))).getTables()
                .isEmpty());
    }

    /** What the BE forwards: the session's user, activated roles and groups, as the FE planned the scan. */
    private static TUserIdentity caller(Set<String> groups) {
        ConnectContext session = new ConnectContext();
        session.setCurrentUserIdentity(UserIdentity.createEphemeralUserIdent("ldap_hive_user", "%"));
        session.setCurrentRoleIds(new HashSet<>());
        session.setGroups(new HashSet<>(groups));
        return UserIdentityUtils.callerToThrift(session);
    }

    private static TDescribeTableParams describe(TUserIdentity caller) {
        TDescribeTableParams params = new TDescribeTableParams();
        params.setCatalog_name(HIVE);
        params.setDb(DB);
        params.setTable_name("region");
        params.setCurrent_user_ident(caller);
        return params;
    }

    private static TGetTablesParams tables(TUserIdentity caller) {
        TGetTablesParams params = new TGetTablesParams();
        params.setCatalog_name(HIVE);
        params.setDb(DB);
        params.setCurrent_user_ident(caller);
        return params;
    }

    private static ServicePolicies selectOnRegionForGroup() throws Exception {
        RangerServiceDef serviceDef;
        try (InputStream in = RangerBasePlugin.class.getClassLoader()
                .getResourceAsStream("service-defs/ranger-servicedef-hive.json")) {
            serviceDef = new ObjectMapper()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                    .readValue(in, RangerServiceDef.class);
        }

        RangerPolicy policy = new RangerPolicy();
        policy.setId(1L);
        policy.setService("hive_rpc_test");
        policy.setName("region_select_for_group");
        policy.setPolicyType(RangerPolicy.POLICY_TYPE_ACCESS);
        policy.setIsEnabled(true);
        policy.setResources(Map.of(
                "database", new RangerPolicy.RangerPolicyResource(DB),
                "table", new RangerPolicy.RangerPolicyResource("region"),
                "column", new RangerPolicy.RangerPolicyResource("*")));
        policy.setPolicyItems(List.of(new RangerPolicy.RangerPolicyItem(
                List.of(new RangerPolicy.RangerPolicyItemAccess("select")),
                new ArrayList<>(), List.of(GROUP), new ArrayList<>(), new ArrayList<>(), false)));

        ServicePolicies policies = new ServicePolicies();
        policies.setServiceName("hive_rpc_test");
        policies.setServiceId(1L);
        policies.setPolicyVersion(1L);
        policies.setServiceDef(serviceDef);
        policies.setPolicies(List.of(policy));
        return policies;
    }
}
