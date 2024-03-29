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
package com.starrocks.epack.qe;

import com.google.common.collect.Lists;
import com.starrocks.analysis.TypeDef;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.LDAPSecurityIntegration;
import com.starrocks.authentication.SecurityIntegration;
import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.CaseSensibility;
import com.starrocks.common.FeConstants;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.epack.privilege.AuthorizerEPack;
import com.starrocks.epack.privilege.DbUID;
import com.starrocks.epack.privilege.LDAPRoleMapping;
import com.starrocks.epack.privilege.Policy;
import com.starrocks.epack.privilege.RoleMapping;
import com.starrocks.epack.sql.ast.AstVisitorEPack;
import com.starrocks.epack.sql.ast.CreatePolicyStmt;
import com.starrocks.epack.sql.ast.DescribeFailoverGroupStmt;
import com.starrocks.epack.sql.ast.PolicyName;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.epack.sql.ast.ShowCreatePolicyStmt;
import com.starrocks.epack.sql.ast.ShowCreateSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.ShowFailoverGroupsStmt;
import com.starrocks.epack.sql.ast.ShowNodesStmt;
import com.starrocks.epack.sql.ast.ShowPolicyStmt;
import com.starrocks.epack.sql.ast.ShowRoleMappingStatement;
import com.starrocks.epack.sql.ast.ShowSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.ShowWarehousesStmt;
import com.starrocks.epack.warehouse.Cluster;
import com.starrocks.epack.warehouse.LocalWarehouse;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.AuthorizationMgr;
import com.starrocks.privilege.ObjectType;
import com.starrocks.privilege.PrivilegeEntry;
import com.starrocks.privilege.PrivilegeException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.GrantRevokeClause;
import com.starrocks.sql.ast.ShowAuthenticationStmt;
import com.starrocks.sql.ast.ShowGrantsStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.system.BackendCoreStat;
import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.Warehouse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ShowExecutorVisitorEPack extends ShowExecutor.ShowExecutorVisitor
        implements AstVisitorEPack<ShowResultSet, ConnectContext> {

    private static final ShowExecutorVisitorEPack INSTANCE = new ShowExecutorVisitorEPack();

    public static ShowExecutorVisitorEPack getInstance() {
        return INSTANCE;
    }

    @Override
    public ShowResultSet visitShowWarehousesStatement(ShowWarehousesStmt statement, ConnectContext context) {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        WarehouseManager warehouseMgr = globalStateMgr.getWarehouseMgr();

        PatternMatcher matcher = null;
        if (!statement.getPattern().isEmpty()) {
            matcher = PatternMatcher.createMysqlPattern(statement.getPattern(),
                    CaseSensibility.WAREHOUSE.getCaseSensibility());
        }
        PatternMatcher finalMatcher = matcher;

        List<List<String>> rowSet = warehouseMgr.getAllWarehouses().stream()
                .filter(warehouse -> finalMatcher == null || finalMatcher.match(warehouse.getName()))
                .filter(warehouse -> {
                    try {
                        AuthorizerEPack.checkAnyActionOnWarehouse(context.getCurrentUserIdentity(),
                                context.getCurrentRoleIds(), warehouse.getName());
                    } catch (AccessDeniedException e) {
                        return false;
                    }
                    return true;
                }).sorted(Comparator.comparing(Warehouse::getId))
                .map(warehouse -> ((LocalWarehouse) warehouse).getWarehouseInfo())
                .collect(Collectors.toList());
        return new ShowResultSet(statement.getMetaData(), rowSet);
    }

    @Override
    public ShowResultSet visitShowNodesStatement(ShowNodesStmt statement, ConnectContext context) {
        List<List<String>> rows = Lists.newArrayList();
        WarehouseManager warehouseMgr = GlobalStateMgr.getCurrentState().getWarehouseMgr();

        // filter by pattern or warehouseName
        String warehouseName = null;
        PatternMatcher matcher = null;
        if (statement.getWarehouseName() != null) {
            warehouseName = statement.getWarehouseName();
        } else if (statement.getPattern() != null) {
            matcher = PatternMatcher.createMysqlPattern(statement.getPattern(),
                    CaseSensibility.WAREHOUSE.getCaseSensibility());
        }

        List<Warehouse> warehouseList = warehouseMgr.getAllWarehouses().stream().filter(
                warehouse -> {
                    try {
                        AuthorizerEPack.checkAnyActionOnWarehouse(context.getCurrentUserIdentity(),
                                context.getCurrentRoleIds(), warehouse.getName());
                    } catch (AccessDeniedException e) {
                        return false;
                    }
                    return true;
                }
        ).collect(Collectors.toList());

        for (Warehouse wh : warehouseList) {
            if (warehouseName != null && !wh.getName().equalsIgnoreCase(warehouseName)) {
                continue;
            }

            if (matcher != null && !matcher.match(wh.getName())) {
                continue;
            }

            LocalWarehouse localWarehouse = (LocalWarehouse) wh;
            for (Cluster cluster : localWarehouse.getClusters().values()) {
                List<Long> computeNodes = cluster.getComputeNodeIds();
                for (Long computeNodeId : computeNodes) {
                    ComputeNode node = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                            .getComputeNode(computeNodeId);

                    List<String> computeNodeInfo = Lists.newArrayList();
                    long warehouseId = node.getWarehouseId();
                    Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(warehouseId);
                    computeNodeInfo.add(warehouse.getName());

                    computeNodeInfo.add(String.valueOf(cluster.getId()));
                    computeNodeInfo.add(String.valueOf(cluster.getWorkerGroupId()));
                    long nodeId = node.getId();
                    long workerId = GlobalStateMgr.getCurrentState().getStarOSAgent().getWorkerIdByBackendId(nodeId);
                    computeNodeInfo.add(String.valueOf(nodeId));
                    computeNodeInfo.add(String.valueOf(workerId));

                    computeNodeInfo.add(node.getHost());

                    computeNodeInfo.add(String.valueOf(node.getHeartbeatPort()));
                    computeNodeInfo.add(String.valueOf(node.getBePort()));
                    computeNodeInfo.add(String.valueOf(node.getHttpPort()));
                    computeNodeInfo.add(String.valueOf(node.getBrpcPort()));
                    computeNodeInfo.add(String.valueOf(node.getStarletPort()));

                    computeNodeInfo.add(TimeUtils.longToTimeString(node.getLastStartTime()));
                    computeNodeInfo.add(TimeUtils.longToTimeString(node.getLastUpdateMs()));
                    computeNodeInfo.add(String.valueOf(node.isAlive()));

                    computeNodeInfo.add(node.getHeartbeatErrMsg());
                    computeNodeInfo.add(String.valueOf(node.getVersion()));

                    computeNodeInfo.add(String.valueOf(node.getNumRunningQueries()));
                    computeNodeInfo.add(String.valueOf(BackendCoreStat.getCoresOfBe(nodeId)));
                    double memUsedPct = node.getMemUsedPct();
                    computeNodeInfo.add(String.format("%.2f", memUsedPct * 100) + " %");
                    computeNodeInfo.add(String.format("%.1f", node.getCpuUsedPermille() / 10.0) + " %");

                    rows.add(computeNodeInfo);
                }
            }
        }
        return new ShowResultSet(statement.getMetaData(), rows);
    }

    @Override
    public ShowResultSet visitShowPolicyStatement(ShowPolicyStmt statement, ConnectContext context) {
        Map<String, Policy> policies = GlobalStateMgr.getCurrentState().getSecurityPolicyManager()
                .getOrCreateNamePolicyMapByDBUID(
                        DbUID.generate(statement.getCatalog(), statement.getDbName()),
                        statement.getPolicyType());
        List<List<String>> rows = new ArrayList<>();
        if (policies != null) {
            for (Map.Entry<String, Policy> policyEntry : policies.entrySet()) {
                List<String> row = new ArrayList<>();
                row.add(policyEntry.getKey());
                Policy policy = policyEntry.getValue();
                if (policy.getPolicyType().equals(PolicyType.ROW_ACCESS)) {
                    row.add("ROW ACCESS");
                } else {
                    row.add("MASKING");
                }
                row.add(statement.getCatalog());
                row.add(statement.getDbName());

                rows.add(row);
            }
        }
        return new ShowResultSet(statement.getMetaData(), rows);
    }

    @Override
    public ShowResultSet visitShowCreatePolicyStatement(ShowCreatePolicyStmt statement, ConnectContext context) {
        Policy policy = GlobalStateMgr.getCurrentState().getSecurityPolicyManager()
                .getPolicyByName(statement.getPolicyType(), statement.getPolicyName(), false);

        List<String> row = new ArrayList<>();
        row.add(policy.getName());

        row.add(AstToSQLBuilder.toSQL(new CreatePolicyStmt(false, policy.getPolicyType(),
                new PolicyName("", "", policy.getName(), NodePosition.ZERO),
                policy.getArgNames(),
                policy.getArgTypes().stream().map(TypeDef::new).collect(Collectors.toList()),
                new TypeDef(policy.getRetType()),
                SqlParser.parseSqlToExpr(policy.getPolicyExpressionSQL(), SqlModeHelper.MODE_DEFAULT),
                policy.getComment(), NodePosition.ZERO)));

        return new ShowResultSet(statement.getMetaData(), Collections.singletonList(row));
    }

    @Override
    public ShowResultSet visitShowRoleMappingStatement(ShowRoleMappingStatement statement, ConnectContext context) {
        AuthorizationMgr authorizationManager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        Set<RoleMapping> roleMappings = authorizationManager.getRoleMappingMetaMgr().getAllRoleMappings();
        List<List<String>> infos = new ArrayList<>();
        for (RoleMapping roleMapping : roleMappings) {
            SecurityIntegration securityIntegration = GlobalStateMgr.getCurrentState().getAuthenticationMgr()
                    .getSecurityIntegration(roleMapping.getIntegrationName());
            if (securityIntegration == null) {
                continue;
            }
            List<String> info = new ArrayList<>();
            info.add(roleMapping.getName());
            info.add(roleMapping.getIntegrationName());
            info.add(roleMapping.getRoleName());
            info.add(roleMapping instanceof LDAPRoleMapping ?
                    String.join(";", ((LDAPRoleMapping) roleMapping).getGroupSet()) : FeConstants.NULL_STRING);
            info.add(securityIntegration instanceof LDAPSecurityIntegration ?
                    TimeUtils.format(new Date(((LDAPSecurityIntegration) securityIntegration).getLastRefreshTime()),
                            PrimitiveType.DATETIME) : FeConstants.NULL_STRING);
            infos.add(info);
        }

        // sort by integration name, then by role mapping name
        List<List<String>> sortedList = infos.stream()
                .sorted(
                        Comparator.comparing((List<String> sublist) -> sublist.get(1))
                                .thenComparing((List<String> sublist) -> sublist.get(0))
                )
                .collect(Collectors.toList());

        return new ShowResultSet(statement.getMetaData(), sortedList);
    }

    @Override
    public ShowResultSet visitShowSecurityIntegrationStatement(ShowSecurityIntegrationStatement statement,
                                                               ConnectContext context) {
        AuthenticationMgr authenticationManager = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        Set<SecurityIntegration> securityIntegrations = authenticationManager.getAllSecurityIntegrations();
        List<List<String>> infos = new ArrayList<>();
        for (SecurityIntegration securityIntegration : securityIntegrations) {
            List<String> info = new ArrayList<>();
            info.add(securityIntegration.getName());
            info.add(securityIntegration.getType());
            if (securityIntegration.getComment().isEmpty()) {
                info.add(FeConstants.NULL_STRING);
            } else {
                info.add(securityIntegration.getComment());
            }
            infos.add(info);
        }

        // sort by type, then by name
        List<List<String>> sortedList = infos.stream()
                .sorted(
                        Comparator.comparing((List<String> sublist) -> sublist.get(1))
                                .thenComparing((List<String> sublist) -> sublist.get(0))
                )
                .collect(Collectors.toList());

        return new ShowResultSet(statement.getMetaData(), sortedList);
    }

    @Override
    public ShowResultSet visitShowCreateSecurityIntegrationStatement(ShowCreateSecurityIntegrationStatement statement,
                                                                     ConnectContext context) {
        String name = statement.getName();
        List<List<String>> infos = new ArrayList<>();
        SecurityIntegration securityIntegration = GlobalStateMgr.getCurrentState().getAuthenticationMgr()
                .getSecurityIntegration(name);
        if (securityIntegration != null) {
            Map<String, String> propertyMap = securityIntegration.getPropertyMap();
            String propString = propertyMap.entrySet().stream()
                    .map(entry -> "\"" + entry.getKey() + "\" = \"" + entry.getValue() + "\"")
                    .collect(Collectors.joining(",\n"));
            infos.add(Lists.newArrayList(name,
                    "CREATE SECURITY INTEGRATION `" + name +
                            "` PROPERTIES (\n" + propString + "\n)"));
        }
        return new ShowResultSet(statement.getMetaData(), infos);
    }

    @Override
    public ShowResultSet visitShowFailoverGroupsStatement(ShowFailoverGroupsStmt statement, ConnectContext context) {
        try {
            return new ShowResultSet(statement.getMetaData(), statement.getRows());
        } catch (AnalysisException e) {
            throw new SemanticException(e.getMessage());
        }

    }

    @Override
    public ShowResultSet visitDescribeFailoverGroupStatement(DescribeFailoverGroupStmt statement, ConnectContext context) {
        try {
            return new ShowResultSet(statement.getMetaData(), statement.getRows());
        } catch (AnalysisException e) {
            throw new SemanticException(e.getMessage());
        }
    }

    @Override
    public ShowResultSet visitShowGrantsStatement(ShowGrantsStmt statement, ConnectContext context) {
        AuthorizationMgr authorizationManager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        try {
            List<List<String>> infos = new ArrayList<>();
            if (statement.getRole() != null) {
                List<String> granteeRole = authorizationManager.getGranteeRoleDetailsForRole(statement.getRole());
                if (granteeRole != null) {
                    infos.add(granteeRole);
                }

                Map<ObjectType, List<PrivilegeEntry>> typeToPrivilegeEntryList =
                        authorizationManager.getTypeToPrivilegeEntryListByRole(statement.getRole());
                infos.addAll(privilegeToRowString(authorizationManager,
                        new GrantRevokeClause(null, statement.getRole()), typeToPrivilegeEntryList));
            } else {
                UserIdentity userIdentity = statement.getUserIdent();
                List<String> granteeRole =
                        authorizationManager.getGranteeRoleDetailsForUser(userIdentity);
                if (granteeRole != null) {
                    infos.add(granteeRole);
                }

                if (!userIdentity.isEphemeral()) {
                    Map<ObjectType, List<PrivilegeEntry>> typeToPrivilegeEntryList =
                            authorizationManager.getTypeToPrivilegeEntryListByUser(statement.getUserIdent());
                    infos.addAll(privilegeToRowString(authorizationManager,
                            new GrantRevokeClause(statement.getUserIdent(), null), typeToPrivilegeEntryList));
                }
            }
            return new ShowResultSet(statement.getMetaData(), infos);
        } catch (PrivilegeException e) {
            throw new SemanticException(e.getMessage());
        }
    }

    @Override
    public ShowResultSet visitShowAuthenticationStatement(ShowAuthenticationStmt statement, ConnectContext context) {
        AuthenticationMgr authenticationManager = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
        List<List<String>> userAuthInfos = Lists.newArrayList();

        Map<UserIdentity, UserAuthenticationInfo> authenticationInfoMap = new HashMap<>();
        if (statement.isAll()) {
            authenticationInfoMap.putAll(authenticationManager.getUserToAuthenticationInfo());
        } else {
            UserAuthenticationInfo userAuthenticationInfo;
            UserIdentity userIdentity;
            if (statement.getUserIdent() == null) {
                userIdentity = context.getCurrentUserIdentity();
            } else {
                userIdentity = statement.getUserIdent();
            }
            userAuthenticationInfo = authenticationManager
                    .getUserAuthenticationInfoByUserIdentity(userIdentity);
            authenticationInfoMap.put(userIdentity, userAuthenticationInfo);
        }
        for (Map.Entry<UserIdentity, UserAuthenticationInfo> entry : authenticationInfoMap.entrySet()) {
            UserAuthenticationInfo userAuthenticationInfo = entry.getValue();
            UserIdentity userIdentity = entry.getKey();
            if (userIdentity.isEphemeral()) {
                userAuthInfos.add(Arrays.asList(userIdentity.toString(), "Yes",
                        FeConstants.NULL_STRING, FeConstants.NULL_STRING));
            } else {
                userAuthInfos.add(Lists.newArrayList(
                        userIdentity.toString(),
                        userAuthenticationInfo.getPassword().length == 0 ? "No" : "Yes",
                        userAuthenticationInfo.getAuthPlugin(),
                        userAuthenticationInfo.getTextForAuthPlugin()));
            }
        }

        return new ShowResultSet(statement.getMetaData(), userAuthInfos);
    }
}