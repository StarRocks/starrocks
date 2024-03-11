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
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.CaseSensibility;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.epack.privilege.AuthorizerEPack;
import com.starrocks.epack.privilege.DbUID;
import com.starrocks.epack.privilege.LDAPRoleMapping;
import com.starrocks.epack.privilege.Policy;
import com.starrocks.epack.privilege.RoleMapping;
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
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.AuthorizationMgr;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.warehouse.Warehouse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ShowExecutorEPack extends ShowExecutor {
    public ShowExecutorEPack(ConnectContext connectContext, ShowStmt stmt) {
        super(connectContext, stmt);
    }

    public ShowResultSet execute() throws AnalysisException, DdlException {
        if (stmt instanceof ShowWarehousesStmt) {
            handleShowWarehouses();
            return new ShowResultSet(resultSet.getMetaData(), resultSet.getResultRows());
        } else if (stmt instanceof ShowNodesStmt) {
            if (RunMode.getCurrentRunMode() == RunMode.SHARED_NOTHING) {
                throw new DdlException("unsupported statement in shared_nothing mode");
            }
            handleShowNodes();
            return new ShowResultSet(resultSet.getMetaData(), resultSet.getResultRows());
        } else if (stmt instanceof ShowPolicyStmt) {
            handleShowPolicy();
            return new ShowResultSet(resultSet.getMetaData(), resultSet.getResultRows());
        } else if (stmt instanceof ShowCreatePolicyStmt) {
            handleShowCreatePolicy();
            return new ShowResultSet(resultSet.getMetaData(), resultSet.getResultRows());
        } else if (stmt instanceof ShowRoleMappingStatement) {
            handleShowRoleMapping();
            return new ShowResultSet(resultSet.getMetaData(), resultSet.getResultRows());
        } else if (stmt instanceof ShowSecurityIntegrationStatement) {
            handleShowSecurityIntegration();
            return new ShowResultSet(resultSet.getMetaData(), resultSet.getResultRows());
        } else if (stmt instanceof ShowFailoverGroupsStmt) {
            handleShowFailoverGroups();
            return new ShowResultSet(resultSet.getMetaData(), resultSet.getResultRows());
        } else if (stmt instanceof ShowCreateSecurityIntegrationStatement) {
            handleShowCreateSecurityIntegration();
            return new ShowResultSet(resultSet.getMetaData(), resultSet.getResultRows());
        } else if (stmt instanceof DescribeFailoverGroupStmt) {
            handleDescribeFailoverGroup();
            return new ShowResultSet(resultSet.getMetaData(), resultSet.getResultRows());
        } else {
            return super.execute();
        }
    }

    private void handleShowWarehouses() {
        ShowWarehousesStmt showStmt = (ShowWarehousesStmt) stmt;
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        WarehouseManager warehouseMgr = globalStateMgr.getWarehouseMgr();

        PatternMatcher matcher = null;
        if (!showStmt.getPattern().isEmpty()) {
            matcher = PatternMatcher.createMysqlPattern(showStmt.getPattern(),
                    CaseSensibility.WAREHOUSE.getCaseSensibility());
        }
        PatternMatcher finalMatcher = matcher;
        List<List<String>> rowSet = warehouseMgr.getWarehousesInfo().stream()
                .filter(row -> finalMatcher == null || finalMatcher.match(row.get(1)))
                .filter(row -> {
                    try {
                        AuthorizerEPack.checkAnyActionOnWarehouse(connectContext.getCurrentUserIdentity(),
                                connectContext.getCurrentRoleIds(), row.get(1));
                    } catch (AccessDeniedException e) {
                        return false;
                    }
                    return true;
                }).sorted(Comparator.comparing(o -> o.get(0))).collect(Collectors.toList());
        resultSet = new ShowResultSet(showStmt.getMetaData(), rowSet);
    }

    // show nodes from warehouse
    private void handleShowNodes() {
        ShowNodesStmt showStmt = (ShowNodesStmt) stmt;
        List<List<String>> rows = Lists.newArrayList();
        WarehouseManager warehouseMgr = GlobalStateMgr.getCurrentState().getWarehouseMgr();

        // filter by pattern or warehouseName
        String warehouseName = null;
        PatternMatcher matcher = null;
        if (showStmt.getWarehouseName() != null) {
            warehouseName = showStmt.getWarehouseName();
        } else if (showStmt.getPattern() != null) {
            matcher = PatternMatcher.createMysqlPattern(showStmt.getPattern(),
                    CaseSensibility.WAREHOUSE.getCaseSensibility());
        }

        List<Warehouse> warehouseList = warehouseMgr.getAllWarehouses().stream().filter(
                warehouse -> {
                    try {
                        AuthorizerEPack.checkAnyActionOnWarehouse(connectContext.getCurrentUserIdentity(),
                                connectContext.getCurrentRoleIds(), warehouse.getName());
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

            rows.addAll(wh.getNodesInfo());
        }
        resultSet = new ShowResultSet(showStmt.getMetaData(), rows);
    }

    private void handleShowPolicy() {
        ShowPolicyStmt showPolicyStmt = (ShowPolicyStmt) stmt;
        Map<String, Policy> policies = GlobalStateMgr.getCurrentState().getSecurityPolicyManager()
                .getOrCreateNamePolicyMapByDBUID(
                        DbUID.generate(showPolicyStmt.getCatalog(), showPolicyStmt.getDbName()),
                        showPolicyStmt.getPolicyType());
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
                row.add(showPolicyStmt.getCatalog());
                row.add(showPolicyStmt.getDbName());

                rows.add(row);
            }
        }
        resultSet = new ShowResultSet(stmt.getMetaData(), rows);
    }

    private void handleShowCreatePolicy() {
        ShowCreatePolicyStmt describePolicyStmt = (ShowCreatePolicyStmt) stmt;
        Policy policy = GlobalStateMgr.getCurrentState().getSecurityPolicyManager()
                .getPolicyByName(describePolicyStmt.getPolicyType(), describePolicyStmt.getPolicyName(), false);

        List<String> row = new ArrayList<>();
        row.add(policy.getName());

        row.add(AstToSQLBuilder.toSQL(new CreatePolicyStmt(false, policy.getPolicyType(),
                new PolicyName("", "", policy.getName(), NodePosition.ZERO),
                policy.getArgNames(),
                policy.getArgTypes().stream().map(TypeDef::new).collect(Collectors.toList()),
                new TypeDef(policy.getRetType()),
                SqlParser.parseSqlToExpr(policy.getPolicyExpressionSQL(), SqlModeHelper.MODE_DEFAULT),
                policy.getComment(), NodePosition.ZERO)));

        resultSet = new ShowResultSet(stmt.getMetaData(), Collections.singletonList(row));
    }

    private void handleShowRoleMapping() {
        ShowRoleMappingStatement statement = (ShowRoleMappingStatement) stmt;
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

        resultSet = new ShowResultSet(statement.getMetaData(), sortedList);
    }

    private void handleShowSecurityIntegration() {
        ShowSecurityIntegrationStatement statement = (ShowSecurityIntegrationStatement) stmt;
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

        resultSet = new ShowResultSet(statement.getMetaData(), sortedList);
    }

    private void handleShowFailoverGroups() throws AnalysisException {
        ShowFailoverGroupsStmt showStmt = (ShowFailoverGroupsStmt) stmt;
        resultSet = new ShowResultSet(showStmt.getMetaData(), showStmt.getRows());
    }

    private void handleShowCreateSecurityIntegration() {
        ShowCreateSecurityIntegrationStatement showStmt = (ShowCreateSecurityIntegrationStatement) stmt;
        String name = showStmt.getName();
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
        resultSet = new ShowResultSet(showStmt.getMetaData(), infos);
    }

    private void handleDescribeFailoverGroup() throws AnalysisException {
        DescribeFailoverGroupStmt descStmt = (DescribeFailoverGroupStmt) stmt;
        resultSet = new ShowResultSet(descStmt.getMetaData(), descStmt.getRows());
    }
}
