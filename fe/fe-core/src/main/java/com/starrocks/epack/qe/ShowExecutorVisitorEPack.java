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

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.SecurityIntegration;
import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.AuthorizationMgr;
import com.starrocks.catalog.ConnectorView;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.CaseSensibility;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.epack.authentication.AuthenticationMgrEPack;
import com.starrocks.epack.authentication.LDAPSecurityIntegration;
import com.starrocks.epack.authorization.AuthorizerEPack;
import com.starrocks.epack.authorization.DbUID;
import com.starrocks.epack.authorization.LDAPRoleMapping;
import com.starrocks.epack.authorization.PasswordPolicy;
import com.starrocks.epack.authorization.Policy;
import com.starrocks.epack.authorization.RoleMapping;
import com.starrocks.epack.authorization.SecurityPolicyMgr;
import com.starrocks.epack.connector.lakeformation.LakeFormationCatalogs;
import com.starrocks.epack.connector.lakeformation.LakeFormationDdlProjection;
import com.starrocks.epack.connector.lakeformation.LakeFormationHiveTable;
import com.starrocks.epack.sql.ast.AstVisitorEPack;
import com.starrocks.epack.sql.ast.CreatePasswordPolicyStmt;
import com.starrocks.epack.sql.ast.CreatePolicyStmt;
import com.starrocks.epack.sql.ast.DescribeFailoverGroupStmt;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.epack.sql.ast.ShowCreatePasswordPolicyStmt;
import com.starrocks.epack.sql.ast.ShowCreatePolicyStmt;
import com.starrocks.epack.sql.ast.ShowFailoverGroupsStmt;
import com.starrocks.epack.sql.ast.ShowPasswordPolicyStmt;
import com.starrocks.epack.sql.ast.ShowPolicyStmt;
import com.starrocks.epack.sql.ast.ShowRoleMappingStatement;
import com.starrocks.epack.warehouse.Cluster;
import com.starrocks.epack.warehouse.LocalWarehouse;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.PolicyName;
import com.starrocks.sql.ast.ShowAuthenticationStmt;
import com.starrocks.sql.ast.ShowCreateTableStmt;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.UserRef;
import com.starrocks.sql.ast.expression.TypeDef;
import com.starrocks.sql.ast.warehouse.ShowClustersStmt;
import com.starrocks.sql.ast.warehouse.ShowNodesStmt;
import com.starrocks.sql.ast.warehouse.ShowWarehousesStmt;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.system.ComputeNode;
import com.starrocks.type.TypeFactory;
import com.starrocks.warehouse.Warehouse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class ShowExecutorVisitorEPack extends ShowExecutor.ShowExecutorVisitor
        implements AstVisitorEPack<ShowResultSet, ConnectContext> {

    private static final ShowExecutorVisitorEPack INSTANCE = new ShowExecutorVisitorEPack();

    public static ShowExecutorVisitorEPack getInstance() {
        return INSTANCE;
    }

    /**
     * SHOW CREATE TABLE prints the schema and then the property map verbatim, and that map carries the full
     * physical column names and types - so a narrowed schema would leak them straight back. A governed table
     * is rendered from a display-only projection instead.
     *
     * Two conditions, deliberately different. Catalogs Lake Formation does not govern go straight to super,
     * because resolving here would charge them a second resolution they never needed. Among the rest the
     * decision is made on the resolved table, since only an authorized view is evidence that skipping the
     * projection is safe.
     */
    @Override
    public ShowResultSet visitShowCreateTableStatement(ShowCreateTableStmt statement, ConnectContext context) {
        TableRef tableRef = statement.getTableRef();
        if (tableRef == null) {
            return super.visitShowCreateTableStatement(statement, context);
        }
        String catalogName = tableRef.getCatalogName() != null ? tableRef.getCatalogName() : context.getCurrentCatalog();
        if (CatalogMgr.isInternalCatalog(catalogName)) {
            return super.visitShowCreateTableStatement(statement, context);
        }
        // Only a Lake Formation catalog can resolve to a governed table, so every other external catalog
        // keeps the behaviour it had - including not being resolved twice.
        if (!LakeFormationCatalogs.isLakeFormationCatalog(catalogName)) {
            return super.visitShowCreateTableStatement(statement, context);
        }

        // Same order as upstream: a missing database is reported as such before anything asks Lake
        // Formation about a table inside it, so the error stays ERR_BAD_DB_ERROR rather than an
        // authorization failure for a table that was never there.
        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        if (metadataMgr.getDb(context, catalogName, tableRef.getDbName()) == null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_DB_ERROR, tableRef.getDbName());
        }
        Table table = metadataMgr.getTable(context, catalogName, tableRef.getDbName(), tableRef.getTableName());
        if (table == null) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_TABLE_ERROR, tableRef.getTableName());
        }

        Table display = table instanceof LakeFormationHiveTable lfTable
                ? LakeFormationDdlProjection.projectForDisplay(lfTable)
                : table;
        return renderResolvedExternalTable(statement, tableRef.getTableName(), display);
    }

    /**
     * The same two branches upstream's showCreateExternalCatalogTable uses, applied to a table that has
     * already been resolved. Kept in step with upstream by the behaviour tests, which assert that this and
     * the base implementation produce identical rows and metadata for a non Lake Formation table.
     */
    private ShowResultSet renderResolvedExternalTable(ShowCreateTableStmt statement, String tableName, Table table) {
        List<List<String>> rows = Lists.newArrayList();
        if (table.isConnectorView()) {
            rows.add(Lists.newArrayList(tableName,
                    AstToStringBuilder.getExternalCatalogViewDdlStmt((ConnectorView) table)));
            ShowResultSetMetaData metaData = ShowResultSetMetaData.builder()
                    .addColumn(new com.starrocks.catalog.Column("View", TypeFactory.createVarcharType(20)))
                    .addColumn(new com.starrocks.catalog.Column("Create View", TypeFactory.createVarcharType(30)))
                    .build();
            return new ShowResultSet(metaData, rows);
        }
        rows.add(Lists.newArrayList(tableName, AstToStringBuilder.getExternalCatalogTableDdlStmt(table)));
        return new ShowResultSet(showResultMetaFactory.getMetadata(statement), rows);
    }

    @Override
    public ShowResultSet visitShowWarehousesStatement(ShowWarehousesStmt statement, ConnectContext context) {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        WarehouseManager warehouseMgr = globalStateMgr.getWarehouseMgr();

        if (RunMode.getCurrentRunMode() == RunMode.SHARED_NOTHING) {
            throw ErrorReportException.report(ErrorCode.ERR_NOT_SUPPORTED_STATEMENT_IN_SHARED_NOTHING_MODE);
        }

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
                        AuthorizerEPack.checkAnyActionOnWarehouse(context, warehouse.getName());
                    } catch (AccessDeniedException e) {
                        return false;
                    }
                    return true;
                }).sorted(Comparator.comparing(Warehouse::getId))
                .map(warehouse -> ((LocalWarehouse) warehouse).getWarehouseInfo())
                .collect(Collectors.toList());
        return new ShowResultSet(showResultMetaFactory.getMetadata(statement), rowSet);
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

        String cngroupName = statement.getCnGroupName();

        List<Warehouse> warehouseList = warehouseMgr.getAllWarehouses().stream().filter(
                warehouse -> {
                    try {
                        AuthorizerEPack.checkAnyActionOnWarehouse(context, warehouse.getName());
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
                if (!Strings.isNullOrEmpty(cngroupName) && !cluster.getName().equalsIgnoreCase(cngroupName)) {
                    continue;
                }
                List<Long> computeNodes = cluster.getComputeNodeIds();
                for (Long computeNodeId : computeNodes) {
                    ComputeNode node = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                            .getBackendOrComputeNode(computeNodeId);

                    List<String> computeNodeInfo = Lists.newArrayList();
                    long warehouseId = node.getWarehouseId();
                    Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(warehouseId);
                    computeNodeInfo.add(warehouse.getName());

                    computeNodeInfo.add(String.valueOf(cluster.getId()));
                    computeNodeInfo.add(String.valueOf(cluster.getWorkerGroupId()));
                    long nodeId = node.getId();
                    long workerId = GlobalStateMgr.getCurrentState().getStarOSAgent().getWorkerIdByNodeId(nodeId);
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
                    computeNodeInfo.add(String.valueOf(node.getCpuCores()));
                    double memUsedPct = node.getMemUsedPct();
                    computeNodeInfo.add(String.format("%.2f", memUsedPct * 100) + " %");
                    computeNodeInfo.add(String.format("%.1f", node.getCpuUsedPermille() / 10.0) + " %");
                    computeNodeInfo.add(cluster.getName());

                    rows.add(computeNodeInfo);
                }
            }
        }
        return new ShowResultSet(showResultMetaFactory.getMetadata(statement), rows);
    }

    @Override
    public ShowResultSet visitShowClusterStatement(ShowClustersStmt statement, ConnectContext context) {
        if (RunMode.getCurrentRunMode() == RunMode.SHARED_NOTHING) {
            throw ErrorReportException.report(ErrorCode.ERR_NOT_SUPPORTED_STATEMENT_IN_SHARED_NOTHING_MODE);
        }
        WarehouseManager warehouseMgr = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        Warehouse warehouse = warehouseMgr.getWarehouse(statement.getWarehouseName());
        List<List<String>> rows = null;
        if (warehouse instanceof LocalWarehouse) {
            rows = ((LocalWarehouse) warehouse).getClustersInfo();
        }
        if (rows == null) {
            rows = Lists.newArrayList();
        }
        return new ShowResultSet(showResultMetaFactory.getMetadata(statement), rows);
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
        return new ShowResultSet(showResultMetaFactory.getMetadata(statement), rows);
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

        return new ShowResultSet(showResultMetaFactory.getMetadata(statement), Collections.singletonList(row));
    }

    @Override
    public ShowResultSet visitShowRoleMappingStatement(ShowRoleMappingStatement statement, ConnectContext context) {
        AuthorizationMgr authorizationManager = GlobalStateMgr.getCurrentState().getAuthorizationMgr();
        Set<RoleMapping> roleMappings = authorizationManager.getRoleMappingMetaMgr().getAllRoleMappings();
        List<List<String>> infos = new ArrayList<>();
        for (RoleMapping roleMapping : roleMappings) {
            AuthenticationMgrEPack authenticationMgrEPack =
                    (AuthenticationMgrEPack) GlobalStateMgr.getCurrentState().getAuthenticationMgr();
            SecurityIntegration securityIntegration = authenticationMgrEPack
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
                    TimeUtils.longToTimeString(((LDAPSecurityIntegration) securityIntegration).getLastRefreshTime())
                    : FeConstants.NULL_STRING);
            infos.add(info);
        }

        // sort by integration name, then by role mapping name
        List<List<String>> sortedList = infos.stream()
                .sorted(
                        Comparator.comparing((List<String> sublist) -> sublist.get(1))
                                .thenComparing((List<String> sublist) -> sublist.get(0))
                )
                .collect(Collectors.toList());

        return new ShowResultSet(showResultMetaFactory.getMetadata(statement), sortedList);
    }

    @Override
    public ShowResultSet visitShowFailoverGroupsStatement(ShowFailoverGroupsStmt statement, ConnectContext context) {
        try {
            return new ShowResultSet(showResultMetaFactory.getMetadata(statement), statement.getRows(context));
        } catch (AnalysisException e) {
            throw new SemanticException(e.getMessage());
        }

    }

    @Override
    public ShowResultSet visitDescribeFailoverGroupStatement(DescribeFailoverGroupStmt statement, ConnectContext context) {
        try {
            return new ShowResultSet(showResultMetaFactory.getMetadata(statement), statement.getRows());
        } catch (AnalysisException e) {
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
            if (statement.getUser() == null) {
                userIdentity = context.getCurrentUserIdentity();
            } else {
                UserRef user = statement.getUser();
                userIdentity = new UserIdentity(user.getUser(), user.getHost(), user.isDomain());
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
                        userAuthenticationInfo.getAuthString()));
            }
        }

        return new ShowResultSet(showResultMetaFactory.getMetadata(statement), userAuthInfos);
    }

    @Override
    public ShowResultSet visitShowCreatePasswordPolicyStatement(ShowCreatePasswordPolicyStmt statement, ConnectContext context) {
        SecurityPolicyMgr securityPolicyMgr = GlobalStateMgr.getCurrentState().getSecurityPolicyManager();
        PasswordPolicy passwordPolicy = securityPolicyMgr.getPasswordPolicy(statement.getPolicyName());
        if (passwordPolicy == null) {
            throw new SemanticException("Password Policy " + statement.getPolicyName() + " not exist");
        }

        List<String> passwordPolicyRow = Lists.newArrayList();
        passwordPolicyRow.add(passwordPolicy.getPolicyName());
        passwordPolicyRow.add(AstToSQLBuilder.toSQL(new CreatePasswordPolicyStmt(
                passwordPolicy.getPolicyName(), passwordPolicy.getComment(), passwordPolicy.getProperties(), NodePosition.ZERO)));

        List<List<String>> rows = Lists.newArrayList();
        rows.add(passwordPolicyRow);
        return new ShowResultSet(showResultMetaFactory.getMetadata(statement), rows);
    }

    @Override
    public ShowResultSet visitShowPasswordPolicyStatement(ShowPasswordPolicyStmt statement, ConnectContext context) {
        SecurityPolicyMgr securityPolicyMgr = GlobalStateMgr.getCurrentState().getSecurityPolicyManager();
        List<PasswordPolicy> passwordPolicies = securityPolicyMgr.getAllPasswordPolicies();
        PasswordPolicy globalPasswordPolicy = securityPolicyMgr.getGlobalPasswordPolicy();

        List<List<String>> rows = Lists.newArrayList();
        for (PasswordPolicy passwordPolicy : passwordPolicies) {
            List<String> row = Lists.newArrayList();
            row.add(passwordPolicy.getPolicyName());
            row.add(passwordPolicy.getComment());
            row.add(Joiner.on(", ").join(passwordPolicy.getProperties().entrySet()
                    .stream().map(entry -> entry.getKey() + " = " + entry.getValue()).collect(Collectors.toList())));

            if (globalPasswordPolicy != null
                    && Objects.equals(globalPasswordPolicy.getPolicyId(), passwordPolicy.getPolicyId())) {
                row.add("TRUE");
            } else {
                row.add("FALSE");
            }

            rows.add(row);
        }

        return new ShowResultSet(showResultMetaFactory.getMetadata(statement), rows);
    }
}
