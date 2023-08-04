// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.analyzer;

import com.starrocks.authentication.SecurityIntegration;
import com.starrocks.epack.privilege.LDAPRoleMapping;
import com.starrocks.epack.privilege.RoleMapping;
import com.starrocks.epack.sql.ast.AlterRoleMappingStatement;
import com.starrocks.epack.sql.ast.CreateRoleMappingStatement;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.StatementBase;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class RoleMappingStatementAnalyzer {

    public static void analyze(StatementBase statement, ConnectContext context) {
        new RoleMappingStatementAnalyzerVisitor().analyze(statement, context);
    }

    public static class RoleMappingStatementAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        final Set<String> requiredProperties = new HashSet<>(Arrays.asList(
                RoleMapping.ROLE_MAPPING_PROPERTY_INTEGRATION_NAME_KEY,
                RoleMapping.ROLE_MAPPING_PROPERTY_ROLE_NAME_KEY));

        public void analyze(StatementBase statement, ConnectContext context) {
            visit(statement, context);
        }

        @Override
        public Void visitCreateRoleMappingStatement(CreateRoleMappingStatement statement,
                                                    ConnectContext context) {
            Map<String, String> propertyMap = statement.getPropertyMap();

            requiredProperties.forEach(s -> {
                if (!propertyMap.containsKey(s)) {
                    throw new SemanticException("missing required property: " + s);
                }
            });

            String securityIntegrationName = propertyMap.get(RoleMapping.ROLE_MAPPING_PROPERTY_INTEGRATION_NAME_KEY);
            SecurityIntegration securityIntegration = context.getGlobalStateMgr().getAuthenticationMgr()
                    .getSecurityIntegration(securityIntegrationName);
            if (securityIntegration == null) {
                throw new SemanticException("security integration '" + securityIntegrationName + "' doesn't exist");
            }
            if (Objects.equals(securityIntegration.getType(), SecurityIntegration.SECURITY_INTEGRATION_TYPE_LDAP)) {
                if (!propertyMap.containsKey(LDAPRoleMapping.ROLE_MAPPING_PROPERTY_GROUP_LIST_KEY)) {
                    throw new SemanticException("missing required property: " +
                            LDAPRoleMapping.ROLE_MAPPING_PROPERTY_GROUP_LIST_KEY);
                }
            } else {
                throw new SemanticException("unsupported security integration type '" + securityIntegration.getType() +
                        "' for '" + securityIntegrationName + "'");
            }

            String roleName = propertyMap.get(RoleMapping.ROLE_MAPPING_PROPERTY_ROLE_NAME_KEY);
            if (context.getGlobalStateMgr().getAuthorizationMgr()
                    .getRoleIdByNameAllowNull(roleName) == null) {
                throw new SemanticException("role '" + roleName + "' doesn't exist");
            }

            return null;
        }

        @Override
        public Void visitAlterRoleMappingStatement(AlterRoleMappingStatement statement,
                                                   ConnectContext context) {
            if (statement.getProperties().containsKey(RoleMapping.ROLE_MAPPING_PROPERTY_ROLE_NAME_KEY)) {
                String roleName = statement.getProperties().get(RoleMapping.ROLE_MAPPING_PROPERTY_ROLE_NAME_KEY);
                if (context.getGlobalStateMgr().getAuthorizationMgr()
                        .getRoleIdByNameAllowNull(roleName) == null) {
                    throw new SemanticException("role '" + roleName + "' doesn't exist");
                }
            }

            return null;
        }
    }

}
