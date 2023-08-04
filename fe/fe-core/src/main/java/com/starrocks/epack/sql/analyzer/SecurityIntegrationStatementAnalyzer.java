// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.sql.analyzer;

import com.starrocks.authentication.LDAPSecurityIntegration;
import com.starrocks.authentication.SecurityIntegration;
import com.starrocks.epack.sql.ast.AlterSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.CreateSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.DropSecurityIntegrationStatement;
import com.starrocks.epack.sql.ast.ShowCreateSecurityIntegrationStatement;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.StatementBase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.starrocks.authentication.LDAPSecurityIntegration.LDAP_SEC_INTEGRATION_PROP_CACHE_REFRESH_INTERVAL_KEY;

public class SecurityIntegrationStatementAnalyzer {

    public static void analyze(StatementBase statement, ConnectContext context) {
        new SecurityIntegrationStatementAnalyzerVisitor().analyze(statement, context);
    }

    public static class SecurityIntegrationStatementAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        final Set<String> supportedAuthMechanism =
                new HashSet<>(Collections.singletonList(SecurityIntegration.SECURITY_INTEGRATION_TYPE_LDAP));
        final Set<String> requiredProperties = new HashSet<>(Arrays.asList(
                SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY,
                LDAPSecurityIntegration.LDAP_SEC_INTEGRATION_PROP_BASE_DN_KEY,
                LDAPSecurityIntegration.LDAP_SEC_INTEGRATION_PROP_ROOT_DN_KEY,
                LDAPSecurityIntegration.LDAP_SEC_INTEGRATION_PROP_ROOT_PWD_KEY));

        public void analyze(StatementBase statement, ConnectContext context) {
            visit(statement, context);
        }

        @Override
        public Void visitCreateSecurityIntegrationStatement(CreateSecurityIntegrationStatement statement,
                                                            ConnectContext context) {
            Map<String, String> propertyMap = statement.getPropertyMap();

            requiredProperties.forEach(s -> {
                if (!propertyMap.containsKey(s)) {
                    throw new SemanticException("missing required property: " + s);
                }
            });

            if (!supportedAuthMechanism.contains(propertyMap.get("type"))) {
                throw new SemanticException("unsupported security integration type '" +
                        propertyMap.get(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY) + "'");
            }

            if (context.getGlobalStateMgr().getAuthenticationMgr()
                    .getSecurityIntegration(statement.getName()) != null) {
                throw new SemanticException("security integration '" + statement.getName() + "' already exists");
            }

            if (propertyMap.containsKey(LDAP_SEC_INTEGRATION_PROP_CACHE_REFRESH_INTERVAL_KEY)) {
                try {
                    String val = propertyMap.get(LDAP_SEC_INTEGRATION_PROP_CACHE_REFRESH_INTERVAL_KEY);
                    int interval = Integer.parseInt(val);
                    if (interval < 10) {
                        throw new NumberFormatException("current value of '" +
                                LDAP_SEC_INTEGRATION_PROP_CACHE_REFRESH_INTERVAL_KEY + "' is less than 10");
                    }
                } catch (NumberFormatException e) {
                    throw new SemanticException("invalid '" +
                            LDAP_SEC_INTEGRATION_PROP_CACHE_REFRESH_INTERVAL_KEY +
                            "' property value, error: " + e.getMessage(), e);
                }
            }

            return null;
        }

        @Override
        public Void visitAlterSecurityIntegrationStatement(AlterSecurityIntegrationStatement statement,
                                                           ConnectContext context) {
            if (context.getGlobalStateMgr().getAuthenticationMgr()
                    .getSecurityIntegration(statement.getName()) == null) {
                throw new SemanticException("security integration '" + statement.getName() + "' not found");
            }

            if (statement.getProperties().containsKey(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY)) {
                throw new SemanticException("'type' property cannot be changed");
            }

            return null;
        }

        @Override
        public Void visitDropSecurityIntegrationStatement(DropSecurityIntegrationStatement statement,
                                                          ConnectContext context) {
            if (context.getGlobalStateMgr().getAuthenticationMgr()
                    .getSecurityIntegration(statement.getName()) == null) {
                throw new SemanticException("security integration '" + statement.getName() + "' not found");
            }

            return null;
        }

        @Override
        public Void visitShowCreateSecurityIntegrationStatement(ShowCreateSecurityIntegrationStatement statement,
                                                                ConnectContext context) {
            if (context.getGlobalStateMgr().getAuthenticationMgr()
                    .getSecurityIntegration(statement.getName()) == null) {
                throw new SemanticException("security integration '" + statement.getName() + "' not found");
            }

            return null;
        }
    }

}
