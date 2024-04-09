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

package com.starrocks.sql.analyzer;

import com.starrocks.authentication.LDAPSecurityIntegration;
import com.starrocks.authentication.SecurityIntegration;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CreateSecurityIntegrationStatement;
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

    }

}
