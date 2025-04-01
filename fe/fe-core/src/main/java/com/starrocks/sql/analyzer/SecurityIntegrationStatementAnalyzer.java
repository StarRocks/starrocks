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

import com.google.common.base.Preconditions;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.SecurityIntegration;
import com.starrocks.authentication.SecurityIntegrationFactory;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.integration.AlterSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.CreateSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.DropSecurityIntegrationStatement;
import com.starrocks.sql.ast.integration.ShowCreateSecurityIntegrationStatement;

import java.util.Map;

public class SecurityIntegrationStatementAnalyzer {

    public static void analyze(StatementBase statement, ConnectContext context) {
        new SecurityIntegrationStatementAnalyzerVisitor().analyze(statement, context);
    }

    public static class SecurityIntegrationStatementAnalyzerVisitor implements AstVisitor<Void, ConnectContext> {

        public void analyze(StatementBase statement, ConnectContext context) {
            visit(statement, context);
        }

        @Override
        public Void visitCreateSecurityIntegrationStatement(CreateSecurityIntegrationStatement statement,
                                                            ConnectContext context) {
            Map<String, String> properties = statement.getPropertyMap();
            String securityIntegrationType = properties.get("type");
            if (securityIntegrationType == null) {
                throw new SemanticException("missing required property: type");
            }

            SecurityIntegrationFactory.checkSecurityIntegrationIsSupported(securityIntegrationType);

            SecurityIntegration securityIntegration =
                    SecurityIntegrationFactory.createSecurityIntegration(statement.getName(), statement.getPropertyMap());
            Preconditions.checkNotNull(securityIntegration);
            securityIntegration.checkProperty();

            AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
            if (authenticationMgr.getSecurityIntegration(statement.getName()) != null) {
                throw new SemanticException("security integration '" + statement.getName() + "' already exists");
            }
            return null;
        }

        @Override
        public Void visitAlterSecurityIntegrationStatement(AlterSecurityIntegrationStatement statement,
                                                           ConnectContext context) {
            AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
            if (authenticationMgr.getSecurityIntegration(statement.getName()) == null) {
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
            AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
            if (authenticationMgr.getSecurityIntegration(statement.getName()) == null) {
                throw new SemanticException("security integration '" + statement.getName() + "' not found");
            }

            return null;
        }

        @Override
        public Void visitShowCreateSecurityIntegrationStatement(ShowCreateSecurityIntegrationStatement statement,
                                                                ConnectContext context) {
            AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
            if (authenticationMgr.getSecurityIntegration(statement.getName()) == null) {
                throw new SemanticException("security integration '" + statement.getName() + "' not found");
            }

            return null;
        }
    }
}
