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

import com.starrocks.authentication.SecurityIntegration;
import com.starrocks.authentication.SecurityIntegrationFactory;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CreateSecurityIntegrationStatement;
import com.starrocks.sql.ast.StatementBase;

import java.util.Map;

public class SecurityIntegrationStatementAnalyzer {

    public static void analyze(StatementBase statement, ConnectContext context) {
        new SecurityIntegrationStatementAnalyzerVisitor().analyze(statement, context);
    }

    public static class SecurityIntegrationStatementAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {

        public void analyze(StatementBase statement, ConnectContext context) {
            visit(statement, context);
        }

        @Override
        public Void visitCreateSecurityIntegrationStatement(CreateSecurityIntegrationStatement statement,
                                                            ConnectContext context) {

            Map<String, String> propertyMap = statement.getPropertyMap();
            if (!propertyMap.containsKey(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY)) {
                throw new SemanticException("missing required property: "
                        + SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY);
            }

            String type = propertyMap.get(SecurityIntegration.SECURITY_INTEGRATION_PROPERTY_TYPE_KEY);
            SecurityIntegrationFactory.SecurityIntegrationType securityIntegrationType;
            try {
                securityIntegrationType = SecurityIntegrationFactory.getSecurityIntegrationType(type);
            } catch (DdlException e) {
                throw new SemanticException(e.getMessage());
            }

            if (context.getGlobalStateMgr().getAuthenticationMgr()
                    .getSecurityIntegration(statement.getName()) != null) {
                throw new SemanticException("security integration '" + statement.getName() + "' already exists");
            }

            return null;
        }

    }

}
