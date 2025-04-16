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
import com.starrocks.authentication.GroupProvider;
import com.starrocks.authentication.GroupProviderFactory;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.group.CreateGroupProviderStmt;
import com.starrocks.sql.ast.group.DropGroupProviderStmt;
import com.starrocks.sql.ast.group.ShowCreateGroupProviderStmt;
import com.starrocks.sql.ast.group.ShowGroupProvidersStmt;

import java.util.Map;

public class GroupProviderStatementAnalyzer {
    public static void analyze(StatementBase statement, ConnectContext context) {
        new GroupProviderStatementAnalyzer.GroupProviderStatementAnalyzerVisitor().analyze(statement, context);
    }

    public static class GroupProviderStatementAnalyzerVisitor implements AstVisitor<Void, ConnectContext> {

        public void analyze(StatementBase statement, ConnectContext context) {
            visit(statement, context);
        }

        @Override
        public Void visitCreateGroupProviderStatement(CreateGroupProviderStmt statement, ConnectContext context) {
            Map<String, String> properties = statement.getPropertyMap();
            String groupProviderType = properties.get("type");
            if (groupProviderType == null) {
                throw new SemanticException("missing required property: type");
            }

            GroupProvider groupProvider = GroupProviderFactory.createGroupProvider(statement.getName(), properties);
            Preconditions.checkNotNull(groupProvider);
            groupProvider.checkProperty();

            AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
            if (authenticationMgr.getGroupProvider(statement.getName()) != null) {
                throw new SemanticException("Group Provider '" + statement.getName() + "' already exists");
            }
            return null;
        }

        @Override
        public Void visitDropGroupProviderStatement(DropGroupProviderStmt statement, ConnectContext context) {
            AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
            if (authenticationMgr.getGroupProvider(statement.getName()) == null) {
                throw new SemanticException("Group Provider '" + statement.getName() + "' not found");
            }

            return null;
        }

        @Override
        public Void visitShowCreateGroupProviderStatement(ShowCreateGroupProviderStmt statement, ConnectContext context) {
            AuthenticationMgr authenticationMgr = GlobalStateMgr.getCurrentState().getAuthenticationMgr();
            if (authenticationMgr.getGroupProvider(statement.getName()) == null) {
                throw new SemanticException("Group Provider '" + statement.getName() + "' not found");
            }

            return null;
        }

        @Override
        public Void visitShowGroupProvidersStatement(ShowGroupProvidersStmt statement, ConnectContext context) {
            return null;
        }
    }
}
