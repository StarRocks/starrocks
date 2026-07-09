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

import com.google.common.base.Strings;
import com.starrocks.context.ContextMgr;
import com.starrocks.context.policy.CollectionTypePolicy;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.context.AlterContextBaseRenameStmt;
import com.starrocks.sql.ast.context.AlterContextBaseStmt;
import com.starrocks.sql.ast.context.ContextBaseName;
import com.starrocks.sql.ast.context.ContextCollectionName;
import com.starrocks.sql.ast.context.ContextDeleteStmt;
import com.starrocks.sql.ast.context.ContextUpsertStmt;
import com.starrocks.sql.ast.context.CreateContextBaseStmt;
import com.starrocks.sql.ast.context.CreateContextCollectionStmt;
import com.starrocks.sql.ast.context.CreateRetrievalProfileStmt;
import com.starrocks.sql.ast.context.CreateWorkspaceStmt;
import com.starrocks.sql.ast.context.DropContextBaseStmt;
import com.starrocks.sql.ast.context.DropContextCollectionStmt;
import com.starrocks.sql.ast.context.DropRetrievalProfileStmt;
import com.starrocks.sql.ast.context.DropWorkspaceStmt;
import com.starrocks.sql.ast.context.WorkspaceName;
import com.starrocks.sql.ast.context.WorkspaceUpsertStmt;

/**
 * Minimal semantic-context analyzer: validates identifier shape and option-value form. Collection-type
 * and entity-type compatibility, workspace lifecycle, policy/privilege checks are Milestone 2 work.
 */
public class ContextStmtAnalyzer {

    public static void analyze(StatementBase stmt, ConnectContext context) {
        new Visitor().visit(stmt, context);
    }

    private static class Visitor implements AstVisitorExtendInterface<Void, ConnectContext> {

        @Override
        public Void visitCreateContextBaseStatement(CreateContextBaseStmt stmt, ConnectContext context) {
            checkName(stmt.getName());
            rejectRemovedContextBaseProperties(stmt.getProperties());
            return null;
        }

        @Override
        public Void visitAlterContextBaseStatement(AlterContextBaseStmt stmt, ConnectContext context) {
            checkName(stmt.getName());
            if (stmt.getProperties() == null || stmt.getProperties().isEmpty()) {
                throw new SemanticException("ALTER CONTEXTBASE requires at least one SET property");
            }
            rejectRemovedContextBaseProperties(stmt.getProperties());
            return null;
        }

        private static void rejectRemovedContextBaseProperties(java.util.Map<String, String> properties) {
            if (properties == null) {
                return;
            }
            if (properties.containsKey("default_consistency")) {
                throw new SemanticException(
                        "contextbase property 'default_consistency' is no longer supported; "
                                + "CONTEXT UPSERT is synchronous and always primary-consistent");
            }
        }

        @Override
        public Void visitAlterContextBaseRenameStatement(AlterContextBaseRenameStmt stmt, ConnectContext context) {
            checkName(stmt.getName());
            if (Strings.isNullOrEmpty(stmt.getNewName())) {
                throw new SemanticException("RENAME target contextbase name must not be empty");
            }
            if (stmt.getName().getName().equals(stmt.getNewName())) {
                throw new SemanticException("RENAME target contextbase name must differ from the current name");
            }
            return null;
        }

        @Override
        public Void visitDropContextBaseStatement(DropContextBaseStmt stmt, ConnectContext context) {
            checkName(stmt.getName());
            return null;
        }

        @Override
        public Void visitCreateContextCollectionStatement(CreateContextCollectionStmt stmt, ConnectContext context) {
            checkCollectionName(stmt.getName());
            if (stmt.getProperties() != null) {
                String type = stmt.getProperties().get("collection_type");
                if (type != null && !CollectionTypePolicy.isValidCollectionType(type)) {
                    throw new SemanticException("unknown collection_type: " + type);
                }
            }
            return null;
        }

        @Override
        public Void visitDropContextCollectionStatement(DropContextCollectionStmt stmt, ConnectContext context) {
            checkCollectionName(stmt.getName());
            return null;
        }

        @Override
        public Void visitCreateContextWorkspaceStatement(CreateWorkspaceStmt stmt, ConnectContext context) {
            checkWorkspaceName(stmt.getName());
            return null;
        }

        @Override
        public Void visitDropContextWorkspaceStatement(DropWorkspaceStmt stmt, ConnectContext context) {
            checkWorkspaceName(stmt.getName());
            return null;
        }

        @Override
        public Void visitCreateRetrievalProfileStatement(CreateRetrievalProfileStmt stmt, ConnectContext context) {
            if (Strings.isNullOrEmpty(stmt.getName())) {
                throw new SemanticException("retrieval profile name must not be empty");
            }
            return null;
        }

        @Override
        public Void visitDropRetrievalProfileStatement(DropRetrievalProfileStmt stmt, ConnectContext context) {
            if (Strings.isNullOrEmpty(stmt.getName())) {
                throw new SemanticException("retrieval profile name must not be empty");
            }
            return null;
        }

        @Override
        public Void visitContextUpsertStatement(ContextUpsertStmt stmt, ConnectContext context) {
            checkCollectionName(stmt.getCollection());
            if (stmt.getEntityArgs() == null || stmt.getEntityArgs().isEmpty()) {
                throw new SemanticException("CONTEXT UPSERT requires ENTITY(...) arguments");
            }
            if (stmt.getOptions() != null) {
                if (stmt.getOptions().containsKey("consistency")) {
                    throw new SemanticException(
                            "CONTEXT UPSERT option 'consistency' is no longer supported; "
                                    + "every upsert is synchronous and primary-consistent");
                }
            }
            // Enforce the collection_type → entity_type compatibility matrix when the target
            // collection is known to the FE metadata. If it isn't, the executor will surface a
            // clearer "collection not found" error; we don't block analysis on that case.
            String contextBase = stmt.getCollection().getContextBase();
            if (contextBase != null) {
                ContextMgr mgr = GlobalStateMgr.getCurrentState().getContextMgr();
                for (ContextMgr.CollectionMeta col : mgr.listCollections(contextBase)) {
                    if (col.getName().equals(stmt.getCollection().getCollection())) {
                        String entityType = stringFromArg(stmt.getEntityArgs(), "entity_type");
                        if (entityType != null) {
                            try {
                                CollectionTypePolicy.check(col.getCollectionType(), entityType);
                            } catch (IllegalArgumentException e) {
                                throw new SemanticException(e.getMessage());
                            }
                        }
                        break;
                    }
                }
            }
            return null;
        }

        private static String stringFromArg(java.util.Map<String, com.starrocks.sql.ast.expression.Expr> args,
                                            String key) {
            com.starrocks.sql.ast.expression.Expr e = args.get(key);
            if (e instanceof com.starrocks.sql.ast.expression.StringLiteral) {
                return ((com.starrocks.sql.ast.expression.StringLiteral) e).getValue();
            }
            return null;
        }

        @Override
        public Void visitContextDeleteStatement(ContextDeleteStmt stmt, ConnectContext context) {
            checkCollectionName(stmt.getCollection());
            if (stmt.getPredicate() == null) {
                throw new SemanticException("CONTEXT DELETE requires WHERE predicate");
            }
            return null;
        }

        @Override
        public Void visitWorkspaceUpsertStatement(WorkspaceUpsertStmt stmt, ConnectContext context) {
            checkWorkspaceName(stmt.getWorkspace());
            if (stmt.getObjectArgs() == null || stmt.getObjectArgs().isEmpty()) {
                throw new SemanticException("WORKSPACE UPSERT requires OBJECT(...) arguments");
            }
            return null;
        }

        private static void checkName(ContextBaseName name) {
            if (name == null || Strings.isNullOrEmpty(name.getName())) {
                throw new SemanticException("contextbase name must not be empty");
            }
        }

        private static void checkCollectionName(ContextCollectionName name) {
            if (name == null || Strings.isNullOrEmpty(name.getCollection())) {
                throw new SemanticException("collection name must not be empty");
            }
        }

        private static void checkWorkspaceName(WorkspaceName name) {
            if (name == null || Strings.isNullOrEmpty(name.getWorkspace())) {
                throw new SemanticException("workspace name must not be empty");
            }
        }
    }
}
