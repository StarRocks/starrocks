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
package com.starrocks.epack.sql.analyzer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.epack.privilege.ObjectTypeEPack;
import com.starrocks.epack.sql.ast.PolicyName;
import com.starrocks.privilege.ObjectType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.PrivilegeStmtAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.BaseGrantRevokePrivilegeStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.NodePosition;

import java.util.ArrayList;
import java.util.List;

public class PrivilegeStmtAnalyzerEPack extends PrivilegeStmtAnalyzer {
    public static void analyze(StatementBase statement, ConnectContext session) {
        new PrivilegeStatementAnalyzerVisitorEPack().analyze(statement, session);
    }

    static class PrivilegeStatementAnalyzerVisitorEPack extends PrivilegeStatementAnalyzerVisitor {
        public void analyze(StatementBase statement, ConnectContext session) {
            super.analyze(statement, session);
        }

        @Override
        public List<List<String>> analyzeTokens(BaseGrantRevokePrivilegeStmt stmt, ObjectType objectType,
                                                ConnectContext session) {
            List<List<String>> objectTokenList = new ArrayList<>();

            if (stmt.isGrantOnALL()) {
                Preconditions.checkArgument(stmt.getPrivilegeObjectNameTokensList() != null);
                Preconditions.checkArgument(stmt.getPrivilegeObjectNameTokensList().size() == 1);

                List<String> tokens = stmt.getPrivilegeObjectNameTokensList().get(0);
                if (ObjectTypeEPack.WAREHOUSE.equals(objectType)) {
                    if (tokens.size() != 1) {
                        throw new SemanticException(
                                "Invalid grant statement with error privilege object " + tokens);
                    }
                    objectTokenList.add(tokens);
                } else if (ObjectTypeEPack.MASKING_POLICY.equals(objectType)) {
                    objectTokenList.add(Lists.newArrayList("*", "*", "*"));
                } else if (ObjectTypeEPack.ROW_ACCESS_POLICY.equals(objectType)) {
                    objectTokenList.add(Lists.newArrayList("*", "*", "*"));
                } else {
                    return super.analyzeTokens(stmt, objectType, session);
                }
            } else {
                if (ObjectTypeEPack.WAREHOUSE.equals(objectType)) {
                    for (List<String> tokens : stmt.getPrivilegeObjectNameTokensList()) {
                        if (tokens.size() != 1) {
                            throw new SemanticException(
                                    "Invalid grant statement with error privilege object " + tokens);
                        }

                        objectTokenList.add(tokens);
                    }
                } else if (ObjectTypeEPack.MASKING_POLICY.equals(objectType)) {
                    for (List<String> tokens : stmt.getPrivilegeObjectNameTokensList()) {
                        PolicyName policyName;
                        if (tokens.size() == 2) {
                            policyName = new PolicyName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                                    tokens.get(0), tokens.get(1), NodePosition.ZERO);
                        } else if (tokens.size() == 1) {
                            policyName = new PolicyName(null, null, tokens.get(0), NodePosition.ZERO);
                            AnalyzerUtilsEPack.normalizationPolicyName(session, policyName);
                        } else {
                            throw new SemanticException(
                                    "Invalid grant statement with error privilege object " + tokens);
                        }

                        objectTokenList.add(
                                Lists.newArrayList(policyName.getCatalog(), policyName.getDbName(), policyName.getName()));
                    }
                } else if (ObjectTypeEPack.ROW_ACCESS_POLICY.equals(objectType)) {
                    for (List<String> tokens : stmt.getPrivilegeObjectNameTokensList()) {
                        PolicyName policyName;
                        if (tokens.size() == 2) {
                            policyName = new PolicyName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                                    tokens.get(0), tokens.get(1), NodePosition.ZERO);
                        } else if (tokens.size() == 1) {
                            policyName = new PolicyName(null, null, tokens.get(0), NodePosition.ZERO);
                            AnalyzerUtilsEPack.normalizationPolicyName(session, policyName);
                        } else {
                            throw new SemanticException(
                                    "Invalid grant statement with error privilege object " + tokens);
                        }
                        objectTokenList.add(
                                Lists.newArrayList(policyName.getCatalog(), policyName.getDbName(), policyName.getName()));
                    }
                } else {
                    return super.analyzeTokens(stmt, objectType, session);
                }
            }

            return objectTokenList;
        }
    }
}
