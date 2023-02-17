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
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.backup.Repository;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.ShowSnapshotStmt;
import com.starrocks.sql.ast.ShowStmt;

public class ShowSnapshotAnalyzer {

    public static void analyze(ShowSnapshotStmt showSnapshotStmt, ConnectContext session) {
        new ShowSnapshotAnalyzerVisitor().analyze(showSnapshotStmt, session);
    }

    public static class ShowSnapshotAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        public void analyze(ShowStmt statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitShowSnapshotStatement(ShowSnapshotStmt showSnapshotStmt, ConnectContext context) {
            String repoName = showSnapshotStmt.getRepoName();

            Repository repo =
                    GlobalStateMgr.getCurrentState().getBackupHandler().getRepoMgr().getRepo(repoName);
            if (repo == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Repository [" + repoName + "] does not exist");
            }

            Expr where = showSnapshotStmt.getWhere();
            if (where != null) {
                boolean ok = true;
                CHECK:
                {
                    if (where instanceof BinaryPredicate) {
                        if (!analyzeSubExpr(showSnapshotStmt, (BinaryPredicate) where)) {
                            ok = false;
                            break CHECK;
                        }
                    } else if (where instanceof CompoundPredicate) {
                        CompoundPredicate cp = (CompoundPredicate) where;
                        if (cp.getOp() != CompoundPredicate.Operator.AND) {
                            ok = false;
                            break CHECK;
                        }

                        if (!(cp.getChild(0) instanceof BinaryPredicate)
                                || !(cp.getChild(1) instanceof BinaryPredicate)) {
                            ok = false;
                            break CHECK;
                        }

                        if (!analyzeSubExpr(showSnapshotStmt, (BinaryPredicate) cp.getChild(0))
                                || !analyzeSubExpr(showSnapshotStmt, (BinaryPredicate) cp.getChild(1))) {
                            ok = false;
                            break CHECK;
                        }
                    } else if (where instanceof InPredicate) {
                        InPredicate inPredicate = (InPredicate) where;
                        Expr leftExpr = inPredicate.getChild(0);
                        if (!(leftExpr instanceof SlotRef)) {
                            throw new SemanticException("Left expr of should be snapshot");
                        }
                        String name = ((SlotRef) leftExpr).getColumnName();
                        if (!name.equalsIgnoreCase("snapshot")) {
                            throw new SemanticException("Left expr of should be snapshot");
                        }

                        for (int i = 1; i <= inPredicate.getInElementNum(); i++) {
                            Expr expr = inPredicate.getChild(i);
                            if (!(expr instanceof StringLiteral)) {
                                throw new SemanticException("Child of in predicate should be string value");
                            }

                            String snapshotName = ((StringLiteral) expr).getStringValue();
                            if (Strings.isNullOrEmpty(snapshotName)) {
                                continue;
                            }
                            showSnapshotStmt.addSnapshotName(snapshotName);
                        }
                    }
                }

                if (ok && (Strings.isNullOrEmpty(showSnapshotStmt.getSnapshotName()) &&
                        !Strings.isNullOrEmpty(showSnapshotStmt.getTimestamp()))) {
                    // can not only set timestamp
                    ok = false;
                }

                if (!ok) {
                    throw new SemanticException("Where clause should looks like: SNAPSHOT = 'your_snapshot_name'"
                            + " [AND TIMESTAMP = '2018-04-18-19-19-10']");
                }
            }

            return null;
        }

        private boolean analyzeSubExpr(ShowSnapshotStmt showSnapshotStmt, BinaryPredicate expr) {
            Expr key = expr.getChild(0);
            Expr val = expr.getChild(1);

            if (!(key instanceof SlotRef)) {
                return false;
            }
            if (!(val instanceof StringLiteral)) {
                return false;
            }

            String name = ((SlotRef) key).getColumnName();
            if (name.equalsIgnoreCase("snapshot")) {
                String snapshotName = ((StringLiteral) val).getStringValue();
                if (Strings.isNullOrEmpty(snapshotName)) {
                    return false;
                }
                showSnapshotStmt.setSnapshotName(snapshotName);
                return true;
            } else if (name.equalsIgnoreCase("timestamp")) {
                String timestamp = ((StringLiteral) val).getStringValue();
                if (Strings.isNullOrEmpty(timestamp)) {
                    return false;
                }
                showSnapshotStmt.setTimestamp(timestamp);
                return true;
            }

            return false;

        }

    }
}
