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
import com.starrocks.analysis.BinaryType;
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
import com.starrocks.sql.ast.DropSnapshotStmt;

public class DropSnapshotAnalyzer {

    public static void analyze(DropSnapshotStmt dropSnapshotStmt, ConnectContext session) {
        new DropSnapshotAnalyzerVisitor().analyze(dropSnapshotStmt, session);
    }

    public static class DropSnapshotAnalyzerVisitor implements AstVisitor<Void, ConnectContext> {
        public void analyze(DropSnapshotStmt statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitDropSnapshotStatement(DropSnapshotStmt dropSnapshotStmt, ConnectContext context) {
            String repoName = dropSnapshotStmt.getRepoName();

            Repository repo =
                    GlobalStateMgr.getCurrentState().getBackupHandler().getRepoMgr().getRepo(repoName);
            if (repo == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Repository " + repoName + " does not exist");
            }

            Expr where = dropSnapshotStmt.getWhere();
            if (where != null) {
                boolean ok = true;
                CHECK:
                {
                    if (where instanceof BinaryPredicate) {
                        if (!analyzeSubExpr(dropSnapshotStmt, (BinaryPredicate) where)) {
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

                        if (!analyzeSubExpr(dropSnapshotStmt, (BinaryPredicate) cp.getChild(0))
                                || !analyzeSubExpr(dropSnapshotStmt, (BinaryPredicate) cp.getChild(1))) {
                            ok = false;
                            break CHECK;
                        }
                    } else if (where instanceof InPredicate) {
                        InPredicate inPredicate = (InPredicate) where;
                        if (!(inPredicate.getChild(0) instanceof SlotRef)) {
                            ok = false;
                            break CHECK;
                        }
                        SlotRef slotRef = (SlotRef) inPredicate.getChild(0);
                        if (!slotRef.getColumnName().equalsIgnoreCase("snapshot")) {
                            ok = false;
                            break CHECK;
                        }

                        for (int i = 1; i <= inPredicate.getInElementNum(); i++) {
                            Expr expr = inPredicate.getChild(i);
                            if (!(expr instanceof StringLiteral)) {
                                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                                        "Child of in predicate should be string value");
                            }

                            String snapshotName = ((StringLiteral) expr).getStringValue();
                            if (Strings.isNullOrEmpty(snapshotName)) {
                                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                                        "Snapshot name cannot be empty");
                            }
                            dropSnapshotStmt.addSnapshotName(snapshotName);
                        }
                    } else {
                        // Unsupported predicate type (e.g., LikePredicate, etc.)
                        ok = false;
                        break CHECK;
                    }
                }

                if (!ok) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                            "Where clause should looks like: SNAPSHOT = 'your_snapshot_name'"
                                    + " [AND TIMESTAMP <= '2018-04-18-19-19-10'] or [AND TIMESTAMP >= '2018-04-18-19-19-10']");
                }
            }

            return null;
        }

        private boolean analyzeSubExpr(DropSnapshotStmt dropSnapshotStmt, BinaryPredicate binaryPredicate) {
            if (binaryPredicate.getOp() != BinaryType.EQ
                    && binaryPredicate.getOp() != BinaryType.LE
                    && binaryPredicate.getOp() != BinaryType.GE) {
                return false;
            }

            if (!(binaryPredicate.getChild(0) instanceof SlotRef)
                    || !(binaryPredicate.getChild(1) instanceof StringLiteral)) {
                return false;
            }

            SlotRef slotRef = (SlotRef) binaryPredicate.getChild(0);
            StringLiteral stringLiteral = (StringLiteral) binaryPredicate.getChild(1);

            if (slotRef.getColumnName().equalsIgnoreCase("snapshot")) {
                if (binaryPredicate.getOp() != BinaryType.EQ) {
                    return false;
                }
                String snapshotName = stringLiteral.getStringValue();
                if (Strings.isNullOrEmpty(snapshotName)) {
                    return false;
                }
                dropSnapshotStmt.setSnapshotName(snapshotName);
            } else if (slotRef.getColumnName().equalsIgnoreCase("timestamp")) {
                if (binaryPredicate.getOp() == BinaryType.LE) {
                    dropSnapshotStmt.setTimestampOperator("<=");
                } else if (binaryPredicate.getOp() == BinaryType.GE) {
                    dropSnapshotStmt.setTimestampOperator(">=");
                } else {
                    return false;
                }
                String timestamp = stringLiteral.getStringValue();
                if (Strings.isNullOrEmpty(timestamp)) {
                    return false;
                }
                dropSnapshotStmt.setTimestamp(timestamp);
            } else {
                return false;
            }

            return true;
        }
    }
}
