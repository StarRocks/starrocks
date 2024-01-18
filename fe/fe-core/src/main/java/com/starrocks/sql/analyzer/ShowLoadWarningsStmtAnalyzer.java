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
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.common.error.ErrorCode;
import com.starrocks.common.error.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.ShowLoadWarningsStmt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.MalformedURLException;
import java.net.URL;

public class ShowLoadWarningsStmtAnalyzer {

    public static void analyze(ShowLoadWarningsStmt statement, ConnectContext context) {
        new ShowLoadWarningsStmtAnalyzerVisitor().visit(statement, context);
    }

    static class ShowLoadWarningsStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {

        private String label;
        private long jobId;

        private static final Logger LOG = LogManager.getLogger(ShowLoadWarningsStmtAnalyzerVisitor.class);

        public ShowLoadWarningsStmtAnalyzerVisitor() {
            this.label = null;
            this.jobId = 0;
        }

        public void analyze(ShowLoadWarningsStmt statement, ConnectContext context) {
            visit(statement, context);
        }

        @Override
        public Void visitShowLoadWarningsStatement(ShowLoadWarningsStmt statement, ConnectContext context) {
            String rawUrl = statement.getRawUrl();
            if (rawUrl != null) {
                analyzeUrl(statement, context);
            } else {
                analyzeDbName(statement, context);
                analyzeWhereClause(statement, context);
            }
            return null;
        }

        private void analyzeUrl(ShowLoadWarningsStmt statement, ConnectContext context) {
            // url should like:
            // http://be_ip:be_http_port/api/_load_error_log?file=__shard_xxx/error_log_xxx
            String rawUrl = statement.getRawUrl();
            if (rawUrl.isEmpty()) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_MISSING_PARAM, "Error load url is missing");
            }
            if (statement.getDbName() != null || statement.getWhereClause() != null
                    || statement.getLimitElement() != null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Can not set database, where or limit clause if getting error log from url");
            }
            try {
                URL url = new URL(rawUrl);
                statement.setUrl(url);
            } catch (MalformedURLException e) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR, "Invalid url: " + e.getMessage());
            }
        }

        private void analyzeDbName(ShowLoadWarningsStmt statement, ConnectContext context) {
            String dbName = statement.getDbName();
            if (Strings.isNullOrEmpty(dbName)) {
                dbName = context.getDatabase();
                if (Strings.isNullOrEmpty(dbName)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
                }
            }
            statement.setDbName(dbName);
        }

        private void analyzeWhereClause(ShowLoadWarningsStmt statement, ConnectContext context) {
            Expr whereClause = statement.getWhereClause();
            analyzeSubPredicate(whereClause);
            statement.setLabel(label);
            statement.setJobId(jobId);
        }

        private void analyzeSubPredicate(Expr subExpr) {
            if (subExpr == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "should supply condition like: LABEL = \"your_load_label\","
                                + " or LOAD_JOB_ID = $job_id");
            }

            if (subExpr instanceof CompoundPredicate) {
                CompoundPredicate cp = (CompoundPredicate) subExpr;
                if (cp.getOp() != CompoundPredicate.Operator.AND) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                            "Only allow compound predicate with operator AND");
                }

                analyzeSubPredicate(cp.getChild(0));
                analyzeSubPredicate(cp.getChild(1));
                return;
            }

            boolean valid = false;
            boolean hasLabel = false;
            boolean hasLoadJobId = false;
            do {
                if (subExpr == null) {
                    valid = false;
                    break;
                }

                if (subExpr instanceof BinaryPredicate) {
                    BinaryPredicate binaryPredicate = (BinaryPredicate) subExpr;
                    if (binaryPredicate.getOp() != BinaryType.EQ) {
                        valid = false;
                        break;
                    }
                } else {
                    valid = false;
                    break;
                }

                // left child
                if (!(subExpr.getChild(0) instanceof SlotRef)) {
                    valid = false;
                    break;
                }
                String leftKey = ((SlotRef) subExpr.getChild(0)).getColumnName();
                if (leftKey.equalsIgnoreCase("label")) {
                    hasLabel = true;
                } else if (leftKey.equalsIgnoreCase("load_job_id")) {
                    hasLoadJobId = true;
                } else {
                    valid = false;
                    break;
                }

                if (hasLabel) {
                    if (!(subExpr.getChild(1) instanceof StringLiteral)) {
                        valid = false;
                        break;
                    }

                    String value = ((StringLiteral) subExpr.getChild(1)).getStringValue();
                    if (Strings.isNullOrEmpty(value)) {
                        valid = false;
                        break;
                    }

                    label = value;
                }

                if (hasLoadJobId) {
                    if (!(subExpr.getChild(1) instanceof IntLiteral)) {
                        LOG.warn("load_job_id is not IntLiteral. value: {}", subExpr.toSql());
                        valid = false;
                        break;
                    }
                    jobId = ((IntLiteral) subExpr.getChild(1)).getLongValue();
                }

                valid = true;
            } while (false);

            if (!valid) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Where clause should looks like: LABEL = \"your_load_label\","
                                + " or LOAD_JOB_ID = $job_id");
            }
        }
    }
}
