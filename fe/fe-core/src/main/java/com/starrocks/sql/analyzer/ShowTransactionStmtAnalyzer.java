// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.ShowTransactionStmt;
import com.starrocks.analysis.SlotRef;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ShowTransactionStmtAnalyzer {

    public static void analyze(ShowTransactionStmt statement, ConnectContext context) {
        new ShowTransactionStmtAnalyzerVisitor().visit(statement, context);
    }
    
    static class ShowTransactionStmtAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        
        private long txnId;
        
        private static final Logger LOG = LogManager.getLogger(ShowTransactionStmtAnalyzerVisitor.class);
        
        public ShowTransactionStmtAnalyzerVisitor() {
            this.txnId = 0;
        }
        
        public void analyze(ShowTransactionStmt statement, ConnectContext context) {
            visit(statement, context);
        }

        @Override
        public Void visitShowTransactionStmt(ShowTransactionStmt statement, ConnectContext context) {
            analyzeDbName(statement, context);
            analyzeWhereClause(statement, context);
            return null;
        }
        
        private void analyzeDbName(ShowTransactionStmt statement, ConnectContext context) {
            String dbName = statement.getDbName();
            if (Strings.isNullOrEmpty(dbName)) {
                dbName = context.getDatabase();
                if (Strings.isNullOrEmpty(dbName)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
                }
            }
            statement.setDbName(dbName);
        }
        
        private void analyzeWhereClause(ShowTransactionStmt statement, ConnectContext context) {
            Expr whereClause = statement.getWhereClause();
            analyzeSubPredicate(whereClause);
            statement.setTxnId(txnId);
        }
        
        private void analyzeSubPredicate(Expr subExpr) {
            if (subExpr == null) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "should supply condition like: ID = $transaction_id");
            }

            boolean valid = false;
            boolean hasTxnId = false;
            do {
                if (subExpr == null) {
                    valid = false;
                    break;
                }

                if (subExpr instanceof BinaryPredicate) {
                    BinaryPredicate binaryPredicate = (BinaryPredicate) subExpr;
                    if (binaryPredicate.getOp() != BinaryPredicate.Operator.EQ) {
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
                if (leftKey.equalsIgnoreCase("id")) {
                    hasTxnId = true;
                } else {
                    valid = false;
                    break;
                }

                if (hasTxnId) {
                    if (!(subExpr.getChild(1) instanceof IntLiteral)) {
                        LOG.warn("id is not IntLiteral. value: {}", subExpr.toSql());
                        valid = false;
                        break;
                    }
                    txnId = ((IntLiteral) subExpr.getChild(1)).getLongValue();
                }

                valid = true;
            } while (false);
            
            if (!valid) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                        "Where clause should looks like: ID = $transaction_id");
            }
        }
    }
}
