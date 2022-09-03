// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.google.common.base.Strings;
import com.starrocks.analysis.BinaryPredicate.Operator;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.common.proc.TransProcDir;
import com.starrocks.qe.ShowResultSetMetaData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// syntax:
//      SHOW TRANSACTION  WHERE id=123
public class ShowTransactionStmt extends ShowStmt {
    private static final Logger LOG = LogManager.getLogger(ShowTransactionStmt.class);

    private String dbName;
    private Expr whereClause;
    private long txnId;

    public ShowTransactionStmt(String dbName, Expr whereClause) {
        this.dbName = dbName;
        this.whereClause = whereClause;
    }

    public String getDbName() {
        return dbName;
    }

    public long getTxnId() {
        return txnId;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);

        if (Strings.isNullOrEmpty(dbName)) {
            dbName = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }

        if (whereClause == null) {
            throw new AnalysisException("Missing transaction id");
        }

        analyzeWhereClause();
    }

    private void analyzeWhereClause() throws AnalysisException {
        if (whereClause == null) {
            return;
        }

        boolean valid = true;
        CHECK:
        {
            if (whereClause instanceof BinaryPredicate) {
                BinaryPredicate binaryPredicate = (BinaryPredicate) whereClause;
                if (binaryPredicate.getOp() != Operator.EQ) {
                    valid = false;
                    break CHECK;
                }
            } else {
                valid = false;
                break CHECK;
            }

            // left child
            if (!(whereClause.getChild(0) instanceof SlotRef)) {
                valid = false;
                break CHECK;
            }
            String leftKey = ((SlotRef) whereClause.getChild(0)).getColumnName();
            if (!leftKey.equalsIgnoreCase("id")) {
                valid = false;
                break CHECK;
            }

            // right child
            if (!(whereClause.getChild(1) instanceof IntLiteral)) {
                valid = false;
                break CHECK;
            }

            txnId = ((IntLiteral) whereClause.getChild(1)).getLongValue();
        }

        if (!valid) {
            throw new AnalysisException("Where clause should looks like: id = 123");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW TRANSACTION ");
        if (!Strings.isNullOrEmpty(dbName)) {
            sb.append("FROM `").append(dbName).append("`");
        }

        sb.append(" WHERE ").append(whereClause.toSql());
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TransProcDir.TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
