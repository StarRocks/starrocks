// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.pipe.AlterPipeStmt;
import com.starrocks.sql.ast.pipe.CreatePipeStmt;
import com.starrocks.sql.ast.pipe.DropPipeStmt;
import com.starrocks.sql.ast.pipe.PipeName;
import com.starrocks.sql.ast.pipe.ShowPipeStmt;
import org.apache.ivy.util.StringUtils;

public class PipeAnalyzer {

    private static void analyzePipeName(PipeName pipeName, ConnectContext context) {
        if (StringUtils.isNullOrEmpty(pipeName.getDbName())) {
            if (StringUtils.isNullOrEmpty(context.getDatabase())) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
            }
            pipeName.setDbName(context.getDatabase());
        }
        if (Strings.isNullOrEmpty(pipeName.getPipeName())) {
            throw new SemanticException("empty pipe name");
        }
        FeNameFormat.checkCommonName("db", pipeName.getDbName());
        FeNameFormat.checkCommonName("pipe", pipeName.getPipeName());
    }

    // only analyze create pipe right now
    public static void analyze(CreatePipeStmt stmt, ConnectContext context) {
        analyzePipeName(stmt.getPipeName(), context);

        // Analyze Insert Statement
        // 1. Source table must be s3 table function
        InsertStmt insertStmt = stmt.getInsertStmt();
        InsertAnalyzer.analyze(insertStmt, context);
        if (!Strings.isNullOrEmpty(insertStmt.getLabel())) {
            throw new SemanticException("INSERT INTO cannot with label");
        }
        if (insertStmt.isOverwrite()) {
            throw new SemanticException("INSERT INTO cannot be OVERWRITE");
        }

        QueryStatement queryStatement = insertStmt.getQueryStatement();
        if (!(queryStatement.getQueryRelation() instanceof SelectRelation)) {
            throw new SemanticException("INSERT INTO can only with SELECT");
        }
        /*
        SelectRelation selectRelation = (SelectRelation) queryStatement.getQueryRelation();
        if (!(selectRelation.getRelation() instanceof TableFunctionRelation)) {
            throw new SemanticException("SELECT must FROM table function");
        }
        // FIXME: change to the real relation
        TableFunctionRelation tableFunctionRelation = (TableFunctionRelation) selectRelation.getRelation();
        if (!tableFunctionRelation.getFunctionName().getFunction().equalsIgnoreCase("s3")) {
            throw new SemanticException("Only support S3 table function");
        }
        stmt.setTableFunctionRelation(tableFunctionRelation);
        stmt.setTargetTable(insertStmt.getTargetTable());
        */
    }

    public static void analyze(DropPipeStmt stmt, ConnectContext context) {
        analyzePipeName(stmt.getPipeName(), context);
    }

    public static void analyze(ShowPipeStmt stmt, ConnectContext context) {
        if (StringUtils.isNullOrEmpty(stmt.getDbName()) &&
                StringUtils.isNullOrEmpty(context.getDatabase())) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
        }
    }

    public static void analyze(AlterPipeStmt stmt, ConnectContext context) {
        analyzePipeName(stmt.getPipeName(), context);
    }
}
