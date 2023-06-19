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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.load.pipe.EmptyPipeSource;
import com.starrocks.load.pipe.FilePipeSource;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.VariableMgr;
import com.starrocks.sql.ast.FileTableFunctionRelation;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.pipe.AlterPipeStmt;
import com.starrocks.sql.ast.pipe.CreatePipeStmt;
import com.starrocks.sql.ast.pipe.DescPipeStmt;
import com.starrocks.sql.ast.pipe.DropPipeStmt;
import com.starrocks.sql.ast.pipe.PipeName;
import com.starrocks.sql.ast.pipe.ShowPipeStmt;
import org.apache.commons.collections4.MapUtils;

import java.util.Map;

public class PipeAnalyzer {

    public static final String PROPERTY_AUTO_INGEST = "auto_ingest";
    public static final String POLL_INTERVAL = "poll_interval";

    private static final ImmutableSet<String> SUPPORTED_PROPERTIES =
            new ImmutableSortedSet.Builder<String>(String.CASE_INSENSITIVE_ORDER)
                    .add(PROPERTY_AUTO_INGEST)
                    .add(POLL_INTERVAL)
                    .build();

    private static void analyzePipeName(PipeName pipeName, ConnectContext context) {
        if (Strings.isNullOrEmpty(pipeName.getDbName())) {
            if (Strings.isNullOrEmpty(context.getDatabase())) {
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

    private static void analyzeProperties(Map<String, String> properties) {
        if (MapUtils.isEmpty(properties)) {
            return;
        }
        for (String propertyName : properties.keySet()) {
            if (!SUPPORTED_PROPERTIES.contains(propertyName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_UNKNOWN_PROPERTY, propertyName);
            }
            switch (propertyName) {
                case POLL_INTERVAL: {
                    int value = Integer.parseInt(properties.get(propertyName));
                    if (value < 1 || value > 1024) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_INVALID_PARAMETER,
                                POLL_INTERVAL + " should in [1, 1024]");
                    }
                    break;
                }
                default:

            }
        }
    }

    public static void analyze(CreatePipeStmt stmt, ConnectContext context) {
        analyzePipeName(stmt.getPipeName(), context);
        analyzeProperties(stmt.getProperties());
        Map<String, String> properties = stmt.getProperties();

        InsertStmt insertStmt = stmt.getInsertStmt();
        stmt.setTargetTable(insertStmt.getTableName());
        String insertSql = stmt.getOrigStmt().originStmt.substring(stmt.getInsertSqlStartIndex());
        stmt.setInsertSql(insertSql);
        if (!context.getSessionVariable().isEnablePipeValidate()) {
            stmt.setDataSource(new EmptyPipeSource());
            return;
        }
        InsertAnalyzer.analyze(insertStmt, context);

        // Must be the form: insert into <target_table> select <projection> from <source_table> [where_clause]
        if (!Strings.isNullOrEmpty(insertStmt.getLabel())) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_PIPE_STATEMENT, "INSERT INTO cannot with label");
        }
        if (insertStmt.isOverwrite()) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_PIPE_STATEMENT, "INSERT INTO cannot be OVERWRITE");
        }
        QueryStatement queryStatement = insertStmt.getQueryStatement();
        if (!(queryStatement.getQueryRelation() instanceof SelectRelation)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_PIPE_STATEMENT, "must be select statement");
        }
        SelectRelation selectRelation = (SelectRelation) queryStatement.getQueryRelation();
        if (selectRelation.hasAggregation() || selectRelation.hasOrderByClause() || selectRelation.hasLimit()) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_PIPE_STATEMENT, "must be a vanilla select statement");
        }
        if (!(selectRelation.getRelation() instanceof FileTableFunctionRelation)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_PIPE_STATEMENT, "only support FileTableFunction");
        }
        FileTableFunctionRelation tableFunctionRelation = (FileTableFunctionRelation) selectRelation.getRelation();
        Table rawTable = tableFunctionRelation.getTable();
        if (rawTable == null || rawTable.getType() != Table.TableType.TABLE_FUNCTION) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_BAD_PIPE_STATEMENT, "only support FileTableFunction");
        }

        TableFunctionTable sourceTable = (TableFunctionTable) rawTable;
        FilePipeSource source =
                new FilePipeSource(sourceTable.getPath(), sourceTable.getFormat(), sourceTable.getProperties());
        if (properties.containsKey(PROPERTY_AUTO_INGEST)) {
            boolean value = VariableMgr.parseBooleanVariable(properties.get(PROPERTY_AUTO_INGEST));
            source.setAutoIngest(value);
        }
        stmt.setDataSource(source);
    }

    public static void analyze(DropPipeStmt stmt, ConnectContext context) {
        analyzePipeName(stmt.getPipeName(), context);
    }

    public static void analyze(ShowPipeStmt stmt, ConnectContext context) {
        if (Strings.isNullOrEmpty(stmt.getDbName())) {
            if (Strings.isNullOrEmpty(context.getDatabase())) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_NO_DB_ERROR);
            }
            stmt.setDbName(context.getDatabase());
        }
    }

    public static void analyze(AlterPipeStmt stmt, ConnectContext context) {
        analyzePipeName(stmt.getPipeName(), context);
    }

    public static void analyze(DescPipeStmt stmt, ConnectContext context) {
        analyzePipeName(stmt.getName(), context);
    }
}
