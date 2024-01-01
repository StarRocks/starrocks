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
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.util.OrderByPair;
import com.starrocks.common.util.ParseUtil;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.load.pipe.FilePipeSource;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.VariableMgr;
import com.starrocks.sql.ast.FileTableFunctionRelation;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.pipe.AlterPipeSetProperty;
import com.starrocks.sql.ast.pipe.AlterPipeStmt;
import com.starrocks.sql.ast.pipe.CreatePipeStmt;
import com.starrocks.sql.ast.pipe.DescPipeStmt;
import com.starrocks.sql.ast.pipe.DropPipeStmt;
import com.starrocks.sql.ast.pipe.PipeName;
import com.starrocks.sql.ast.pipe.ShowPipeStmt;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PipeAnalyzer {

    public static final String TASK_VARIABLES_PREFIX = "TASK.";
    public static final String PROPERTY_AUTO_INGEST = "auto_ingest";
    public static final String PROPERTY_POLL_INTERVAL = "poll_interval";
    public static final String PROPERTY_BATCH_SIZE = "batch_size";
    public static final String PROPERTY_BATCH_FILES = "batch_files";

    public static final ImmutableSet<String> SUPPORTED_PROPERTIES =
            new ImmutableSortedSet.Builder<String>(String.CASE_INSENSITIVE_ORDER)
                    .add(PROPERTY_AUTO_INGEST)
                    .add(PROPERTY_POLL_INTERVAL)
                    .add(PROPERTY_BATCH_SIZE)
                    .add(PROPERTY_BATCH_FILES)
                    .add(PropertyAnalyzer.PROPERTIES_WAREHOUSE)
                    .build();

    public static void analyzePipeName(PipeName pipeName, ConnectContext context) {
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
            if (propertyName.toUpperCase().startsWith(TASK_VARIABLES_PREFIX)) {
                // Task execution variable
                String taskVariableName = StringUtils.removeStartIgnoreCase(propertyName, TASK_VARIABLES_PREFIX);
                if (!VariableMgr.containsVariable(taskVariableName)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_UNKNOWN_PROPERTY, propertyName);
                }
                continue;
            }
            if (!SUPPORTED_PROPERTIES.contains(propertyName)) {
                ErrorReport.reportSemanticException(ErrorCode.ERR_UNKNOWN_PROPERTY, propertyName);
            }
            String valueStr = properties.get(propertyName);
            switch (propertyName.toLowerCase()) {
                case PROPERTY_POLL_INTERVAL: {
                    int value = -1;
                    try {
                        value = Integer.parseInt(valueStr);
                    } catch (NumberFormatException ignored) {
                    }
                    if (value < 1 || value > 1024) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_INVALID_PARAMETER,
                                PROPERTY_POLL_INTERVAL + " should in [1, 1024]");
                    }
                    break;
                }
                case PROPERTY_BATCH_SIZE: {
                    long value = -1;
                    try {
                        value = ParseUtil.parseDataVolumeStr(valueStr);
                    } catch (Exception ignored) {
                    }
                    if (value < 0) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_INVALID_PARAMETER,
                                PROPERTY_BATCH_SIZE + " should be greater than 0");
                    }
                    break;
                }
                case PROPERTY_BATCH_FILES: {
                    int value = -1;
                    try {
                        value = Integer.parseInt(valueStr);
                    } catch (NumberFormatException ignored) {
                    }
                    if (value < 1 || value > 1024) {
                        ErrorReport.reportSemanticException(ErrorCode.ERR_INVALID_PARAMETER,
                                PROPERTY_BATCH_FILES + " should in [1, 1024]");
                    }
                    break;
                }
                case PROPERTY_AUTO_INGEST: {
                    VariableMgr.parseBooleanVariable(valueStr);
                    break;
                }
                case PropertyAnalyzer.PROPERTIES_WAREHOUSE: {
                    analyzeWarehouseProperty(valueStr);
                    break;
                }
                default: {
                    break;
                }
            }
        }
    }

    public static void analyzeWarehouseProperty(String warehouseName) {
        ErrorReport.reportSemanticException(ErrorCode.ERR_INVALID_PARAMETER, warehouseName);
    }

    public static void analyze(CreatePipeStmt stmt, ConnectContext context) {
        analyzePipeName(stmt.getPipeName(), context);
        analyzeProperties(stmt.getProperties());
        Map<String, String> properties = stmt.getProperties();

        InsertStmt insertStmt = stmt.getInsertStmt();
        stmt.setTargetTable(insertStmt.getTableName());
        String insertSql = stmt.getOrigStmt().originStmt.substring(stmt.getInsertSqlStartIndex());
        stmt.setInsertSql(insertSql);
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
        stmt.setDataSource(
                new FilePipeSource(sourceTable.getPath(), sourceTable.getFormat(), sourceTable.getProperties()));
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

        // Analyze order by
        if (CollectionUtils.isNotEmpty(stmt.getOrderBy())) {
            List<OrderByPair> orderByPairs = new ArrayList<>();
            for (OrderByElement element : stmt.getOrderBy()) {
                if (!(element.getExpr() instanceof SlotRef)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_COMMON_ERROR,
                            "only support order by specific column");
                }
                SlotRef slot = (SlotRef) element.getExpr();
                int index = ShowPipeStmt.findSlotIndex(slot.getColumnName());
                orderByPairs.add(new OrderByPair(index, !element.getIsAsc()));
            }
            stmt.setOrderByPairs(orderByPairs);
        }
    }

    public static void analyze(AlterPipeStmt stmt, ConnectContext context) {
        analyzePipeName(stmt.getPipeName(), context);
        if (stmt.getAlterPipeClause() instanceof AlterPipeSetProperty) {
            AlterPipeSetProperty setProperty = (AlterPipeSetProperty) stmt.getAlterPipeClause();
            analyzeProperties(setProperty.getProperties());
        }
    }

    public static void analyze(DescPipeStmt stmt, ConnectContext context) {
        analyzePipeName(stmt.getName(), context);
    }
}
