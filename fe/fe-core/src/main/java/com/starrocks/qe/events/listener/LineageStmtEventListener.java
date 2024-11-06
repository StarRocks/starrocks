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

package com.starrocks.qe.events.listener;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.starrocks.alter.AlterOpType;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.FunctionParams;
import com.starrocks.analysis.LabelName;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.qe.events.StmtEvent;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.ast.AddColumnClause;
import com.starrocks.sql.ast.AddColumnsClause;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.AddRollupClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterDatabaseQuotaStmt;
import com.starrocks.sql.ast.AlterDatabaseRenameStatement;
import com.starrocks.sql.ast.AlterMaterializedViewStatusClause;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AlterResourceStmt;
import com.starrocks.sql.ast.AlterTableClause;
import com.starrocks.sql.ast.AlterTableCommentClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.AlterViewClause;
import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.AsyncRefreshSchemeDesc;
import com.starrocks.sql.ast.CreateCatalogStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateFunctionStmt;
import com.starrocks.sql.ast.CreateIndexClause;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.ast.CreateResourceStmt;
import com.starrocks.sql.ast.CreateTableAsSelectStmt;
import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DataDescription;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.DropCatalogStmt;
import com.starrocks.sql.ast.DropColumnClause;
import com.starrocks.sql.ast.DropDbStmt;
import com.starrocks.sql.ast.DropFunctionStmt;
import com.starrocks.sql.ast.DropIndexClause;
import com.starrocks.sql.ast.DropMaterializedViewStmt;
import com.starrocks.sql.ast.DropPartitionClause;
import com.starrocks.sql.ast.DropResourceStmt;
import com.starrocks.sql.ast.DropRollupClause;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.ExceptRelation;
import com.starrocks.sql.ast.ExpressionPartitionDesc;
import com.starrocks.sql.ast.FileTableFunctionRelation;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.IntersectRelation;
import com.starrocks.sql.ast.IntervalLiteral;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.KeysDesc;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.LoadStmt;
import com.starrocks.sql.ast.MVColumnItem;
import com.starrocks.sql.ast.ModifyPartitionClause;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.sql.ast.MultiItemListPartitionDesc;
import com.starrocks.sql.ast.NormalizedTableFunctionRelation;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.PartitionRenameClause;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.RangePartitionDesc;
import com.starrocks.sql.ast.RefreshSchemeClause;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.SingleItemListPartitionDesc;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SwapTableClause;
import com.starrocks.sql.ast.TableFunctionRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.TableRenameClause;
import com.starrocks.sql.ast.UnionRelation;
import com.starrocks.sql.ast.UnitIdentifier;
import com.starrocks.sql.common.MetaUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class LineageStmtEventListener implements StmtEventListener {
    private static final Logger LINEAGE_LOG = LogManager.getLogger("lineage");
    private static final Logger LOG = LogManager.getLogger(LineageStmtEventListener.class);
    private static final String VERSION = "0.1.0";
    private static final Gson GSON = new Gson();
    private final List<EventProcessor> processors = new ArrayList<>();

    public LineageStmtEventListener() {
        processors.add(new OutputProcessor());
        processors.add(new ChangeLogProcessor());
    }

    @Override
    public void onEvent(StmtEvent event) {
        if (event == null || event.getStatementBase() == null) {
            return;
        }
        for (EventProcessor processor : processors) {
            processor.processEvent(event);
        }
    }

    private abstract static class EventProcessor {

        public abstract ActionType getActionType();

        public void processEvent(StmtEvent event) {
            try {
                long start = System.currentTimeMillis();
                StatementBase statementBase = event.getStatementBase();
                Lineage lineage = doProcess(statementBase);
                long end = System.currentTimeMillis();
                if (lineage != null) {
                    lineage.setState(State.SUCCESS);
                    lineage.setCostTime(end - start);
                    printLog(lineage, event);
                }
            } catch (Exception e) {
                LOG.warn("failed to process {} event", getActionType(), e);
                try {
                    Lineage lineage = new Lineage();
                    lineage.setState(State.ERROR);
                    Target target = new Target();
                    target.putExtra("error", e.getMessage());
                    target.putExtra("stack", summaryExceptionString(e));
                    target.putExtra("version", VERSION);
                    lineage.setTarget(target);
                    printLog(lineage, event);
                } catch (Exception ex) {
                    LOG.warn("failed to parse exception", ex);
                }
            }
        }

        /**
         * only for debug exception
         */
        private String summaryExceptionString(Exception e) {
            String stack = "";
            StackTraceElement[] trace = e.getStackTrace();
            int count = 0;
            for (StackTraceElement traceElement : trace) {
                stack = stack + traceElement + "\n";
                if (++count > 1) {
                    break;
                }
            }
            return stack;
        }

        private void printLog(Lineage lineage, StmtEvent event) {
            try {
                lineage.setQueryText(event.getStatementBase().getOrigStmt().originStmt);
                lineage.setActionType(getActionType());
                lineage.setQueryId(event.getQueryId());
                lineage.setTimestamp(event.getTimestamp());
                lineage.setClientIp(event.getClientIp());
                lineage.setUser(event.getUser());
                LINEAGE_LOG.warn(GSON.toJson(lineage));
            } catch (Exception e) {
                LOG.warn("failed to print {} event", getActionType(), e);
            }
        }

        abstract Lineage doProcess(StatementBase statementBase);
    }

    /**
     * output，including ddl\dml, when an object is produced
     */
    static final class OutputProcessor extends EventProcessor {
        @Override
        public ActionType getActionType() {
            return ActionType.Lineage;
        }

        @Override
        public Lineage doProcess(StatementBase statementBase) {
            return parseLineage(statementBase);
        }

        public Lineage parseLineage(StatementBase stmt) {
            if (stmt instanceof CreateTableStmt) {
                return getCreateTableLineage((CreateTableStmt) stmt);
            } else if (stmt instanceof CreateTableAsSelectStmt) {
                return getCreateTableAsSelectLineage((CreateTableAsSelectStmt) stmt);
            } else if (stmt instanceof CreateTableLikeStmt) {
                return getCreateTableLikeLineage((CreateTableLikeStmt) stmt);
            } else if (stmt instanceof CreateViewStmt) {
                return getCreateViewLineage((CreateViewStmt) stmt);
            } else if (stmt instanceof CreateMaterializedViewStmt) {
                return getCreateMaterializedViewLineage((CreateMaterializedViewStmt) stmt);
            } else if (stmt instanceof CreateMaterializedViewStatement) {
                return getCreateMaterializedViewLineage((CreateMaterializedViewStatement) stmt);
            } else if (stmt instanceof InsertStmt) {
                return getInsertLineage((InsertStmt) stmt);
            } else if (stmt instanceof AlterTableStmt) {
                return getAlterTableLineage((AlterTableStmt) stmt);
            } else if (stmt instanceof LoadStmt) {
                return getDataLoadingLineage((LoadStmt) stmt);
            } else {
                return null;
            }
        }

        private Lineage getCreateTableLineage(CreateTableStmt stmt) {
            Lineage lineage = new Lineage();
            lineage.setAction(Action.CreateTable);
            TableName tableName = stmt.getDbTbl();
            ColumnLineage columnLineage = new ColumnLineage()
                    .destCatalog(tableName.getCatalog())
                    .destDatabase(tableName.getDb())
                    .destTable(tableName.getTbl());
            Map<String, Set<String>> columnMap = toColumnMap(stmt.getColumns());
            columnLineage.setColumnMap(columnMap);
            lineage.setColumnLineages(Arrays.asList(columnLineage));
            Target target = new Target(tableName.getCatalog(), tableName.getDb(), tableName.getTbl());
            lineage.setTarget(target);
            parseCreateTableExtra(stmt, target);
            return lineage;
        }

        private Map<String, Set<String>> toColumnMap(List<Column> columns) {
            Map<String, Set<String>> columnMap = new LinkedHashMap<>();
            for (Column c : columns) {
                Set<String> values = columnMap.get(c.getName());
                if (values != null) {
                    values.add(c.getName());
                } else {
                    values = new LinkedHashSet<>();
                    values.add(c.getName());
                    columnMap.put(c.getName(), values);
                }
            }
            return columnMap;
        }

        private Lineage getCreateTableAsSelectLineage(CreateTableAsSelectStmt stmt) {
            Map<ColumnInfo, List<ColumnInfo>> source = new TableCollector().visit(stmt.getQueryStatement());
            Map<String, TableName> target = parseCreateTable(stmt.getCreateTableStmt());
            Lineage lineage = createLineage(source, target);
            if (lineage == null) {
                return null;
            }
            lineage.setAction(Action.CreateTableAsSelect);
            return lineage;
        }

        private Lineage getCreateTableLikeLineage(CreateTableLikeStmt stmt) {
            Map<String, TableName> target = parseCreateTable(stmt.getCreateTableStmt());
            Lineage lineage = createLineage(stmt.getExistedDbTbl(), target);
            if (lineage == null) {
                return null;
            }
            lineage.setAction(Action.CreateTableLike);
            return lineage;
        }

        private Lineage getCreateViewLineage(CreateViewStmt stmt) {
            Map<ColumnInfo, List<ColumnInfo>> source = new TableCollector().visit(stmt.getQueryStatement());
            Map<String, TableName> target = parseCreateView(stmt);
            Lineage lineage = createLineage(source, target);
            if (lineage == null) {
                return null;
            }
            lineage.setAction(Action.CreateView);
            return lineage;
        }

        private Lineage getCreateMaterializedViewLineage(CreateMaterializedViewStmt stmt) {
            Map<ColumnInfo, List<ColumnInfo>> source = new TableCollector().visit(stmt.getQueryStatement());
            Map<String, TableName> target = parseCreateMaterializedView(stmt);
            Lineage lineage = createLineage(source, target);
            if (lineage == null) {
                return null;
            }
            lineage.setAction(Action.CreateMaterializedView);
            return lineage;
        }

        private Lineage getCreateMaterializedViewLineage(CreateMaterializedViewStatement stmt) {
            Map<ColumnInfo, List<ColumnInfo>> source = new TableCollector().visit(stmt.getQueryStatement());
            Map<String, TableName> target = parseCreateMaterializedView(stmt);
            Lineage lineage = createLineage(source, target);
            if (lineage == null) {
                return null;
            }
            lineage.setAction(Action.CreateMaterializedView);
            return lineage;
        }

        private Lineage getInsertLineage(InsertStmt stmt) {
            Map<ColumnInfo, List<ColumnInfo>> source = new TableCollector().visit(stmt.getQueryStatement());
            if (source == null) {
                //insert value
                Lineage lineage = new Lineage();
                lineage.setAction(Action.Insert);
                ColumnLineage columnLineage = getColumnLineage(stmt);
                lineage.setColumnLineages(Arrays.asList(columnLineage));
                return lineage;
            } else {
                //insert select
                List<ColumnInfo> target = parseInsert(stmt);
                Lineage lineage = insertLineage(source, target);
                if (lineage == null) {
                    return null;
                }
                lineage.setAction(Action.Insert);
                return lineage;
            }
        }

        private ColumnLineage getColumnLineage(InsertStmt stmt) {
            TableName tableName = stmt.getTableName();
            ColumnLineage columnLineage = new ColumnLineage()
                    .destCatalog(tableName.getCatalog())
                    .destDatabase(tableName.getDb())
                    .destTable(tableName.getTbl());
            Map<String, Set<String>> columnMap = new LinkedHashMap<>();
            if (stmt.getTargetColumnNames() != null) {
                for (String c : stmt.getTargetColumnNames()) {
                    Set<String> values = new LinkedHashSet<>();
                    values.add(c);
                    columnMap.put(c, values);
                }
            } else {
                Table targetTable = stmt.getTargetTable();
                if (targetTable != null && targetTable.getFullSchema() != null) {
                    columnMap = toColumnMap(targetTable.getFullSchema());
                }
            }
            columnLineage.setColumnMap(columnMap);
            return columnLineage;
        }

        private Lineage getAlterTableLineage(AlterTableStmt stmt) {
            Lineage lineage = new Lineage();
            Target target = parsePartition(stmt);
            if (target == null) {
                return null;
            }
            lineage.setTarget(target);
            lineage.setAction(Action.AddPartition);
            return lineage;
        }

        private Lineage getDataLoadingLineage(LoadStmt stmt) {
            LabelName labelName = stmt.getLabel();
            Target target = new Target();
            target.setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
            target.setDatabase(labelName.getDbName());
            target.putExtra("labelName", labelName.getLabelName());
            List<DataDescription> dataDescriptions = stmt.getDataDescriptions();
            if (dataDescriptions != null && !dataDescriptions.isEmpty()) {
                Map<String, Object> dataDescription = new LinkedHashMap<>();
                for (DataDescription desc : dataDescriptions) {
                    dataDescription.put("tableName", desc.getTableName());
                    dataDescription.put("filePaths", desc.getFilePaths());
                    dataDescription.put("fileFormat", desc.getFileFormat());
                    dataDescription.put("fileFieldNames", desc.getFileFieldNames());
                    dataDescription.put("columnsFromPath", desc.getColumnsFromPath());
                    target.addExtraList("dataDescriptions", dataDescription);
                }
            }

            if (stmt.getBrokerDesc() != null) {
                target.putExtra("brokerDesc", brokerDesc(stmt.getBrokerDesc()));
            }
            Lineage lineage = new Lineage();
            lineage.setTarget(target);
            lineage.setAction(Action.DataLoading);
            return lineage;
        }

        private Map<String, Object> brokerDesc(BrokerDesc brokerDesc) {
            if (brokerDesc != null) {
                Map<String, Object> brokerDescMap = new LinkedHashMap<>();
                brokerDescMap.put("brokerName", brokerDesc.getName());
                brokerDescMap.put("properties", filterProperties(brokerDesc.getProperties()));
                return brokerDescMap;
            }
            return null;
        }

        private List<ColumnInfo> parseInsert(InsertStmt insertStmt) {
            List<ColumnInfo> targetColumns = new ArrayList<>();
            TableName target = insertStmt.getTableName();
            if (insertStmt.getTargetColumnNames() == null) {
                List<Column> columns = insertStmt.getTargetTable().getFullSchema();
                for (Column c : columns) {
                    ColumnInfo columnInfo = new ColumnInfo(c.getName(), target.getTbl(), target.getDb(), target.getCatalog());
                    targetColumns.add(columnInfo);
                }
            } else {
                for (String c : insertStmt.getTargetColumnNames()) {
                    ColumnInfo columnInfo = new ColumnInfo(c, target.getTbl(), target.getDb(), target.getCatalog());
                    targetColumns.add(columnInfo);
                }
            }

            return targetColumns;
        }

        private Lineage insertLineage(Map<ColumnInfo, List<ColumnInfo>> sourceMap, List<ColumnInfo> target) {
            if (sourceMap == null || target == null) {
                return null;
            }
            Lineage lineage = new Lineage();
            Map<String, ColumnLineage> keyColumnLineageMap = new LinkedHashMap<>();
            Iterator<Map.Entry<ColumnInfo, List<ColumnInfo>>> sourceIterator = sourceMap.entrySet().iterator();
            Iterator<ColumnInfo> targetIterator = target.iterator();
            while (sourceIterator.hasNext()) {
                buildColumnInfo(keyColumnLineageMap, sourceIterator.next().getValue(), targetIterator.next());
            }

            lineage.setColumnLineages(new ArrayList<>(keyColumnLineageMap.values()));
            return lineage;
        }

        private Lineage createLineage(Map<ColumnInfo, List<ColumnInfo>> sourceMap, Map<String, TableName> targetMap) {
            Map<String, ColumnLineage> keyColumnLineageMap = new LinkedHashMap<>();
            Iterator<Map.Entry<ColumnInfo, List<ColumnInfo>>> sourceIterator = sourceMap.entrySet().iterator();
            Iterator<Map.Entry<String, TableName>> targetIterator = targetMap.entrySet().iterator();
            while (sourceIterator.hasNext()) {
                Map.Entry<String, TableName> entry = targetIterator.next();
                buildColumnInfo(keyColumnLineageMap, sourceIterator.next().getValue(), entry.getValue(), entry.getKey());
            }
            Lineage lineage = new Lineage();
            lineage.setColumnLineages(new ArrayList<>(keyColumnLineageMap.values()));
            return lineage;
        }

        private Lineage createLineage(TableName source, Map<String, TableName> targetMap) {
            Lineage lineage = new Lineage();
            List<ColumnLineage> columnLineages = new ArrayList<>();
            TableName tableName = targetMap.entrySet().iterator().next().getValue();
            ColumnLineage columnLineage = new ColumnLineage(tableName.getCatalog(), source.getCatalog(),
                    tableName.getDb(), source.getDb(), tableName.getTbl(), source.getTbl());
            Map<String, Set<String>> columnMap = new LinkedHashMap<>();
            for (Map.Entry<String, TableName> targetEntry : targetMap.entrySet()) {
                Set<String> values = new LinkedHashSet<>();
                values.add(targetEntry.getKey());
                columnMap.put(targetEntry.getKey(), values);
            }
            columnLineage.setColumnMap(columnMap);
            columnLineages.add(columnLineage);
            lineage.setColumnLineages(columnLineages);
            return lineage;
        }

        private void buildColumnInfo(Map<String, ColumnLineage> keyColumnLineageMap,
                List<ColumnInfo> sources, ColumnInfo target) {
            for (ColumnInfo info : sources) {
                String key = String.format("%s.%s.%s.%s.%s.%s",
                        target.getCatalog(), info.getCatalog(),
                        target.getDbName(), info.getDbName(),
                        target.getTableName(), info.getTableName());
                ColumnLineage lineageInfo = keyColumnLineageMap.get(key);
                if (lineageInfo != null) {
                    Set<String> values = lineageInfo.getColumnMap().get(target.getColName());
                    if (values == null) {
                        values = new LinkedHashSet<>();
                        lineageInfo.getColumnMap().put(target.getColName(), values);
                    }
                    values.add(info.getColName());
                } else {
                    String destCatalog = target.getCatalog();
                    //materialized view only support starrocks native catalog and maybe there's no catalog
                    //set default
                    if (destCatalog == null) {
                        destCatalog = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
                    }
                    lineageInfo = new ColumnLineage(destCatalog, info.getCatalog(),
                            target.getDbName(), info.getDbName(),
                            target.getTableName(), info.getTableName());
                    Set<String> values = lineageInfo.getColumnMap().get(target.getColName());
                    if (values == null) {
                        values = new LinkedHashSet<>();
                        lineageInfo.getColumnMap().put(target.getColName(), values);
                    }
                    values.add(info.getColName());
                    keyColumnLineageMap.put(key, lineageInfo);
                }
            }
        }

        private void buildColumnInfo(Map<String, ColumnLineage> keyColumnLineageMap,
                List<ColumnInfo> sources, TableName target, String targetCol) {
            for (ColumnInfo info : sources) {
                String key = String.format("%s.%s.%s.%s.%s.%s",
                        target.getCatalog(), info.getCatalog(),
                        target.getDb(), info.getDbName(),
                        target.getTbl(), info.getTableName());
                ColumnLineage lineageInfo = keyColumnLineageMap.get(key);
                if (lineageInfo != null) {
                    Set<String> values = lineageInfo.getColumnMap().get(targetCol);
                    if (values == null) {
                        values = new LinkedHashSet<>();
                        lineageInfo.getColumnMap().put(targetCol, values);
                    }
                    if (info.getResource() != null) {
                        lineageInfo.setResource(info.getResource());
                    }
                    values.add(info.getColName());
                } else {
                    String destCatalog = target.getCatalog();
                    //materialized view only support starrocks native catalog and maybe there's no catalog
                    //set default
                    if (destCatalog == null) {
                        destCatalog = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
                    }
                    lineageInfo = new ColumnLineage(destCatalog, info.getCatalog(),
                            target.getDb(), info.getDbName(),
                            target.getTbl(), info.getTableName());
                    Set<String> values = lineageInfo.getColumnMap().get(targetCol);
                    if (values == null) {
                        values = new LinkedHashSet<>();
                        lineageInfo.getColumnMap().put(targetCol, values);
                    }
                    values.add(info.getColName());
                    if (info.getResource() != null) {
                        lineageInfo.setResource(info.getResource());
                    }
                    keyColumnLineageMap.put(key, lineageInfo);
                }
            }
        }

        private Map<String, TableName> parseCreateTable(CreateTableStmt createTableStmt) {
            Map<String, TableName> targetMap = new LinkedHashMap<>();
            TableName target = createTableStmt.getDbTbl();
            List<Column> columns = createTableStmt.getColumns();
            for (Column c : columns) {
                targetMap.put(c.getName(), target);
            }
            return targetMap;
        }

        private Map<String, TableName> parseCreateView(CreateViewStmt createViewStmt) {
            Map<String, TableName> targetMap = new LinkedHashMap<>();
            TableName target = createViewStmt.getTableName();
            List<Column> columns = createViewStmt.getColumns();
            for (Column c : columns) {
                targetMap.put(c.getName(), target);
            }
            return targetMap;
        }

        private Map<String, TableName> parseCreateMaterializedView(CreateMaterializedViewStmt stmt) {
            Map<String, TableName> targetMap = new LinkedHashMap<>();
            TableName target = new TableName();
            target.setDb(stmt.getDBName());
            target.setTbl(stmt.getMVName());
            List<MVColumnItem> columns = stmt.getMVColumnItemList();
            for (MVColumnItem c : columns) {
                targetMap.put(c.getName(), target);
            }
            return targetMap;
        }

        private Map<String, TableName> parseCreateMaterializedView(CreateMaterializedViewStatement stmt) {
            Map<String, TableName> targetMap = new LinkedHashMap<>();
            TableName tableName = stmt.getTableName();
            List<Column> columns = stmt.getMvColumnItems();
            for (Column c : columns) {
                targetMap.put(c.getName(), tableName);
            }
            return targetMap;
        }

        public Target parsePartition(AlterTableStmt stmt) {
            Target target = null;
            for (AlterClause alterClause : stmt.getAlterClauseList()) {
                AlterOpType alterOpType = alterClause.getOpType();
                if (AlterOpType.ADD_PARTITION.equals(alterOpType)) {
                    TableName tableName = stmt.getTbl();
                    target = new Target(tableName.getCatalog(), tableName.getDb(), tableName.getTbl());
                    target.getExtra().putAll(parseClausePartition((AddPartitionClause) alterClause));
                }
            }
            return target;
        }
    }

    private static Map<String, Object> parseClausePartition(AddPartitionClause clause) {
        PartitionDesc partitionDesc = clause.getPartitionDesc();
        if (partitionDesc != null) {
            List<Map<String, Object>> partitions = parsePartitions(partitionDesc);
            return ImmutableMap.of("partitions", partitions, "isTempPartition", clause.isTempPartition());
        } else {
            return ImmutableMap.of("isTempPartition", clause.isTempPartition());
        }
    }

    /**
     * ddl change, when schema change
     */
    static final class ChangeLogProcessor extends EventProcessor {
        @Override
        public ActionType getActionType() {
            return ActionType.ChangeLog;
        }

        @Override
        public Lineage doProcess(StatementBase stmt) {
            Lineage lineage = new Lineage();
            Target target = null;
            if (stmt instanceof CreateResourceStmt) {
                target = parse((CreateResourceStmt) stmt);
                lineage.setAction(Action.CreateResource);
            } else if (stmt instanceof AlterResourceStmt) {
                target = parse((AlterResourceStmt) stmt);
                lineage.setAction(Action.AlterResource);
            } else if (stmt instanceof DropResourceStmt) {
                target = parse((DropResourceStmt) stmt);
                lineage.setAction(Action.DropResource);
            } else if (stmt instanceof CreateCatalogStmt) {
                target = parse((CreateCatalogStmt) stmt);
                lineage.setAction(Action.CreateCatalog);
            } else if (stmt instanceof CreateDbStmt) {
                target = parse((CreateDbStmt) stmt);
                lineage.setAction(Action.CreateDb);
            } else if (stmt instanceof CreateTableStmt) {
                target = parse((CreateTableStmt) stmt);
                lineage.setAction(Action.CreateTable);
            } else if (stmt instanceof CreateTableAsSelectStmt) {
                target = parse((CreateTableAsSelectStmt) stmt);
                lineage.setAction(Action.CreateTableAsSelect);
            } else if (stmt instanceof CreateTableLikeStmt) {
                target = parse((CreateTableLikeStmt) stmt);
                lineage.setAction(Action.CreateTableLike);
            } else if (stmt instanceof CreateViewStmt) {
                target = parse((CreateViewStmt) stmt);
                lineage.setAction(Action.CreateView);
            } else if (stmt instanceof CreateMaterializedViewStmt) {
                target = parse((CreateMaterializedViewStmt) stmt);
                lineage.setAction(Action.CreateMaterializedView);
            } else if (stmt instanceof CreateMaterializedViewStatement) {
                target = parse((CreateMaterializedViewStatement) stmt);
                lineage.setAction(Action.CreateMaterializedView);
            } else if (stmt instanceof AlterDatabaseQuotaStmt) {
                target = parse((AlterDatabaseQuotaStmt) stmt);
                lineage.setAction(Action.AlterDatabaseQuota);
            } else if (stmt instanceof AlterDatabaseRenameStatement) {
                target = parse((AlterDatabaseRenameStatement) stmt);
                lineage.setAction(Action.RenameDatabase);
            } else if (stmt instanceof AlterTableStmt) {
                target = parse((AlterTableStmt) stmt, lineage);
            } else if (stmt instanceof AlterViewStmt) {
                target = parse((AlterViewStmt) stmt);
                lineage.setAction(Action.AlterView);
            } else if (stmt instanceof AlterMaterializedViewStmt) {
                target = parse((AlterMaterializedViewStmt) stmt);
                lineage.setAction(Action.AlterMaterializedView);
            } else if (stmt instanceof DropCatalogStmt) {
                target = parse((DropCatalogStmt) stmt);
                lineage.setAction(Action.DropCatalog);
            } else if (stmt instanceof DropDbStmt) {
                target = parse((DropDbStmt) stmt);
                lineage.setAction(Action.DropDb);
            } else if (stmt instanceof DropTableStmt) {
                target = parse((DropTableStmt) stmt);
                if (((DropTableStmt) stmt).isView()) {
                    lineage.setAction(Action.DropView);
                } else {
                    lineage.setAction(Action.DropTable);
                }
            } else if (stmt instanceof DropMaterializedViewStmt) {
                target = parse((DropMaterializedViewStmt) stmt);
                lineage.setAction(Action.DropMaterializedView);
            } else if (stmt instanceof CreateFunctionStmt) {
                target = parse((CreateFunctionStmt) stmt);
                lineage.setAction(Action.CreateFunction);
            } else if (stmt instanceof DropFunctionStmt) {
                target = parse((DropFunctionStmt) stmt);
                lineage.setAction(Action.DropFunction);
            }
            if (target == null) {
                return null;
            }
            lineage.setTarget(target);
            return lineage;
        }

        public Target parse(CreateResourceStmt stmt) {
            Target target = new Target();
            target.putExtra("resourceType", stmt.getResourceType().name());
            target.putExtra("resourceName", stmt.getResourceName());
            target.putExtra("properties", filterProperties(stmt.getProperties()));
            return target;
        }

        public Target parse(AlterResourceStmt stmt) {
            Target target = new Target();
            target.putExtra("resourceName", stmt.getResourceName());
            target.putExtra("properties", filterProperties(stmt.getProperties()));
            return target;
        }

        public Target parse(DropResourceStmt stmt) {
            Target target = new Target();
            target.putExtra("resourceName", stmt.getResourceName());
            return target;
        }

        public Target parse(CreateCatalogStmt stmt) {
            String catalogName = stmt.getCatalogName();
            Target target = new Target();
            target.setCatalog(catalogName);
            target.putExtra("comment", stmt.getComment());
            target.putExtra("catalogType", stmt.getCatalogType());
            target.putExtra("properties", stmt.getProperties());
            return target;
        }

        public Target parse(CreateDbStmt stmt) {
            String catalogName = stmt.getCatalogName();
            Target target = new Target();
            target.setCatalog(catalogName);
            target.setDatabase(stmt.getFullDbName());
            target.putExtra("properties", stmt.getProperties());
            return target;
        }

        public Target parse(CreateTableStmt stmt) {
            TableName tableName = stmt.getDbTbl();
            Target target = new Target(tableName.getCatalog(), tableName.getDb(), tableName.getTbl());
            target.setComment(stmt.getComment());
            List<ColumnSpec> columns = new ArrayList<>();
            for (Column c : stmt.getColumns()) {
                ColumnSpec spec = new ColumnSpec();
                spec.setName(c.getName());
                spec.setComment(c.getComment());
                spec.setType(c.getType().toString());
                columns.add(spec);
            }
            target.setColumns(columns);
            parseCreateTableExtra(stmt, target);
            return target;
        }

        public Target parse(CreateTableAsSelectStmt stmt) {
            CreateTableStmt createTableStmt = stmt.getCreateTableStmt();
            return parse(createTableStmt);
        }

        public Target parse(CreateTableLikeStmt stmt) {
            CreateTableStmt createTableStmt = stmt.getCreateTableStmt();
            return parse(createTableStmt);
        }

        public Target parse(CreateViewStmt stmt) {
            TableName tableName = stmt.getTableName();
            Target target = new Target(tableName.getCatalog(), tableName.getDb(), tableName.getTbl());
            target.setComment(stmt.getComment());
            List<ColumnSpec> columns = toColumns(stmt.getColumns());
            target.setColumns(columns);
            return target;
        }

        public Target parse(CreateMaterializedViewStmt stmt) {
            Target target = new Target(null, stmt.getDBName(), stmt.getMVName());
            List<ColumnSpec> columns = new ArrayList<>();
            for (MVColumnItem c : stmt.getMVColumnItemList()) {
                ColumnSpec spec = new ColumnSpec();
                spec.setName(c.getName());
                spec.setType(c.getType().toString());
                columns.add(spec);
            }
            target.setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
            target.setColumns(columns);
            if (stmt.getProperties() != null) {
                target.putExtra("properties", stmt.getProperties());
            }
            return target;
        }

        public Target parse(CreateMaterializedViewStatement stmt) {
            TableName tableName = stmt.getTableName();
            Target target = new Target(tableName.getCatalog(), tableName.getDb(), tableName.getTbl());
            target.setComment(stmt.getComment());
            List<ColumnSpec> columns = new ArrayList<>();
            for (Column c : stmt.getMvColumnItems()) {
                ColumnSpec spec = new ColumnSpec();
                spec.setName(c.getName());
                spec.setType(c.getType().toString());
                spec.setComment(c.getComment());
                columns.add(spec);
            }
            target.setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
            target.setColumns(columns);
            if (stmt.getRefreshSchemeDesc() != null) {
                target.putExtra("refreshSchema", parseRefreshSchema(stmt.getRefreshSchemeDesc()));
            }
            if (stmt.getProperties() != null) {
                target.putExtra("properties", stmt.getProperties());
            }
            if (stmt.getPartitionColumn() != null) {
                target.putExtra("partitionColumn", stmt.getPartitionColumn().getName());
            }
            if (stmt.getDistributionDesc() != null) {
                target.putExtra("distributionInfo", parseDistributionDesc(stmt.getDistributionDesc(), stmt.getMvColumnItems()));
            }
            if (stmt.getMvIndexes() != null && !stmt.getMvIndexes().isEmpty()) {
                target.putExtra("indexes", parseIndexes(stmt.getMvIndexes()));
            }
            return target;
        }

        public Target parse(AlterTableStmt stmt, Lineage lineage) {
            List<AlterClause> ops = stmt.getAlterClauseList();
            TableName tableName = stmt.getTbl();
            Target target = new Target(tableName.getCatalog(), tableName.getDb(), tableName.getTbl());
            for (AlterClause alterClause : ops) {
                AlterOpType alterOpType = alterClause.getOpType();
                if (alterClause instanceof AddPartitionClause) {
                    handleAddPartition(alterClause, lineage, target);
                } else if (alterClause instanceof ModifyPartitionClause) {
                    handleModifyPartition(alterClause, lineage, target);
                } else if (alterClause instanceof DropPartitionClause) {
                    handleDropPartition(alterClause, lineage, target);
                } else if (alterClause instanceof PartitionRenameClause) {
                    handlePartitionRename(alterClause, lineage, target);
                } else if (alterClause instanceof TableRenameClause) {
                    handleTableRename(alterClause, lineage, target);
                } else if (alterClause instanceof ModifyTablePropertiesClause) {
                    handleModifyTableProperties(alterClause, lineage, target);
                } else if (alterClause instanceof AlterTableCommentClause) {
                    handleAlterTableComment(alterClause, lineage, target);
                } else if (alterClause instanceof AddRollupClause) {
                    handleAddRollUp(alterClause, lineage, target);
                } else if (alterClause instanceof DropRollupClause) {
                    handleDropRollUp(alterClause, lineage, target);
                } else if (AlterOpType.SCHEMA_CHANGE.equals(alterOpType)) {
                    handelSchemaChange(alterClause, lineage, target);
                }
            }

            return target;
        }

        private void handleAddPartition(AlterClause alterClause, Lineage lineage, Target target) {
            lineage.setAction(Action.AddPartition);
            target.getExtra().putAll(parseClausePartition((AddPartitionClause) alterClause));
        }

        private void handleModifyPartition(AlterClause alterClause, Lineage lineage, Target target) {
            lineage.setAction(Action.ModifyPartition);
            ModifyPartitionClause clause = (ModifyPartitionClause) alterClause;
            target.putExtra("partitionNames", clause.getPartitionNames());
            if (clause.getProperties() != null && !clause.getProperties().isEmpty()) {
                target.putExtra("properties", clause.getProperties());
            }
        }

        private void handleDropPartition(AlterClause alterClause, Lineage lineage, Target target) {
            lineage.setAction(Action.DropPartition);
            DropPartitionClause clause = (DropPartitionClause) alterClause;
            target.addExtraList("partitionNames", clause.getPartitionName());
            target.putExtra("isTempPartition", clause.isTempPartition());
        }

        private void handlePartitionRename(AlterClause alterClause, Lineage lineage, Target target) {
            lineage.setAction(Action.RenamePartition);
            PartitionRenameClause partitionRenameClause = (PartitionRenameClause) alterClause;
            target.putExtra("partitionName", partitionRenameClause.getPartitionName());
            target.putExtra("newPartitionName", partitionRenameClause.getNewPartitionName());
        }

        private void handleTableRename(AlterClause alterClause, Lineage lineage, Target target) {
            lineage.setAction(Action.RenameTableName);
            TableRenameClause tableRenameClause = (TableRenameClause) alterClause;
            target.putExtra("newTableName", tableRenameClause.getNewTableName());
        }

        private void handleModifyTableProperties(AlterClause alterClause, Lineage lineage, Target target) {
            lineage.setAction(Action.ModifyTableProperties);
            ModifyTablePropertiesClause clause = (ModifyTablePropertiesClause) alterClause;
            target.putExtra("properties", clause.getProperties());
        }

        private void handleAlterTableComment(AlterClause alterClause, Lineage lineage, Target target) {
            lineage.setAction(Action.AlterTableComment);
            AlterTableCommentClause clause = (AlterTableCommentClause) alterClause;
            target.putExtra("newComment", clause.getNewComment());
        }

        private void handleAddRollUp(AlterClause alterClause, Lineage lineage, Target target) {
            lineage.setAction(Action.AddRollup);
            AddRollupClause addRollupClause = (AddRollupClause) alterClause;
            String rollupName = addRollupClause.getRollupName();
            Map<String, Object> rollup = new LinkedHashMap<>();
            rollup.put("rollupName", rollupName);
            rollup.put("columns", addRollupClause.getColumnNames());
            rollup.put("properties", addRollupClause.getProperties());
            target.addExtraList("rollups", rollup);
        }

        private void handleDropRollUp(AlterClause alterClause, Lineage lineage, Target target) {
            lineage.setAction(Action.DropRollup);
            DropRollupClause dropRollupClause = (DropRollupClause) alterClause;
            String rollupName = dropRollupClause.getRollupName();
            target.addExtraList("rollupNames", rollupName);
        }

        private void handelSchemaChange(AlterClause alterClause, Lineage lineage, Target target) {
            if (alterClause instanceof CreateIndexClause) {
                lineage.setAction(Action.CreateIndex);
                CreateIndexClause clause = (CreateIndexClause) alterClause;
                Map<String, Object> index = new LinkedHashMap<>();
                index.put("indexName", clause.getIndex().getIndexName());
                if (clause.getIndex().getColumns() != null && !clause.getIndex().getColumns().isEmpty()) {
                    index.put("columns", clause.getIndex().getColumns());
                }
                index.put("indexType", clause.getIndex().getIndexType().name());
                index.put("comment", clause.getIndex().getComment());
                target.addExtraList("indexes", index);
            } else if (alterClause instanceof DropIndexClause) {
                lineage.setAction(Action.DropIndex);
                DropIndexClause clause = (DropIndexClause) alterClause;
                target.addExtraList("indexNames", clause.getIndexName());
            } else if (alterClause instanceof AddColumnClause) {
                lineage.setAction(Action.AddColumn);
                AddColumnClause clause = (AddColumnClause) alterClause;
                Map<String, Object> map = new LinkedHashMap<>();
                Column column = clause.getColumn();
                map.put("name", column.getName());
                map.put("type", column.getType().toString());
                if (clause.getColPos() != null) {
                    map.put("position", clause.getColPos().toSql());
                }
                if (StringUtils.isNotEmpty(column.getComment())) {
                    map.put("comment", column.getComment());
                }
                if (clause.getProperties() != null && !clause.getProperties().isEmpty()) {
                    map.put("properties", clause.getProperties());
                }
                target.addExtraList("columns", map);
            } else if (alterClause instanceof AddColumnsClause) {
                lineage.setAction(Action.AddColumn);
                AddColumnsClause clause = (AddColumnsClause) alterClause;
                for (Column column : clause.getColumns()) {
                    Map<String, Object> map = new LinkedHashMap<>();
                    map.put("name", column.getName());
                    map.put("type", column.getType().toString());
                    if (StringUtils.isNotEmpty(column.getComment())) {
                        map.put("comment", column.getComment());
                    }
                    if (clause.getProperties() != null && !clause.getProperties().isEmpty()) {
                        map.put("properties", clause.getProperties());
                    }
                    target.addExtraList("columns", map);
                }
            } else if (alterClause instanceof DropColumnClause) {
                lineage.setAction(Action.DropColumn);
                DropColumnClause clause = (DropColumnClause) alterClause;
                target.addExtraList("columnNames", clause.getColName());
                target.putExtra("properties", clause.getProperties());
            }
        }

        public Target parse(AlterViewStmt stmt) {
            TableName tableName = stmt.getTableName();
            Target target = new Target(tableName.getCatalog(), tableName.getDb(), tableName.getTbl());
            AlterClause clause = stmt.getAlterClause();
            if (clause instanceof AlterViewClause) {
                AlterViewClause alterViewClause = (AlterViewClause) clause;
                List<Column> columns = alterViewClause.getColumns();
                List<ColumnSpec> columnSpecs = toColumns(columns);
                target.setColumns(columnSpecs);
            }
            return target;
        }

        public Target parse(AlterMaterializedViewStmt stmt) {
            TableName tableName = stmt.getMvName();
            Target target = new Target(tableName.getCatalog(), tableName.getDb(), tableName.getTbl());
            AlterTableClause alterTableClause = stmt.getAlterTableClause();
            if (alterTableClause instanceof AlterMaterializedViewStatusClause) {
                AlterMaterializedViewStatusClause alterMaterializedViewStatusClause =
                        (AlterMaterializedViewStatusClause) alterTableClause;
                target.putExtra("status", alterMaterializedViewStatusClause.getStatus());
            } else if (alterTableClause instanceof TableRenameClause) {
                TableRenameClause tableRenameClause = (TableRenameClause) alterTableClause;
                target.putExtra("newMvName", tableRenameClause.getNewTableName());
            } else if (alterTableClause instanceof RefreshSchemeClause) {
                RefreshSchemeClause refreshSchemeClause = (RefreshSchemeClause) alterTableClause;
                target.putExtra("refreshSchema", parseRefreshSchema(refreshSchemeClause));
            } else if (alterTableClause instanceof SwapTableClause) {
                SwapTableClause swapTableClause = (SwapTableClause) alterTableClause;
                Map<String, Object> swapTable = new LinkedHashMap<>();
                swapTable.put("tableName", swapTableClause.getTblName());
                swapTable.put("opType", swapTableClause.getOpType().name());
                target.putExtra("swapTable", swapTable);
            } else if (alterTableClause instanceof ModifyTablePropertiesClause) {
                ModifyTablePropertiesClause modifyTablePropertiesClause = (ModifyTablePropertiesClause) alterTableClause;
                target.putExtra("modifyTableProperties", modifyTablePropertiesClause.getProperties());
            }
            return target;
        }

        private Map<String, String> parseRefreshSchema(RefreshSchemeClause desc) {
            Map<String, String> refreshSchema = new LinkedHashMap<>();
            MaterializedView.RefreshType refreshType = desc.getType();
            refreshSchema.put("refreshType", refreshType.name());
            refreshSchema.put("refreshMoment", desc.getMoment().name());
            switch (refreshType) {
                case ASYNC:
                    refreshSchema.putAll(asyncRefreshSchema((AsyncRefreshSchemeDesc) desc));
                    break;
                case SYNC:
                case MANUAL:
                case INCREMENTAL:
                    break;
            }
            return refreshSchema;
        }

        private Map<String, String> asyncRefreshSchema(AsyncRefreshSchemeDesc asyncRefreshSchemeDesc) {
            String startTime = asyncRefreshSchemeDesc.getStartTime().toString();
            IntervalLiteral intervalLiteral = asyncRefreshSchemeDesc.getIntervalLiteral();
            if (intervalLiteral != null) {
                Expr expr = intervalLiteral.getValue();
                UnitIdentifier unitIdentifier = intervalLiteral.getUnitIdentifier();
                if (expr != null && unitIdentifier != null) {
                    String interval = expr.toSql() + " " + unitIdentifier.getDescription();
                    return ImmutableMap.of("startTime", startTime, "interval", interval);
                }
            }
            return ImmutableMap.of("startTime", startTime);
        }

        public Target parse(AlterDatabaseQuotaStmt stmt) {
            Target target = new Target(stmt.getCatalogName(), stmt.getDbName(), null);
            target.putExtra("quotaType", stmt.getQuotaType().name());
            target.putExtra("quotaValue", stmt.getQuotaValue());
            target.putExtra("quota", stmt.getQuota());
            return target;
        }

        public Target parse(AlterDatabaseRenameStatement stmt) {
            Target target = new Target(stmt.getCatalogName(), stmt.getDbName(), null);
            target.putExtra("newDbName", stmt.getNewDbName());
            return target;
        }

        public Target parse(DropCatalogStmt stmt) {
            Target target = new Target();
            target.setCatalog(stmt.getName());
            return target;
        }

        public Target parse(DropDbStmt stmt) {
            Target target = new Target();
            target.setCatalog(stmt.getCatalogName());
            target.setDatabase(stmt.getDbName());
            return target;
        }

        public Target parse(DropTableStmt stmt) {
            Target target = new Target(stmt.getCatalogName(), stmt.getDbName(), stmt.getTableName());
            return target;
        }

        public Target parse(DropMaterializedViewStmt stmt) {
            TableName mvName = stmt.getDbMvName();
            Target target = new Target(mvName.getCatalog(), mvName.getDb(), mvName.getTbl());
            return target;
        }

        public Target parse(CreateFunctionStmt stmt) {
            FunctionName functionName = stmt.getFunctionName();
            Target target = new Target();
            target.setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
            target.setDatabase(functionName.getDb());
            target.putExtra("functionName", functionName.getFunction());
            Function function = stmt.getFunction();
            if (function.getArgs() != null && function.getArgs().length > 0) {
                List<String> args = Arrays.stream(function.getArgs()).map(type -> type.toString()).collect(Collectors.toList());
                target.putExtra("functionArgsType", args);
            }
            if (function.getReturnType() != null) {
                target.putExtra("functionReturnType", function.getReturnType().toString());
            }
            target.putExtra("properties", function.getProperties());
            return target;
        }

        public Target parse(DropFunctionStmt stmt) {
            FunctionName functionName = stmt.getFunctionName();
            Target target = new Target();
            target.setCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
            target.setDatabase(functionName.getDb());
            target.putExtra("functionName", functionName.getFunction());
            Type[] types = stmt.getFunctionSearchDesc().getArgTypes();
            if (types != null && types.length > 0) {
                List<String> args = Arrays.stream(types).map(type -> type.toString()).collect(Collectors.toList());
                target.putExtra("functionArgsType", args);
            }

            return target;
        }

        protected List<ColumnSpec> toColumns(List<Column> originColumns) {
            List<ColumnSpec> columns = new ArrayList<>();
            for (Column c : originColumns) {
                ColumnSpec spec = new ColumnSpec();
                spec.setName(c.getName());
                spec.setComment(c.getComment());
                spec.setType(c.getType().toString());
                columns.add(spec);
            }
            return columns;
        }
    }

    static final class TableCollector extends AstTraverser<Map<ColumnInfo, List<ColumnInfo>>, List<ColumnInfo>> {
        public TableCollector() {
        }

        @Override
        public Map<ColumnInfo, List<ColumnInfo>> visitTableFunction(TableFunctionRelation node,
                                                                    List<ColumnInfo> context) {
            if (CollectionUtils.isEmpty(node.getChildExpressions()) ||
                    CollectionUtils.isEmpty(node.getExplicitColumnNames())) {
                return Collections.emptyMap();
            }
            TableName targetTableName = node.getResolveTableName();
            List<ColumnInfo> targetColumns = new ArrayList<>(node.getExplicitColumnNames().size());
            for (String targetColumnName : node.getExplicitColumnNames()) {
                targetColumns.add(new ColumnInfo(targetColumnName, targetTableName.getTbl(), targetTableName.getDb(),
                        targetTableName.getCatalog()));
            }
            Map<ColumnInfo, List<ColumnInfo>> resultMap = new LinkedHashMap<>();
            List<Expr> exprList = node.getChildExpressions();
            for (Expr expr : exprList) {
                Map<ColumnInfo, List<ColumnInfo>> aMap = this.visitExpression(expr, targetColumns);
                if (MapUtils.isNotEmpty(aMap)) {
                    mergeIntoResultMap(aMap, resultMap);
                }
            }
            return resultMap;
        }

        private static void mergeIntoResultMap(Map<ColumnInfo, List<ColumnInfo>> aMap,
                                               Map<ColumnInfo, List<ColumnInfo>> resultMap) {
            for (Entry<ColumnInfo, List<ColumnInfo>> entry : aMap.entrySet()) {
                if (resultMap.containsKey(entry.getKey())) {
                    List<ColumnInfo> mergedList = new LinkedList<>(resultMap.get(entry.getKey()));
                    mergedList.addAll(entry.getValue());
                    resultMap.put(entry.getKey(), mergedList);
                } else {
                    resultMap.put(entry.getKey(), entry.getValue());
                }
            }
        }

        @Override
        public Map<ColumnInfo, List<ColumnInfo>> visitNormalizedTableFunction(NormalizedTableFunctionRelation node,
                                                                              List<ColumnInfo> context) {
            return visitJoin(node, context);
        }

        @Override
        public Map<ColumnInfo, List<ColumnInfo>> visitFunctionCall(FunctionCallExpr node, List<ColumnInfo> context) {
            Map<ColumnInfo, List<ColumnInfo>> resultMap = new LinkedHashMap<>();
            FunctionParams params = node.getParams();
            List<Expr> paramExpressions = params.exprs();
            for (Expr expr : paramExpressions) {
                Map<ColumnInfo, List<ColumnInfo>> paramResultMap = visitExpression(expr, context);
                if (MapUtils.isNotEmpty(paramResultMap)) {
                    mergeIntoResultMap(paramResultMap, resultMap);
                }
            }
            return resultMap;
        }

        @Override
        public Map<ColumnInfo, List<ColumnInfo>> visitSlot(SlotRef node, List<ColumnInfo> context) {
            Map<ColumnInfo, List<ColumnInfo>> resultMap = new LinkedHashMap<>();
            TableName tn = node.getTblNameWithoutAnalyzed();
            ColumnInfo srcCol = new ColumnInfo(node.getColumnName(), tn.getTbl(), tn.getDb(), tn.getCatalog());
            for (ColumnInfo targetColumn : context) {
                resultMap.put(targetColumn, Collections.singletonList(srcCol));
            }
            return resultMap;
        }

        @Override
        public Map<ColumnInfo, List<ColumnInfo>> visitExpression(Expr node, List<ColumnInfo> context) {
            // make sure context is always passed
            node.getChildren().forEach(child -> {
                visit(child, context);
            });
            return node.accept(this, context);
        }

        @Override
        public Map<ColumnInfo, List<ColumnInfo>> visitLiteral(LiteralExpr node, List<ColumnInfo> context) {
            return Collections.emptyMap();
        }

        @Override
        public Map<ColumnInfo, List<ColumnInfo>> visitQueryStatement(QueryStatement statement,
                                                                     List<ColumnInfo> context) {
            QueryRelation queryRelation = statement.getQueryRelation();
            return visit(queryRelation);
        }

        @Override
        public Map<ColumnInfo, List<ColumnInfo>> visitUnion(UnionRelation node, List<ColumnInfo> request) {
            return mergeSetOperation(node);
        }

        @Override
        public Map<ColumnInfo, List<ColumnInfo>> visitExcept(ExceptRelation node, List<ColumnInfo> request) {
            return mergeSetOperation(node);
        }

        @Override
        public Map<ColumnInfo, List<ColumnInfo>> visitIntersect(IntersectRelation node, List<ColumnInfo> request) {
            return mergeSetOperation(node);
        }

        private Map<ColumnInfo, List<ColumnInfo>> mergeSetOperation(SetOperationRelation node) {
            final Map<ColumnInfo, List<ColumnInfo>> columnMap = new LinkedHashMap<>();
            node.getRelations().forEach(queryRelation -> {
                Map<ColumnInfo, List<ColumnInfo>> ret = visit(queryRelation, null);
                if (MapUtils.isEmpty(ret)) {
                    return;
                }
                if (columnMap.isEmpty()) {
                    columnMap.putAll(ret);
                } else {
                    Iterator<Entry<ColumnInfo, List<ColumnInfo>>> mapIter = columnMap.entrySet().iterator();
                    Iterator<Map.Entry<ColumnInfo, List<ColumnInfo>>> retIter = ret.entrySet().iterator();
                    while (mapIter.hasNext()) {
                        Map.Entry<ColumnInfo, List<ColumnInfo>> mapEntry = mapIter.next();
                        Map.Entry<ColumnInfo, List<ColumnInfo>> retEntry = retIter.next();
                        mapEntry.getValue().addAll(retEntry.getValue());
                    }
                }
            });
            return columnMap;
        }

        @Override
        public Map<ColumnInfo, List<ColumnInfo>> visitJoin(JoinRelation node, List<ColumnInfo> request) {
            Map<ColumnInfo, List<ColumnInfo>> leftResult = visit(node.getLeft(), request);
            Map<ColumnInfo, List<ColumnInfo>> rightResult = visit(node.getRight(), request);
            Map<ColumnInfo, List<ColumnInfo>> result = new LinkedHashMap<>();
            if (!MapUtils.isEmpty(leftResult)) {
                result.putAll(leftResult);
            }
            if (!MapUtils.isEmpty(rightResult)) {
                result.putAll(rightResult);
            }
            return result;
        }

        @Override
        public Map<ColumnInfo, List<ColumnInfo>> visitSelect(SelectRelation node, List<ColumnInfo> request) {
            if (node.hasWithClause()) {
                node.getCteRelations().forEach(this::visit);
            }
            List<Expr> outputExpressions = node.getOutputExpression();
            List<ColumnInfo> newRequest = new ArrayList<>();
            Map<ColumnInfo, List<ColumnInfo>> columnsMap = new LinkedHashMap<>();
            List<Field> fields = node.getScope().getRelationFields().getAllFields();
            for (int i = 0; i < outputExpressions.size(); i++) {
                Expr expr = outputExpressions.get(i);
                List<ColumnInfo> queryColumn = visitExpr(expr);
                if (queryColumn != null) {
                    newRequest.addAll(queryColumn);
                }
                ColumnInfo targetColumn = getColumnInfo(fields, i);
                if (queryColumn != null) {
                    columnsMap.put(targetColumn, queryColumn);
                } else {
                    columnsMap.put(targetColumn, Arrays.asList(targetColumn));
                }
            }

            Map<ColumnInfo, List<ColumnInfo>> mapResult = visit(node.getRelation(), newRequest);
            // table function may return null value
            if (MapUtils.isEmpty(mapResult)) {
                return Collections.emptyMap();
            }
            Map<String, List<ColumnInfo>> nameMapResult = new LinkedHashMap<>();
            mapResult.forEach((k, v) -> nameMapResult.put(k.getColName(), v));
            Map<ColumnInfo, List<ColumnInfo>> rs = new LinkedHashMap<>();
            for (Map.Entry<ColumnInfo, List<ColumnInfo>> entry : columnsMap.entrySet()) {
                List<ColumnInfo> sources = new ArrayList<>();
                if (entry.getValue() != null && !entry.getValue().isEmpty()) {
                    for (ColumnInfo key : entry.getValue()) {
                        List<ColumnInfo> value;
                        if (key.getDbName() != null) {
                            value = mapResult.get(key);
                        } else {
                            value = nameMapResult.get(key.getColName());
                        }
                        if (value != null) {
                            sources.addAll(value);
                        }
                    }
                }
                rs.put(entry.getKey(), sources);
            }

            return rs;
        }

        @Override
        public Map<ColumnInfo, List<ColumnInfo>> visitFileTableFunction(FileTableFunctionRelation node,
                List<ColumnInfo> request) {
            return visitRelation(node, request);
        }

        @Override
        public Map<ColumnInfo, List<ColumnInfo>> visitTable(TableRelation node, List<ColumnInfo> request) {
            return visitRelation(node, request);
        }

        public Map<ColumnInfo, List<ColumnInfo>> visitRelation(TableRelation node, List<ColumnInfo> request) {
            TableName tableAlias = node.getAlias();
            TableName tblName = node.getName();
            TableName current = tableAlias;
            if (current == null) {
                current = tblName;
            }
            String currentCatalog = (null != tableAlias && StringUtils.isNotEmpty(tableAlias.getCatalog())) ?
                    tableAlias.getCatalog() : tblName.getCatalog();
            String currentDb = (null != tableAlias && StringUtils.isNotEmpty(tableAlias.getDb())) ?
                    tableAlias.getDb() : tblName.getDb();
            Map<String, String> resource = parseResource(node);
            Map<ColumnInfo, List<ColumnInfo>> map = new LinkedHashMap<>();
            if (request == null || request.isEmpty()) {
                for (Map.Entry<Field, Column> entry : node.getColumns().entrySet()) {
                    ColumnInfo target = new ColumnInfo(entry.getValue().getName(), current.getTbl(),
                            currentDb, currentCatalog);
                    ColumnInfo source = new ColumnInfo(entry.getValue().getName(), tblName.getTbl(),
                            tblName.getDb(), tblName.getCatalog());
                    source.setResource(resource);
                    map.put(target, Arrays.asList(source));
                }
            } else {
                for (ColumnInfo columnInfo : request) {
                    if (columnInfo.getTableName() != current.getTbl()) {
                        continue;
                    }
                    ColumnInfo source = new ColumnInfo(columnInfo.getColName(), tblName.getTbl(),
                            tblName.getDb(), tblName.getCatalog());
                    source.setResource(resource);
                    map.put(columnInfo, Arrays.asList(source));
                }
            }

            return map;
        }

        private Map<String, String> parseResource(TableRelation relation) {
            if (relation instanceof FileTableFunctionRelation) {
                FileTableFunctionRelation fileTableFunctionRelation = (FileTableFunctionRelation) relation;
                if (fileTableFunctionRelation.getProperties() != null && !fileTableFunctionRelation.getProperties().isEmpty()) {
                    Map<String, String> resource = getStringStringMap(fileTableFunctionRelation);
                    return resource;
                }
            }

            return null;
        }

        private Map<String, String> getStringStringMap(FileTableFunctionRelation fileTableFunctionRelation) {
            Map<String, String> resource = new LinkedHashMap<>();
            for (Entry<String, String> entry : fileTableFunctionRelation.getProperties().entrySet()) {
                if ("path".equals(entry.getKey())) {
                    resource.put("path", entry.getValue());
                } else if ("format".equals(entry.getKey())) {
                    resource.put("format", entry.getValue());
                } else if (entry.getKey().contains("endpoint")) {
                    resource.put(entry.getKey(), entry.getValue());
                }
            }
            return resource;
        }

        private ColumnInfo getColumnInfo(List<Field> fields, int i) {
            Field field = fields.get(i);
            String targetCol = field.getName();
            TableName tableName = field.getRelationAlias();
            ColumnInfo targetColumn;
            if (tableName != null && tableName.getDb() != null) {
                //slot ref
                targetColumn = new ColumnInfo(targetCol, tableName.getTbl(), tableName.getDb(), tableName.getCatalog());
            } else {
                targetColumn = new ColumnInfo(targetCol);
            }
            return targetColumn;
        }

        public List<ColumnInfo> visitExpr(Expr expr) {
            List<ColumnInfo> infos = new ArrayList<>();
            SlotRef slotRef = null;
            if (expr instanceof SlotRef) {
                slotRef = (SlotRef) expr;
            } else {
                if (expr.getChildren() != null && !expr.getChildren().isEmpty()) {
                    for (Expr child : expr.getChildren()) {
                        List<ColumnInfo> info = visitExpr(child);
                        if (info != null) {
                            infos.addAll(info);
                        }
                    }
                    return infos;
                }
            }

            if (slotRef != null) {
                if (slotRef.getTblNameWithoutAnalyzed() == null) {
                    return null;
                }
                TableName alias = slotRef.getTblNameWithoutAnalyzed();
                TableName current = alias;
                String srcCol = slotRef.getColumnName();
                ColumnInfo srcInfo = new ColumnInfo(srcCol, current.getTbl(), current.getDb(), current.getCatalog());

                return Arrays.asList(srcInfo);
            }
            return null;
        }
    }

    private static void parseCreateTableExtra(CreateTableStmt stmt, Target target) {
        target.putExtra("properties", stmt.getProperties());
        target.putExtra("engineName", stmt.getEngineName());
        target.putExtra("extProperties", stmt.getExtProperties());
        if (stmt.getDistributionDesc() != null) {
            target.putExtra("distributionInfo", parseDistributionDesc(stmt.getDistributionDesc(), stmt.getColumns()));
        }
        if (stmt.getIndexes() != null && !stmt.getIndexes().isEmpty()) {
            target.putExtra("indexes", parseIndexes(stmt.getIndexes()));
        }
        if (stmt.getKeysDesc() != null) {
            KeysDesc keysDesc = stmt.getKeysDesc();
            String namesStr = keysDesc.getKeysColumnNames().stream()
                    .map(s -> "`" + s + "`").collect(Collectors.joining(","));
            target.putExtra("keysDesc", String.format("%s(%s)", keysDesc.getKeysType(), String.join(",", namesStr)));
        }
        if (stmt.getPartitionDesc() != null) {
            PartitionDesc partitionDesc = stmt.getPartitionDesc();
            Map<String, Object> partitionSpec = parsePartitionSpec(partitionDesc);
            if (partitionDesc.getType() != null) {
                partitionSpec.put("partitionType", partitionDesc.getType().typeString);
            }
            target.putExtra("partitionDesc", partitionSpec);
            target.putExtra("partitions", parsePartitions((partitionDesc)));
        }
    }

    private static Map<String, Object> parseDistributionDesc(DistributionDesc distributionDesc, List<Column> columns) {
        Map<String, Object> distributionMap = new LinkedHashMap<>();
        try {
            DistributionInfo info = distributionDesc.toDistributionInfo(columns);
            distributionMap.put("type", info.getType().name());
            distributionMap.put("bucketNum", info.getBucketNum());
            Map<ColumnId, Column> idColumnMap = MetaUtils.buildIdToColumn(columns);
            distributionMap.put("distributionKey", info.getDistributionKey(idColumnMap));
            distributionMap.put("sql", info.toSql(idColumnMap));
            return distributionMap;
        } catch (DdlException e) {
            LOG.debug("ddl exception", e);
            return null;
        }
    }

    private static List<Map<String, Object>> parseIndexes(List<Index> indexSpecs) {
        List<Map<String, Object>> indexes = new ArrayList<>();
        for (Index index : indexSpecs) {
            Map<String, Object> indexProperties = new LinkedHashMap<>();
            indexProperties.put("indexName", index.getIndexName());
            indexProperties.put("indexType", index.getIndexType().name());
            indexProperties.put("comment", index.getComment());
            indexProperties.put("columns", index.getColumns());
            indexes.add(indexProperties);
        }
        return indexes;
    }

    private static Map<String, Object> parsePartitionSpec(PartitionDesc partitionDesc) {
        Map<String, Object> partitionSpec = new LinkedHashMap<>();
        if (partitionDesc instanceof RangePartitionDesc) {
            RangePartitionDesc rangePartitionDesc = (RangePartitionDesc) partitionDesc;
            partitionSpec.put("partitionColNames", rangePartitionDesc.getPartitionColNames());
            partitionSpec.put("sql", rangePartitionDesc.toString());
        } else if (partitionDesc instanceof ListPartitionDesc) {
            ListPartitionDesc listPartitionDesc = (ListPartitionDesc) partitionDesc;
            partitionSpec.put("partitionColNames", listPartitionDesc.getPartitionColNames());
        } else if (partitionDesc instanceof SingleItemListPartitionDesc) {
            SingleItemListPartitionDesc desc = (SingleItemListPartitionDesc) partitionDesc;
            partitionSpec.put("partitionColNames", Arrays.asList(desc.getPartitionName()));
        } else if (partitionDesc instanceof MultiItemListPartitionDesc) {
            MultiItemListPartitionDesc desc = (MultiItemListPartitionDesc) partitionDesc;
            partitionSpec.put("partitionColNames", Arrays.asList(desc.getPartitionName()));
        } else if (partitionDesc instanceof SingleRangePartitionDesc) {
            SingleRangePartitionDesc desc = (SingleRangePartitionDesc) partitionDesc;
            partitionSpec.put("partitionColNames", Arrays.asList(desc.getPartitionName()));
        } else if (partitionDesc instanceof ExpressionPartitionDesc) {
            ExpressionPartitionDesc expressionPartitionDesc = (ExpressionPartitionDesc) partitionDesc;
            Expr expr = expressionPartitionDesc.getExpr();
            String sql = expr.toMySql();
            partitionSpec.put("sql", sql);
            partitionSpec.put("partitionColNames", visitColumn(expr));
        }
        return partitionSpec;
    }

    private static List<Map<String, Object>> parsePartitions(PartitionDesc partitionDesc) {
        List<Map<String, Object>> partitionInfoList = new ArrayList<>();
        if (partitionDesc instanceof RangePartitionDesc) {
            RangePartitionDesc rangePartitionDesc = (RangePartitionDesc) partitionDesc;
            for (SingleRangePartitionDesc desc : rangePartitionDesc.getSingleRangePartitionDescs()) {
                Map<String, Object> map = rangePartition(desc);
                partitionInfoList.add(map);
            }
        } else if (partitionDesc instanceof ListPartitionDesc) {
            ListPartitionDesc listPartitionDesc = (ListPartitionDesc) partitionDesc;
            List<PartitionDesc> partitionDescs = listPartitionDesc.getPartitionDescs();
            for (PartitionDesc desc : partitionDescs) {
                if (desc instanceof SingleItemListPartitionDesc) {
                    SingleItemListPartitionDesc singleItemListPartitionDesc = (SingleItemListPartitionDesc) desc;
                    Map<String, Object> partition = basePartition(desc);
                    partition.put("values", singleItemListPartitionDesc.getValues());
                    partitionInfoList.add(partition);
                } else if (desc instanceof MultiItemListPartitionDesc) {
                    MultiItemListPartitionDesc multiItemListPartitionDesc = (MultiItemListPartitionDesc) desc;
                    Map<String, Object> partition = basePartition(desc);
                    partition.put("values", multiItemListPartitionDesc.getMultiValues());
                    partitionInfoList.add(partition);
                }
            }
        } else if (partitionDesc instanceof SingleItemListPartitionDesc) {
            SingleItemListPartitionDesc desc = (SingleItemListPartitionDesc) partitionDesc;
            Map<String, Object> partition = basePartition(desc);
            partition.put("values", desc.getValues());
            partitionInfoList.add(partition);
        } else if (partitionDesc instanceof MultiItemListPartitionDesc) {
            MultiItemListPartitionDesc desc = (MultiItemListPartitionDesc) partitionDesc;
            Map<String, Object> partition = basePartition(desc);
            partition.put("values", desc.getMultiValues());
            partitionInfoList.add(partition);
        } else if (partitionDesc instanceof SingleRangePartitionDesc) {
            SingleRangePartitionDesc desc = (SingleRangePartitionDesc) partitionDesc;
            Map<String, Object> map = rangePartition(desc);
            partitionInfoList.add(map);
        } else if (partitionDesc instanceof ExpressionPartitionDesc) {
            ExpressionPartitionDesc expressionPartitionDesc = (ExpressionPartitionDesc) partitionDesc;
            RangePartitionDesc rangePartitionDesc = expressionPartitionDesc.getRangePartitionDesc();
            if (rangePartitionDesc != null) {
                List<SingleRangePartitionDesc> singleRangePartitionDescList = rangePartitionDesc.getSingleRangePartitionDescs();
                if (singleRangePartitionDescList != null) {
                    for (SingleRangePartitionDesc desc : singleRangePartitionDescList) {
                        Map<String, Object> map = rangePartition(desc);
                        partitionInfoList.add(map);
                    }
                }
            }
        }
        return partitionInfoList;
    }

    private static List<String> visitColumn(Expr expr) {
        List<String> cols = new ArrayList<>();
        if (expr instanceof SlotRef) {
            SlotRef slotRef = (SlotRef) expr;
            String col = slotRef.getColumnName();
            cols.add(col);
        } else if (expr.getChildren() != null && !expr.getChildren().isEmpty()) {
            for (Expr child : expr.getChildren()) {
                List<String> childCols = visitColumn(child);
                if (CollectionUtils.isNotEmpty(childCols)) {
                    cols.addAll(childCols);
                }
            }
        }
        return cols;
    }

    private static Map<String, Object> rangePartition(SingleRangePartitionDesc desc) {
        Map<String, Object> partition = basePartition(desc);
        partition.put("partitionKeyDesc", desc.getPartitionKeyDesc().toString());
        return partition;
    }

    private static Map<String, Object> basePartition(PartitionDesc desc) {
        Map<String, Object> partition = new LinkedHashMap<>();
        partition.put("name", desc.getPartitionName());
        if (desc.getType() != null) {
            partition.put("type", desc.getType().typeString);
        }
        partition.put("properties", desc.getProperties());
        partition.put("replicationNum", desc.getReplicationNum());
        partition.put("dataProperty", desc.getPartitionDataProperty());
        return partition;
    }

    private static Map<String, String> filterProperties(Map<String, String> stmtProperties) {
        Map<String, String> properties = new LinkedHashMap<>();
        if (stmtProperties == null) {
            return properties;
        }
        for (Map.Entry<String, String> entry : stmtProperties.entrySet()) {
            if (entry.getKey().contains("username") || entry.getKey().contains("password")) {
                continue;
            }
            if (entry.getKey().contains("accessKeyId") || entry.getKey().contains("accessKeySecret")) {
                continue;
            }
            if (entry.getKey().contains("access_key") || entry.getKey().contains("secret_key")) {
                continue;
            }
            properties.put(entry.getKey(), entry.getValue());
        }
        return properties;
    }

    static class ColumnInfo {
        private String colName;
        private String tableName;
        private String dbName;
        private String catalog;

        private Map<String, String> resource;

        public Map<String, String> getResource() {
            return resource;
        }

        public void setResource(Map<String, String> resource) {
            this.resource = resource;
        }

        public ColumnInfo(String colName) {
            this.colName = colName;
        }

        public ColumnInfo(String colName, String tableName, String dbName, String catalog) {
            this.colName = colName;
            this.tableName = tableName;
            this.dbName = dbName;
            this.catalog = catalog;
        }

        public String getColName() {
            return colName;
        }

        public void setColName(String colName) {
            this.colName = colName;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getDbName() {
            return dbName;
        }

        public void setDbName(String dbName) {
            this.dbName = dbName;
        }

        public String getCatalog() {
            return catalog;
        }

        public void setCatalog(String catalog) {
            this.catalog = catalog;
        }

        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ColumnInfo info = (ColumnInfo) o;
            return Objects.equals(colName, info.colName) && Objects.equals(tableName, info.tableName)
                    && Objects.equals(dbName, info.dbName) && Objects.equals(catalog, info.catalog);
        }

        public int hashCode() {
            return Objects.hash(colName, tableName, dbName, catalog);
        }
    }

    static class ColumnLineage {
        private Map<String, String> resource;
        private String destCatalog;
        private String srcCatalog;
        private String destDatabase;
        private String srcDatabase;
        private String destTable;
        private String srcTable;
        //one target many sources
        private Map<String, Set<String>> columnMap = new LinkedHashMap<>();

        public ColumnLineage() {

        }

        public ColumnLineage(String destCatalog, String srcCatalog,
                String destDatabase, String srcDatabase,
                String destTable, String srcTable) {
            this.destCatalog = destCatalog;
            this.srcCatalog = srcCatalog;
            this.destDatabase = destDatabase;
            this.srcDatabase = srcDatabase;
            this.destTable = destTable;
            this.srcTable = srcTable;
        }

        public ColumnLineage destCatalog(String destCatalog) {
            this.destCatalog = destCatalog;
            return this;
        }

        public ColumnLineage srcCatalog(String srcCatalog) {
            this.srcCatalog = srcCatalog;
            return this;
        }

        public ColumnLineage destDatabase(String destDatabase) {
            this.destDatabase = destDatabase;
            return this;
        }

        public ColumnLineage srcDatabase(String srcDatabase) {
            this.srcDatabase = srcDatabase;
            return this;
        }

        public ColumnLineage destTable(String destTable) {
            this.destTable = destTable;
            return this;
        }

        public ColumnLineage srcTable(String srcTable) {
            this.srcTable = srcTable;
            return this;
        }

        public Map<String, String> getResource() {
            return resource;
        }

        public void setResource(Map<String, String> resource) {
            this.resource = resource;
        }

        public String getDestCatalog() {
            return destCatalog;
        }

        public void setDestCatalog(String destCatalog) {
            this.destCatalog = destCatalog;
        }

        public String getSrcCatalog() {
            return srcCatalog;
        }

        public void setSrcCatalog(String srcCatalog) {
            this.srcCatalog = srcCatalog;
        }

        public String getDestDatabase() {
            return destDatabase;
        }

        public void setDestDatabase(String destDatabase) {
            this.destDatabase = destDatabase;
        }

        public String getSrcDatabase() {
            return srcDatabase;
        }

        public void setSrcDatabase(String srcDatabase) {
            this.srcDatabase = srcDatabase;
        }

        public String getDestTable() {
            return destTable;
        }

        public void setDestTable(String destTable) {
            this.destTable = destTable;
        }

        public String getSrcTable() {
            return srcTable;
        }

        public void setSrcTable(String srcTable) {
            this.srcTable = srcTable;
        }

        public Map<String, Set<String>> getColumnMap() {
            return columnMap;
        }

        public void setColumnMap(Map<String, Set<String>> columnMap) {
            this.columnMap = columnMap;
        }
    }

    enum Action {
        CreateResource,
        AlterResource,
        DropResource,
        CreateCatalog,
        DropCatalog,
        CreateDb,
        DropDb,
        CreateTableAsSelect,
        CreateTableLike,
        CreateTable,
        DropTable,
        CreateView,
        AlterView,
        DropView,
        CreateMaterializedView,
        AlterMaterializedView,
        DropMaterializedView,
        AddPartition,
        DropPartition,
        ModifyPartition,
        RenamePartition,
        TruncatePartition,
        RenameTableName,
        AddRollup,
        DropRollup,
        CreateIndex,
        DropIndex,
        AddColumn,
        DropColumn,
        ModifyTableProperties,
        AlterTableComment,
        AlterDatabaseQuota,
        RenameDatabase,
        CreateFunction,
        DropFunction,
        DataLoading,
        Insert,
        Update,
        Delete,
    }

    enum ActionType {
        Lineage,
        ChangeLog
    }

    static class Target {
        public Target() {

        }

        public Target(String catalog, String database, String table) {
            this.catalog = catalog;
            this.database = database;
            this.table = table;
        }

        private String catalog;
        private String database;
        private String table;
        private String comment;
        private List<ColumnSpec> columns;
        private Map<String, Object> extra = new LinkedHashMap<>();

        public String getCatalog() {
            return catalog;
        }

        public void setCatalog(String catalog) {
            this.catalog = catalog;
        }

        public String getDatabase() {
            return database;
        }

        public void setDatabase(String database) {
            this.database = database;
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public String getComment() {
            return comment;
        }

        public void setComment(String comment) {
            this.comment = comment;
        }

        public List<ColumnSpec> getColumns() {
            return columns;
        }

        public void setColumns(List<ColumnSpec> columns) {
            this.columns = columns;
        }

        public Map<String, Object> getExtra() {
            return extra;
        }

        public Object putExtra(String key, Object value) {
            return extra.put(key, value);
        }

        public void addExtraList(String key, Object value) {
            if (extra.get(key) == null) {
                List<Object> valueList = new ArrayList<>();
                valueList.add(value);
                extra.put(key, valueList);
            } else {
                List valueList = (List) extra.get(key);
                valueList.add(value);
            }
        }

        public void setExtra(Map<String, Object> extra) {
            this.extra = extra;
        }
    }

    static class ColumnSpec {
        private String name;
        private String type;
        private String comment;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getComment() {
            return comment;
        }

        public void setComment(String comment) {
            this.comment = comment;
        }
    }

    public static class Lineage {
        private long timestamp;
        private ActionType actionType;
        private Action action;
        private State state;
        private String queryText;
        private String queryId;
        private long costTime;
        private String clientIp;
        private String user;
        private Target target;
        private List<ColumnLineage> columnLineages;

        public State getState() {
            return state;
        }

        public void setState(State state) {
            this.state = state;
        }

        public ActionType getActionType() {
            return actionType;
        }

        public void setActionType(ActionType actionType) {
            this.actionType = actionType;
        }

        public Action getAction() {
            return action;
        }

        public void setAction(Action action) {
            this.action = action;
        }

        public Target getTarget() {
            return target;
        }

        public void setTarget(Target target) {
            this.target = target;
        }

        public String getClientIp() {
            return clientIp;
        }

        public void setClientIp(String clientIp) {
            this.clientIp = clientIp;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public String getQueryText() {
            return queryText;
        }

        public void setQueryText(String queryText) {
            this.queryText = queryText;
        }

        public String getQueryId() {
            return queryId;
        }

        public void setQueryId(String queryId) {
            this.queryId = queryId;
        }

        public long getCostTime() {
            return costTime;
        }

        public void setCostTime(long costTime) {
            this.costTime = costTime;
        }

        public List<ColumnLineage> getColumnLineages() {
            return columnLineages;
        }

        public void setColumnLineages(List<ColumnLineage> columnLineages) {
            this.columnLineages = columnLineages;
        }
    }

    enum State {
        SUCCESS,
        ERROR
    }
}
