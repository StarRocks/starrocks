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

package com.starrocks.authorization;

import com.google.common.collect.Maps;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.BasicTable;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.View;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.connector.metadata.MetadataTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.ast.ViewRelation;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.OptimizerOptions;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.MVTransformerContext;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.optimizer.transformer.TransformerContext;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ColumnPrivilege {
    public static void check(ConnectContext context, QueryStatement stmt, List<TableName> excludeTables) {
        if (stmt == null) {
            return;
        }

        Map<TableName, Table> tableNameTableObj = Maps.newHashMap();
        Map<Table, TableName> tableObjectToTableName = Maps.newHashMap();
        new TableNameCollector(tableNameTableObj, tableObjectToTableName).visit(stmt);

        for (Map.Entry<TableName, Table> entry : tableNameTableObj.entrySet()) {
            TableName tableName = entry.getKey();
            Table table = entry.getValue();

            if (excludeTables.contains(tableName) || table instanceof MetadataTable) {
                continue;
            }

            if (table instanceof SystemTable && ((SystemTable) table).requireOperatePrivilege()) {
                try {
                    Authorizer.checkSystemAction(context, PrivilegeType.OPERATE);
                } catch (AccessDeniedException e) {
                    AccessDeniedException.reportAccessDenied(
                            InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                            context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                            PrivilegeType.OPERATE.name(), ObjectType.SYSTEM.name(), null);
                }
            }
        }

        Set<TableName> tableUsedExternalAccessController = new HashSet<>();
        for (TableName tableName : tableNameTableObj.keySet()) {
            String catalog = tableName.getCatalog() == null ?
                    InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME : tableName.getCatalog();
            if (Authorizer.getInstance().getAccessControlOrDefault(catalog) instanceof ExternalAccessController) {
                tableUsedExternalAccessController.add(tableName);
            }
        }

        Map<TableName, Set<String>> scanColumns = new HashMap<>();
        OptExpression optimizedPlan;
        if (!tableUsedExternalAccessController.isEmpty()) {
            /*
             * The column access privilege of the query need to use the pruned column list.
             * Because the unused columns will not check the column access privilege.
             * For example, the table contains two columns v1 and v2, and user u1 only has
             * the access privilege to v1, the select v1 from (select * from tbl) t can be checked because v2 has been pruned.
             */
            ColumnRefFactory columnRefFactory = new ColumnRefFactory();
            LogicalPlan logicalPlan;
            MVTransformerContext mvTransformerContext = StatementPlanner.makeMVTransformerContext(context.getSessionVariable());
            TransformerContext transformerContext = new TransformerContext(columnRefFactory, context, mvTransformerContext);
            logicalPlan = new RelationTransformer(transformerContext).transformWithSelectLimit(stmt.getQueryRelation());

            OptimizerOptions optimizerOptions = new OptimizerOptions(OptimizerOptions.OptimizerStrategy.RULE_BASED);
            optimizerOptions.disableRule(RuleType.GP_SINGLE_TABLE_MV_REWRITE);
            optimizerOptions.disableRule(RuleType.GP_MULTI_TABLE_MV_REWRITE);
            optimizerOptions.disableRule(RuleType.GP_PRUNE_EMPTY_OPERATOR);
            Optimizer optimizer =
                    OptimizerFactory.create(OptimizerFactory.initContext(context, columnRefFactory, optimizerOptions));
            optimizedPlan = optimizer.optimize(logicalPlan.getRoot(),
                    new PhysicalPropertySet(), new ColumnRefSet(logicalPlan.getOutputColumn()));

            optimizedPlan.getOp().accept(new ScanColumnCollector(tableObjectToTableName, scanColumns), optimizedPlan, null);
        }

        for (TableName tableName : tableNameTableObj.keySet()) {
            if (excludeTables.contains(tableName)) {
                continue;
            }

            if (tableUsedExternalAccessController.contains(tableName)) {
                Set<String> columns = scanColumns.getOrDefault(tableName, new HashSet<>());
                for (String column : columns) {
                    try {
                        Authorizer.checkColumnAction(context,
                                tableName, column, PrivilegeType.SELECT);
                    } catch (AccessDeniedException e) {
                        AccessDeniedException.reportAccessDenied(
                                tableName.getCatalog(),
                                context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                PrivilegeType.SELECT.name(), ObjectType.COLUMN.name(), column);
                    }
                }
            } else {
                Table table = tableNameTableObj.get(tableName);

                if (table instanceof View) {
                    try {
                        // for privilege checking, treat connector view as table
                        if (table.isConnectorView()) {
                            Authorizer.checkTableAction(context,
                                    tableName, PrivilegeType.SELECT);
                        } else {
                            View view = (View) table;
                            if (view.isSecurity()) {
                                List<TableName> allTables = view.getTableRefs();
                                for (TableName t : allTables) {
                                    BasicTable basicTable = GlobalStateMgr.getCurrentState().getMetadataMgr().getBasicTable(
                                            t.getCatalog(), t.getDb(), t.getTbl());
                                    if (basicTable.isOlapView()) {
                                        View subView = (View) basicTable;
                                        QueryStatement queryStatement = subView.getQueryStatement();
                                        Analyzer.analyze(queryStatement, context);
                                        Authorizer.check(queryStatement, context);
                                    } else if (basicTable.isMaterializedView()) {
                                        Authorizer.checkMaterializedViewAction(context, t, PrivilegeType.SELECT);
                                    } else {
                                        Authorizer.checkTableAction(context, t, PrivilegeType.SELECT);
                                    }
                                }
                            }

                            Authorizer.checkViewAction(context,
                                    tableName, PrivilegeType.SELECT);
                        }
                    } catch (AccessDeniedException e) {
                        AccessDeniedException.reportAccessDenied(
                                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                                context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                PrivilegeType.SELECT.name(), ObjectType.VIEW.name(), tableName.getTbl());
                    }
                } else if (table.isMaterializedView()) {
                    try {
                        Authorizer.checkMaterializedViewAction(context,
                                tableName, PrivilegeType.SELECT);
                    } catch (AccessDeniedException e) {
                        AccessDeniedException.reportAccessDenied(
                                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                                context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                PrivilegeType.SELECT.name(), ObjectType.MATERIALIZED_VIEW.name(), tableName.getTbl());
                    }
                } else {
                    try {
                        Authorizer.checkTableAction(context,
                                tableName.getCatalog(), tableName.getDb(), table.getName(), PrivilegeType.SELECT);
                    } catch (AccessDeniedException e) {
                        AccessDeniedException.reportAccessDenied(
                                tableName.getCatalog(),
                                context.getCurrentUserIdentity(), context.getCurrentRoleIds(),
                                PrivilegeType.SELECT.name(), ObjectType.TABLE.name(), tableName.getTbl());
                    }
                }
            }
        }
    }

    public static class ScanColumnCollector extends OptExpressionVisitor<Void, Void> {
        private final Map<TableName, Set<String>> scanColumns;
        private final Map<Table, TableName> tableObjToTableName;

        public ScanColumnCollector(
                Map<Table, TableName> tableObjToTableName,
                Map<TableName, Set<String>> scanColumns) {
            this.scanColumns = scanColumns;
            this.tableObjToTableName = tableObjToTableName;
        }

        @Override
        public Void visit(OptExpression optExpression, Void context) {
            for (OptExpression input : optExpression.getInputs()) {
                input.getOp().accept(this, input, null);
            }
            return null;
        }

        @Override
        public Void visitLogicalTableScan(OptExpression node, Void context) {
            LogicalScanOperator operator = (LogicalScanOperator) node.getOp();
            Table table = operator.getTable();
            TableName tableName = tableObjToTableName.get(table);

            if (!scanColumns.containsKey(tableName)) {
                scanColumns.put(tableName, new HashSet<>());
            }

            Set<String> tableColumns = scanColumns.get(tableName);
            for (Map.Entry<ColumnRefOperator, Column> c : operator.getColRefToColumnMetaMap().entrySet()) {
                String columName = c.getValue().getName();
                tableColumns.add(columName);
            }
            return null;
        }
    }

    private static class TableNameCollector extends AstTraverser<Void, Void> {
        private final Map<TableName, Table> tableNameToTableObj;
        private final Map<Table, TableName> tableTableNameMap;

        public TableNameCollector(Map<TableName, Table> tableNameToTableObj, Map<Table, TableName> tableTableNameMap) {
            this.tableNameToTableObj = tableNameToTableObj;
            this.tableTableNameMap = tableTableNameMap;
        }

        @Override
        public Void visitQueryStatement(QueryStatement statement, Void context) {
            return visit(statement.getQueryRelation());
        }

        @Override
        public Void visitInsertStatement(InsertStmt node, Void context) {
            Table table = node.getTargetTable();
            tableNameToTableObj.put(node.getTableName(), table);
            tableTableNameMap.put(table, node.getTableName());
            return super.visitInsertStatement(node, context);
        }

        @Override
        public Void visitUpdateStatement(UpdateStmt node, Void context) {
            Table table = node.getTable();
            tableNameToTableObj.put(node.getTableName(), table);
            tableTableNameMap.put(table, node.getTableName());
            return super.visitUpdateStatement(node, context);
        }

        @Override
        public Void visitDeleteStatement(DeleteStmt node, Void context) {
            Table table = node.getTable();
            tableNameToTableObj.put(node.getTableName(), table);
            tableTableNameMap.put(table, node.getTableName());
            return super.visitDeleteStatement(node, context);
        }

        @Override
        public Void visitTable(TableRelation node, Void context) {
            Table table = node.getTable();
            tableNameToTableObj.put(node.getName(), table);
            tableTableNameMap.put(table, node.getName());
            return null;
        }

        @Override
        public Void visitView(ViewRelation node, Void context) {
            Table table = node.getView();
            tableNameToTableObj.put(node.getName(), table);
            tableTableNameMap.put(table, node.getName());
            return null;
        }
    }
}
