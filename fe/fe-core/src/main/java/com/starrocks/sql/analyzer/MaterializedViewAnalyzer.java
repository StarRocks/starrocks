// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.analyzer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.DistributionDesc;
import com.starrocks.analysis.DropMaterializedViewStmt;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.HashDistributionDesc;
import com.starrocks.analysis.SelectListItem;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.ExpressionPartitionDesc;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MaterializedViewAnalyzer {

    private static final Logger LOG = LoggerFactory.getLogger(MaterializedViewAnalyzer.class);

    public static void analyze(StatementBase stmt, ConnectContext session) {
        new MaterializedViewAnalyzerVisitor().visit(stmt, session);
    }

    static class MaterializedViewAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {

        @Override
        public Void visitCreateMaterializedViewStatement(CreateMaterializedViewStatement statement,
                                                         ConnectContext context) {
            statement.getTableName().normalization(context);
            QueryStatement queryStatement = statement.getQueryStatement();
            //check query relation is select relation
            if (!(queryStatement.getQueryRelation() instanceof SelectRelation)) {
                throw new SemanticException("Materialized view query statement only support select");
            }
            SelectRelation selectRelation = ((SelectRelation) queryStatement.getQueryRelation());
            // check alias except * and SlotRef
            List<SelectListItem> selectListItems = selectRelation.getSelectList().getItems();
            for (SelectListItem selectListItem : selectListItems) {
                if (selectListItem.isStar()) {
                    throw new SemanticException("Select * is not supported in materialized view");
                } else if (!(selectListItem.getExpr() instanceof SlotRef)
                        && selectListItem.getAlias() == null) {
                    throw new SemanticException("Materialized view query statement select item " +
                            selectListItem.getExpr().toSql() + " must has an alias");
                }
                // check select item has nondeterministic function
                checkNondeterministicFunction(selectListItem.getExpr());
            }
            // analyze query statement, can check whether tables and columns exist in catalog
            Analyzer.analyze(queryStatement, context);
            // collect table from query statement
            Map<TableName, Table> tableNameTableMap = AnalyzerUtils.collectAllTableAndViewWithAlias(queryStatement);
            tableNameTableMap.forEach((tableName, table) -> {
                if (!tableName.getDb().equals(statement.getTableName().getDb())) {
                    throw new SemanticException(
                            "Materialized view do not support table which is in other database:" + tableName.getDb());
                }
                if (!(table instanceof OlapTable)) {
                    throw new SemanticException(
                            "Materialized view only support olap table:" + tableName.getTbl() + " type:" +
                                    table.getType().name());
                }
            });
            Map<Column, Expr> columnExprMap = Maps.newHashMap();
            // get outputExpressions and convert it to columns which in selectRelation
            // set the columns into createMaterializedViewStatement
            // record the relationship between columns and outputExpressions for next check
            genColumnAndSetIntoStmt(statement, selectRelation, columnExprMap);
            // some check if partition exp exists
            if (statement.getPartitionExpDesc() != null) {
                // check partition expression all in column list and
                // write the expr into partitionExpDesc if partition expression exists
                checkExpInColumn(statement, columnExprMap);
                // check whether partition expression functions are allowed if it exists
                checkPartitionExpParams(statement);
                // check partition column must be base table's partition column
                checkPartitionColumnWithBaseTable(statement, tableNameTableMap);
            }
            // check and analyze distribution
            checkDistribution(statement);
            // convert queryStatement to sql and set
            statement.setInlineViewDef(ViewDefBuilder.build(queryStatement));
            return null;
        }

        private void checkNondeterministicFunction(Expr expr) {
            if (expr instanceof FunctionCallExpr) {
                if (((FunctionCallExpr) expr).isNondeterministicBuiltinFnName()) {
                    throw new SemanticException("Materialized view query statement select item " +
                            expr.toSql() + " not supported nondeterministic function");
                }
            }
            ArrayList<Expr> children = expr.getChildren();
            for (Expr child : children) {
                checkNondeterministicFunction(child);
            }
        }

        private void genColumnAndSetIntoStmt(CreateMaterializedViewStatement statement, QueryRelation queryRelation,
                                             Map<Column, Expr> columnExprMap) {
            List<Column> mvColumns = Lists.newArrayList();
            List<String> columnOutputNames = queryRelation.getColumnOutputNames();
            List<Expr> outputExpression = queryRelation.getOutputExpression();
            for (int i = 0; i < outputExpression.size(); ++i) {
                Column column = new Column(columnOutputNames.get(i), outputExpression.get(i).getType());
                mvColumns.add(column);
                columnExprMap.put(column, outputExpression.get(i));
            }
            statement.setMvColumnItems(mvColumns);
        }

        private void checkExpInColumn(CreateMaterializedViewStatement statement,
                                      Map<Column, Expr> columnExprMap) {
            ExpressionPartitionDesc expressionPartitionDesc = statement.getPartitionExpDesc();
            List<Column> columns = statement.getMvColumnItems();
            SlotRef slotRef = expressionPartitionDesc.getSlotRef();
            boolean hasColumn = false;
            for (Column column : columns) {
                if (slotRef.getColumnName().equals(column.getName())) {
                    hasColumn = true;
                    Expr refExpr = columnExprMap.get(column);
                    // check exp with ref expr which in columnExprMap
                    checkExpWithRefExpr(expressionPartitionDesc, refExpr);
                    break;
                }
            }
            if (!hasColumn) {
                throw new SemanticException("Materialized view partition exp column is not found in query statement");
            }
        }

        private void checkExpWithRefExpr(ExpressionPartitionDesc expressionPartitionDesc, Expr refExpr) {
            if (expressionPartitionDesc.isFunction()) {
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) expressionPartitionDesc.getExpr();
                if (!(refExpr instanceof SlotRef)) {
                    throw new SemanticException("Materialized view partition function " +
                            functionCallExpr.getFnName().getFunction() +
                            " must related with column");
                }
                ArrayList<Expr> children = functionCallExpr.getChildren();
                for (int i = 0; i < children.size(); i++) {
                    if (children.get(i) instanceof SlotRef) {
                        functionCallExpr.setChild(i, refExpr);
                        break;
                    }
                }
                // analyze function, must after update child
                FunctionAnalyzer.analyze(functionCallExpr);
            } else {
                if (refExpr instanceof FunctionCallExpr || refExpr instanceof SlotRef) {
                    expressionPartitionDesc.setExpr(refExpr);
                } else {
                    throw new SemanticException(
                            "Materialized view partition function must related with column");
                }
            }
        }

        private void checkPartitionExpParams(CreateMaterializedViewStatement statement) {
            Expr expr = statement.getPartitionExpDesc().getExpr();
            if (expr instanceof FunctionCallExpr) {
                FunctionCallExpr functionCallExpr = ((FunctionCallExpr) expr);
                String functionName = functionCallExpr.getFnName().getFunction();
                CheckPartitionFunction checkPartitionFunction =
                        PartitionFunctionChecker.FN_NAME_TO_PATTERN.get(functionName);
                if (checkPartitionFunction == null) {
                    throw new SemanticException("Materialized view partition function " +
                            functionName + " is not support");
                }
                if (!checkPartitionFunction.check(functionCallExpr)) {
                    throw new SemanticException("Materialized view partition function " +
                            functionName + " check failed");
                }
            }
        }

        private void checkPartitionColumnWithBaseTable(CreateMaterializedViewStatement statement,
                                                       Map<TableName, Table> tableNameTableMap) {
            SlotRef slotRef = statement.getPartitionExpDesc().getSlotRef();
            // must have table
            Table table = tableNameTableMap.get(slotRef.getTblNameWithoutAnalyzed());
            PartitionInfo partitionInfo = ((OlapTable) table).getPartitionInfo();
            if (partitionInfo instanceof SinglePartitionInfo) {
                throw new SemanticException("Materialized view partition column in partition exp " +
                        "must be base table partition column");
            } else if (partitionInfo instanceof RangePartitionInfo) {
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                List<Column> partitionColumns = rangePartitionInfo.getPartitionColumns();
                if (partitionColumns.size() > 1) {
                    throw new SemanticException("Materialized view related base table partition columns " +
                            "only supports single column");
                }
                boolean isInPartitionColumns = false;
                for (Column partitionColumn : partitionColumns) {
                    if (partitionColumn.getName().equals(slotRef.getColumnName())) {
                        isInPartitionColumns = true;
                        break;
                    }
                }
                if (!isInPartitionColumns) {
                    throw new SemanticException("Materialized view partition column in partition exp " +
                            "must be base table partition column");
                }
            } else {
                throw new SemanticException("Materialized view related base table partition type:" +
                        partitionInfo.getType().name() + "not supports");
            }
        }

        private void checkDistribution(CreateMaterializedViewStatement statement) {
            DistributionDesc distributionDesc = statement.getDistributionDesc();
            Map<String, String> properties = statement.getProperties();
            List<Column> mvColumnItems = statement.getMvColumnItems();
            if (distributionDesc == null) {
                if (ConnectContext.get().getSessionVariable().isAllowDefaultPartition()) {
                    if (properties == null) {
                        properties = Maps.newHashMap();
                        properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, "1");
                    }
                    distributionDesc = new HashDistributionDesc(Config.default_bucket_num,
                            Lists.newArrayList(mvColumnItems.get(0).getName()));
                    statement.setDistributionDesc(distributionDesc);
                } else {
                    throw new SemanticException("Materialized view should contain distribution desc");
                }
            }
            distributionDesc.analyze(
                    mvColumnItems.stream().map(column -> column.getName()).collect(Collectors.toSet()));
        }

        @Override
        public Void visitDropMaterializedViewStatement(DropMaterializedViewStmt stmt, ConnectContext context) {
            stmt.getDbMvName().normalization(context);
            return null;
        }
    }

    @FunctionalInterface
    public interface CheckPartitionFunction {

        boolean check(Expr expr);
    }

    static class PartitionFunctionChecker {

        public static final Map<String, CheckPartitionFunction> FN_NAME_TO_PATTERN;

        static {
            FN_NAME_TO_PATTERN = Maps.newHashMap();
            // can add some other functions
            FN_NAME_TO_PATTERN.put("date_trunc", PartitionFunctionChecker::checkDateTrunc);
        }

        public static boolean checkDateTrunc(Expr expr) {
            if (!(expr instanceof FunctionCallExpr)) {
                return false;
            }
            FunctionCallExpr fnExpr = (FunctionCallExpr) expr;
            String fnNameString = fnExpr.getFnName().getFunction();
            if (!fnNameString.equalsIgnoreCase(FunctionSet.DATE_TRUNC)) {
                return false;
            }
            if (fnExpr.getChild(0) instanceof StringLiteral && fnExpr.getChild(1) instanceof SlotRef) {
                SlotRef slotRef = (SlotRef) fnExpr.getChild(1);
                PrimitiveType primitiveType = slotRef.getType().getPrimitiveType();
                // must check slotRef type, because function analyze don't check it.
                if ((primitiveType == PrimitiveType.DATETIME || primitiveType == PrimitiveType.DATE)
                        && slotRef.getTblNameWithoutAnalyzed() != null) {
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }
    }
}
