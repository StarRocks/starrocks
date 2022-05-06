// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.analyzer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.DistributionDesc;
import com.starrocks.analysis.DropMaterializedViewStmt;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.HashDistributionDesc;
import com.starrocks.analysis.SelectListItem;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
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
            ExpressionPartitionDesc expressionPartitionDesc = statement.getPartitionExpDesc();
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
                            selectListItem.getExpr().toSql() + " must has alias except base select item");
                }
            }
            // analyze query statement, can check whether tables and columns exist in catalog
            Analyzer.analyze(queryStatement, context);
            // collect table from query statement
            Map<TableName, Table> tableNameTableMap = AnalyzerUtils.collectAllTable(queryStatement);
            tableNameTableMap.forEach((tableName, table) -> {
                if (!tableName.getDb().equals(statement.getTableName().getDb())) {
                    throw new SemanticException("Materialized view do not support table which is in other database");
                }
                if (!(table instanceof OlapTable)) {
                    throw new SemanticException("Materialized view only support olap tables");
                }
            });
            Map<Column, Expr> columnExprMap = Maps.newHashMap();
            // get outputExpressions and convert it to columns which in selectRelation
            // write the columns into createMaterializedViewStatement
            // record the relationship between columns and outputExpressions for next check
            setColumn(statement, selectRelation, columnExprMap);
            // some check if partition exp exists
            if (expressionPartitionDesc != null) {
                // check partition expression all in column list which in createMaterializedViewStatement
                // write the expr into partitionExpDesc if partition expression exists
                checkExpInColumn(expressionPartitionDesc, statement, columnExprMap);
                // check partition expression functions is in allow list if it exists
                checkPartitionExpParams(expressionPartitionDesc);
                // check partition key must be base table partition key
                checkPartitionKeyWithBaseTable(expressionPartitionDesc, tableNameTableMap);
            }
            // check and analyze distribution
            checkDistribution(statement);
            // convert queryStatement to sql and set
            statement.setInlineViewDef(ViewDefBuilder.build(queryStatement));
            return null;
        }

        private void setColumn(CreateMaterializedViewStatement statement, QueryRelation queryRelation,
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

        private void checkExpInColumn(ExpressionPartitionDesc expressionPartitionDesc, CreateMaterializedViewStatement statement,
                                      Map<Column, Expr> columnExprMap) {
            List<Column> mvColumnItems = statement.getMvColumnItems();
            SlotRef slotRef = expressionPartitionDesc.getSlotRef();
            for (Column mvColumnItem : mvColumnItems) {
                if (slotRef.getColumnName().equals(mvColumnItem.getName())) {
                    expressionPartitionDesc.setExpr(columnExprMap.get(mvColumnItem));
                    break;
                }
            }
            if (expressionPartitionDesc.getExpr() == null) {
                throw new SemanticException("Materialized view partition exp column can't find in query statement");
            }
        }

        private void checkPartitionExpParams(ExpressionPartitionDesc expressionPartitionDesc) {
            Expr expr = expressionPartitionDesc.getExpr();
            if (expr instanceof FunctionCallExpr) {
                FunctionCallExpr functionCallExpr = ((FunctionCallExpr) expr);
                String functionName = functionCallExpr.getFnName().getFunction();
                CheckFunction checkFunction = FunctionChecker.FN_NAME_TO_PATTERN.get(functionName);
                if (checkFunction == null) {
                    throw new SemanticException("Materialized view partition function " +
                            functionName + " is not support");
                }
                if (!checkFunction.check(functionCallExpr)) {
                    throw new SemanticException("Materialized view partition function " +
                            functionName + " check failed");
                }
            }
        }

        private void checkPartitionKeyWithBaseTable(ExpressionPartitionDesc expressionPartitionDesc,
                                                    Map<TableName, Table> tableNameTableMap) {
            SlotRef slotRef = null;
            Expr expr = expressionPartitionDesc.getExpr();
            if (expr instanceof FunctionCallExpr) {
                // get column from expression
                slotRef = getColumnFromFunctionCallExpr(((FunctionCallExpr) expr));
            } else {
                slotRef = ((SlotRef) expr);
            }
            // must have table
            Table table = tableNameTableMap.get(slotRef.getTblNameWithoutAnalyzed());
            PartitionInfo partitionInfo = ((OlapTable) table).getPartitionInfo();
            if (partitionInfo instanceof SinglePartitionInfo) {
                if (expressionPartitionDesc.getExpr() != null) {
                    throw new SemanticException("Materialized view partition key in partition exp " +
                            "must be base table partition key");
                }
            } else {
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                List<Column> partitionColumns = rangePartitionInfo.getPartitionColumns();
                SlotRef finalSlotRef = slotRef;
                if (partitionColumns.stream().filter(column -> {
                    return column.getName().equals(finalSlotRef.getColumnName());
                }).count() == 0) {
                    throw new SemanticException("Materialized view partition key in partition exp " +
                            "must be base table partition key");
                }
            }
        }

        private SlotRef getColumnFromFunctionCallExpr(FunctionCallExpr expr) {
            FunctionName fnName = expr.getFnName();
            if (fnName.getFunction().equals("date_trunc")) {
                return (SlotRef) expr.getChild(1);
            }
            return (SlotRef) expr.getChild(0);
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
            try {
                distributionDesc.analyze(
                        mvColumnItems.stream().map(column -> column.getName()).collect(Collectors.toSet()));
            } catch (AnalysisException e) {
                LOG.error("distributionDesc " + distributionDesc + "analyze failed", e);
                throw new SemanticException(e.getMessage());
            }
        }

        @Override
        public Void visitDropMaterializedViewStatement(DropMaterializedViewStmt stmt, ConnectContext context) {
            stmt.getDbMvName().normalization(context);
            return null;
        }
    }

    @FunctionalInterface
    public interface CheckFunction {

        boolean check(Expr expr);
    }

    static class FunctionChecker {

        public static final Map<String, CheckFunction> FN_NAME_TO_PATTERN;

        static {
            FN_NAME_TO_PATTERN = Maps.newHashMap();
            // can add some other functions
            FN_NAME_TO_PATTERN.put("date_trunc", FunctionChecker::checkDateTrunc);
        }

        public static boolean checkDateTrunc(Expr expr) {
            if (!(expr instanceof FunctionCallExpr)) {
                return false;
            }
            FunctionCallExpr fnExpr = (FunctionCallExpr) expr;
            String fnNameString = fnExpr.getFnName().getFunction();
            if (!fnNameString.equalsIgnoreCase("date_trunc")) {
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
