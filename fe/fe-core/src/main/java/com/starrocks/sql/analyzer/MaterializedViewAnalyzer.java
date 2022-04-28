// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.DistributionDesc;
import com.starrocks.analysis.DropMaterializedViewStmt;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.HashDistributionDesc;
import com.starrocks.analysis.MVColumnDateFormatPattern;
import com.starrocks.analysis.MVColumnPattern;
import com.starrocks.analysis.SelectListItem;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StatementBase;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.PartitionExpDesc;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MaterializedViewAnalyzer {

    private static final Logger LOG = LoggerFactory.getLogger(MaterializedViewAnalyzer.class);

    public static final Map<String, MVColumnPattern> FN_NAME_TO_PATTERN;

    static {
        FN_NAME_TO_PATTERN = Maps.newHashMap();
        // can add some other functions
        FN_NAME_TO_PATTERN.put("date_format", new MVColumnDateFormatPattern());
    }

    public static void analyze(StatementBase stmt, ConnectContext session) {
        new MaterializedViewAnalyzerVisitor().visit(stmt, session);
    }

    static class MaterializedViewAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {

        @Override
        public Void visitCreateMaterializedViewStatement(CreateMaterializedViewStatement statement,
                                                         ConnectContext context) {
            statement.getTableName().normalization(context);
            QueryStatement queryStatement = statement.getQueryStatement();
            PartitionExpDesc partitionExpDesc = statement.getPartitionExpDesc();
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
            // analyze query statement, can check table and column is exists in meta
            Analyzer.analyze(queryStatement, context);
            // check table is in this database and is OlapTable
            Map<String, TableRelation> tableRelationHashMap = new HashMap<>();
            extractTableRelation(selectRelation.getRelation(), tableRelationHashMap);
            tableRelationHashMap.forEach((tbl, tableRelation) -> {
                if (!tableRelation.getName().getDb().equals(statement.getTableName().getDb())) {
                    throw new SemanticException("Materialized view do not support table which is in other database");
                }
                if (!(tableRelation.getTable() instanceof OlapTable)) {
                    throw new SemanticException("Materialized view only support olap tables");
                }
            });
            // set column
            setColumn(statement, selectRelation);
            // check partition SlotRef
            checkExpInColumn(partitionExpDesc, statement);
            // check partition Expr
            checkPartitionExpParams(partitionExpDesc);
            //check partition key must be base table partition key
            checkPartitionKey(partitionExpDesc, tableRelationHashMap);
            // check distribution
            checkDistribution(statement);
            //set view def
            String viewSql = ViewDefBuilder.build(queryStatement);
            statement.setInlineViewDef(viewSql);
            return null;
        }

        private void setColumn(CreateMaterializedViewStatement statement, QueryRelation queryRelation) {
            List<Column> mvColumns = Lists.newArrayList();
            Map<Column, Expr> columnExprMap = Maps.newHashMap();
            List<String> columnOutputNames = queryRelation.getColumnOutputNames();
            List<Expr> outputExpression = queryRelation.getOutputExpression();
            for (int i = 0; i < outputExpression.size(); ++i) {
                Column column = new Column(columnOutputNames.get(i), outputExpression.get(i).getType());
                mvColumns.add(column);
                columnExprMap.put(column, outputExpression.get(i));
            }
            statement.setMvColumnItems(mvColumns);
            statement.setColumnExprMap(columnExprMap);
        }

        private void checkExpInColumn(PartitionExpDesc partitionExpDesc, CreateMaterializedViewStatement statement) {
            List<Column> mvColumnItems = statement.getMvColumnItems();
            Map<Column, Expr> columnExprMap = statement.getColumnExprMap();
            for (Column mvColumnItem : mvColumnItems) {
                List<SlotRef> slotRefs = partitionExpDesc.getSlotRefs();
                for (SlotRef slotRef : slotRefs) {
                    if (slotRef.getColumnName().equals(mvColumnItem.getName())) {
                        partitionExpDesc.getExprs().add(columnExprMap.get(mvColumnItem));
                        break;
                    }
                }
            }
            if (partitionExpDesc.getExprs().size() != partitionExpDesc.getSlotRefs().size()) {
                throw new SemanticException("Materialized view partition exp column can't find in query statement");
            }
        }

        private void checkPartitionExpParams(PartitionExpDesc partitionExpDesc) {
            List<Expr> exprs = partitionExpDesc.getExprs();
            for (Expr expr : exprs) {
                if (expr instanceof FunctionCallExpr) {
                    FunctionCallExpr functionCallExpr = ((FunctionCallExpr) expr);
                    String functionName = functionCallExpr.getFnName().getFunction();
                    MVColumnPattern mvColumnPattern = FN_NAME_TO_PATTERN.get(functionName);
                    if (mvColumnPattern == null) {
                        throw new SemanticException("Materialized view partition function " +
                                functionName + " is not support");
                    }
                    if (!mvColumnPattern.match(functionCallExpr)) {
                        throw new SemanticException("Materialized view partition function " +
                                functionName + " must match pattern:" + mvColumnPattern);
                    }
                }
            }
        }

        private void checkPartitionKey(PartitionExpDesc partitionExpDesc,
                                       Map<String, TableRelation> tableRelationHashMap) {
            SlotRef slotRef = null;
            List<Expr> exprs = partitionExpDesc.getExprs();
            for (Expr expr : exprs) {
                if (expr instanceof FunctionCallExpr) {
                    slotRef = (SlotRef) expr.getChild(0);
                } else {
                    slotRef = ((SlotRef) expr);
                }
                //must have table relation
                TableRelation tableRelation = tableRelationHashMap.get(slotRef.getTblNameWithoutAnalyzed().getTbl());
                PartitionInfo partitionInfo = ((OlapTable) tableRelation.getTable()).getPartitionInfo();
                if (partitionInfo instanceof SinglePartitionInfo) {
                    if (partitionExpDesc.getExprs().size() > 0) {
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

        private void extractTableRelation(Relation relation, Map<String, TableRelation> tableRelationHashMap) {
            if (relation instanceof TableRelation) {
                tableRelationHashMap.put(((TableRelation) relation).getName().getTbl(), (TableRelation) relation);
            } else if (relation instanceof JoinRelation) {
                extractTableRelation(((JoinRelation) relation).getLeft(), tableRelationHashMap);
                extractTableRelation(((JoinRelation) relation).getRight(), tableRelationHashMap);
            }
        }

        @Override
        public Void visitDropMaterializedViewStatement(DropMaterializedViewStmt stmt, ConnectContext context) {
            stmt.getDbMvName().normalization(context);
            return null;
        }
    }

}