// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.analyzer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.DistributionDesc;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MaterializedViewAnalyzer {

    public static final Map<String, MVColumnPattern> FN_NAME_TO_PATTERN;

    static {
        FN_NAME_TO_PATTERN = Maps.newHashMap();
        // can add some other functions
        FN_NAME_TO_PATTERN.put("date_format", new MVColumnDateFormatPattern());
    }


    public static void analyze(StatementBase statement, ConnectContext session) {
        new MaterializedViewAnalyzerVisitor().visit(statement, session);
    }

    static class MaterializedViewAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {

        public void analyze(StatementBase statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitCreateMaterializedViewStatement(CreateMaterializedViewStatement statement, ConnectContext context) {
            QueryStatement queryStatement = statement.getQueryStatement();
            //check query relation is select relation
            if (!(queryStatement.getQueryRelation() instanceof SelectRelation)) {
                throw new SemanticException("Materialized view query statement only support select");
            }
            SelectRelation selectRelation = ((SelectRelation) queryStatement.getQueryRelation());
            // check alias except * and SlotRef
            List<SelectListItem> selectListItems = selectRelation.getSelectList().getItems();
            for (SelectListItem selectListItem : selectListItems) {
                if (!selectListItem.isStar()
                        && !(selectListItem.getExpr() instanceof SlotRef)
                        && selectListItem.getAlias() == null) {
                    throw new SemanticException("Materialized view query statement select item" +
                            " must has alias except base select item");
                }
            }
            // check partitionExpDesc
            PartitionExpDesc partitionExpDesc = statement.getPartitionExpDesc();
            checkPartitionExpDesc(partitionExpDesc, selectListItems);
            // analyze query statement, can check table and column is exists in meta
            Analyzer.analyze(queryStatement, context);
            // check table is in this database and is OlapTable
            Map<String, TableRelation> tableRelationHashMap = new HashMap<>();
            extractTableRelation(selectRelation.getRelation(), tableRelationHashMap);
            tableRelationHashMap.forEach((tableName, tableRelation) -> {
                if (!tableRelation.getName().getDb().equals(context.getDatabase())) {
                    throw new SemanticException("Materialized view not support table which in other database");
                }
                if (!(tableRelation.getTable() instanceof OlapTable)) {
                    throw new SemanticException("Materialized view only support olap tables");
                }
            });
            // set column
            setColumn(statement, selectRelation);
            //check partition key must be base table partition key
            // checkPartitionKey(partitionExpDesc, tableRelationHashMap);
            // check distribution
            checkDistribution(statement);
            statement.setDbName(context.getDatabase());
            return null;
        }

        private void checkPartitionKey(PartitionExpDesc partitionExpDesc, Map<String, TableRelation> tableRelationHashMap) {
            SlotRef slotRef = null;
            Expr expr = partitionExpDesc.getExpr();
            if (expr instanceof FunctionCallExpr) {
                slotRef = (SlotRef) expr.getChild(0);
            } else {
                slotRef = ((SlotRef) expr);
            }
            //must have table relation
            TableRelation tableRelation = tableRelationHashMap.get(slotRef.getTblNameWithoutAnalyzed());
            Set<String> partitionNames = ((OlapTable) tableRelation.getTable()).getPartitionNames();
            if (!partitionNames.contains(slotRef.getColumnName())) {
                throw new SemanticException("Materialized view partition key in partition exp " +
                        " must be base table partition key");
            }
        }

        private void checkPartitionExpDesc(PartitionExpDesc partitionExpDesc, List<SelectListItem> selectListItems) {
            Expr expr = partitionExpDesc.getExpr();
            if (expr instanceof FunctionCallExpr) {
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
                String functionName = functionCallExpr.getFnName().getFunction();
                Expr leftChild = functionCallExpr.getChild(0);
                if (!(leftChild instanceof SlotRef)) {
                    throw new SemanticException("Materialized view partition function " +
                            functionName + " first params must be column");
                }
                checkExpInSelectList(expr, selectListItems);
                functionCallExpr = ((FunctionCallExpr) partitionExpDesc.getExpr());
                MVColumnPattern mvColumnPattern = FN_NAME_TO_PATTERN.get(functionName);
                if (mvColumnPattern == null) {
                    throw new SemanticException("Materialized view partition function " +
                            functionName + " is not support");
                }
                if (!mvColumnPattern.match(functionCallExpr)) {
                    throw new SemanticException("Materialized view partition function " +
                            functionName + " must match pattern:" + mvColumnPattern);
                }
            } else if (expr instanceof SlotRef) {
                checkExpInSelectList(expr, selectListItems);
            } else {
                throw new SemanticException("Materialized view partition exp only support function and column");
            }
        }

        private void checkExpInSelectList(Expr expr,
                                              List<SelectListItem> selectListItems) {
            boolean isInSelectList = false;
            for (SelectListItem selectListItem : selectListItems) {
                if (expr.equals(selectListItem.getExpr())) {
                    isInSelectList = true;
                    break;
                }
            }
            if (!isInSelectList) {
                throw new SemanticException("Materialized view partition exp column can't find in query statement");
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
                distributionDesc.analyze(mvColumnItems.stream().map(column -> column.getName()).collect(Collectors.toSet()));
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }
        }


        private void setColumn(CreateMaterializedViewStatement statement, QueryRelation queryRelation) {
            //todo column define column reference with bast table
            // set SlotRef in column
            // or base tableId and columnName in column?
            List<Column> mvColumns = Lists.newArrayList();
            List<String> columnOutputNames = queryRelation.getColumnOutputNames();
            List<Expr> outputExpression = queryRelation.getOutputExpression();
            for (int i = 0; i < outputExpression.size(); ++i) {
                mvColumns.add(new Column(columnOutputNames.get(i), outputExpression.get(i).getType()));
            }
            statement.setMvColumnItems(mvColumns);
        }

        private void extractTableRelation(Relation relation, Map<String, TableRelation> tableRelationHashMap) {
            if (relation instanceof TableRelation) {
                tableRelationHashMap.put(((TableRelation) relation).getName().getTbl(), (TableRelation) relation);
            } else if (relation instanceof JoinRelation) {
                extractTableRelation(((JoinRelation) relation).getLeft(), tableRelationHashMap);
                extractTableRelation(((JoinRelation) relation).getRight(), tableRelationHashMap);
            }
        }

    }


}
