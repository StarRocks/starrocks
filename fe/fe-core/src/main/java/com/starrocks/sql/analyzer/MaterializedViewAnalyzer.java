// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.analyzer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.DistributionDesc;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.HashDistributionDesc;
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
import java.util.TreeSet;
import java.util.stream.Collectors;

// todo PrivilegeChecker add permission
public class MaterializedViewAnalyzer {

    public static final Set<String> ALLOW_FN_NAMES;

    static {
        ALLOW_FN_NAMES = new TreeSet<>();
        ALLOW_FN_NAMES.add("date_format");
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
            // get table name and column name first
            PartitionExpDesc partitionExpDesc = (PartitionExpDesc) statement.getPartitionDesc();
            String functionName = partitionExpDesc.getFunctionName();
            if (functionName != null && !ALLOW_FN_NAMES.contains(functionName)) {
                throw new SemanticException("Materialized view partition function name " +
                        functionName + " is not support");
            }
            // check partition key in query select statement, check partition key in used by partition in base tables
            QueryStatement queryStatement = statement.getQueryStatement();
            // analyze query statement, can check table and column is exists in meta
            Analyzer.analyze(queryStatement, context);
            //check query relation
            if (!(queryStatement.getQueryRelation() instanceof SelectRelation)) {
                throw new SemanticException("Materialized view query statement only support select");
            }
            SelectRelation selectRelation = ((SelectRelation) queryStatement.getQueryRelation());
            // set column
            setColumn(statement, selectRelation);
            // check table is in this database
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
            // check alias except * and SlotRef
            // check partition by expression in selectItem
            boolean partitionExpIsInSelectItem = false;
            List<SelectListItem> selectListItems = selectRelation.getSelectList().getItems();
            for (SelectListItem selectListItem : selectListItems) {
                if (!selectListItem.isStar()
                        && !(selectListItem.getExpr() instanceof SlotRef)
                        && selectListItem.getAlias() == null) {
                    throw new SemanticException("Materialized view query statement select item" +
                            " must has alias except base select item");
                }
                if (!partitionExpIsInSelectItem) {
                    // todo if table has alias
                    partitionExpIsInSelectItem = checkPartitionExpIsInSelectItem(selectListItem, partitionExpDesc);
                }
            }
            if (!partitionExpIsInSelectItem) {
                throw new SemanticException("Materialized view Partition by expression must in query statement select item");
            }
            checkDistribution(statement);
            return null;
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
            //todo column define
            List<Column> mvColumns = Lists.newArrayList();
            List<String> columnOutputNames = queryRelation.getColumnOutputNames();
            List<Expr> outputExpression = queryRelation.getOutputExpression();
            for (int i = 0; i < outputExpression.size(); ++i) {
                mvColumns.add(new Column(columnOutputNames.get(i), outputExpression.get(i).getType()));
            }
            statement.setMvColumnItems(mvColumns);
        }

        // todo can rewrite with visitor
        private void extractTableRelation(Relation relation, Map<String, TableRelation> tableRelationHashMap) {
            if (relation instanceof TableRelation) {
                tableRelationHashMap.put(((TableRelation) relation).getName().getTbl(), (TableRelation) relation);
            } else if (relation instanceof JoinRelation) {
                extractTableRelation(((JoinRelation) relation).getLeft(), tableRelationHashMap);
                extractTableRelation(((JoinRelation) relation).getRight(), tableRelationHashMap);
            }
        }

        // todo can rewrite with visitor
        private boolean checkPartitionExpIsInSelectItem(SelectListItem selectListItem, PartitionExpDesc partitionExpDesc) {
            Expr expr = selectListItem.getExpr();
            if (partitionExpDesc.getFunctionName() != null) {
                if (!(expr instanceof FunctionCallExpr)) {
                    return false;
                }
                // todo function args compare?
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
                if (!functionCallExpr.getFnName().getFunction().equals(partitionExpDesc.getFunctionName())) {
                    return false;
                }
                return checkPartitionExpIsInSlotRef((SlotRef) functionCallExpr.getChild(0), partitionExpDesc);
            } else {
                if (!(expr instanceof SlotRef)) {
                    return false;
                }
                return checkPartitionExpIsInSlotRef((SlotRef) expr, partitionExpDesc);
            }
        }

        private boolean checkPartitionExpIsInSlotRef(SlotRef slotRef, PartitionExpDesc partitionExpDesc) {
            if (!slotRef.getTblNameWithoutAnalyzed().getTbl().equals(partitionExpDesc.getTableName())) {
                return false;
            }
            if (!slotRef.getColumnName().equals(partitionExpDesc.getColumnName())) {
                return false;
            }
            return true;
        }


    }


}
