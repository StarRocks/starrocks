// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.analyzer;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.SelectListItem;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.RefreshType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.PartitionExpDesc;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.RefreshSchemeDesc;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

// todo PrivilegeChecker add permission
public class MaterializedViewAnalyzer {

    public static void analyze(StatementBase statement, ConnectContext session) {
        new MaterializedViewAnalyzerVisitor().visit(statement, session);
    }

    static class MaterializedViewAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {

        public void analyze(StatementBase statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitCreateMaterializedViewStatement(CreateMaterializedViewStatement statement, ConnectContext context) {

            /**
             * STEP 1. check without metadata
             * STEP 2. check with metadata
             * STEP 3. generate or convert columnItems & partition desc & distribution desc
             */
            // STEP 1
            RefreshSchemeDesc refreshSchemeDesc = statement.getRefreshSchemeDesc();
            if (refreshSchemeDesc.getType() != RefreshType.ASYNC) {
                throw new SemanticException("Materialized view refresh type only support async");
            }
            // get table name and column name first
            PartitionExpDesc partitionExpDesc = (PartitionExpDesc) statement.getPartitionDesc();
            FunctionName functionName = partitionExpDesc.getFunctionName();
            if (functionName != null && !functionName.getFunction().equals("date_format")) {
                throw new SemanticException("Materialized view partition function name" + functionName.getFunction() + "is not support");
            }
            // check partition key in query select statement, check partition key in used by partition in base tables
            QueryStatement queryStatement = statement.getQueryStatement();
            // analyze query statement
            Analyzer.analyze(queryStatement, context);
            //check query relation
            if (!(queryStatement.getQueryRelation() instanceof SelectRelation)) {
                throw new SemanticException("Materialized view query statement only support select");
            }
            SelectRelation selectRelation = ((SelectRelation) queryStatement.getQueryRelation());
            List<SelectListItem> selectListItems = selectRelation.getSelectList().getItems();
            // check table is in this database
            Set<TableName> tableNames = new HashSet<>();
            extractTableNamesInRelation(selectRelation.getRelation(), tableNames);
            for (TableName tableName : tableNames) {
                if(!tableName.getDb().equals(context.getDatabase())) {
                    throw new SemanticException("Materialized view not support table which in other database");
                }
            }
            // check alias except * and SlotRef
            // check partition by expression in selectItem
            boolean partitionExpIsInSelectItem = false;
            for (SelectListItem selectListItem : selectListItems) {
                if (!selectListItem.isStar()
                        && !(selectListItem.getExpr() instanceof SlotRef)
                        && selectListItem.getAlias() == null) {
                    throw new SemanticException("Materialized view query statement select item must has alias except base select item");
                }
                if (!partitionExpIsInSelectItem) {
                    partitionExpIsInSelectItem = checkPartitionExpIsInSelectItem(selectListItem, partitionExpDesc);
                }
            }
            if (!partitionExpIsInSelectItem) {
                throw new SemanticException("Materialized view Partition by expression must in query statement select item");
            }

            // STEP 2
            // todo check exists in meta, name must different with view/mv/table which exists in metadata
            // todo check table and column exists in meta
            // todo check properties
            // STEP 3
            // todo some expression parse to mv item

            return null;
        }


        private void extractTableNamesInRelation(Relation relation, Set<TableName> tableNames) {
            if(relation instanceof TableRelation) {
                tableNames.add(((TableRelation) relation).getName());
            } else if (relation instanceof JoinRelation){
                extractTableNamesInRelation(((JoinRelation) relation).getLeft(), tableNames);
                extractTableNamesInRelation(((JoinRelation) relation).getRight(), tableNames);
            }
        }

        private boolean checkPartitionExpIsInSelectItem(SelectListItem selectListItem, PartitionExpDesc partitionExpDesc) {
            Expr expr = selectListItem.getExpr();
            if (partitionExpDesc.getFunctionName() != null) {
                if (!(expr instanceof FunctionCallExpr)) {
                    return false;
                }
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
                if (!functionCallExpr.getFnName().equals(partitionExpDesc.getFunctionName())) {
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
