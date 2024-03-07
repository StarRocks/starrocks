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

package com.starrocks.sql.analyzer;

//import com.google.common.collect.Lists;
//import com.starrocks.analysis.BinaryPredicate;
//import com.starrocks.analysis.BinaryType;
import com.google.common.collect.Lists;
import com.starrocks.analysis.AnalyticExpr;
import com.starrocks.analysis.AnalyticWindow;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.CaseExpr;
import com.starrocks.analysis.CaseWhenClause;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.OrderByElement;
//import com.starrocks.analysis.Predicate;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.MergeCase;
import com.starrocks.sql.ast.MergeDelete;
import com.starrocks.sql.ast.MergeInsert;
import com.starrocks.sql.ast.MergeStmt;
//import com.starrocks.sql.ast.MergeUpdate;
import com.starrocks.sql.ast.MergeUpdate;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.common.MetaUtils;
import org.antlr.v4.runtime.RuleContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toList;

public class MergeAnalyzer {
    private static final Logger LOG = LogManager.getLogger(MergeAnalyzer.class);

    public static void analyze(MergeStmt mergeStatement, ConnectContext session) {
        TableRelation target = (TableRelation) mergeStatement.target;
        TableName targetTableName = target.getName();
        MetaUtils.normalizationTableName(session, targetTableName);
        MetaUtils.getDatabase(session, targetTableName);
        Table table = MetaUtils.getTable(session, targetTableName);
        mergeStatement.setTable(table);

        TableRelation source = (TableRelation) mergeStatement.source;
        MetaUtils.normalizationTableName(session, source.getName());
        Table srcTable = MetaUtils.getTable(session, source.getName());

        List<Expr> args = new ArrayList<>();

        SelectList subSelectList = new SelectList();
        for (Column col : table.getBaseSchema()) {
            SelectListItem item = new SelectListItem(new SlotRef(target.getName(), col.getName()), col.getName());
            subSelectList.addItem(item);
            args.add(new SlotRef(target.getName(), col.getName()));
        }

        if (table.isIcebergTable()) {
            subSelectList.addItem(new SelectListItem(new SlotRef(target.getName(), "file_path"), "file_path"));
            subSelectList.addItem(new SelectListItem(new SlotRef(target.getName(), "pos"), "pos"));

            args.add(new SlotRef(target.getName(), "file_path"));
            args.add(new SlotRef(target.getName(), "pos"));
            table.getBaseSchema()
                    .add(new Column("file_path", Type.VARCHAR, true));

            table.getBaseSchema().add(new Column("pos", Type.BIGINT, true));
        }
        subSelectList.addItem(new SelectListItem(new IntLiteral(1,  Type.BIGINT), "present"));


        SelectRelation targetTableScan = new SelectRelation(subSelectList, target, null, null, null);
        List<OrderByElement> orderByElements = new ArrayList<>();
        targetTableScan.setOrderBy(orderByElements);
        QueryStatement targetTableScanStmt = new QueryStatement(targetTableScan);

        //targetTableScanStmt.setSo
        SubqueryRelation subQueryRelation = new SubqueryRelation(targetTableScanStmt);
        subQueryRelation.setAlias(new TableName(null, "a"));
        for (Expr expr : args) {
            ((SlotRef) expr).setTblName(subQueryRelation.getAlias());
        }

        JoinRelation joinRelation = new JoinRelation(JoinOperator.RIGHT_OUTER_JOIN,
                subQueryRelation, mergeStatement.source, mergeStatement.getExpression(), false);
        Expr mergePredicate = mergeStatement.getExpression();
        ((SlotRef) mergePredicate.getChild(0)).setTblName(subQueryRelation.getAlias());
//        Map<String,Integer> predicateCol = new HashMap<>();
        Set<Integer> predicateCol = new HashSet<>();
        processPredicate(predicateCol, mergePredicate, "a", args);

        SelectList selectList =  new SelectList();
        //construct case when expression
        Expr caseExpr = new SlotRef(subQueryRelation.getAlias(), "present"); // age列
        // 创建when表达式
        List<CaseWhenClause> caseWhenList = new ArrayList<CaseWhenClause>();


        for (MergeCase mergeCase : mergeStatement.getMergeCases()) {

            if (mergeCase instanceof MergeInsert) {
                Expr whenExpr = new IsNullPredicate(caseExpr, false);
                //extract value from source table and then insert into target table
                List<Expr> branchArgs = buildExpressionList(srcTable, source.getName());

                branchArgs.add(new SlotRef(subQueryRelation.getAlias(), "file_path"));
                branchArgs.add(new SlotRef(subQueryRelation.getAlias(), "pos"));
                branchArgs.add(new IntLiteral(1, Type.BIGINT));
                branchArgs.add(new IntLiteral(1, Type.BIGINT));
                FunctionCallExpr funcExpr = new FunctionCallExpr("row", branchArgs);
                caseWhenList.add(new CaseWhenClause(whenExpr, funcExpr));
            } else if (mergeCase instanceof MergeDelete) {
                //delete rows
                Expr whenExpr = new IsNullPredicate(caseExpr, true);
                List<Expr> branchArgs = new ArrayList<>(args);
                branchArgs.add(new IntLiteral(1,  Type.BIGINT));
                branchArgs.add(new IntLiteral(2,  Type.BIGINT));
                FunctionCallExpr funcExpr = new FunctionCallExpr("row", branchArgs);
                caseWhenList.add(new CaseWhenClause(whenExpr, funcExpr));
            } else if (mergeCase instanceof MergeUpdate) {
                //update rows from source side
                List<Expr> branchArgs = buildExpressionList(srcTable, source.getName());
                Expr whenExpr = new IsNullPredicate(caseExpr, true);

                branchArgs.add(new SlotRef(subQueryRelation.getAlias(), "file_path"));
                branchArgs.add(new SlotRef(subQueryRelation.getAlias(), "pos"));
                branchArgs.add(new IntLiteral(1, Type.BIGINT));
                branchArgs.add(new IntLiteral(3, Type.BIGINT));
                FunctionCallExpr funcExpr = new FunctionCallExpr("row", branchArgs);
                caseWhenList.add(new CaseWhenClause(whenExpr, funcExpr));
            }
        }

        CaseExpr caseWhenExpr = new CaseExpr(null, caseWhenList, null);
        SelectListItem presentItem = new SelectListItem(caseWhenExpr,  "data");
        selectList.addItem(presentItem);

        SelectRelation selectRelation = new SelectRelation(selectList, joinRelation,
                new IsNullPredicate(presentItem.getExpr(),  true),
                null, null);

        selectRelation.setOrderBy(orderByElements);
        QueryStatement query = new QueryStatement(selectRelation);
        SubqueryRelation topSubquery = new SubqueryRelation(query);
        topSubquery.setAlias(new TableName(null, "c"));

        SelectList finalSelectList = new SelectList();


        List<Integer> positionList = new ArrayList<>();

        //StructType fieldSchema = new StructType();

        ArrayList<StructField> structFields = new ArrayList<>();

        Integer i = 1;
        for (Column col : table.getBaseSchema()) {
            StructField field = new StructField("col" + i, col.getType());
            i = i + 1;
            structFields.add(field);
        }

        structFields.add(new StructField("col" + i, Type.INT));
        structFields.add(new StructField("col" + (i + 1), Type.INT));

        StructType structSchema = new StructType(structFields);
        List<Expr> partitionExprs = new ArrayList<>();

        i = 0;
        for (StructField field : structFields) {
            positionList.clear();
            positionList.add(i);
            SlotRef slot = new SlotRef(topSubquery.getAlias(), "data");
            slot.setType(structSchema);
            slot.setUsedStructFieldPos(positionList);
            slot.resetStructInfo();
            slot.setQualifiedName(QualifiedName.of(List.of("data", field.getName())));
            SelectListItem listItem = new SelectListItem(slot, field.getName());
            finalSelectList.addItem(listItem);
            if (predicateCol.contains(i)){
                partitionExprs.add(slot);
            }
            i = i + 1;
        }
        FunctionCallExpr fnCall = new FunctionCallExpr(FunctionSet.ROW_NUMBER, Lists.newArrayList());
        fnCall.setIsAnalyticFnCall(true);
        finalSelectList.addItem(new SelectListItem(new AnalyticExpr(fnCall, partitionExprs, new ArrayList<>(),null,null),"is_distinct"));

        SelectRelation finalSelectRelation = new SelectRelation(finalSelectList, topSubquery, null, null, null);
        finalSelectRelation.setOrderBy(orderByElements);
        QueryStatement finalQuery = new QueryStatement(finalSelectRelation);

        String sqlInPlainText = AstToSQLBuilder.toSQL(finalQuery);
        new QueryAnalyzer(session).analyze(finalQuery);
        mergeStatement.setQueryStatement(finalQuery);
        //throw new SemanticException(sqlInPlainText);
    }

//    private static void processPredicate(Map<String, Integer> predicateCol, Expr mergePredicate, String targeTableName, List<Expr> args) {
    private static void processPredicate(Set<Integer> predicateCol, Expr mergePredicate, String targeTableName, List<Expr> args) {
        if (mergePredicate instanceof CompoundPredicate){
            ArrayList<Expr> children = mergePredicate.getChildren();
            for (Expr child : children) {
                processPredicate(predicateCol, child, targeTableName, args);
            }
        }else if (mergePredicate instanceof BinaryPredicate){
            ArrayList<Expr> children = mergePredicate.getChildren();
            for (Expr child : children) {
                if (child instanceof SlotRef){
                    if (((SlotRef) child).getTblNameWithoutAnalyzed().getTbl().equalsIgnoreCase(targeTableName)){
                        String pkName = ((SlotRef) child).getColumnName();
                        for (int i = 0; i < args.size() ; i++) {
                            if (((SlotRef)args.get(i)).getColumnName().equalsIgnoreCase(pkName)){
                                predicateCol.add(i);
//                                predicateCol.put(pkName,i+1);
                                break;
                            }
                        }
                    }
                }else {
                    throw new SemanticException("expr[" + child +"] is not supported");
                }
            }
        }else {
            throw new SemanticException("predicate[" + mergePredicate +"] is not supported");
        }


    }

    private static List<Expr> buildExpressionList(Table table, TableName tblName) {
        List<Expr> list = new ArrayList<>();
        for (Column col : table.getBaseSchema()) {
            list.add(new SlotRef(tblName, col.getName()));
        }
        return list;
    }
}