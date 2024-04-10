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

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.common.Pair;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.PartitionExprAnalyzer;
import com.starrocks.sql.analyzer.SlotRefResolver;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UnionRelation;

import java.util.List;
import java.util.Map;
import java.util.Set;


// The MvJoinFilter class is responsible for optimizing partition pruning during the refresh of materialized views.
// It accomplishes this by analyzing join conditions and efficiently mapping expressions to corresponding slot references.
// This optimization is crucial for handling large datasets where partition pruning can significantly reduce refresh execution time.
// The class includes mechanisms to process various types of expressions and predicates, ensuring compatibility with
// different SQL functions, only 'date_trunc' function is supported now.
public class MvJoinFilter {
    static final Set<String> FN_NAME_TO_PARTITION = Sets.newHashSet("date_trunc");

    public static void insertIntoMapper(List<Map<Expr, SlotRef>> mapper,
                                        Pair<Expr, SlotRef> a, Pair<Expr, SlotRef> b) {
        Map<Expr, SlotRef> targetMap = null;

        // Iterate through the LinkedList to find a HashSet that already contains either a or b
        for (Map<Expr, SlotRef> map : mapper) {
            if (map.containsKey(a.first) || map.containsKey(b.first)) {
                targetMap = map;
                break;
            }
        }

        // If a matching HashSet is found, add both a and b to it. If not, create a new HashSet
        // that includes a and b, and add this new HashSet to the LinkedList.
        if (targetMap != null) {
            targetMap.put(a.first, a.second);
            targetMap.put(b.first, b.second);
        } else {
            Map<Expr, SlotRef> newMap = Maps.newHashMap();
            newMap.put(a.first, a.second);
            newMap.put(b.first, b.second);
            mapper.add(newMap);
        }
    }

    // Only date_trunc are supported
    // A' => A, date_trunc(A') => date_trunc(A)
    static Pair<Expr, SlotRef> getSupportMvPartitionExpr(Expr expr) {
        if (expr instanceof SlotRef) {
            return new Pair<>(expr, (SlotRef) expr);
        }
        if (expr instanceof FunctionCallExpr) {
            FunctionCallExpr functionCallExpr = ((FunctionCallExpr) expr);
            String functionName = functionCallExpr.getFnName().getFunction();
            if (!FN_NAME_TO_PARTITION.contains(functionName.toLowerCase())) {
                return null;
            }
            if (!(functionCallExpr.getChild(1) instanceof SlotRef)) {
                return null;
            }
            return new Pair<>(functionCallExpr, (SlotRef) functionCallExpr.getChild(1));
        }
        return null;
    }

    private static Expr getExprFromRelations(Expr expr, Relation[] relations) {
        Preconditions.checkArgument(relations.length == 2);
        Expr resolved = SlotRefResolver.resolveExpr(expr, relations[0]);
        if (resolved != null) {
            return resolved;
        }
        return SlotRefResolver.resolveExpr(expr, relations[1]);
    }

    private static Pair<Expr, Expr> getBinaryPredicate(BinaryPredicate expr, Relation[] relations) {
        if (!BinaryPredicate.IS_EQ_PREDICATE.apply(expr)) {
            return null;
        }
        Expr expr1 = getExprFromRelations(expr.getChild(0), relations);
        Expr expr2 = getExprFromRelations(expr.getChild(1), relations);
        if (expr1 == null || expr2 == null) {
            return null;
        }
        return Pair.create(expr1, expr2);
    }

    private static List<Pair<Expr, Expr>> getCompoundPredicate(CompoundPredicate compoundPredicate, Relation[] relations) {
        List<Pair<Expr, Expr>> predicateExprs = Lists.newArrayList();
        if (compoundPredicate.getOp() != CompoundPredicate.Operator.AND) {
            return predicateExprs;
        }
        for (Expr expr : compoundPredicate.getChildren()) {
            predicateExprs.addAll(mvGetJoinEqualPredicate(expr, relations));
        }
        return predicateExprs;
    }

    // A = B => {A, B}
    // A = B OR C = D => {}
    // A = B AND C = D => {A, B}, {C, D}
    // A = B AND A = C => {A, B}, {A, C}
    // A = B AND (C = D OR E = F) => {A, B}
    private static List<Pair<Expr, Expr>> mvGetJoinEqualPredicate(Expr expr, Relation[] relations) {
        List<Pair<Expr, Expr>> predicateExprs = Lists.newArrayList();
        if (expr instanceof BinaryPredicate) {
            Pair<Expr, Expr> exprs = getBinaryPredicate((BinaryPredicate) expr, relations);
            if (exprs != null) {
                predicateExprs.add(exprs);
            }
        }
        if (expr instanceof CompoundPredicate) {
            predicateExprs.addAll(getCompoundPredicate((CompoundPredicate) expr, relations));
        }
        return predicateExprs;
    }

    // Constructs a mapping of partitioned expressions to slot references for join operations in a materialized view.
    public static Map<Expr, SlotRef> getPartitionJoinMap(Expr partitionExpr, QueryStatement statement) {
        Map<Expr, SlotRef> mapper = Maps.newHashMap();
        if (statement.getQueryRelation() instanceof UnionRelation) {
            UnionRelation relations = (UnionRelation) statement.getQueryRelation();
            SlotRef slot = MaterializedView.getMvPartitionSlotRef(partitionExpr);
            Integer slotIndex = null;
            for (int i = 0; i < relations.getOutputExpression().size(); i++) {
                String column = relations.getRelations().get(0).getColumnOutputNames().get(i);
                if (com.starrocks.common.util.StringUtils.areColumnNamesEqual(slot.getColumnName(), column)) {
                    slotIndex = i;
                    break;
                }
            }
            if (slotIndex == null) {
                return mapper;
            }
            for (QueryRelation relation : relations.getRelations()) {
                Expr unionExpr = partitionExpr.clone();
                if (unionExpr instanceof FunctionCallExpr) {
                    FunctionCallExpr functionCallExpr = (FunctionCallExpr) unionExpr;
                    if (functionCallExpr.getFnName().getFunction().equalsIgnoreCase(FunctionSet.DATE_TRUNC)) {
                        functionCallExpr.setChild(1, relation.getOutputExpression().get(slotIndex));
                    } else if (functionCallExpr.getFnName().getFunction().equalsIgnoreCase(FunctionSet.STR2DATE)) {
                        functionCallExpr.setChild(0, relation.getOutputExpression().get(slotIndex));
                    }
                } else {
                    unionExpr = relation.getOutputExpression().get(slotIndex);
                }
                mapper.putAll(getPartitionJoinMap(unionExpr, new QueryStatement(relation)));
            }
            return mapper;
        }
        Expr partitionRefExpr = SlotRefResolver.resolveExpr(partitionExpr, statement);
        PartitionExprAnalyzer.analyzePartitionExpr(partitionRefExpr,
                MaterializedView.getMvPartitionSlotRef(partitionRefExpr));
        List<Pair<Expr, Expr>> predicateExprs = Lists.newArrayList();
        for (Pair<Expr, Relation[]> predicate : AnalyzerUtils.collectAllJoinPredicatesRelations(statement)) {
            predicateExprs.addAll(mvGetJoinEqualPredicate(predicate.first, predicate.second));
        }
        SlotRef partitionSlotRef = MaterializedView.getMvPartitionSlotRef(partitionRefExpr);
        //TODO: Should make sure the slotref (may be a wrapper?) with different desc and label is equal
        //TODO: hive table with uppercase table name
        //clear desc to avoid the slotref is not equal
        partitionSlotRef.setDesc(null);
        if (predicateExprs.isEmpty()) {
            // no join predicate
            mapper.put(partitionRefExpr, partitionSlotRef);
            return mapper;
        }
        class TableInfo {
            final List<Expr> exprs = Lists.newArrayList();
            final List<TableRelation> relations = Lists.newArrayList();
        }
        Map<TableName, TableInfo> tableInfos = Maps.newHashMap();
        List<TableRelation> tables = AnalyzerUtils.collectTableRelations(statement);
        tables.forEach(table -> {
            TableName tblName = table.getName();
            TableInfo tableInfo = tableInfos.getOrDefault(tblName, new TableInfo());
            tableInfo.relations.add(table);
            tableInfos.put(tblName, tableInfo);
        });
        List<Map<Expr, SlotRef>> mappers = Lists.newArrayList();
        predicateExprs.forEach(pair -> {
            Pair<Expr, SlotRef> node1 = getSupportMvPartitionExpr(pair.first);
            Pair<Expr, SlotRef> node2 = getSupportMvPartitionExpr(pair.second);
            if (node1 == null || node2 == null) {
                return;
            }
            insertIntoMapper(mappers, node1, node2);
            tableInfos.get(node1.second.getTblNameWithoutAnalyzed()).exprs.add(node1.first);
            tableInfos.get(node2.second.getTblNameWithoutAnalyzed()).exprs.add(node2.first);
        });

        for (Map<Expr, SlotRef> map : mappers) {
            if (map.containsKey(partitionRefExpr)) {
                mapper.putAll(map);
                break;
            }
        }
        if (mapper.isEmpty()) {
            mapper.put(partitionRefExpr, partitionSlotRef);
            return mapper;
        }

        tableInfos.forEach((table, tableInfo) -> tableInfo.exprs.removeIf(expr -> !mapper.containsKey(expr)));

        // For a table appears more than once, we should remove them
        // For example:
        // t1 join a on t1.k1 = a.k1 join a on t1.k1 = date_trunc(a.k1)
        // t1 join a on t1.k1 = a.k1 join a on t1.k1 = a.k2
        mapper.entrySet().removeIf(entry -> {
            TableName table = entry.getValue().getTblNameWithoutAnalyzed();
            TableInfo tableInfo = tableInfos.get(table);
            return tableInfo.relations.size() != 1;
        });

        // at least the partition expr should be in mapper
        mapper.put(partitionRefExpr, partitionSlotRef);
        return mapper;
    }
}
