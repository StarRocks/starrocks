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

import com.google.common.collect.Lists;
import com.starrocks.catalog.TableName;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprSubstitutionMap;
import com.starrocks.sql.ast.expression.ExprSubstitutionVisitor;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.SlotRef;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

final class MergeIntoPredicateDeriver {
    private MergeIntoPredicateDeriver() {
    }

    static Expr appendDerivedTargetPredicate(Relation sourceRelation, Expr mergeCondition, TableName targetSlotTableName) {
        Expr derivedPredicate = deriveTargetPredicate(sourceRelation, mergeCondition, targetSlotTableName);
        if (derivedPredicate == null) {
            return mergeCondition;
        }
        return new CompoundPredicate(CompoundPredicate.Operator.AND, mergeCondition, derivedPredicate);
    }

    static Expr deriveTargetPredicate(Relation sourceRelation, Expr mergeCondition, TableName targetSlotTableName) {
        SourcePredicateInfo sourcePredicateInfo = extractSourcePredicate(sourceRelation);
        if (sourcePredicateInfo == null) {
            return null;
        }
        Expr sourcePredicate = sourcePredicateInfo.predicate;
        if (sourcePredicate == null || mergeCondition == null || targetSlotTableName == null) {
            return null;
        }

        Map<String, SlotRef> sourceToTargetSlots = collectSourceToTargetSlots(
                mergeCondition, targetSlotTableName, sourcePredicateInfo);
        if (sourceToTargetSlots.isEmpty()) {
            return null;
        }

        List<Expr> derivedConjuncts = Lists.newArrayList();
        for (Expr conjunct : AnalyzerUtils.extractConjuncts(sourcePredicate)) {
            Expr rewritten = rewriteSourcePredicateToTarget(conjunct, sourceToTargetSlots);
            if (rewritten != null) {
                derivedConjuncts.add(rewritten);
            }
        }
        return combineAnd(derivedConjuncts);
    }

    private static SourcePredicateInfo extractSourcePredicate(Relation sourceRelation) {
        // Keep this deliberately conservative. We only derive predicates from a materialized
        // USING subquery or direct SelectRelation over a non-join source, and only when the
        // source output column is a transparent SlotRef (or SELECT * from one table). Bare
        // TableRelation sources and CTE references do not carry a local predicate here, and
        // non-slot projections such as `a + 1 AS id` are not equivalent to their input slot.
        if (sourceRelation instanceof SubqueryRelation subqueryRelation) {
            QueryRelation queryRelation = subqueryRelation.getQueryStatement().getQueryRelation();
            if (queryRelation instanceof SelectRelation selectRelation
                    && isSimpleSelectSource(selectRelation.getRelation())) {
                return new SourcePredicateInfo(selectRelation.getPredicate(),
                        collectTransparentOutputSlots(selectRelation),
                        hasPureTableStarProjection(selectRelation));
            }
        } else if (sourceRelation instanceof SelectRelation selectRelation
                && isSimpleSelectSource(selectRelation.getRelation())) {
            return new SourcePredicateInfo(selectRelation.getPredicate(),
                    collectTransparentOutputSlots(selectRelation),
                    hasPureTableStarProjection(selectRelation));
        }
        return null;
    }

    private static boolean isSimpleSelectSource(Relation relation) {
        return relation == null || !(relation instanceof JoinRelation);
    }

    private static Map<String, String> collectTransparentOutputSlots(SelectRelation selectRelation) {
        Map<String, String> outputToSourceSlots = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Set<String> ambiguousOutputSlots = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        if (selectRelation.getSelectList() == null) {
            return outputToSourceSlots;
        }

        for (SelectListItem item : selectRelation.getSelectList().getItems()) {
            if (item.isStar() || !(item.getExpr() instanceof SlotRef sourceSlot)) {
                continue;
            }

            String sourceColumnName = sourceSlot.getColumnName();
            String outputColumnName = item.getAlias() == null ? sourceColumnName : item.getAlias();
            if (sourceColumnName == null || outputColumnName == null) {
                continue;
            }

            String normalizedOutputColumnName = normalizeName(outputColumnName);
            if (outputToSourceSlots.containsKey(normalizedOutputColumnName)) {
                outputToSourceSlots.remove(normalizedOutputColumnName);
                ambiguousOutputSlots.add(normalizedOutputColumnName);
            } else if (!ambiguousOutputSlots.contains(normalizedOutputColumnName)) {
                outputToSourceSlots.put(normalizedOutputColumnName, sourceColumnName);
            }
        }
        return outputToSourceSlots;
    }

    private static boolean hasPureTableStarProjection(SelectRelation selectRelation) {
        return selectRelation.getRelation() instanceof TableRelation
                && selectRelation.getSelectList() != null
                && selectRelation.getSelectList().getItems().size() == 1
                && selectRelation.getSelectList().getItems().get(0).isStar();
    }

    private static Map<String, SlotRef> collectSourceToTargetSlots(
            Expr mergeCondition, TableName targetSlotTableName, SourcePredicateInfo sourcePredicateInfo) {
        Map<String, SlotRef> sourceToTargetSlots = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Expr conjunct : AnalyzerUtils.extractConjuncts(mergeCondition)) {
            if (!(conjunct instanceof BinaryPredicate binaryPredicate)
                    || binaryPredicate.getOp() != BinaryType.EQ) {
                continue;
            }

            Expr left = binaryPredicate.getChild(0);
            Expr right = binaryPredicate.getChild(1);
            if (left instanceof SlotRef leftSlot && right instanceof SlotRef rightSlot) {
                putSourceToTargetSlot(sourceToTargetSlots, leftSlot, rightSlot, targetSlotTableName,
                        sourcePredicateInfo);
                putSourceToTargetSlot(sourceToTargetSlots, rightSlot, leftSlot, targetSlotTableName,
                        sourcePredicateInfo);
            }
        }
        return sourceToTargetSlots;
    }

    private static void putSourceToTargetSlot(Map<String, SlotRef> sourceToTargetSlots, SlotRef sourceSlot,
                                              SlotRef targetSlot, TableName targetSlotTableName,
                                              SourcePredicateInfo sourcePredicateInfo) {
        if (isTargetSlot(sourceSlot, targetSlotTableName) || !isTargetSlot(targetSlot, targetSlotTableName)) {
            return;
        }
        if (sourceSlot.getColumnName() == null || targetSlot.getColumnName() == null) {
            return;
        }

        String sourceColumnName = sourcePredicateInfo.getSourceColumnName(sourceSlot.getColumnName());
        if (sourceColumnName == null) {
            return;
        }
        sourceToTargetSlots.put(normalizeName(sourceColumnName), (SlotRef) targetSlot.clone());
    }

    private static Expr rewriteSourcePredicateToTarget(Expr sourcePredicate,
                                                       Map<String, SlotRef> sourceToTargetSlots) {
        List<FunctionCallExpr> functionCalls = Lists.newArrayList();
        sourcePredicate.collect(FunctionCallExpr.class, functionCalls);
        if (functionCalls.stream().anyMatch(FunctionCallExpr::isNondeterministicBuiltinFnName)) {
            return null;
        }

        List<SlotRef> slotRefs = Lists.newArrayList();
        sourcePredicate.collect(SlotRef.class, slotRefs);
        if (slotRefs.isEmpty()) {
            return null;
        }

        ExprSubstitutionMap substitutionMap = new ExprSubstitutionMap();
        for (SlotRef slotRef : slotRefs) {
            SlotRef targetSlot = sourceToTargetSlots.get(normalizeName(slotRef.getColumnName()));
            if (targetSlot == null) {
                return null;
            }
            substitutionMap.put(slotRef, targetSlot);
        }
        return ExprSubstitutionVisitor.rewrite(sourcePredicate, substitutionMap);
    }

    private static Expr combineAnd(List<Expr> conjuncts) {
        if (conjuncts.isEmpty()) {
            return null;
        }
        Expr result = conjuncts.get(0);
        for (int i = 1; i < conjuncts.size(); i++) {
            result = new CompoundPredicate(CompoundPredicate.Operator.AND, result, conjuncts.get(i));
        }
        return result;
    }

    private static boolean isTargetSlot(SlotRef slotRef, TableName targetSlotTableName) {
        TableName slotTableName = slotRef.getTblName();
        return slotTableName != null
                && slotTableName.getTbl() != null
                && targetSlotTableName.getTbl() != null
                && slotTableName.getTbl().equalsIgnoreCase(targetSlotTableName.getTbl());
    }

    private static String normalizeName(String name) {
        return name == null ? null : name.toLowerCase(Locale.ROOT);
    }

    private static final class SourcePredicateInfo {
        private final Expr predicate;
        private final Map<String, String> outputToSourceSlots;
        private final boolean pureTableStarProjection;

        private SourcePredicateInfo(Expr predicate, Map<String, String> outputToSourceSlots,
                                    boolean pureTableStarProjection) {
            this.predicate = predicate;
            this.outputToSourceSlots = outputToSourceSlots;
            this.pureTableStarProjection = pureTableStarProjection;
        }

        private String getSourceColumnName(String outputColumnName) {
            String sourceColumnName = outputToSourceSlots.get(normalizeName(outputColumnName));
            if (sourceColumnName != null) {
                return sourceColumnName;
            }
            return pureTableStarProjection ? outputColumnName : null;
        }
    }
}
