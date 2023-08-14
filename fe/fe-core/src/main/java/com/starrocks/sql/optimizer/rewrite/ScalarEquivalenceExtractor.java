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


package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.PredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

// Equivalent derive
//
// e.g:
//  a = b AND b = 1  ==>  a = 1
//  a = b AND b > 1  ==>  a > 1
//  a = cos(b) AND b = abs(c) AND c = 1  ==>  a = cos(abs(1))
//  a = b AND abs(b) in (1, 2, 3)  ==>  abs(a) in (1, 2, 3)
//  a = abs(b) AND abs(b) IS NULL  ==>  a IS NULL
public class ScalarEquivalenceExtractor {
    private final EquivalenceBuilder equivalenceBuilder = new EquivalenceBuilder();

    // column-predicate relation
    private final Map<ColumnRefOperator, Set<ScalarOperator>> columnValuesMap = Maps.newHashMap();

    // column equivalence relation
    private final Map<ColumnRefOperator, Set<ScalarOperator>> columnRefEquivalenceMap = Maps.newHashMap();

    /**
     * Derive equivalent predicate by column
     */
    public Set<ScalarOperator> getEquivalentScalar(ColumnRefOperator columnRef) {
        Set<ScalarOperator> search = Sets.newLinkedHashSet();
        search.add(columnRef);

        Set<ScalarOperator> searchResult = Sets.newLinkedHashSet();
        Set<ScalarOperator> memory = Sets.newHashSet();
        searchEquivalentRefs(memory, search, searchResult);
        Set<ScalarOperator> valueResult = searchEquivalentValues(searchResult, columnRef);

        // merge all equivalent result
        searchResult.remove(columnRef);
        Set<ScalarOperator> result = Sets.newLinkedHashSet();
        for (ScalarOperator s : searchResult) {
            result.add(new BinaryPredicateOperator(BinaryType.EQ, columnRef, s));
        }

        result.addAll(valueResult);
        return result;
    }

    /**
     * Derive equivalent column by column
     */
    public Set<ScalarOperator> getEquivalentColumnRefs(ColumnRefOperator columnRef) {
        Set<ScalarOperator> search = Sets.newLinkedHashSet();
        search.add(columnRef);

        Set<ScalarOperator> searchResult = Sets.newLinkedHashSet();
        Set<ScalarOperator> memory = Sets.newHashSet();
        searchEquivalentRefs(memory, search, searchResult);

        searchResult.remove(columnRef);
        return searchResult;
    }

    private void searchEquivalentRefs(Set<ScalarOperator> memo, Set<ScalarOperator> search,
                                      Set<ScalarOperator> result) {
        Set<ScalarOperator> tempResult = Sets.newLinkedHashSet();

        for (ScalarOperator operator : search) {
            if (memo.contains(operator)) {
                continue;
            }

            // Derive columnRef = columnRef, like {a = b, b = c} => {a = c}
            if (operator.isColumnRef()) {
                ColumnRefOperator ref = (ColumnRefOperator) operator;
                tempResult.addAll(columnRefEquivalenceMap.getOrDefault(ref, Collections.emptySet()));
                continue;
            } else if (operator.isConstant()) {
                tempResult.add(operator);
                continue;
            }

            // Derive columnRef = function(columnRef), like {a = abs(b), b = sin(c), c = 1} => {a = abs(sin(1))}
            // The Derive process is too expensive, so the number of right columnRef must less or equal 1
            ColumnRefOperator ref = Utils.extractColumnRef(operator).get(0);

            Set<ScalarOperator> childSearch = Sets.newLinkedHashSet();
            childSearch.add(ref);

            Set<ScalarOperator> childResult = Sets.newLinkedHashSet();

            searchEquivalentRefs(memo, childSearch, childResult);

            for (ScalarOperator so : childResult) {
                Map<ColumnRefOperator, ScalarOperator> rewriteMap = new HashMap<>();
                rewriteMap.put(ref, so);

                ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(rewriteMap);

                // will modify the operator, must use clone
                ScalarOperator r = rewriter.rewrite(operator);
                tempResult.add(r);
            }
        }

        if (tempResult.isEmpty()) {
            return;
        }

        result.addAll(tempResult);
        memo.addAll(search);

        search.clear();
        search.addAll(tempResult);
        searchEquivalentRefs(memo, search, result);
    }

    private Set<ScalarOperator> searchEquivalentValues(Set<ScalarOperator> search,
                                                       ColumnRefOperator replaceColumnRef) {
        Set<ScalarOperator> result = Sets.newLinkedHashSet();
        Map<ColumnRefOperator, ScalarOperator> rewriteMap = Maps.newHashMap();

        for (ScalarOperator operator : search) {
            // can't resolve function(columnRef)
            if (!operator.isColumnRef()) {
                continue;
            }

            rewriteMap.put((ColumnRefOperator) operator, replaceColumnRef);
        }

        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(rewriteMap);

        for (ScalarOperator operator : search) {
            if (!operator.isColumnRef()) {
                continue;
            }

            Set<ScalarOperator> values =
                    columnValuesMap.getOrDefault((ColumnRefOperator) operator, Collections.emptySet());
            // avoid use Collection::toSet
            values.stream().map(rewriter::rewrite).forEach(result::add);
        }

        return result;
    }

    // Operators must be extract conjuncts
    public void union(List<ScalarOperator> operators) {
        for (ScalarOperator op : operators) {
            op.accept(equivalenceBuilder, null);
        }
    }

    // Requirement:
    // 1. ScalarOperator has been normalize(constant operator always in right)
    // 2. ScalarOperator left child only contain one ColumnRefOperator
    private class EquivalenceBuilder extends ScalarOperatorVisitor<Void, Void> {
        @Override
        public Void visit(ScalarOperator scalarOperator, Void context) {
            return null;
        }

        @Override
        public Void visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
            List<ColumnRefOperator> child1Lists = Utils.extractColumnRef(predicate.getChild(0));
            if (child1Lists.size() != 1) {
                return null;
            }

            // add values equivalence, just save last one in fact
            ColumnRefOperator child1 = child1Lists.get(0);
            if (predicate.getChild(1).isConstant()) {
                // handle column-value relation
                if (!columnValuesMap.containsKey(child1)) {
                    columnValuesMap.put(child1, Sets.newLinkedHashSet());
                }

                columnValuesMap.get(child1).add(predicate);

                // handle column-constant relation, should check left is column, use for optimizer derive function
                if (!predicate.getChild(0).isColumnRef()) {
                    return null;
                }

                child1 = (ColumnRefOperator) predicate.getChild(0);
                if (BinaryType.EQ.equals(predicate.getBinaryType()) &&
                        predicate.getChild(1).isConstantRef()) {
                    if (!columnRefEquivalenceMap.containsKey(child1)) {
                        columnRefEquivalenceMap.put(child1, Sets.newLinkedHashSet());
                    }

                    columnRefEquivalenceMap.get(child1).add(predicate.getChild(1));
                }
                return null;
            }

            // column equivalence only support EQ
            if (!BinaryType.EQ.equals(predicate.getBinaryType())) {
                return null;
            }

            // add (right column)-(left column) relation in columnRef-columnRef mapping
            ScalarOperator child2 = predicate.getChild(1);
            if (child2.isColumnRef()) {
                ColumnRefOperator ref2 = (ColumnRefOperator) child2;
                Set<ScalarOperator> child2Set = columnRefEquivalenceMap.getOrDefault(ref2, Sets.newLinkedHashSet());
                child2Set.add(predicate.getChild(0));
                columnRefEquivalenceMap.put(ref2, child2Set);
            }

            if (!predicate.getChild(0).isColumnRef()) {
                return null;
            }

            // columnRef-Function(columnRef) mapping
            Set<ScalarOperator> child1Set = columnRefEquivalenceMap.getOrDefault(child1, Sets.newLinkedHashSet());

            // The search complexity will increase greatly with the number of columnRefs, so limit the number max 1
            if (Utils.countColumnRef(child2) > 1) {
                return null;
            }

            child1Set.add(child2);
            columnRefEquivalenceMap.put(child1, child1Set);

            return null;
        }

        @Override
        public Void visitInPredicate(InPredicateOperator predicate, Void context) {
            return buildEquivalenceValue(predicate);
        }

        @Override
        public Void visitIsNullPredicate(IsNullPredicateOperator predicate, Void context) {
            return buildEquivalenceValue(predicate);
        }

        @Override
        public Void visitLikePredicateOperator(LikePredicateOperator predicate, Void context) {
            return buildEquivalenceValue(predicate);
        }

        private Void buildEquivalenceValue(PredicateOperator predicate) {
            List<ColumnRefOperator> child1Lists = Utils.extractColumnRef(predicate.getChild(0));
            if (child1Lists.size() != 1) {
                return null;
            }

            // add values equivalence, just save last one in fact
            ColumnRefOperator child1 = child1Lists.get(0);

            if (predicate.getChildren().stream().skip(1).allMatch(ScalarOperator::isConstant)) {
                if (!columnValuesMap.containsKey(child1)) {
                    columnValuesMap.put(child1, Sets.newLinkedHashSet());
                }
                columnValuesMap.get(child1).add(predicate);
            }

            return null;
        }
    }

}
