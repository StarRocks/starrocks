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


package com.starrocks.connector;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TupleDescriptor;

import java.util.List;

public class PredicateUtils {
    /**
     * Analyzes 'conjuncts', populates 'minMaxTuple' with slots for statistics values,
     * and populates 'minMaxConjuncts' with conjuncts pointing into the 'minMaxTuple'.
     * Only conjuncts of the form <slot> <op> <constant> are supported,
     * and <op> must be one of LT, LE, GE, GT, or EQ.
     */
    public static void computeMinMaxTupleAndConjuncts(Analyzer analyzer,
                                                      TupleDescriptor minMaxTuple,
                                                      List<Expr> minMaxConjuncts,
                                                      List<Expr> conjuncts) {
        // Adds predicates for scalar
        for (Expr pred : conjuncts) {
            computeMinMaxPredicate(analyzer, pred, minMaxTuple, minMaxConjuncts);
        }
        minMaxTuple.computeMemLayout();
    }

    private static void buildStatsPredicate(Analyzer analyzer,
                                            SlotRef slotRef,
                                            BinaryPredicate binaryPred,
                                            BinaryPredicate.Operator op,
                                            TupleDescriptor minMaxTuple,
                                            List<Expr> minMaxConjuncts) {
        Expr literal = binaryPred.getChild(1);
        Preconditions.checkState(literal.isConstant());

        // Make a new slot descriptor, which adds it to the tuple descriptor.
        SlotDescriptor slotDesc = analyzer.getDescTbl().copySlotDescriptor(minMaxTuple, slotRef.getDesc());
        SlotRef slot = new SlotRef(slotDesc);
        BinaryPredicate statsPred = new BinaryPredicate(op, slot, literal);
        statsPred.analyzeNoThrow(analyzer);
        minMaxConjuncts.add(statsPred);
    }

    private static void computeMinMaxPredicate(Analyzer analyzer,
                                               Expr pred,
                                               TupleDescriptor minMaxTuple,
                                               List<Expr> minMaxConjuncts) {
        if (pred instanceof BinaryPredicate) {
            computeBinaryMinMaxPredicate(analyzer, (BinaryPredicate) pred, minMaxTuple, minMaxConjuncts);
        } else if (pred instanceof InPredicate) {
            computeInListMinMaxPredicate(analyzer, (InPredicate) pred, minMaxTuple, minMaxConjuncts);
        }
    }

    private static void computeBinaryMinMaxPredicate(Analyzer analyzer,
                                                     BinaryPredicate binaryPred,
                                                     TupleDescriptor minMaxTuple,
                                                     List<Expr> minMaxConjuncts) {
        SlotRef slotRef = binaryPred.getChild(0).unwrapSlotRef(true);
        if (slotRef == null) {
            return;
        }

        Expr literal = binaryPred.getChild(1);
        if (!literal.isConstant()) {
            return;
        }
        if (Expr.IS_NULL_LITERAL.apply(literal)) {
            return;
        }

        if (BinaryPredicate.IS_RANGE_PREDICATE.apply(binaryPred)) {
            buildStatsPredicate(analyzer, slotRef, binaryPred, binaryPred.getOp(), minMaxTuple, minMaxConjuncts);
        } else if (BinaryPredicate.IS_EQ_PREDICATE.apply(binaryPred)) {
            buildStatsPredicate(analyzer, slotRef, binaryPred, BinaryPredicate.Operator.GE, minMaxTuple,
                    minMaxConjuncts);
            buildStatsPredicate(analyzer, slotRef, binaryPred, BinaryPredicate.Operator.LE, minMaxTuple,
                    minMaxConjuncts);
        }
    }

    private static void computeInListMinMaxPredicate(Analyzer analyzer,
                                                     InPredicate inPred,
                                                     TupleDescriptor minMaxTuple,
                                                     List<Expr> minMaxConjuncts) {
        SlotRef slotRef = inPred.getChild(0).unwrapSlotRef(true);
        if (slotRef == null) {
            return;
        }

        if (inPred.isNotIn()) {
            return;
        }
        List<Expr> children = inPred.getChildren();
        LiteralExpr min = null;
        LiteralExpr max = null;
        for (int i = 1; i < children.size(); i++) {
            Expr child = children.get(i);
            if (!Expr.IS_LITERAL.apply(child) || Expr.IS_NULL_LITERAL.apply(child)) {
                return;
            }

            LiteralExpr literal = (LiteralExpr) child;
            if (min == null || literal.compareLiteral(min) < 0) {
                min = literal;
            }
            if (max == null || literal.compareLiteral(max) > 0) {
                max = literal;
            }
        }

        Preconditions.checkState(min != null);
        Preconditions.checkState(max != null);
        BinaryPredicate minBound = new BinaryPredicate(BinaryPredicate.Operator.GE,
                children.get(0).clone(), min.clone());
        BinaryPredicate maxBound = new BinaryPredicate(BinaryPredicate.Operator.LE,
                children.get(0).clone(), max.clone());
        buildStatsPredicate(analyzer, slotRef, minBound, minBound.getOp(), minMaxTuple, minMaxConjuncts);
        buildStatsPredicate(analyzer, slotRef, maxBound, maxBound.getOp(), minMaxTuple, minMaxConjuncts);
    }
}
