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

package com.starrocks.sql.automv.pieces;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.common.Pair;
import com.starrocks.common.util.UnionFind;
import com.starrocks.sql.automv.pn.Op;
import com.starrocks.sql.automv.pn.SymTab;
import org.apache.thrift.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class HoistTriple {
    public static final int BIT_LHS_SHIFT = 0;
    public static final int BIT_RHS_SHIFT = 1;
    public static final int BIT_ON_SHIFT = 2;
    public static final int BIT_LHS = 1;
    public static final int BIT_RHS = 2;
    public static final int BIT_ON = 4;

    public static final int BIT_NONE = 0;
    public static final int BIT_LHS_ON = BIT_LHS | BIT_ON;
    public static final int BIT_LHS_RHS = BIT_LHS | BIT_RHS;
    public static final int BIT_RHS_ON = BIT_RHS | BIT_ON;
    public static final int BIT_LHS_RHS_ON = BIT_LHS | BIT_RHS | BIT_ON;
    private final int bits;
    private final Op[] ops;

    HoistTriple(int bits, @Nullable Op lhsOp, @Nullable Op rhsOp, @Nullable Op onOp) {
        Preconditions.checkArgument(!Boolean.logicalXor((bits & BIT_LHS) == BIT_LHS, lhsOp != null));
        Preconditions.checkArgument(!Boolean.logicalXor((bits & BIT_RHS) == BIT_RHS, rhsOp != null));
        Preconditions.checkArgument(!Boolean.logicalXor((bits & BIT_ON) == BIT_ON, onOp != null));
        this.bits = bits;
        ops = new Op[] {lhsOp, rhsOp, onOp};
    }

    public static List<HoistTriple> prepare(List<Op> onEqConjuncts,
                                            List<Op> onOtherConjuncts,
                                            List<Op> lhsConjuncts,
                                            List<Op> rhsConjuncts) {
        UnionFind<Integer> eqColumns = new UnionFind<>();
        onEqConjuncts.forEach(eqConj -> eqColumns.union(eqConj.arg(0).getId(), eqConj.arg(1).getId()));
        List<Pair<Integer, Op>> conjunctsWithMark = Lists.newArrayList();
        lhsConjuncts.forEach(conj -> conjunctsWithMark.add(Pair.create(BIT_LHS_SHIFT, conj)));
        rhsConjuncts.forEach(conj -> conjunctsWithMark.add(Pair.create(BIT_RHS_SHIFT, conj)));
        onOtherConjuncts.forEach(conj -> conjunctsWithMark.add(Pair.create(BIT_ON_SHIFT, conj)));

        Map<String, List<Pair<Integer, Op>>> candidateEquivGroups =
                conjunctsWithMark.stream().collect(Collectors.groupingBy(p -> p.second.toString()));

        final UnionFind<Integer> sealedEqColumns = eqColumns.sealed();
        List<List<Op>> restConjuncts = ImmutableList.of(
                Lists.newArrayList(),
                Lists.newArrayList(),
                Lists.newArrayList());
        List<HoistTriple> triples = Lists.newArrayList();
        for (List<Pair<Integer, Op>> candidateGroup : candidateEquivGroups.values()) {
            int n = candidateGroup.size();
            if (n == 1) {
                candidateGroup.forEach(p -> restConjuncts.get(p.first).add(p.second));
                continue;
            }
            UnionFind<Pair<Integer, Op>> equivPredicates = new UnionFind<>();
            for (int i = 0; i < candidateGroup.size(); ++i) {
                for (int j = i + 1; j < candidateGroup.size(); ++j) {
                    Pair<Integer, Op> a = candidateGroup.get(i);
                    Pair<Integer, Op> b = candidateGroup.get(j);
                    if (SymTab.equivalent(a.second, b.second, sealedEqColumns)) {
                        equivPredicates.union(a, b);
                    }
                }
            }
            for (Set<Pair<Integer, Op>> equivGroup : equivPredicates.getAllGroups()) {
                if (equivGroup.size() == 1) {
                    equivGroup.forEach(opWithMark -> restConjuncts.get(opWithMark.first).add(opWithMark.second));
                }
                int bits = equivGroup.stream().map(p -> (1 << p.first)).reduce(0, (a, b) -> a | b);
                Map<Integer, List<Pair<Integer, Op>>> opGroup =
                        equivGroup.stream().collect(Collectors.groupingBy(p -> p.first));
                Op lhsOp = Optional.ofNullable(opGroup.get(BIT_LHS_SHIFT)).map(l -> l.get(0).second).orElse(null);
                Op rhsOp = Optional.ofNullable(opGroup.get(BIT_RHS_SHIFT)).map(l -> l.get(0).second).orElse(null);
                Op onOp = Optional.ofNullable(opGroup.get(BIT_ON_SHIFT)).map(l -> l.get(0).second).orElse(null);
                triples.add(new HoistTriple(bits, lhsOp, rhsOp, onOp));
            }
        }
        restConjuncts.get(0).forEach(op -> triples.add(new HoistTriple(BIT_LHS, op, null, null)));
        restConjuncts.get(1).forEach(op -> triples.add(new HoistTriple(BIT_RHS, null, op, null)));
        restConjuncts.get(2).forEach(op -> triples.add(new HoistTriple(BIT_ON, null, null, op)));
        return triples;
    }

    public Op mustGetOp(int i) {
        Preconditions.checkArgument(0 <= i && i < ops.length);
        return Objects.requireNonNull(ops[i]);
    }

    public Optional<Op> mayGetOp(int i) {
        Preconditions.checkArgument(0 <= i && i < ops.length);
        return Optional.ofNullable(ops[i]);
    }

    public int getBits() {
        return bits;
    }
}
