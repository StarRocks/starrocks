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

package com.starrocks.sql.optimizer;

import com.google.common.collect.Lists;
import com.starrocks.catalog.ForeignKeyConstraint;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

// Cardinality-preserving bi-relation, two scan operator form a CPBiRel instance
// when their underlying OlapTable has {foreign,primary,unique} key constraints and
// the join equality predicates can match these constraints.
public class CPBiRel {
    // CPBiRel is a bi-relation consisting OptExpression
    // lhs->rhs means lhs and rhs joins on foreign key constraints or primary key. for an example:
    // lineitem inner join orders on lineitem.l_orderkey = orders.o_orderkey
    // The corresponding CPBiRel is lineitem->orders.
    private final OptExpression lhs;
    private final OptExpression rhs;

    // if fromForeignKey is false, then it means CPBiRel originates from same tables joining on primary key;
    // else it means CPBiRel originates from foreign key constraints.
    private final boolean fromForeignKey;
    // leftToRight means the relative position of lhs and rhs in sub-plan. for examples:
    // A left join B infers A->B: so leftToRight is true;
    // B right join A infers A->B: so leftToRight is false;
    // A inner join B infers B->A: so leftToRight is false.
    private final boolean leftToRight;
    private final Set<Pair<ColumnRefOperator, ColumnRefOperator>> pairs;

    public CPBiRel(
            OptExpression lhs,
            OptExpression rhs,
            boolean fromForeignKey,
            boolean leftToRight,
            Set<Pair<ColumnRefOperator, ColumnRefOperator>> pairs) {
        this.lhs = lhs;
        this.rhs = rhs;
        this.fromForeignKey = fromForeignKey;
        this.leftToRight = leftToRight;
        this.pairs = pairs;
    }

    public OptExpression getLhs() {
        return lhs;
    }

    public OptExpression getRhs() {
        return rhs;
    }

    public boolean isFromForeignKey() {
        return fromForeignKey;
    }

    public boolean isLeftToRight() {
        return leftToRight;
    }

    public Set<Pair<ColumnRefOperator, ColumnRefOperator>> getPairs() {
        return pairs;
    }

    // check if referencing table's foreign key constraint aims at referenced Table
    public static boolean isForeignKeyConstraintReferenceToUniqueKey(
            ForeignKeyConstraint foreignKeyConstraint,
            OlapTable referencedTable) {
        if (foreignKeyConstraint.getParentTableInfo().getTableId() != referencedTable.getId()) {
            return false;
        }
        Set<String> referencedColumnNames =
                foreignKeyConstraint.getColumnRefPairs().stream().map(p -> p.second).collect(Collectors.toSet());
        return referencedTable.getUniqueConstraints().stream()
                .anyMatch(uk -> new HashSet<>(uk.getUniqueColumns()).equals(referencedColumnNames));
    }

    public static List<CPBiRel> getCPBiRels(OptExpression lhs, OptExpression rhs,
                                            boolean leftToRight) {
        LogicalScanOperator lhsScanOp = lhs.getOp().cast();
        LogicalScanOperator rhsScanOp = rhs.getOp().cast();
        if (!(lhsScanOp.getTable() instanceof OlapTable) || !(rhsScanOp.getTable() instanceof OlapTable)) {
            return Collections.emptyList();
        }
        OlapTable lhsTable = (OlapTable) lhsScanOp.getTable();
        OlapTable rhsTable = (OlapTable) rhsScanOp.getTable();
        Map<String, ColumnRefOperator> lhsColumnName2ColRef =
                lhsScanOp.getColumnMetaToColRefMap().entrySet().stream()
                        .collect(Collectors.toMap(e -> e.getKey().getName(), e -> e.getValue()));
        Map<String, ColumnRefOperator> rhsColumnName2ColRef =
                rhsScanOp.getColumnMetaToColRefMap().entrySet().stream()
                        .collect(Collectors.toMap(e -> e.getKey().getName(), e -> e.getValue()));
        List<CPBiRel> biRels = Lists.newArrayList();
        if (lhsTable.hasForeignKeyConstraints() && rhsTable.hasUniqueConstraints()) {
            lhsTable.getForeignKeyConstraints().stream()
                    .filter(fk -> isForeignKeyConstraintReferenceToUniqueKey(fk, rhsTable)).forEach(fk -> {
                        Set<String> lhsColumNames =
                                fk.getColumnRefPairs().stream().map(p -> p.first).collect(Collectors.toSet());
                        Set<String> rhsColumNames =
                                fk.getColumnRefPairs().stream().map(p -> p.second).collect(Collectors.toSet());
                        if (lhsColumnName2ColRef.keySet().containsAll(lhsColumNames) &&
                                rhsColumnName2ColRef.keySet().containsAll(rhsColumNames)) {
                            Set<Pair<ColumnRefOperator, ColumnRefOperator>> fkColumnRefPairs =
                                    fk.getColumnRefPairs().stream()
                                            .map(p ->
                                                    Pair.create(
                                                            lhsColumnName2ColRef.get(p.first),
                                                            rhsColumnName2ColRef.get(p.second))
                                            ).collect(Collectors.toSet());
                            biRels.add(new CPBiRel(lhs, rhs, true, leftToRight, fkColumnRefPairs));
                        }
                    });
        }

        if (lhsTable.getId() == rhsTable.getId() && lhsTable.hasUniqueConstraints()) {
            lhsTable.getUniqueConstraints().stream().filter(uk ->
                            lhsColumnName2ColRef.keySet().containsAll(uk.getUniqueColumns()) &&
                                    rhsColumnName2ColRef.keySet().containsAll(uk.getUniqueColumns())
                    ).map(uk ->
                            uk.getUniqueColumns().stream().map(colName ->
                                    Pair.create(
                                            lhsColumnName2ColRef.get(colName),
                                            rhsColumnName2ColRef.get(colName))
                            ).collect(Collectors.toSet()))
                    .forEach(ukColumnRefPairs ->
                            biRels.add(new CPBiRel(lhs, rhs, false, leftToRight, ukColumnRefPairs)));
        }
        return biRels;
    }
}