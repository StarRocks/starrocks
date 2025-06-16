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

package com.starrocks.sql.optimizer.property;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator.CompoundType;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DomainPropertyDeriver extends ScalarOperatorVisitor<Map<ScalarOperator, DomainPropertyDeriver.DomainMessage>, Void> {

    public DomainProperty derive(ScalarOperator scalarOperator) {
        Map<ScalarOperator, DomainMessage> domainMap = scalarOperator.accept(this, null);
        Map<ScalarOperator, DomainProperty.DomainWrapper> newMap = domainMap.entrySet().stream()
                .filter(e -> !e.getValue().predicateDesc.equals(ConstantOperator.TRUE))
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> new DomainProperty.DomainWrapper(entry.getValue().predicateDesc,
                                entry.getValue().needDeriveRange)));
        return new DomainProperty(newMap);
    }


    @Override
    public Map<ScalarOperator, DomainMessage> visit(ScalarOperator scalarOperator, Void context) {
        return Maps.newHashMap();
    }

    @Override
    public Map<ScalarOperator, DomainMessage> visitBinaryPredicate(BinaryPredicateOperator binaryPredicate, Void context) {
        Map<ScalarOperator, DomainMessage> domainMap = Maps.newHashMap();
        if (!binaryPredicate.getBinaryType().isEqualOrRange()) {
            return domainMap;
        }
        ScalarOperator left = binaryPredicate.getChild(0);
        ScalarOperator right = binaryPredicate.getChild(1);
        if (!right.isConstant()) {
            return domainMap;
        }

        if (right instanceof CallOperator) {
            String fnName = ((CallOperator) right).getFnName();
            if (FunctionSet.nonDeterministicFunctions.contains(fnName)) {
                return domainMap;
            }
        }

        domainMap.put(left, new DomainMessage(binaryPredicate));
        List<ColumnRefOperator> usedCols = left.getColumnRefs();
        if (Sets.newHashSet(usedCols).size() == 1 && !domainMap.containsKey(usedCols.get(0))) {
            domainMap.put(usedCols.get(0), new DomainMessage(binaryPredicate, false));
        }
        return domainMap;
    }

    @Override
    public Map<ScalarOperator, DomainMessage> visitCompoundPredicate(CompoundPredicateOperator predicate, Void context) {
        Map<ScalarOperator, DomainMessage> domainMap = Maps.newHashMap();
        if (predicate.isNot()) {
            return domainMap;
        } else {
            ScalarOperator left = predicate.getChild(0);
            ScalarOperator right = predicate.getChild(1);
            Map<ScalarOperator, DomainMessage> leftDomainMap = left.accept(this, context);
            Map<ScalarOperator, DomainMessage> rightDomainMap = right.accept(this, context);
            return predicate.isAnd() ? mergeAndMap(leftDomainMap, rightDomainMap) : mergeOrMap(leftDomainMap, rightDomainMap);
        }

    }

    @Override
    public Map<ScalarOperator, DomainMessage> visitInPredicate(InPredicateOperator predicate, Void context) {
        Map<ScalarOperator, DomainMessage> domainMap = Maps.newHashMap();
        if (predicate.isNotIn()) {
            return domainMap;
        }
        if (!predicate.getChildren().stream().skip(1).allMatch(ScalarOperator::isConstant)) {
            return domainMap;
        }
        ScalarOperator left = predicate.getChild(0);
        domainMap.put(left, new DomainMessage(predicate));

        List<ColumnRefOperator> usedCols = left.getColumnRefs();
        if (Sets.newHashSet(usedCols).size() == 1 && !domainMap.containsKey(usedCols.get(0))) {
            domainMap.put(usedCols.get(0), new DomainMessage(predicate, false));
        }
        return domainMap;
    }

    @Override
    public Map<ScalarOperator, DomainMessage> visitCall(CallOperator callOperator, Void context) {
        Map<ScalarOperator, DomainMessage> domainMap = Maps.newHashMap();
        if (!callOperator.getType().isBoolean()) {
            return domainMap;
        }
        List<ColumnRefOperator> usedCols = callOperator.getColumnRefs();
        if (Sets.newHashSet(usedCols).size() == 1) {
            domainMap.put(usedCols.get(0), new DomainMessage(callOperator, false));
        }
        return domainMap;
    }

    private Map<ScalarOperator, DomainMessage> mergeAndMap(Map<ScalarOperator, DomainMessage> leftDomainMap,
                                                                    Map<ScalarOperator, DomainMessage> rightDomainMap) {
        Map<ScalarOperator, DomainMessage> domainMap = Maps.newHashMap();
        for (Map.Entry<ScalarOperator, DomainMessage> entry : leftDomainMap.entrySet()) {
            if (rightDomainMap.containsKey(entry.getKey())) {
                ScalarOperator andPredicate = andPredicate(entry.getValue().predicateDesc,
                        rightDomainMap.get(entry.getKey()).predicateDesc);
                boolean needDeriveRange = entry.getValue().needDeriveRange || rightDomainMap.get(entry.getKey()).needDeriveRange;
                domainMap.put(entry.getKey(), new DomainMessage(andPredicate, needDeriveRange));
                rightDomainMap.remove(entry.getKey());
            } else {
                domainMap.put(entry.getKey(), entry.getValue());
            }
        }

        for (Map.Entry<ScalarOperator, DomainMessage> entry : rightDomainMap.entrySet()) {
            domainMap.put(entry.getKey(), entry.getValue());
        }

        return domainMap;
    }

    private Map<ScalarOperator, DomainMessage> mergeOrMap(Map<ScalarOperator, DomainMessage> leftDomainMap,
                                                           Map<ScalarOperator, DomainMessage> rightDomainMap) {
        Map<ScalarOperator, DomainMessage> domainMap = Maps.newHashMap();
        for (Map.Entry<ScalarOperator, DomainMessage> entry : leftDomainMap.entrySet()) {
            if (rightDomainMap.containsKey(entry.getKey())) {
                ScalarOperator orPredicate = orPredicate(entry.getValue().predicateDesc,
                        rightDomainMap.get(entry.getKey()).predicateDesc);
                boolean needDeriveRange = entry.getValue().needDeriveRange && rightDomainMap.get(entry.getKey()).needDeriveRange;
                domainMap.put(entry.getKey(), new DomainMessage(orPredicate, needDeriveRange));
                rightDomainMap.remove(entry.getKey());
            }
        }

        return domainMap;
    }

    private ScalarOperator andPredicate(ScalarOperator left, ScalarOperator right) {
        if (left.equals(ConstantOperator.TRUE)) {
            return right;
        } else if (right.equals(ConstantOperator.TRUE)) {
            return left;
        } else {
            return new CompoundPredicateOperator(CompoundType.AND, left, right);
        }
    }

    private ScalarOperator orPredicate(ScalarOperator left, ScalarOperator right) {
        if (left.equals(ConstantOperator.TRUE) || right.equals(ConstantOperator.TRUE)) {
            return ConstantOperator.TRUE;
        } else {
            return new CompoundPredicateOperator(CompoundType.OR, left, right);
        }
    }

    public static class DomainMessage {

        private final ScalarOperator predicateDesc;

        private final boolean needDeriveRange;

        public DomainMessage(ScalarOperator predicateDesc) {
            this(predicateDesc, true);
        }

        public DomainMessage(ScalarOperator predicateDesc, boolean needDeriveRange) {
            this.predicateDesc = predicateDesc;
            this.needDeriveRange = needDeriveRange;
        }
    }

}
