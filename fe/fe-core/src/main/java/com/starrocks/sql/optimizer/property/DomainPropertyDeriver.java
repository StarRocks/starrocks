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
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator.CompoundType;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;

import java.util.Map;
import java.util.stream.Collectors;

public class DomainPropertyDeriver extends ScalarOperatorVisitor<Map<ScalarOperator, ScalarOperator>, Void> {

    public DomainProperty derive(ScalarOperator scalarOperator) {
        Map<ScalarOperator, ScalarOperator> domainMap = scalarOperator.accept(this, null);
        Map<ScalarOperator, DomainProperty.DomainWrapper> newMap = domainMap.entrySet().stream()
                .filter(e -> !e.getValue().equals(ConstantOperator.TRUE))
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> new DomainProperty.DomainWrapper(entry.getValue())));
        return new DomainProperty(newMap);
    }


    @Override
    public Map<ScalarOperator, ScalarOperator> visit(ScalarOperator scalarOperator, Void context) {
        return Maps.newHashMap();
    }

    @Override
    public Map<ScalarOperator, ScalarOperator> visitBinaryPredicate(BinaryPredicateOperator binaryPredicate, Void context) {
        Map<ScalarOperator, ScalarOperator> domainMap = Maps.newHashMap();
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

        Map<ScalarOperator, ScalarOperator> leftExprMap = left.accept(this, context);

        if (!leftExprMap.containsKey(left) || !leftExprMap.get(left).equals(ConstantOperator.TRUE)) {
            return domainMap;
        }

        domainMap.put(left, binaryPredicate);
        return domainMap;
    }

    @Override
    public Map<ScalarOperator, ScalarOperator> visitCompoundPredicate(CompoundPredicateOperator predicate, Void context) {
        Map<ScalarOperator, ScalarOperator> domainMap = Maps.newHashMap();
        if (predicate.isNot()) {
            return domainMap;
        } else {
            ScalarOperator left = predicate.getChild(0);
            ScalarOperator right = predicate.getChild(1);
            Map<ScalarOperator, ScalarOperator> leftDomainMap = left.accept(this, context);
            Map<ScalarOperator, ScalarOperator> rightDomainMap = right.accept(this, context);
            return predicate.isAnd() ? mergeAndMap(leftDomainMap, rightDomainMap) : mergeOrMap(leftDomainMap, rightDomainMap);
        }

    }

    @Override
    public Map<ScalarOperator, ScalarOperator> visitInPredicate(InPredicateOperator predicate, Void context) {
        Map<ScalarOperator, ScalarOperator> domainMap = Maps.newHashMap();
        if (predicate.isNotIn()) {
            return domainMap;
        }
        if (!predicate.getChildren().stream().skip(1).allMatch(ScalarOperator::isConstant)) {
            return domainMap;
        }
        ScalarOperator left = predicate.getChild(0);
        Map<ScalarOperator, ScalarOperator> leftExprMap = left.accept(this, context);

        if (!leftExprMap.containsKey(left) || !leftExprMap.get(left).equals(ConstantOperator.TRUE)) {
            return domainMap;
        }

        domainMap.put(left, predicate);
        return domainMap;
    }



    @Override
    public Map<ScalarOperator, ScalarOperator> visitVariableReference(ColumnRefOperator columnRefOperator, Void context) {
        Map<ScalarOperator, ScalarOperator> domainMap = Maps.newHashMap();
        domainMap.put(columnRefOperator, ConstantOperator.TRUE);
        return domainMap;
    }

    @Override
    public Map<ScalarOperator, ScalarOperator> visitCall(CallOperator callOperator, Void context) {
        Map<ScalarOperator, ScalarOperator> domainMap = Maps.newHashMap();
        domainMap.put(callOperator, ConstantOperator.TRUE);
        return domainMap;
    }

    @Override
    public Map<ScalarOperator, ScalarOperator> visitIsNullPredicate(IsNullPredicateOperator predicate, Void context) {
        Map<ScalarOperator, ScalarOperator> domainMap = Maps.newHashMap();
        ScalarOperator left = predicate.getChild(0);
        Map<ScalarOperator, ScalarOperator> leftExprMap = left.accept(this, context);

        if (!leftExprMap.containsKey(left) || !leftExprMap.get(left).equals(ConstantOperator.TRUE)) {
            return domainMap;
        }

        domainMap.put(left, ConstantOperator.TRUE);
        return domainMap;
    }


    private Map<ScalarOperator, ScalarOperator> mergeAndMap(Map<ScalarOperator, ScalarOperator> leftDomainMap,
                                                                    Map<ScalarOperator, ScalarOperator> rightDomainMap) {
        Map<ScalarOperator, ScalarOperator> domainMap = Maps.newHashMap();
        for (Map.Entry<ScalarOperator, ScalarOperator> entry : leftDomainMap.entrySet()) {
            if (rightDomainMap.containsKey(entry.getKey())) {
                domainMap.put(entry.getKey(), andPredicate(entry.getValue(), rightDomainMap.get(entry.getKey())));
                rightDomainMap.remove(entry.getKey());
            } else {
                domainMap.put(entry.getKey(), entry.getValue());
            }
        }

        for (Map.Entry<ScalarOperator, ScalarOperator> entry : rightDomainMap.entrySet()) {
            domainMap.put(entry.getKey(), entry.getValue());
        }

        return domainMap;
    }

    private Map<ScalarOperator, ScalarOperator> mergeOrMap(Map<ScalarOperator, ScalarOperator> leftDomainMap,
                                                           Map<ScalarOperator, ScalarOperator> rightDomainMap) {
        Map<ScalarOperator, ScalarOperator> domainMap = Maps.newHashMap();
        for (Map.Entry<ScalarOperator, ScalarOperator> entry : leftDomainMap.entrySet()) {
            if (rightDomainMap.containsKey(entry.getKey())) {
                domainMap.put(entry.getKey(), orPredicate(entry.getValue(), rightDomainMap.get(entry.getKey())));
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

}
