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
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

public class ValueProperty {

    private Map<ScalarOperator, ValueWrapper> valueMap;


    public ValueProperty(Map<ScalarOperator, ValueWrapper> valueMap) {
        this.valueMap = valueMap;
    }

    public boolean contains(ScalarOperator scalarOperator) {
        return valueMap.containsKey(scalarOperator);
    }

    public Map<ScalarOperator, ValueWrapper> getValueMap() {
        return valueMap;
    }

    public ValueWrapper getValueWrapper(ScalarOperator scalarOperator) {
        return valueMap.get(scalarOperator);
    }
    public ScalarOperator getPredicateDesc(ScalarOperator scalarOperator) {
        return valueMap.get(scalarOperator).getPredicateDesc();
    }

    public ValueProperty projectValueProperty(Map<ColumnRefOperator, ScalarOperator> columnRefMap) {
        Map<ScalarOperator, ValueWrapper> newValueMap = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : columnRefMap.entrySet()) {
            if (valueMap.containsKey(entry.getValue()) && !entry.getValue().equals(entry.getKey())) {
                ReplaceShuttle shuttle = new ReplaceShuttle(Map.of(entry.getValue(), entry.getKey()));
                ScalarOperator rewriteResult = shuttle.rewrite(valueMap.get(entry.getValue()).getPredicateDesc());
                newValueMap.put(entry.getKey(), new ValueWrapper(rewriteResult));
            }
        }
        ColumnRefSet outputCols = new ColumnRefSet(columnRefMap.keySet());
        for (Map.Entry<ScalarOperator, ValueWrapper> entry : valueMap.entrySet()) {
            if (!newValueMap.containsKey(entry.getKey()) && outputCols.containsAll(entry.getKey().getUsedColumns())) {
                newValueMap.put(entry.getKey(), entry.getValue());
            }
        }
        return new ValueProperty(newValueMap);
    }

    public ValueProperty filterValueProperty(ValueProperty filterValueProperty) {
        Map<ScalarOperator, ValueWrapper> newValueMap = Maps.newHashMap(valueMap);

        for (Map.Entry<ScalarOperator, ValueWrapper> entry : filterValueProperty.valueMap.entrySet()) {
            if (valueMap.containsKey(entry.getKey())) {
                newValueMap.put(entry.getKey(), valueMap.get(entry.getKey()).andValueWrapper(entry.getValue()));
            } else {
                newValueMap.put(entry.getKey(), entry.getValue());
            }
        }
        return new ValueProperty(newValueMap);
    }

    public static ValueProperty mergeValueProperty(List<ValueProperty> valuePropertyList) {
        Map<ScalarOperator, ValueWrapper> newValueMap = Maps.newHashMap();
        for (ValueProperty valueProperty : valuePropertyList) {
            for (Map.Entry<ScalarOperator, ValueWrapper> entry : valueProperty.valueMap.entrySet()) {
                if (newValueMap.containsKey(entry.getKey())) {
                    newValueMap.put(entry.getKey(), newValueMap.get(entry.getKey()).orValueWrapper(entry.getValue()));
                } else {
                    newValueMap.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return new ValueProperty(newValueMap);
    }

    public static class ValueWrapper {
        private ScalarOperator predicateDesc;

        @Nullable
        private RangeExtractor.RangeDescriptor valueDesc;

        private boolean withNull;

        public ValueWrapper(ScalarOperator predicateDesc) {
            this(predicateDesc, false);
        }

        public ValueWrapper(ScalarOperator predicateDesc, boolean withNull) {
            this.predicateDesc = predicateDesc;
            this.valueDesc = deriveRange(predicateDesc);
            this.withNull = withNull;
        }

        public ScalarOperator getPredicateDesc() {
            return predicateDesc;
        }

        public RangeExtractor.RangeDescriptor getValueDesc() {
            return valueDesc;
        }

        public void setWithNull(boolean withNull) {
            this.withNull = withNull;
        }

        public ValueWrapper andValueWrapper(ValueWrapper valueWrapper) {
            ScalarOperator newScalarOperator = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                    predicateDesc, valueWrapper.predicateDesc);
            return new ValueWrapper(newScalarOperator);
        }

        public ValueWrapper orValueWrapper(ValueWrapper valueWrapper) {
            ScalarOperator newScalarOperator = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                    predicateDesc, valueWrapper.predicateDesc);
            return new ValueWrapper(newScalarOperator, withNull || valueWrapper.withNull);
        }

        private RangeExtractor.RangeDescriptor deriveRange(ScalarOperator scalarOperator) {
            RangeExtractor extractor = new RangeExtractor();

            Map<ScalarOperator, RangeExtractor.ValueDescriptor> res = extractor.apply(scalarOperator, null);

            if (res.size() != 1) {
                return new RangeExtractor.RangeDescriptor(ConstantOperator.FALSE);
            } else {
                RangeExtractor.ValueDescriptor valueDesc = res.values().stream().findFirst().get();
                if (valueDesc instanceof RangeExtractor.MultiValuesDescriptor) {
                    RangeExtractor.MultiValuesDescriptor multiValues = (RangeExtractor.MultiValuesDescriptor) valueDesc;
                    RangeExtractor.RangeDescriptor rangeDesc = new RangeExtractor.RangeDescriptor(multiValues.columnRef);
                    rangeDesc = (RangeExtractor.RangeDescriptor) rangeDesc.union(multiValues);
                    return rangeDesc;
                } else {
                    return (RangeExtractor.RangeDescriptor) valueDesc;
                }
            }
        }
    }
}
