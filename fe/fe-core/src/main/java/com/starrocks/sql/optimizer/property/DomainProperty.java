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

/**
 * DomainProperty is used to store the domain of a column or expr.
 * For a sql like `select col_a, col_b, col_c from table where col_a > 10 and col_b + col_c = 20`,
 * we can get the domain property of col_a, col_b + col_c from the where clause like
 * col_a -> {predicateDesc : col_a > 10, rangeDesc : [10, +inf)}
 * col_b + col_c -> {predicateDesc : col_b + col_c = 20, rangeDesc : [20, 20]}
 */
public class DomainProperty {

    private Map<ScalarOperator, DomainWrapper> domainMap;


    public DomainProperty(Map<ScalarOperator, DomainWrapper> domainMap) {
        this.domainMap = domainMap;
    }

    public boolean contains(ScalarOperator scalarOperator) {
        return domainMap.containsKey(scalarOperator);
    }

    public Map<ScalarOperator, DomainWrapper> getDomainMap() {
        return domainMap;
    }

    public DomainWrapper getValueWrapper(ScalarOperator scalarOperator) {
        return domainMap.get(scalarOperator);
    }
    public ScalarOperator getPredicateDesc(ScalarOperator scalarOperator) {
        return domainMap.get(scalarOperator).getPredicateDesc();
    }

    public DomainProperty projectDomainProperty(Map<ColumnRefOperator, ScalarOperator> columnRefMap) {
        Map<ScalarOperator, DomainWrapper> newDomainMap = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : columnRefMap.entrySet()) {
            if (domainMap.containsKey(entry.getValue()) && !entry.getValue().equals(entry.getKey())) {
                ReplaceShuttle shuttle = new ReplaceShuttle(Map.of(entry.getValue(), entry.getKey()));
                ScalarOperator rewriteResult = shuttle.rewrite(domainMap.get(entry.getValue()).getPredicateDesc());
                if (rewriteResult != null) {
                    newDomainMap.put(entry.getKey(),
                            new DomainWrapper(rewriteResult, domainMap.get(entry.getValue()).isNeedDeriveRange()));
                }
            }
        }
        ColumnRefSet outputCols = new ColumnRefSet(columnRefMap.keySet());
        for (Map.Entry<ScalarOperator, DomainWrapper> entry : domainMap.entrySet()) {
            if (!newDomainMap.containsKey(entry.getKey()) && outputCols.containsAll(entry.getKey().getUsedColumns())) {
                newDomainMap.put(entry.getKey(), entry.getValue());
            }
        }
        return new DomainProperty(newDomainMap);
    }

    public DomainProperty filterDomainProperty(DomainProperty filterDomainProperty) {
        Map<ScalarOperator, DomainWrapper> newDomainMap = Maps.newHashMap(domainMap);

        for (Map.Entry<ScalarOperator, DomainWrapper> entry : filterDomainProperty.domainMap.entrySet()) {
            if (domainMap.containsKey(entry.getKey())) {
                newDomainMap.put(entry.getKey(), domainMap.get(entry.getKey()).andValueWrapper(entry.getValue()));
            } else {
                newDomainMap.put(entry.getKey(), entry.getValue());
            }
        }
        return new DomainProperty(newDomainMap);
    }

    public static DomainProperty mergeDomainProperty(List<DomainProperty> domainPropertyList) {
        Map<ScalarOperator, DomainWrapper> newDomainMap = Maps.newHashMap();
        for (DomainProperty domainProperty : domainPropertyList) {
            for (Map.Entry<ScalarOperator, DomainWrapper> entry : domainProperty.domainMap.entrySet()) {
                if (newDomainMap.containsKey(entry.getKey())) {
                    newDomainMap.put(entry.getKey(), newDomainMap.get(entry.getKey()).orValueWrapper(entry.getValue()));
                } else {
                    newDomainMap.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return new DomainProperty(newDomainMap);
    }

    public static class DomainWrapper {
        private final ScalarOperator predicateDesc;

        private final boolean needDeriveRange;

        @Nullable
        private final RangeExtractor.RangeDescriptor rangeDesc;


        public DomainWrapper(ScalarOperator predicateDesc, boolean needDeriveRange) {
            this.predicateDesc = predicateDesc;
            this.needDeriveRange = needDeriveRange;
            if (needDeriveRange) {
                this.rangeDesc = deriveRange(predicateDesc);
            } else {
                this.rangeDesc = new RangeExtractor.RangeDescriptor(ConstantOperator.FALSE);
            }
        }


        public ScalarOperator getPredicateDesc() {
            return predicateDesc;
        }

        public RangeExtractor.RangeDescriptor getRangeDesc() {
            return rangeDesc;
        }

        public boolean isNeedDeriveRange() {
            return needDeriveRange;
        }

        public DomainWrapper andValueWrapper(DomainWrapper domainWrapper) {
            ScalarOperator newScalarOperator = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                    predicateDesc, domainWrapper.predicateDesc);
            return new DomainWrapper(newScalarOperator, needDeriveRange || domainWrapper.needDeriveRange);
        }

        public DomainWrapper orValueWrapper(DomainWrapper domainWrapper) {
            ScalarOperator newScalarOperator = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                    predicateDesc, domainWrapper.predicateDesc);
            return new DomainWrapper(newScalarOperator, needDeriveRange && domainWrapper.needDeriveRange);
        }

        private RangeExtractor.RangeDescriptor deriveRange(ScalarOperator scalarOperator) {
            RangeExtractor extractor = new RangeExtractor();

            Map<ScalarOperator, RangeExtractor.ValueDescriptor> res = extractor.apply(scalarOperator, null);

            if (res.size() != 1) {
                return new RangeExtractor.RangeDescriptor(ConstantOperator.FALSE);
            } else {
                RangeExtractor.ValueDescriptor desc = res.values().stream().findFirst().get();
                if (desc instanceof RangeExtractor.MultiValuesDescriptor) {
                    RangeExtractor.MultiValuesDescriptor multiValues = (RangeExtractor.MultiValuesDescriptor) desc;
                    RangeExtractor.RangeDescriptor rangeDesc = new RangeExtractor.RangeDescriptor(multiValues.columnRef);
                    rangeDesc = (RangeExtractor.RangeDescriptor) rangeDesc.union(multiValues);
                    return rangeDesc;
                } else {
                    return (RangeExtractor.RangeDescriptor) desc;
                }
            }
        }
    }
}
