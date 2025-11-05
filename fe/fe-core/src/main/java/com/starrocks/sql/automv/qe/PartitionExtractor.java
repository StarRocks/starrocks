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

package com.starrocks.sql.automv.qe;

import com.google.api.client.util.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.sql.automv.pieces.FQTable;
import com.starrocks.sql.automv.pn.Op;
import com.starrocks.sql.automv.pn.OpUtil;
import com.starrocks.sql.automv.pn.Val;
import com.starrocks.sql.automv.util.MetaUtil;
import com.starrocks.sql.common.PCellWithName;
import com.starrocks.sql.common.PRangeCell;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PartitionExtractor {
    private final Map<String, Map<String, List<Op>>> cache = Maps.newHashMap();

    private static List<Op> rangeToOp(Range<PartitionKey> rangeKey) {
        List<Op> upperBounds = Lists.newArrayList();
        List<Op> lowerBounds = Lists.newArrayList();

        if (rangeKey.hasUpperBound()) {
            upperBounds = rangeKey.upperEndpoint().getKeys()
                    .stream()
                    .map(OpUtil::literalExprToOp)
                    .collect(Collectors.toList());
        }
        if (rangeKey.hasLowerBound()) {
            lowerBounds = rangeKey.lowerEndpoint().getKeys()
                    .stream()
                    .map(OpUtil::literalExprToOp)
                    .collect(Collectors.toList());
        }
        int n = Math.max(upperBounds.size(), lowerBounds.size());
        List<Op> rangeOpList = Lists.newArrayList();
        for (int i = 0; i < n; ++i) {
            Op lb = lowerBounds.isEmpty() ? Val.NULL_VAL : lowerBounds.get(i);
            Op ub = upperBounds.isEmpty() ? Val.NULL_VAL : upperBounds.get(i);
            Op rangeOp;
            if (lb.equals(Val.NULL_VAL)) {
                rangeOp = Op.openClosedRangeOf(lb, ub);
            } else if (ub.equals(Val.NULL_VAL)) {
                rangeOp = Op.openRangeOf(lb, ub);
            } else {
                rangeOp = Op.openClosedRangeOf(lb, ub);
            }
            rangeOpList.add(rangeOp);
        }
        return rangeOpList;
    }

    public Map<String, List<Op>> getCachedOrExtract(FQTable fqTable) {
        return cache.computeIfAbsent(fqTable.getFQName(), (name) -> extractLocked(fqTable));
    }

    private Map<String, List<Op>> extractLocked(FQTable fqTable) {
        return MetaUtil.criticalRegion(fqTable.getDatabase(), fqTable.getTable(), LockType.READ,
                () -> extract(fqTable)).unwrap().orElseGet(Collections::emptyMap);
    }

    private Map<String, List<Op>> extract(FQTable fqTable) {
        Table table = fqTable.getTable();
        if (table.isNativeTableOrMaterializedView()) {
            OlapTable olapTable = (OlapTable) table;
            if (olapTable.getPartitionInfo().isListPartition()) {
                return extractOlapListPartition(table);
            } else if (olapTable.getPartitionInfo().isRangePartition()) {
                return extractRangePartition(table);
            } else {
                return extractUnpartitioned(table);
            }
        } else {
            return extractExternalListPartition(table);
        }
    }

    private Map<String, List<Op>> extractRangePartition(Table table) {
        OlapTable olapTable = (OlapTable) table;
        if (olapTable.getPartitionInfo().isUnPartitioned()) {
            return Collections.emptyMap();
        }
        return olapTable.getRangePartitionMap().getPartitions()
                .stream()
                .collect(Collectors.toMap(PCellWithName::name, e -> rangeToOp(((PRangeCell) e.cell()).getRange())));
    }

    private List<Op> multiValuesToSetOp(List<List<String>> values) {
        return values.stream()
                .map(ss -> ss.stream()
                        .map(s -> (Op) Op.val(ConstantOperator.createChar(s)))
                        .collect(Collectors.toList()))
                .map(Op::setOf)
                .collect(Collectors.toList());
    }

    private List<Op> valuesToSetOp(List<String> values) {
        return values.stream()
                .map(s -> Op.val(ConstantOperator.createChar(s)))
                .map(Op::setOf)
                .collect(Collectors.toList());
    }

    private Map<String, List<Op>> extractOlapListPartition(Table table) {
        OlapTable olapTable = (OlapTable) table;
        return olapTable.getListPartitionValues().entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> multiValuesToSetOp(e.getValue())));
    }

    private Map<String, List<Op>> extractExternalListPartition(Table table) {
        return PartitionUtil.getPartitionNames(table)
                .stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        name -> valuesToSetOp(PartitionUtil.toPartitionValues(name))));

    }

    private Map<String, List<Op>> extractUnpartitioned(Table table) {
        return Collections.emptyMap();
    }
}
