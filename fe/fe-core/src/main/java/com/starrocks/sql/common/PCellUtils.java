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
package com.starrocks.sql.common;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.LiteralExpr;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.connector.PartitionUtil.getMVPartitionNameWithRange;
import static com.starrocks.connector.PartitionUtil.getMVPartitionToCells;

public class PCellUtils {
    private static final Logger LOG = LogManager.getLogger(PCellUtils.class);

    public static boolean isNotEmpty(PCellSortedSet cellSet) {
        return cellSet != null && !cellSet.isEmpty();
    }

    public static boolean isEmpty(PCellSortedSet cellSet) {
        return cellSet == null || cellSet.isEmpty();
    }

    public static PCellSortedSet ofTable(MaterializedView mv,
                                         Table baseTable,
                                         Set<String> partitionNames) {
        if (baseTable instanceof OlapTable) {
            return ofOlapTable((OlapTable) baseTable, partitionNames);
        } else {
            try {
                PartitionInfo partitionInfo = mv.getPartitionInfo();
                Map<Table, List<Column>> refBaseTablePartitionColumns = mv.getRefBaseTablePartitionColumns();
                if (!refBaseTablePartitionColumns.containsKey(baseTable)) {
                    Set<PCellWithName> defaultCells = partitionNames.stream()
                            .map(name -> new PCellWithName(name, new PCellNone()))
                            .collect(Collectors.toSet());
                    return PCellSortedSet.of(defaultCells);
                }
                List<Column> refPartitionColumns = refBaseTablePartitionColumns.get(baseTable);
                if (partitionInfo.isListPartition()) {
                    return getMVPartitionToCells(baseTable, refPartitionColumns, partitionNames);
                } else if (partitionInfo.isRangePartition()) {
                    Preconditions.checkArgument(refPartitionColumns.size() == 1,
                            "Range partition column size must be 1");
                    Column partitionColumn = refPartitionColumns.get(0);
                    Optional<Expr> partitionExprOpt = mv.getRangePartitionFirstExpr();
                    Preconditions.checkArgument(partitionExprOpt.isPresent(),
                            "Range partition expr must be present");
                    return getMVPartitionNameWithRange(baseTable, partitionColumn,
                            partitionNames, partitionExprOpt.get());
                } else {
                    return null;
                }
            } catch (Exception e) {
                LOG.warn("Failed to get MV partition cells for table: {}", baseTable.getName(), e);
                return null;
            }
        }
    }

    public static PCellSortedSet ofOlapTable(OlapTable table, Set<String> partitionNames) {
        PCellSortedSet cellSet = PCellSortedSet.of();
        partitionNames.stream()
                .map(partitionName -> of(table, partitionName))
                .filter(Optional::isPresent)
                .forEach(optCell -> cellSet.add(optCell.get()));
        return cellSet;
    }

    public static Optional<PCellWithName> of(OlapTable table, String partitionName) {
        if (partitionName == null || partitionName.startsWith(ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX)) {
            return  Optional.empty();
        }
        Partition partition = table.getPartition(partitionName);
        if (partition == null) {
            return Optional.empty();
        }
        long partitionId = table.getPartition(partitionName).getId();
        return of(table, partitionId).map(cell -> new PCellWithName(partitionName, cell));
    }

    public static Optional<PCell> of(OlapTable table, long partitionId) {
        PartitionInfo partitionInfo = table.getPartitionInfo();
        Preconditions.checkNotNull(partitionInfo);
        if (partitionInfo.isUnPartitioned()) {
            return Optional.of(new PCellNone());
        } else if (partitionInfo.isRangePartition()) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            return Optional.ofNullable(rangePartitionInfo.getRange(partitionId))
                    .map(range -> PRangeCell.of(range));
        } else if (partitionInfo.isListPartition()) {
            ListPartitionInfo listPartitionInfo = (ListPartitionInfo) partitionInfo;
            List<LiteralExpr> literalValues = listPartitionInfo.getLiteralExprValues().get(partitionId);
            if (CollectionUtils.isNotEmpty(literalValues)) {
                List<List<String>> cellValue = Lists.newArrayList();
                // for one item(single value), treat it as multi values.
                for (LiteralExpr val : literalValues) {
                    cellValue.add(Lists.newArrayList(val.getStringValue()));
                }
                return Optional.of(new PListCell(cellValue));
            }
            List<List<LiteralExpr>> multiExprValues = listPartitionInfo.getMultiLiteralExprValues().get(partitionId);
            if (CollectionUtils.isNotEmpty(multiExprValues)) {
                List<List<String>> multiValues = Lists.newArrayList();
                for (List<LiteralExpr> exprValues : multiExprValues) {
                    List<String> values = Lists.newArrayList();
                    for (LiteralExpr literalExpr : exprValues) {
                        values.add(literalExpr.getStringValue());
                    }
                    multiValues.add(values);
                }
                return Optional.of(new PListCell(multiValues));
            }
            return Optional.empty();
        } else {
            throw new IllegalStateException("Unsupported partition type: " + partitionInfo);
        }
    }
}
