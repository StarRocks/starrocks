// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.planner;

import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.Column;
import com.starrocks.common.AnalysisException;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ListPartitionForOlapPruner implements PartitionPruner {

    private final Map<Long, List<LiteralExpr>> literalExprValues;
    private final Map<Long, List<List<LiteralExpr>>> multiLiteralExprValues;
    private final List<Column> partitionColumns;
    private final Map<String, PartitionColumnFilter> columnFilters;

    public ListPartitionForOlapPruner(Map<Long, List<LiteralExpr>> literalExprValues,
                                 Map<Long, List<List<LiteralExpr>>> multiLiteralExprValues,
                                 List<Column> partitionColumns,
                                 Map<String, PartitionColumnFilter> columnFilters) {
        this.literalExprValues = literalExprValues;
        this.multiLiteralExprValues = multiLiteralExprValues;
        this.partitionColumns = partitionColumns;
        this.columnFilters = columnFilters;
    }


    @Override
    public List<Long> prune() throws AnalysisException {

        if (columnFilters == null || columnFilters.size() == 0){
            return null;
        }

        if (this.literalExprValues != null && !this.literalExprValues.isEmpty()) {
            return this.singleItemListPartitionPrune(partitionColumns.get(0));
        }

        if (this.multiLiteralExprValues != null && !this.multiLiteralExprValues.isEmpty()) {
            return this.multiItemListPartitionPrune(partitionColumns);
        }

        return null;
    }

    private List<Long> multiItemListPartitionPrune(List<Column> multiColumn) {
        Set<Long> result = null;
        for (int i = 0; i < multiColumn.size(); i++){

            Column partitionColItem = multiColumn.get(i);
            PartitionColumnFilter partitionColumnFilter = columnFilters.get(partitionColItem.getName());

            // 1. partitionColumnFilter is null , return null selected partition
            if (partitionColumnFilter == null || (partitionColumnFilter.upperBound == null
                    && partitionColumnFilter.lowerBound == null
                    && partitionColumnFilter.getInPredicateLiterals() == null)) {
                continue;
            }

            // 2. lowerBound not equal to upperBound , return null selected partition
            // eg: select * from xxx where province > 'beijing'
            if ((partitionColumnFilter.lowerBound != null || partitionColumnFilter.upperBound != null) &&
                    !(partitionColumnFilter.lowerBound != null && partitionColumnFilter.upperBound != null
                            && partitionColumnFilter.lowerBoundInclusive && partitionColumnFilter.upperBoundInclusive
                            && partitionColumnFilter.lowerBound.compareLiteral(partitionColumnFilter.upperBound) == 0)) {
                continue;
            }

            if (result == null) {
                result = new HashSet<>();
            }

            Set<Long> selectedResult = new HashSet<>();
            if (partitionColumnFilter.lowerBound != null && partitionColumnFilter.upperBound != null
                    && partitionColumnFilter.lowerBoundInclusive && partitionColumnFilter.upperBoundInclusive
                    && partitionColumnFilter.lowerBound.compareLiteral(partitionColumnFilter.upperBound) == 0) {
                // traverse for partitions
                for (Map.Entry<Long, List<List<LiteralExpr>>> entry : multiLiteralExprValues.entrySet()){
                    Long partitionId = entry.getKey();
                    List<List<LiteralExpr>> values = entry.getValue();
                    // traverse for valueItems
                    for (List<LiteralExpr> partitionValueItems : values) {
                        // get the partition column position value and compare
                        LiteralExpr partitionValueItem = partitionValueItems.get(i);
                        if (partitionValueItem.compareLiteral(partitionColumnFilter.lowerBound) == 0) {
                            selectedResult.add(partitionId);
                        }
                    }
                }
            }

            List<LiteralExpr> inPredicateLiterals = partitionColumnFilter.getInPredicateLiterals();
            if (inPredicateLiterals != null && inPredicateLiterals.size() > 0) {
                // traverse for partitions
                for (Map.Entry<Long, List<List<LiteralExpr>>> entry : multiLiteralExprValues.entrySet()){
                    Long partitionId = entry.getKey();
                    List<List<LiteralExpr>> values = entry.getValue();
                    // traverse for valueItems
                    for (List<LiteralExpr> partitionValueItems : values) {
                        // get the partition column position value and compare
                        LiteralExpr partitionValueItem = partitionValueItems.get(i);
                        for (LiteralExpr inValueItem : inPredicateLiterals) {
                            if (inValueItem.compareLiteral(partitionValueItem) == 0) {
                                selectedResult.add(partitionId);
                            }
                        }
                    }
                }
            }

            result.addAll(selectedResult);
        }
        return result == null ? null : result.stream().collect(Collectors.toList());
    }

    private List<Long> singleItemListPartitionPrune(Column partitionColumn) {

        PartitionColumnFilter partitionColumnFilter = columnFilters.get(partitionColumn.getName());

        // 1. partitionColumnFilter is null , return null selected partition
        if (partitionColumnFilter == null || (partitionColumnFilter.upperBound == null
                && partitionColumnFilter.lowerBound == null
                && partitionColumnFilter.getInPredicateLiterals() == null)) {
            return null;
        }

        // 2. lowerBound not equal to upperBound , return null selected partition
        // eg: select * from xxx where province > 'beijing'
        if ((partitionColumnFilter.lowerBound != null || partitionColumnFilter.upperBound != null) &&
                !(partitionColumnFilter.lowerBound != null && partitionColumnFilter.upperBound != null
                        && partitionColumnFilter.lowerBoundInclusive && partitionColumnFilter.upperBoundInclusive
                        && partitionColumnFilter.lowerBound.compareLiteral(partitionColumnFilter.upperBound) == 0)) {
            return null;
        }

        Set<Long> result = new HashSet<>();
        if (partitionColumnFilter.lowerBound != null && partitionColumnFilter.upperBound != null
                && partitionColumnFilter.lowerBoundInclusive && partitionColumnFilter.upperBoundInclusive
                && partitionColumnFilter.lowerBound.compareLiteral(partitionColumnFilter.upperBound) == 0) {
            literalExprValues.forEach((partitionId, values) -> {
                for (LiteralExpr partitionValueItem : values) {
                    if (partitionValueItem.compareLiteral(partitionColumnFilter.lowerBound) == 0) {
                        result.add(partitionId);
                    }
                }
            });
        }

        List<LiteralExpr> inPredicateLiterals = partitionColumnFilter.getInPredicateLiterals();
        if (inPredicateLiterals != null && inPredicateLiterals.size() > 0) {
            literalExprValues.forEach((partitionId, values) -> {
                for (LiteralExpr partitionValueItem : values) {
                    for (LiteralExpr inValueItem : inPredicateLiterals) {
                        if (inValueItem.compareLiteral(partitionValueItem) == 0) {
                            result.add(partitionId);
                        }
                    }
                }
            });
        }

        return result.stream().collect(Collectors.toList());
    }
}
