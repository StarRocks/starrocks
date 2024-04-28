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

package com.starrocks.datacache.statistic;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.common.AnalysisException;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class DataCacheCopilotCollector {

    private static final Logger LOG = LogManager.getLogger(DataCacheCopilotCollector.class);

    public static void collectFrequency(OptExpression expr) {
        DataCacheCopilotOptVisitor visitor = new DataCacheCopilotOptVisitor();
        expr.getOp().accept(visitor, expr, null);
        List<AccessItem> items = visitor.getAccessItems();
        CachedFrequencyStatisticStorage.addAccessItems(items);
    }

    private static class DataCacheCopilotOptVisitor extends OptExpressionVisitor<Void, Void> {
        private final List<AccessItem> accessItems = new LinkedList<>();

        @Override
        public Void visit(OptExpression optExpression, Void context) {
            for (OptExpression input : optExpression.getInputs()) {
                input.getOp().accept(this, input, null);
            }
            return null;
        }

        @Override
        public Void visitPhysicalScan(OptExpression optExpression, Void context) {
            PhysicalScanOperator scanOperator = (PhysicalScanOperator) optExpression.getOp();
            OperatorType opType = scanOperator.getOpType();
            if (opType == OperatorType.PHYSICAL_HIVE_SCAN) {
                // todo
                HiveTable table = (HiveTable) scanOperator.getTable();
                String catalogName = table.getCatalogName();
                String dbName = table.getDbName();
                String tableName = table.getTableName();
                Map<ColumnRefOperator, Column> map = scanOperator.getColRefToColumnMetaMap();
                Optional<ScanOperatorPredicates> predicates = Optional.empty();
                long accessTime = 0L;
                try {
                    predicates = Optional.ofNullable(scanOperator.getScanOperatorPredicates());
                } catch (AnalysisException e) {
                    LOG.warn(e.getMessage());
                }


                List<PartitionKey> partitionKeyList = new LinkedList<>();
                List<String> partitionNameLists = table.getPartitionColumnNames();
                predicates.ifPresent(scanOperatorPredicates -> partitionKeyList.addAll(
                        scanOperatorPredicates.getSelectedPartitionKeys()));

                for (Map.Entry<ColumnRefOperator, Column> entry : map.entrySet()) {
                    Column column = entry.getValue();
                    if (partitionKeyList.isEmpty()) {
                        accessItems.add(
                                new AccessItem(Optional.empty(), catalogName, dbName, tableName, column.getName(),
                                        accessTime));
                    } else {
                        for (PartitionKey partitionKey : partitionKeyList) {
                            accessItems.add(new AccessItem(
                                    Optional.of(PartitionUtil.toHivePartitionName(partitionNameLists, partitionKey)),
                                    catalogName, dbName, tableName, column.getName(), accessTime));
                        }
                    }
                }
            }

            return null;
        }

        public List<AccessItem> getAccessItems() {
            return accessItems;
        }
    }
}
