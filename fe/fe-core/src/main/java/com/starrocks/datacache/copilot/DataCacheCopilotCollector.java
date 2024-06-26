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

package com.starrocks.datacache.copilot;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.HudiTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class DataCacheCopilotCollector {

    private static final Logger LOG = LogManager.getLogger(DataCacheCopilotCollector.class);

    public static void collectFromPhysicalPlan(OptExpression expr, boolean enableFullCollect) {
        DataCacheCopilotOptVisitor visitor = new DataCacheCopilotOptVisitor(enableFullCollect);
        expr.getOp().accept(visitor, expr, null);
        List<AccessLog> logs = visitor.getAccessLogs();
        DataCacheCopilotStorage.getInstance().addAccessLogs(logs);
    }

    private static class DataCacheCopilotOptVisitor extends OptExpressionVisitor<Void, Void> {
        private final boolean enableFullCollect;
        private final List<AccessLog> accessLogs = new LinkedList<>();
        private final long curAccessTimeSec;

        private DataCacheCopilotOptVisitor(boolean enableFullCollect) {
            this.enableFullCollect = enableFullCollect;
            // only accurate to hour, to reduce cardinality
            this.curAccessTimeSec = System.currentTimeMillis() / 1000 / 3600 * 3600;
        }

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
            Table table = scanOperator.getTable();
            String catalogName;
            String dbName;
            String tblName;
            switch (scanOperator.getOpType()) {
                case PHYSICAL_HIVE_SCAN:
                    HiveTable hiveTable = (HiveTable) table;
                    catalogName = hiveTable.getCatalogName();
                    dbName = hiveTable.getDbName();
                    tblName = hiveTable.getTableName();
                    break;
                case PHYSICAL_ICEBERG_SCAN:
                    IcebergTable icebergTable = (IcebergTable) table;
                    catalogName = icebergTable.getCatalogName();
                    dbName = icebergTable.getRemoteDbName();
                    tblName = icebergTable.getRemoteTableName();
                    break;
                case PHYSICAL_HUDI_SCAN:
                    HudiTable hudiTable = (HudiTable) table;
                    catalogName = hudiTable.getCatalogName();
                    dbName = hudiTable.getDbName();
                    tblName = hudiTable.getTableName();
                    break;
                case PHYSICAL_DELTALAKE_SCAN:
                    DeltaLakeTable deltaLakeTable = (DeltaLakeTable) table;
                    catalogName = deltaLakeTable.getCatalogName();
                    dbName = deltaLakeTable.getDbName();
                    tblName = deltaLakeTable.getTableName();
                    break;
                case PHYSICAL_PAIMON_SCAN:
                    PaimonTable paimonTable = (PaimonTable) table;
                    catalogName = paimonTable.getCatalogName();
                    dbName = paimonTable.getDbName();
                    tblName = paimonTable.getTableName();
                    break;
                default:
                    return null;
            }

            // ignore full table scan
            if (!enableFullCollect && checkIsFullColumnScan(table, scanOperator)) {
                return null;
            }

            ScanOperatorPredicates predicates = null;
            try {
                // ScanOperatorPredicates maybe nullptr
                predicates = scanOperator.getScanOperatorPredicates();
            } catch (AnalysisException e) {
                LOG.warn("Failed to get ScanOperatorPredicates", e);
            }
            if (predicates == null) {
                LOG.warn("ScanOperatorPredicates can't be null");
                return null;
            }

            // ignore full partition scan
            if (!enableFullCollect && checkIsFullPartitionScan(predicates)) {
                return null;
            }

            List<PartitionKey> partitionKeyList = predicates.getSelectedPartitionKeys();
            List<String> partitionNameLists = table.getPartitionColumnNames();

            for (Map.Entry<ColumnRefOperator, Column> entry : scanOperator.getColRefToColumnMetaMap().entrySet()) {
                Column column = entry.getValue();
                // TODO Ignore complex type collect in this pr
                if (column.getType().isComplexType()) {
                    continue;
                }

                // for none-partition table, partitionKeyList is one element with empty partition key
                Preconditions.checkArgument(!partitionKeyList.isEmpty(), "PartitionKey must existed.");
                for (PartitionKey partitionKey : partitionKeyList) {
                    accessLogs.add(new AccessLog(catalogName, dbName, tblName,
                            PartitionUtil.toHivePartitionName(partitionNameLists, partitionKey), column.getName(),
                            curAccessTimeSec));
                }
            }

            return null;
        }

        private List<AccessLog> getAccessLogs() {
            return accessLogs;
        }

        private boolean checkIsFullColumnScan(Table table, PhysicalScanOperator scanOperator) {
            int usedColumns = scanOperator.getUsedColumns().size();
            int totalColumns = table.getColumns().size();
            return usedColumns == totalColumns;
        }

        private boolean checkIsFullPartitionScan(ScanOperatorPredicates scanOperatorPredicates) {
            return scanOperatorPredicates.getSelectedPartitionIds().size() ==
                    scanOperatorPredicates.getIdToPartitionKey().size();
        }
    }
}