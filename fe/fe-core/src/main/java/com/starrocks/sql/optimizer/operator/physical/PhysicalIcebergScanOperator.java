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

package com.starrocks.sql.optimizer.operator.physical;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnDict;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class PhysicalIcebergScanOperator extends PhysicalScanOperator {
    private ScanOperatorPredicates predicates;
    private List<Pair<Integer, ColumnDict>> globalDicts = Lists.newArrayList();

    public PhysicalIcebergScanOperator(LogicalIcebergScanOperator scanOperator) {
        super(OperatorType.PHYSICAL_ICEBERG_SCAN, scanOperator);
        this.predicates = scanOperator.getScanOperatorPredicates();
    }

    public PhysicalIcebergScanOperator(Table table,
                                       Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                       long limit,
                                       ScalarOperator predicate,
                                       Projection projection,
                                       ScanOperatorPredicates predicates) {
        super(OperatorType.PHYSICAL_ICEBERG_SCAN, table, colRefToColumnMetaMap, limit, predicate, projection);
        this.predicates = predicates;
    }

    @Override
    public ScanOperatorPredicates getScanOperatorPredicates() {
        return this.predicates;
    }

    @Override
    public void setScanOperatorPredicates(ScanOperatorPredicates predicates) {
        this.predicates = predicates;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalIcebergScan(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalIcebergScan(optExpression, context);
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        ColumnRefSet refs = super.getUsedColumns();
        predicates.getNoEvalPartitionConjuncts().forEach(d -> refs.union(d.getUsedColumns()));
        predicates.getPartitionConjuncts().forEach(d -> refs.union(d.getUsedColumns()));
        predicates.getMinMaxConjuncts().forEach(d -> refs.union(d.getUsedColumns()));
        predicates.getMinMaxColumnRefMap().keySet().forEach(refs::union);
        return refs;
    }

    public void setOutputColumns(List<ColumnRefOperator> outputColumns) {
        this.outputColumns = outputColumns;
    }

    public List<Pair<Integer, ColumnDict>> getGlobalDicts() {
        return globalDicts;
    }

    public void setGlobalDicts(
            List<Pair<Integer, ColumnDict>> globalDicts) {
        this.globalDicts = globalDicts;
    }

    public boolean hasOnlyScanParquetFiles() {
        IcebergTable icebergTable = (IcebergTable) table;
        Optional<Snapshot> snapshot = icebergTable.getSnapshot();
        if (!snapshot.isPresent()) {
            return false;
        }
        long snapshotId = snapshot.get().snapshotId();
        List<RemoteFileInfo> splits = GlobalStateMgr.getCurrentState().getMetadataMgr().getRemoteFileInfos(
                icebergTable.getCatalogName(), icebergTable, null, snapshotId, predicate, null);
        if (splits.isEmpty()) {
            return false;
        }
        RemoteFileDesc remoteFileDesc = splits.get(0).getFiles().get(0);
        if (remoteFileDesc == null) {
            return false;
        }
        for (FileScanTask task : remoteFileDesc.getIcebergScanTasks()) {
            DataFile file = task.file();
            if (file.format() != FileFormat.PARQUET) {
                return false;
            }
        }
        return true;
    }
}
