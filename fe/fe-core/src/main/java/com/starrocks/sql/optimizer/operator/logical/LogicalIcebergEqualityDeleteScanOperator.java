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

package com.starrocks.sql.optimizer.operator.logical;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.TableVersionRange;
import com.starrocks.connector.iceberg.IcebergMORParams;
import com.starrocks.connector.iceberg.IcebergTableMORParams;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;

public class LogicalIcebergEqualityDeleteScanOperator extends LogicalScanOperator {
    // Origin query predicate may have some filters that don't belong to delete scan table.
    // Therefore, we cannot use origin predicate as delete table predicate because it does not conform to semantics.
    // Because we need to use the origin predicate to get iceberg scan task in the query level cache, we record it in here.
    // At the same time, this is also the reason why there is no to reuse LogicalIcebergScanOperator.
    private ScalarOperator originPredicate;

    // Mainly used for table with iceberg equality delete files. Record full iceberg mor params in the table,
    // used for the first build to associate multiple scan nodes RemoteFileInfoSource.
    private IcebergTableMORParams tableFullMORParams = IcebergTableMORParams.EMPTY;

    // Mainly used for table with iceberg equality delete files.
    // Marking this split scan node type after IcebergEqualityDeleteRewriteRule rewriting.
    private IcebergMORParams morParams;

    public LogicalIcebergEqualityDeleteScanOperator(Table table,
                                      Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                      Map<Column, ColumnRefOperator> columnMetaToColRefMap,
                                      long limit,
                                      ScalarOperator predicate) {
        this(table, colRefToColumnMetaMap, columnMetaToColRefMap, limit, predicate, TableVersionRange.empty());
    }

    public LogicalIcebergEqualityDeleteScanOperator(Table table,
                                      Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                      Map<Column, ColumnRefOperator> columnMetaToColRefMap,
                                      long limit,
                                      ScalarOperator predicate,
                                      TableVersionRange versionRange) {
        super(OperatorType.LOGICAL_ICEBERG_EQUALITY_DELETE_SCAN,
                table,
                colRefToColumnMetaMap,
                columnMetaToColRefMap,
                limit,
                predicate, null, versionRange);

        Preconditions.checkState(table instanceof IcebergTable);
    }

    public ScalarOperator getOriginPredicate() {
        return originPredicate;
    }

    public void setOriginPredicate(ScalarOperator originPredicate) {
        this.originPredicate = originPredicate;
    }


    public IcebergTableMORParams getTableFullMORParams() {
        return tableFullMORParams;
    }

    public void setTableFullMORParams(IcebergTableMORParams tableFullMORParams) {
        this.tableFullMORParams = tableFullMORParams;
    }

    public IcebergMORParams getMORParams() {
        return morParams;
    }

    public void setMORParams(IcebergMORParams morParams) {
        this.morParams = morParams;
    }

    private LogicalIcebergEqualityDeleteScanOperator() {
        super(OperatorType.LOGICAL_ICEBERG_EQUALITY_DELETE_SCAN);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalIcebergEqualityDeleteScan(this, context);
    }

    public static class Builder
            extends LogicalScanOperator.Builder<LogicalIcebergEqualityDeleteScanOperator,
            LogicalIcebergEqualityDeleteScanOperator.Builder> {

        @Override
        protected LogicalIcebergEqualityDeleteScanOperator newInstance() {
            return new LogicalIcebergEqualityDeleteScanOperator();
        }

        @Override
        public LogicalIcebergEqualityDeleteScanOperator.Builder withOperator(
                LogicalIcebergEqualityDeleteScanOperator scanOperator) {
            super.withOperator(scanOperator);
            builder.originPredicate = scanOperator.originPredicate;
            builder.tableFullMORParams = scanOperator.tableFullMORParams;
            builder.morParams = scanOperator.morParams;
            return this;
        }
    }
}
