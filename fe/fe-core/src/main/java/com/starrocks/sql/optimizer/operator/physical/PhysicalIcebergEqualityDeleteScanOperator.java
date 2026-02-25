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

import com.starrocks.connector.iceberg.IcebergMORParams;
import com.starrocks.connector.iceberg.IcebergTableMORParams;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergEqualityDeleteScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

public class PhysicalIcebergEqualityDeleteScanOperator extends PhysicalScanOperator {

    private ScalarOperator originPredicate;

    private IcebergTableMORParams tableFullMORParams = IcebergTableMORParams.EMPTY;
    private IcebergMORParams morParams;

    public PhysicalIcebergEqualityDeleteScanOperator(LogicalIcebergEqualityDeleteScanOperator scanOperator) {
        super(OperatorType.PHYSICAL_ICEBERG_EQUALITY_DELETE_SCAN, scanOperator);
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

    @Override
    public ScanOperatorPredicates getScanOperatorPredicates() {
        return new ScanOperatorPredicates();
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalIcebergEqualityDeleteScan(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalIcebergEqualityDeleteScan(optExpression, context);
    }
}
