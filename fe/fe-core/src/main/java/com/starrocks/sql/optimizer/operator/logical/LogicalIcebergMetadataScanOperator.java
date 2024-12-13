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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
<<<<<<< HEAD
=======
import com.starrocks.connector.TableVersionRange;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;

public class LogicalIcebergMetadataScanOperator extends LogicalScanOperator {
<<<<<<< HEAD
    private String temporalClause;
    private ScanOperatorPredicates predicates = new ScanOperatorPredicates();
=======
    private ScanOperatorPredicates predicates = new ScanOperatorPredicates();
    private boolean isTransformed;

    public LogicalIcebergMetadataScanOperator(Table table,
                                      Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                      Map<Column, ColumnRefOperator> columnMetaToColRefMap,
                                      long limit,
                                      ScalarOperator predicate) {
        this(table, colRefToColumnMetaMap, columnMetaToColRefMap, limit, predicate, TableVersionRange.empty());
    }
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

    public LogicalIcebergMetadataScanOperator(Table table,
                                              Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                              Map<Column, ColumnRefOperator> columnMetaToColRefMap,
                                              long limit,
<<<<<<< HEAD
                                              ScalarOperator predicate) {

=======
                                              ScalarOperator predicate,
                                              TableVersionRange versionRange) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        super(OperatorType.LOGICAL_ICEBERG_METADATA_SCAN,
                table,
                colRefToColumnMetaMap,
                columnMetaToColRefMap,
                limit,
<<<<<<< HEAD
                predicate, null);

    }

    public String getTemporalClause() {
        return temporalClause;
    }

    public void setTemporalClause(String temporalClause) {
        this.temporalClause = temporalClause;
    }

=======
                predicate,
                null,
                versionRange);

    }

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    private LogicalIcebergMetadataScanOperator() {
        super(OperatorType.LOGICAL_ICEBERG_METADATA_SCAN);
    }

    public ScanOperatorPredicates getScanOperatorPredicates() {
        return predicates;
    }

    public void setScanOperatorPredicates(ScanOperatorPredicates predicates) {
        this.predicates = predicates;
    }

<<<<<<< HEAD
=======
    public boolean isTransformed() {
        return isTransformed;
    }

    public void setTransformed(boolean transformed) {
        isTransformed = transformed;
    }

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalIcebergMetadataScan(this, context);
    }

    public static class Builder
            extends LogicalScanOperator.Builder<LogicalIcebergMetadataScanOperator, LogicalIcebergMetadataScanOperator.Builder> {

        @Override
        protected LogicalIcebergMetadataScanOperator newInstance() {
            return new LogicalIcebergMetadataScanOperator();
        }

        @Override
        public LogicalIcebergMetadataScanOperator.Builder withOperator(LogicalIcebergMetadataScanOperator scanOperator) {
            super.withOperator(scanOperator);
            builder.predicates = scanOperator.predicates;
<<<<<<< HEAD
            builder.temporalClause = scanOperator.temporalClause;
=======
            builder.isTransformed = scanOperator.isTransformed;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            return this;
        }
    }
}