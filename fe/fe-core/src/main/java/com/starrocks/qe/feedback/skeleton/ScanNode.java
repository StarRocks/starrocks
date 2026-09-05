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

package com.starrocks.qe.feedback.skeleton;

import com.starrocks.catalog.Table;
import com.starrocks.qe.feedback.NodeExecStats;
import com.starrocks.qe.feedback.ParameterizedPredicate;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;

import java.util.Objects;

public class ScanNode extends SkeletonNode {

    // The tableId of the external table is monotonically increasing for each instance.
    // There is no need to compare between hashcode and equals
    private final long tableId;

    private final String tableIdentifier;

    private final ParameterizedPredicate parameterizedPredicate;

    public ScanNode(OptExpression optExpression,
                    NodeExecStats nodeExecStats, SkeletonNode parent) {
        super(optExpression, nodeExecStats, parent);
        PhysicalScanOperator scanOperator = (PhysicalScanOperator) optExpression.getOp();
        Table table = scanOperator.getTable();
        tableId = table.isExternalTableWithFileSystem() ? -1 : table.getId();
        tableIdentifier = scanOperator.getTable().getTableIdentifier();
        this.parameterizedPredicate = new ParameterizedPredicate(predicate);
    }

    public ScanNode(OptExpression optExpression) {
        super(optExpression, null, null);
        this.tableId = 0;
        this.tableIdentifier = null;
        this.parameterizedPredicate = new ParameterizedPredicate(predicate);
    }

    public long getTableId() {
        return tableId;
    }

    public String getTableIdentifier() {
        return tableIdentifier;
    }

    public boolean isEnableParameterizedMode() {
        return parameterizedPredicate.isEnableParameterizedMode();
    }

    public void enableParameterizedMode() {
        parameterizedPredicate.enableParameterizedMode();
    }

    public void disableParameterizedMode() {
        parameterizedPredicate.disableParameterizedMode();
    }

    public void mergeColumnRangePredicate(ScanNode scanNode) {
        parameterizedPredicate.mergeColumnRange(scanNode.parameterizedPredicate);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ScanNode other = (ScanNode) o;

        if (operatorId != other.operatorId || limit != other.limit || type != other.type ||
                tableId != other.tableId || !Objects.equals(tableIdentifier, other.tableIdentifier)) {
            return false;
        }

        if (Objects.equals(predicate, other.predicate)) {
            return true;
        }

        try {
            return this.parameterizedPredicate.match(other.parameterizedPredicate);
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(operatorId, type, limit, tableId, tableIdentifier);
    }
}
