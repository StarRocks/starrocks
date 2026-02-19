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

package com.starrocks.planner;

import com.starrocks.catalog.IcebergTable;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;

import java.util.ArrayList;

/**
 * PlanNode for Iceberg metadata-level delete operation.
 * This node represents a DELETE that can be performed without generating
 * position delete files, by using Iceberg's DeleteFiles API directly.
 */
public class IcebergMetadataDeleteNode extends PlanNode {
    private final IcebergTable table;
    private final ScalarOperator predicate;

    public IcebergMetadataDeleteNode(PlanNodeId id, IcebergTable table,
                                     ScalarOperator predicate) {
        super(id, new ArrayList<>(), "ICEBERG_METADATA_DELETE");
        this.table = table;
        this.predicate = predicate;
    }

    @Override
    public void computeStats() {
        avgRowSize = 0;
        cardinality = 0;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.EMPTY_SET_NODE;
    }

    @Override
    protected String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("ICEBERG METADATA DELETE\n");
        sb.append(prefix).append("  catalog: ").append(table.getCatalogName()).append("\n");
        sb.append(prefix).append("  database: ").append(table.getCatalogDBName()).append("\n");
        sb.append(prefix).append("  table: ").append(table.getCatalogTableName()).append("\n");

        if (predicate != null) {
            sb.append(prefix).append("  predicate: ").append(predicate.toString()).append("\n");
        } else {
            sb.append(prefix).append("  predicate: ALL (delete all rows)\n");
        }

        return sb.toString();
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return false;
    }

    public IcebergTable getTable() {
        return table;
    }

    public ScalarOperator getPredicate() {
        return predicate;
    }
}
