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

package com.starrocks.sql.optimizer.base;

import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorSinkShuffleMode;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.InsertPartitionEstimator;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.system.SystemInfoService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;

/**
 * Per-connector strategy that decides whether to plan a global shuffle before
 * a connector sink, and computes the partition column IDs used as the hash key.
 *
 * <p>Iceberg and Hive share the same decision rule (mode + partitions + AUTO
 * adaptive check on cluster worker count and partition cardinality) — provided
 * as default methods here — but differ in how they materialize partition column
 * IDs:
 *
 * <ul>
 *   <li><b>Iceberg</b>: may inject transform projection columns (bucket/truncate/
 *       year/month/day/hour) and use their virtual column refs as the hash key.</li>
 *   <li><b>Hive</b>: identity columns only, hash key is the partition column
 *       refs directly.</li>
 * </ul>
 *
 * <p>InsertPlanner dispatches via {@link #forTable(Table)}; non-eligible tables
 * (OlapTable, TableFunctionTable, Paimon, ...) return {@link Optional#empty()}
 * and the planner falls back to its existing connector-specific code paths.
 */
public interface ConnectorSinkShuffleSpec {

    Logger LOG = LogManager.getLogger(ConnectorSinkShuffleSpec.class);

    /** The connector sink target this spec wraps. */
    Table table();

    /**
     * Compute partition column IDs and (optionally) rewrite the logical plan to
     * inject transform projection columns. Hive returns identity column IDs and
     * leaves the plan unchanged; Iceberg may add a {@code LogicalProjectOperator}
     * for non-identity transforms.
     */
    PreOptimizeResult prepare(InsertStmt insertStmt,
                              List<ColumnRefOperator> outputColumns,
                              ColumnRefFactory columnRefFactory,
                              OptExpression root,
                              ColumnRefSet requiredColumns);

    /**
     * Optional sort property override (Iceberg may want host-level sort by the
     * table's native sort order). Default: no sort.
     */
    default SortProperty computeSortProperty(SessionVariable session,
                                             List<ColumnRefOperator> outputColumns) {
        return SortProperty.createProperty(java.util.Collections.emptyList());
    }

    /**
     * Connector-agnostic decision: mode NEVER / unpartitioned → no shuffle;
     * FORCE → always; AUTO → delegate to {@link #shouldEnableAdaptiveGlobalShuffle}.
     */
    default boolean shouldShuffle(InsertStmt insertStmt, SessionVariable sv) {
        ConnectorSinkShuffleMode mode = effectiveShuffleMode(sv);
        if (mode == ConnectorSinkShuffleMode.NEVER || table().getPartitionColumns().isEmpty()) {
            return false;
        }
        return mode == ConnectorSinkShuffleMode.FORCE
                || (mode == ConnectorSinkShuffleMode.AUTO
                        && shouldEnableAdaptiveGlobalShuffle(insertStmt, sv));
    }

    /**
     * The shuffle mode used by {@link #shouldShuffle}. Connectors override this
     * only when they need a connector-specific view of the mode (e.g. Iceberg
     * honours the legacy {@code enable_iceberg_sink_global_shuffle} boolean).
     */
    default ConnectorSinkShuffleMode effectiveShuffleMode(SessionVariable sv) {
        return sv.getConnectorSinkShuffleMode();
    }

    /**
     * AUTO-mode adaptive check, identical to the previous Iceberg-only logic:
     * enable shuffle when worker count > 1 AND
     *   (estimated partitions >= workerCount * ratio
     *    OR estimated partitions >= absolute threshold).
     *
     * <p>Uses {@link SystemInfoService} worker counts and {@link InsertPartitionEstimator}
     * — both already work on the generic {@link Table} type.
     */
    default boolean shouldEnableAdaptiveGlobalShuffle(InsertStmt insertStmt, SessionVariable sv) {
        SystemInfoService clusterInfo = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        int totalWorkerNum = clusterInfo.getAliveBackendNumber() + clusterInfo.getTotalComputeNodeNumber();
        if (totalWorkerNum <= 1) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Disable adaptive global shuffle for connector table {}.{}: " +
                                "total worker nodes ({}) <= 1",
                        table().getCatalogDBName(), table().getCatalogTableName(), totalWorkerNum);
            }
            return false;
        }

        long estimatedPartitionCount = InsertPartitionEstimator.estimatePartitionCountForInsert(insertStmt, table());
        if (estimatedPartitionCount <= 1) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Disable adaptive global shuffle for connector table {}.{}: " +
                                "estimated partition count ({}) <= 1",
                        table().getCatalogDBName(), table().getCatalogTableName(), estimatedPartitionCount);
            }
            return false;
        }

        long partitionCountThreshold = sv.getConnectorSinkShufflePartitionThreshold();
        double partitionCountNodeRatio = sv.getConnectorSinkShufflePartitionNodeRatio();
        boolean shouldEnable = estimatedPartitionCount >= (long) (totalWorkerNum * partitionCountNodeRatio)
                || estimatedPartitionCount >= partitionCountThreshold;

        if (shouldEnable && LOG.isDebugEnabled()) {
            LOG.debug("Enable adaptive global shuffle for connector table {}.{}: " +
                            "estimated partitions={}, total workers={}, threshold={}, ratio={}",
                    table().getCatalogDBName(), table().getCatalogTableName(),
                    estimatedPartitionCount, totalWorkerNum,
                    partitionCountThreshold, partitionCountNodeRatio);
        }
        return shouldEnable;
    }

    /** Factory: returns the spec for connector-sink-eligible tables, else empty. */
    static Optional<ConnectorSinkShuffleSpec> forTable(Table table) {
        if (table instanceof IcebergTable) {
            return Optional.of(new IcebergSinkShuffleSpec((IcebergTable) table));
        }
        if (table instanceof HiveTable) {
            return Optional.of(new HiveSinkShuffleSpec((HiveTable) table));
        }
        return Optional.empty();
    }

    /**
     * Result of plan preparation. {@code root} may differ from the original (Iceberg
     * transform projection); {@code requiredColumns} may carry newly-introduced
     * column refs; {@code partitionColumnIDs} are the hash key.
     */
    final class PreOptimizeResult {
        public final OptExpression root;
        public final ColumnRefSet requiredColumns;
        public final List<Integer> partitionColumnIDs;

        public PreOptimizeResult(OptExpression root,
                                 ColumnRefSet requiredColumns,
                                 List<Integer> partitionColumnIDs) {
            this.root = root;
            this.requiredColumns = requiredColumns;
            this.partitionColumnIDs = partitionColumnIDs;
        }
    }
}
