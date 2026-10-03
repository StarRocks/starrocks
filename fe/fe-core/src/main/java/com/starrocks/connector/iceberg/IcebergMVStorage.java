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

package com.starrocks.connector.iceberg;

import com.google.common.collect.Lists;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorViewDefinition;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.view.RefreshState;
import org.apache.iceberg.view.RefreshStateParser;
import org.apache.iceberg.view.SourceState;
import org.apache.iceberg.view.SourceTableState;

import java.util.List;
import java.util.Optional;

/**
 * Pure derivation helpers for "Mechanism C" Iceberg materialized views (the MV lives in an Iceberg REST catalog;
 * StarRocks keeps no native master). Works entirely from primitives (catalog/db/name + columns + query SQL +
 * base tables) because there is no local {@code MaterializedView} object. All Iceberg-catalog side effects are
 * driven from {@link IcebergMetadata}; this class holds only small, unit-testable logic.
 */
public final class IcebergMVStorage {
    /** The backing storage table is named "&lt;mv&gt;__storage"; the linked Iceberg View uses the MV name. */
    public static final String STORAGE_TABLE_SUFFIX = "__storage";

    private IcebergMVStorage() {
    }

    public static String storageTableName(String mvName) {
        return mvName + STORAGE_TABLE_SUFFIX;
    }

    public static String storageViewName(String mvName) {
        return mvName;
    }

    /**
     * Build the view definition carrying the Iceberg spec materialized view. The view SQL is the MV's defining
     * query in the "starrocks" dialect; the view property marks it as an SR-produced materialized view.
     */
    public static ConnectorViewDefinition buildViewDefinition(String catalogName, String dbName, String mvName,
                                                              String comment, List<Column> columns, String querySql) {
        return new ConnectorViewDefinition(
                catalogName,
                dbName,
                storageViewName(mvName),
                comment,
                columns,
                querySql,
                AlterViewStmt.AlterDialectType.NONE,
                java.util.Map.of(IcebergMetadata.MV_STORAGE_VIEW_PROPERTY, "true"));
    }

    /**
     * Validate that every base table is an Iceberg table. Mechanism C records freshness via the Iceberg
     * {@link RefreshState} (one snapshot-id per source table); a non-Iceberg source has no snapshot-id, which
     * makes staleness undecidable, so such MVs are rejected at CREATE time (C.0 scope).
     */
    public static void validateAllIcebergBaseTables(List<BaseTableInfo> baseTableInfos) {
        for (BaseTableInfo baseTableInfo : baseTableInfos) {
            Optional<Table> tableOpt = MvUtils.getTableWithIdentifier(baseTableInfo);
            if (tableOpt.isEmpty() || !(tableOpt.get() instanceof IcebergTable)) {
                throw new SemanticException(
                        "An Iceberg-catalog materialized view requires all base tables to be Iceberg tables; "
                                + "offending base table: " + baseTableInfo.getTableName());
            }
        }
    }

    /** The MV output columns must be representable in an Iceberg schema (the &lt;mv&gt;__storage table schema). */
    public static void validateIcebergSchema(List<Column> columns) {
        try {
            IcebergApiConverter.toIcebergApiSchema(columns);
        } catch (Exception e) {
            throw new SemanticException(
                    "This materialized view schema is not representable in an Iceberg catalog: " + e.getMessage());
        }
    }

    /**
     * Assemble the Iceberg RefreshState from the MV's Iceberg base tables, recording the snapshot each base table
     * was read at. Uses the 1.10.0-mv1 field model (catalog nullable, snapshot-id required, ref unused).
     */
    public static RefreshState buildRefreshState(List<BaseTableInfo> baseTableInfos, int viewVersionId) {
        List<SourceState> sourceStates = Lists.newArrayList();
        for (BaseTableInfo baseTableInfo : baseTableInfos) {
            Optional<Table> tableOpt = MvUtils.getTableWithIdentifier(baseTableInfo);
            if (tableOpt.isEmpty() || !(tableOpt.get() instanceof IcebergTable icebergTable)) {
                continue;
            }
            Snapshot snapshot = icebergTable.getNativeTable().currentSnapshot();
            long snapshotId = snapshot == null ? -1L : snapshot.snapshotId();
            SourceTableState state = new SourceTableState(
                    icebergTable.getCatalogTableName(),
                    List.of(icebergTable.getCatalogDBName()),
                    icebergTable.getCatalogName(),
                    icebergTable.getUUID(),
                    snapshotId,
                    null /* ref: branch/tag pinning not supported in C.0 */);
            sourceStates.add(state);
        }
        return new RefreshState(viewVersionId, sourceStates, System.currentTimeMillis());
    }

    public static String serializeRefreshState(RefreshState refreshState) {
        return RefreshStateParser.toJson(refreshState);
    }
}
