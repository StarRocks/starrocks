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

package com.starrocks.sql.analyzer;

import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.connector.ConnectorTableVersion;
import com.starrocks.connector.PointerType;
import com.starrocks.connector.iceberg.IcebergMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.QueryPeriod;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.transformer.ExpressionMapping;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Optional;

public class QueryPeriodResolver {
    private static final Logger LOG = LogManager.getLogger(QueryPeriodResolver.class);

    private QueryPeriodResolver() {
    }

    public static Optional<ConnectorTableVersion> resolve(Optional<Expr> version,
                                                          QueryPeriod.PeriodType type,
                                                          ConnectContext session) {
        if (version.isEmpty()) {
            return Optional.empty();
        }
        ScalarOperator result;
        try {
            Scope scope = new Scope(RelationId.anonymous(), new RelationFields());
            ExpressionAnalyzer.analyzeExpression(version.get(), new AnalyzeState(), scope, session);
            ExpressionMapping expressionMapping = new ExpressionMapping(scope);
            result = SqlToScalarOperatorTranslator.translate(version.get(), expressionMapping, new ColumnRefFactory());
        } catch (Exception e) {
            throw new SemanticException("Failed to resolve query period [type: %s, value: %s]. msg: %s",
                    type.toString(), version.get().toString(), e.getMessage());
        }

        if (!(result instanceof ConstantOperator)) {
            if (version.get() instanceof FunctionCallExpr) {
                throw new SemanticException("Invalid datetime function: [type: %s, value: %s]. " +
                        "The function requirement must be inferred in frontend.", type.toString(),
                        version.get().toString());
            } else {
                throw new SemanticException("Invalid version value. [type: %s, value: %s]",
                        type.toString(), version.get().toString());
            }
        }
        PointerType pointerType = type == QueryPeriod.PeriodType.TIMESTAMP ? PointerType.TEMPORAL : PointerType.VERSION;
        return Optional.of(new ConnectorTableVersion(pointerType, (ConstantOperator) result));
    }

    /**
     * Relation-level entry for the analyzer: resolve the relation's query period to a version range
     * once, pin it on the relation (so RelationTransformer reuses it), and rebind the table to the
     * targeted snapshot's read metadata, so column resolution and the BE descriptor honor that snapshot.
     * Returns the (possibly rebound) table; the caller owns writing it back to the relation. No-op when
     * the relation has no query period or its version range is already pinned (e.g. pre-resolved in the
     * unlocked phase).
     */
    public static Table resolveAndBindTable(TableRelation tableRelation, Table table,
                                            ConnectContext session, MetadataMgr metadataMgr) {
        if (tableRelation.getQueryPeriod() == null || tableRelation.getTvrVersionRange() != null) {
            return table;
        }
        // Only Iceberg resolves its version range and binds a per-query read view during analysis today;
        // other temporal tables (Paimon, MySQL) keep resolving at transform time, so gate here to avoid
        // adding any analysis-time overhead for them.
        if (!(table instanceof IcebergTable)) {
            return table;
        }

        QueryPeriod queryPeriod = tableRelation.getQueryPeriod();
        QueryPeriod.PeriodType periodType = queryPeriod.getPeriodType();
        TvrVersionRange versionRange;
        try {
            Optional<ConnectorTableVersion> startVersion = resolve(queryPeriod.getStart(), periodType, session);
            Optional<ConnectorTableVersion> endVersion = resolve(queryPeriod.getEnd(), periodType, session);
            versionRange = metadataMgr.getTableVersionRange(
                    tableRelation.getName().getDb(), table, startVersion, endVersion);
        } catch (Exception e) {
            // Best-effort version resolution: on failure keep the current table and leave the range unpinned.
            // Time-travel-prohibited DDL (CREATE VIEW/MV, ALTER VIEW) then rejects the clause right after
            // analysis; a genuine query re-resolves at transform time and surfaces the real error there.
            // Binding stays outside this catch so its own failures are not swallowed.
            LOG.debug("Deferring Iceberg time-travel version resolution for table {}: {}",
                    tableRelation.getName(), e.getMessage());
            return table;
        }
        // Pin only after a successful bind, so a failed (or retried) bind never leaves the relation
        // pinned-but-unbound, which the guard above would later mistake for "already resolved".
        Table boundTable = bindIcebergSnapshotReadView((IcebergTable) table, versionRange);
        tableRelation.setTvrVersionRange(versionRange);
        return boundTable;
    }

    // Rebind the Iceberg table to the resolved snapshot's read metadata (schema + partition specs),
    // returning an immutable per-query copy.
    private static Table bindIcebergSnapshotReadView(IcebergTable icebergTable, TvrVersionRange versionRange) {
        Optional<Long> snapshotId = versionRange.end();
        if (snapshotId.isEmpty()) {
            return icebergTable;
        }
        org.apache.iceberg.Table nativeTable = icebergTable.getNativeTable();
        Schema snapshotSchema = IcebergMetadata.getSnapshotSchema(nativeTable, snapshotId.get());
        if (snapshotSchema == null) {
            // Legacy metadata without a per-snapshot schema id: keep the current table state.
            return icebergTable;
        }
        // Pin the snapshot's read metadata (schema + partition specs). Bind it whenever time travel
        // resolves a snapshot, not only when the schema id differs: partition evolution can change the
        // spec while leaving the schema id unchanged, and the descriptor/partition metadata must then
        // still reflect the snapshot rather than the current spec.
        Map<Integer, PartitionSpec> snapshotSpecs = IcebergMetadata.getSnapshotSpecs(nativeTable, snapshotId.get());
        return icebergTable.withReadMetadata(snapshotSchema, snapshotSpecs);
    }
}
