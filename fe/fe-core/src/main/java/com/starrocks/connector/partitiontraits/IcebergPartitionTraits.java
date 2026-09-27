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
package com.starrocks.connector.partitiontraits;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergPartitionKey;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.NullablePartitionKey;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.connector.ConnectorMetadataRequestContext;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.iceberg.IcebergPartitionUtils;
import com.starrocks.connector.iceberg.Partition;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.LiteralExprFactory;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.type.Type;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Snapshot;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class IcebergPartitionTraits extends DefaultTraits {
    @Override
    public boolean isSupportPCTRefresh() {
        return true;
    }

    @Override
    public String getTableName() {
        return table.getCatalogTableName();
    }

    @Override
    public PartitionKey createEmptyKey() {
        return new IcebergPartitionKey();
    }

    @Override
    public List<PartitionInfo> getPartitions(List<String> partitionNames) {
        IcebergTable icebergTable = (IcebergTable) table;
        if (pinnedVersionRange != null) {
            ConnectorMetadataRequestContext ctx = new ConnectorMetadataRequestContext();
            ctx.setTableVersionRange(pinnedVersionRange);
            return GlobalStateMgr.getCurrentState().getMetadataMgr().
                    getPartitions(icebergTable.getCatalogName(), table, partitionNames, ctx);
        }
        return GlobalStateMgr.getCurrentState().getMetadataMgr().
                getPartitions(icebergTable.getCatalogName(), table, partitionNames);
    }

    // record_count from the PARTITIONS table is pre-delete (live data-file rows), while a MOR read returns
    // post-delete rows. For POSITION deletes (exactly one row removed per record) a small delete fraction
    // keeps record_count a trustworthy live-row total; above this fraction the difference can no longer be
    // attributed to truncation vs. deletes, so the partition falls back to raw sample. EQUALITY deletes are
    // handled separately: their record count is not a row count, so the ratio is meaningless there.
    private static final double MAX_DELETE_RATIO_FOR_ROW_COUNT = 0.1;

    @Override
    public Map<String, Long> getPartitionRowCounts(List<String> partitionNames) {
        // The Iceberg PARTITIONS metadata table carries per-partition row/delete counts, so one metadata scan
        // yields all totals without opening data files (see IcebergCatalog#getPartitions).
        Map<String, Long> result = new HashMap<>();
        List<PartitionInfo> partitions = getPartitions(partitionNames);
        for (int i = 0; i < partitionNames.size() && i < partitions.size(); i++) {
            PartitionInfo info = partitions.get(i);
            if (!(info instanceof Partition)) {
                continue;
            }
            Partition partition = (Partition) info;
            long liveTotal = liveRowCountForExtrapolation(partition.getRecordCount(),
                    partition.getPositionDeleteRecordCount(), partition.getEqualityDeleteRecordCount());
            if (liveTotal > 0) {
                result.put(partitionNames.get(i), liveTotal);
            }
        }
        return result;
    }

    // Package-private for unit testing. Returns a trustworthy live-row total for extrapolation, or -1 when it
    // should not be trusted, in which case the caller stores the raw sample instead.
    //
    // Returns -1 when:
    //  - record_count is unknown; or
    //  - any equality deletes are present: equality_delete_record_count counts delete predicates, not rows
    //    removed - one predicate can match many rows - so it yields neither a trustworthy live total nor a
    //    trustworthy ratio (a tiny count can still hide a huge deletion). No exact live total is available
    //    cheaply from metadata, so we bail; or
    //  - position deletes exceed MAX_DELETE_RATIO_FOR_ROW_COUNT of record_count.
    //
    // Otherwise position deletes (exactly one row each) are subtracted to give the live-row total.
    static long liveRowCountForExtrapolation(long recordCount, long positionDeleteRecordCount,
                                             long equalityDeleteRecordCount) {
        if (recordCount <= 0) {
            return -1;
        }
        if (equalityDeleteRecordCount > 0) {
            return -1;
        }
        long posDelete = Math.max(0, positionDeleteRecordCount);
        double deleteRatio = (double) posDelete / recordCount;
        if (deleteRatio > MAX_DELETE_RATIO_FOR_ROW_COUNT) {
            return -1;
        }
        return Math.max(1, recordCount - posDelete);
    }

    @Override
    public Optional<Long> maxPartitionRefreshTs() {
        IcebergTable icebergTable = (IcebergTable) table;
        return Optional.ofNullable(icebergTable.getNativeTable().currentSnapshot()).map(Snapshot::timestampMillis);
    }

    @Override
    public List<String> getPartitionNames() {
        if (table.isUnPartitioned()) {
            return Lists.newArrayList(table.getName());
        }

        IcebergTable icebergTable = (IcebergTable) table;
        ConnectorMetadataRequestContext requestContext = new ConnectorMetadataRequestContext();
        requestContext.setQueryMVRewrite(isQueryMVRewrite());
        if (pinnedVersionRange != null) {
            requestContext.setTableVersionRange(pinnedVersionRange);
        } else {
            Optional<Long> snapshotId = Optional.ofNullable(icebergTable.getNativeTable().currentSnapshot())
                    .map(Snapshot::snapshotId);
            requestContext.setTableVersionRange(TvrTableSnapshot.of(snapshotId));
        }
        return GlobalStateMgr.getCurrentState().getMetadataMgr().listPartitionNames(
                table.getCatalogName(), getCatalogDBName(), getTableName(), requestContext);
    }

    @Override
    public PartitionKey createPartitionKey(List<String> partitionValues, List<Column> partitionColumns)
            throws AnalysisException {
        Preconditions.checkState(partitionValues.size() == partitionColumns.size(),
                "columns size is %s, but values size is %s", partitionColumns.size(),
                partitionValues.size());

        IcebergTable icebergTable = (IcebergTable) table;
        List<PartitionField> partitionFields = Lists.newArrayList();
        for (Column column : partitionColumns) {
            for (PartitionField field : icebergTable.getNativeTable().spec().fields()) {
                String partitionFieldName = icebergTable.getNativeTable().schema().findColumnName(field.sourceId());
                if (partitionFieldName.equalsIgnoreCase(column.getName())) {
                    partitionFields.add(field);
                }
            }
        }
        Preconditions.checkState(partitionFields.size() == partitionColumns.size(),
                "columns size is %s, but partitionFields size is %s", partitionColumns.size(), partitionFields.size());

        PartitionKey partitionKey = createEmptyKey();

        // change string value to LiteralExpr,
        for (int i = 0; i < partitionValues.size(); i++) {
            String rawValue = partitionValues.get(i);
            Column column = partitionColumns.get(i);
            PartitionField field = partitionFields.get(i);
            LiteralExpr exprValue;
            // rawValue could be null for delta table
            if (rawValue == null) {
                rawValue = "null";
            }
            if (((NullablePartitionKey) partitionKey).nullPartitionValueList().contains(rawValue)) {
                partitionKey.setNullPartitionValue(rawValue);
                exprValue = NullLiteral.create(column.getType());
            } else {
                // transform year/month/day/hour dedup name is time
                if (field.transform().dedupName().equalsIgnoreCase("time")) {
                    rawValue = IcebergPartitionUtils.normalizeTimePartitionName(rawValue, field,
                            icebergTable.getNativeTable().schema(), column.getType());
                    exprValue = LiteralExprFactory.create(rawValue, column.getType());
                } else {
                    exprValue = LiteralExprFactory.create(rawValue, column.getType());
                }
            }
            partitionKey.pushColumn(exprValue, column.getType().getPrimitiveType());
        }
        return partitionKey;
    }

    @Override
    public LocalDateTime getTableLastUpdateTime(int extraSeconds) {
        IcebergTable icebergTable = (IcebergTable) table;
        Optional<Snapshot> snapshot = Optional.ofNullable(icebergTable.getNativeTable().currentSnapshot());
        return snapshot.map(value -> LocalDateTime.ofInstant(Instant.ofEpochMilli(value.timestampMillis()).
                plusSeconds(extraSeconds), Clock.systemDefaultZone().getZone())).orElse(null);
    }

    @Override
    public PartitionKey createPartitionKeyWithType(List<String> values, List<Type> types) throws AnalysisException {
        Preconditions.checkState(values.size() == types.size(),
                "columns size is %s, but values size is %s", types.size(), values.size());

        PartitionKey partitionKey = createEmptyKey();
        for (int i = 0; i < values.size(); i++) {
            String rawValue = values.get(i);
            Type type = types.get(i);
            LiteralExpr exprValue;
            if (rawValue == null) {
                exprValue = NullLiteral.create(type);
            } else {
                exprValue = LiteralExprFactory.create(rawValue, type);
            }
            partitionKey.pushColumn(exprValue, type.getPrimitiveType());
        }

        for (int i = 0; i < types.size(); i++) {
            LiteralExpr exprValue = partitionKey.getKeys().get(i);
            if (exprValue.getType().isDecimalV3()) {
                exprValue.setType(types.get(i)); //keep the precision and scale.
            }
        }
        return partitionKey;
    }
}

