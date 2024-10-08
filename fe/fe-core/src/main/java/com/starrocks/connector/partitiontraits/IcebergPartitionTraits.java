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
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergPartitionKey;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.NullablePartitionKey;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.common.AnalysisException;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.TableVersionRange;
import com.starrocks.connector.iceberg.IcebergPartitionUtils;
import com.starrocks.server.GlobalStateMgr;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Snapshot;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public class IcebergPartitionTraits extends DefaultTraits {

    @Override
    public String getDbName() {
        return ((IcebergTable) table).getRemoteDbName();
    }

    @Override
    public boolean isSupportPCTRefresh() {
        return true;
    }

    @Override
    public String getTableName() {
        return ((IcebergTable) table).getRemoteTableName();
    }

    @Override
    public PartitionKey createEmptyKey() {
        return new IcebergPartitionKey();
    }

    @Override
    public List<PartitionInfo> getPartitions(List<String> partitionNames) {
        IcebergTable icebergTable = (IcebergTable) table;
        return GlobalStateMgr.getCurrentState().getMetadataMgr().
                getPartitions(icebergTable.getCatalogName(), table, partitionNames);
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
        Optional<Long> snapshotId = Optional.ofNullable(icebergTable.getNativeTable().currentSnapshot())
                .map(Snapshot::snapshotId);
        return GlobalStateMgr.getCurrentState().getMetadataMgr().listPartitionNames(
                table.getCatalogName(), getDbName(), getTableName(), TableVersionRange.withEnd(snapshotId));
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
                    exprValue = LiteralExpr.create(rawValue,  column.getType());
                } else {
                    exprValue = LiteralExpr.create(rawValue,  column.getType());
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
}

