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
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.NullablePartitionKey;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
<<<<<<< HEAD
=======
import com.starrocks.connector.ConnectorMetadatRequestContext;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.starrocks.connector.ConnectorPartitionTraits;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.server.GlobalStateMgr;
<<<<<<< HEAD
import org.apache.commons.lang.NotImplementedException;

=======
import com.starrocks.sql.common.PCell;
import org.apache.commons.lang.NotImplementedException;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
<<<<<<< HEAD

public abstract class DefaultTraits extends ConnectorPartitionTraits  {
    @Override
    public boolean supportPartitionRefresh() {
        return false;
    }

    @Override
    public PartitionKey createPartitionKey(List<String> values, List<Column> columns) throws AnalysisException {
        Preconditions.checkState(values.size() == columns.size(),
                "columns size is %s, but values size is %s", columns.size(), values.size());
=======
import java.util.stream.Collectors;

public abstract class DefaultTraits extends ConnectorPartitionTraits {

    @Override
    public PartitionKey createPartitionKeyWithType(List<String> values, List<Type> types) throws AnalysisException {
        Preconditions.checkState(values.size() == types.size(),
                "columns size is %s, but values size is %s", types.size(), values.size());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

        PartitionKey partitionKey = createEmptyKey();

        // change string value to LiteralExpr,
        for (int i = 0; i < values.size(); i++) {
            String rawValue = values.get(i);
<<<<<<< HEAD
            Type type = columns.get(i).getType();
=======
            Type type = types.get(i);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            LiteralExpr exprValue;
            // rawValue could be null for delta table
            if (rawValue == null) {
                rawValue = "null";
            }
            if (((NullablePartitionKey) partitionKey).nullPartitionValueList().contains(rawValue)) {
                partitionKey.setNullPartitionValue(rawValue);
                exprValue = NullLiteral.create(type);
            } else {
                exprValue = LiteralExpr.create(rawValue, type);
            }
            partitionKey.pushColumn(exprValue, type.getPrimitiveType());
        }
        return partitionKey;
    }

    @Override
<<<<<<< HEAD
=======
    public PartitionKey createPartitionKey(List<String> partitionValues, List<Column> partitionColumns)
            throws AnalysisException {
        return createPartitionKeyWithType(partitionValues,
                partitionColumns.stream().map(Column::getType).collect(Collectors.toList()));
    }

    @Override
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    public List<String> getPartitionNames() {
        if (table.isUnPartitioned()) {
            return Lists.newArrayList(table.getName());
        }

<<<<<<< HEAD
        return GlobalStateMgr.getCurrentState().getMetadataMgr().listPartitionNames(
                table.getCatalogName(), getDbName(), getTableName());
=======
        ConnectorMetadatRequestContext requestContext = new ConnectorMetadatRequestContext();
        requestContext.setQueryMVRewrite(this.isQueryMVRewrite());
        return GlobalStateMgr.getCurrentState().getMetadataMgr().listPartitionNames(
                table.getCatalogName(), getCatalogDBName(), getTableName(), requestContext);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    @Override
    public List<Column> getPartitionColumns() {
        return table.getPartitionColumns();
    }

    @Override
    public Map<String, Range<PartitionKey>> getPartitionKeyRange(Column partitionColumn, Expr partitionExpr)
            throws AnalysisException {
        return PartitionUtil.getRangePartitionMapOfExternalTable(
                table, partitionColumn, getPartitionNames(), partitionExpr);
    }

    @Override
<<<<<<< HEAD
    public Map<String, List<List<String>>> getPartitionList(Column partitionColumn) throws AnalysisException {
        return PartitionUtil.getMVPartitionNameWithList(table, partitionColumn, getPartitionNames());
=======
    public Map<String, PCell> getPartitionCells(List<Column> partitionColumns) throws AnalysisException {
        return PartitionUtil.getMVPartitionToCells(table, partitionColumns, getPartitionNames());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    @Override
    public Map<String, PartitionInfo> getPartitionNameWithPartitionInfo() {
        Map<String, PartitionInfo> partitionNameWithPartition = Maps.newHashMap();
        List<String> partitionNames = getPartitionNames();
        List<PartitionInfo> partitions = getPartitions(partitionNames);
        Preconditions.checkState(partitions.size() == partitionNames.size(), "corrupted partition meta");
        for (int index = 0; index < partitionNames.size(); ++index) {
            partitionNameWithPartition.put(partitionNames.get(index), partitions.get(index));
        }
        return partitionNameWithPartition;
    }

    @Override
<<<<<<< HEAD
=======
    public Map<String, PartitionInfo> getPartitionNameWithPartitionInfo(List<String> partitionNames) {
        Map<String, PartitionInfo> partitionNameWithPartition = Maps.newHashMap();
        List<PartitionInfo> partitions = getPartitions(partitionNames);
        Preconditions.checkState(partitions.size() == partitionNames.size(), "corrupted partition meta");
        for (int index = 0; index < partitionNames.size(); ++index) {
            partitionNameWithPartition.put(partitionNames.get(index), partitions.get(index));
        }
        return partitionNameWithPartition;
    }

    @Override
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    public Optional<Long> maxPartitionRefreshTs() {
        throw new NotImplementedException("Not support maxPartitionRefreshTs");
    }

    @Override
    public Set<String> getUpdatedPartitionNames(List<BaseTableInfo> baseTables,
                                                MaterializedView.AsyncRefreshContext context) {
        Table baseTable = table;
        Set<String> result = Sets.newHashSet();
        Map<String, PartitionInfo> latestPartitionInfo = getPartitionNameWithPartitionInfo();

        for (BaseTableInfo baseTableInfo : baseTables) {
            if (!baseTableInfo.getTableIdentifier().equalsIgnoreCase(baseTable.getTableIdentifier())) {
                continue;
            }
            Map<String, MaterializedView.BasePartitionInfo> versionMap =
                    context.getBaseTableRefreshInfo(baseTableInfo);

            // check whether there are partitions added
            for (Map.Entry<String, PartitionInfo> entry : latestPartitionInfo.entrySet()) {
                if (!versionMap.containsKey(entry.getKey())) {
                    result.add(entry.getKey());
                }
            }

            for (Map.Entry<String, MaterializedView.BasePartitionInfo> versionEntry : versionMap.entrySet()) {
                String basePartitionName = versionEntry.getKey();
                if (!latestPartitionInfo.containsKey(basePartitionName)) {
<<<<<<< HEAD
                    // partitions deleted
                    return latestPartitionInfo.keySet();
=======
                    // If this partition is dropped, ignore it.
                    continue;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                }
                long basePartitionVersion = latestPartitionInfo.get(basePartitionName).getModifiedTime();

                MaterializedView.BasePartitionInfo basePartitionInfo = versionEntry.getValue();
                // basePartitionVersion less than 0 is illegal
                if ((basePartitionInfo == null || basePartitionVersion != basePartitionInfo.getVersion())
                        && basePartitionVersion >= 0) {
                    result.add(basePartitionName);
                }
            }
        }
        return result;
    }
<<<<<<< HEAD
=======

    @Override
    public Set<String> getUpdatedPartitionNames(LocalDateTime checkTime, int extraSeconds) {
        List<String> updatedPartitions = Lists.newArrayList();
        try {
            getPartitionNameWithPartitionInfo().
                    forEach((partitionName, partitionInfo) -> {
                        long partitionModifiedTimeMillis = partitionInfo.getModifiedTimeUnit().toMillis(
                                partitionInfo.getModifiedTime());

                        LocalDateTime partitionUpdateTime = LocalDateTime.ofInstant(
                                Instant.ofEpochMilli(partitionModifiedTimeMillis).plusSeconds(extraSeconds),
                                Clock.systemDefaultZone().getZone());
                        if (partitionUpdateTime.isAfter(checkTime)) {
                            updatedPartitions.add(partitionName);
                        }
                    });
            return Sets.newHashSet(updatedPartitions);
        } catch (Exception e) {
            // some external table traits do not support getPartitionNameWithPartitionInfo, will throw exception,
            // just return null
            return null;
        }
    }

    @Override
    public LocalDateTime getTableLastUpdateTime(int extraSeconds) {
        try {
            long lastModifiedTimeMillis = getPartitionNameWithPartitionInfo().values().stream().
                    map(partitionInfo -> partitionInfo.getModifiedTimeUnit().toMillis(partitionInfo.getModifiedTime())).
                    max(Long::compareTo).orElse(0L);
            if (lastModifiedTimeMillis != 0L) {
                return LocalDateTime.ofInstant(Instant.ofEpochMilli(lastModifiedTimeMillis).plusSeconds(extraSeconds),
                        Clock.systemDefaultZone().getZone());
            }
        } catch (Exception e) {
            // some external table traits do not support getPartitionNameWithPartitionInfo, will throw exception,
            // just return null
        }
        return null;
    }
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
}
