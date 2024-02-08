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

package com.starrocks.connector;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DeltaLakePartitionKey;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.HivePartitionKey;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.HudiPartitionKey;
import com.starrocks.catalog.IcebergPartitionKey;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.JDBCPartitionKey;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.NullablePartitionKey;
import com.starrocks.catalog.OdpsPartitionKey;
import com.starrocks.catalog.OdpsTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PaimonPartitionKey;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.connector.iceberg.IcebergPartitionUtils;
import com.starrocks.server.GlobalStateMgr;
import org.apache.commons.lang.NotImplementedException;
import org.apache.iceberg.PartitionField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Abstract the partition-related interfaces for different connectors, including Iceberg/Hive/....
 */
public abstract class ConnectorPartitionTraits {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectorPartitionTraits.class);
    private static final Map<Table.TableType, Supplier<ConnectorPartitionTraits>> TRAITS_TABLE =
            ImmutableMap.<Table.TableType, Supplier<ConnectorPartitionTraits>>builder()
                    // Consider all native table as OLAP
                    .put(Table.TableType.OLAP, OlapPartitionTraits::new)
                    .put(Table.TableType.MATERIALIZED_VIEW, OlapPartitionTraits::new)
                    .put(Table.TableType.CLOUD_NATIVE, OlapPartitionTraits::new)
                    .put(Table.TableType.CLOUD_NATIVE_MATERIALIZED_VIEW, OlapPartitionTraits::new)

                    // external tables
                    .put(Table.TableType.HIVE, HivePartitionTraits::new)
                    .put(Table.TableType.HUDI, HudiPartitionTraits::new)
                    .put(Table.TableType.ICEBERG, IcebergPartitionTraits::new)
                    .put(Table.TableType.PAIMON, PaimonPartitionTraits::new)
                    .put(Table.TableType.ODPS, OdpsPartitionTraits::new)
                    .put(Table.TableType.JDBC, JDBCPartitionTraits::new)
                    .put(Table.TableType.DELTALAKE, DeltaLakePartitionTraits::new)
                    .build();

    protected Table table;

    public static boolean isSupported(Table.TableType tableType) {
        return TRAITS_TABLE.containsKey(tableType);
    }

    public static ConnectorPartitionTraits build(Table.TableType tableType) {
        return Preconditions.checkNotNull(TRAITS_TABLE.get(tableType),
                "traits not supported: " + tableType).get();
    }

    public static ConnectorPartitionTraits build(Table table) {
        ConnectorPartitionTraits res = build(table.getType());
        res.table = table;
        return res;
    }

    /**
     * Build a partition key for the table, some of them have specific representations for null values
     */
    abstract PartitionKey createEmptyKey();

    abstract String getDbName();

    /**
     * Whether this table support partition-granular refresh as ref-table
     */
    public abstract boolean supportPartitionRefresh();

    abstract PartitionKey createPartitionKeyWithType(List<String> values, List<Type> types) throws AnalysisException;

    abstract PartitionKey createPartitionKey(List<String> partitionValues, List<Column> partitionColumns)
            throws AnalysisException;
    /**
     * Get all partitions' name
     */
    abstract List<String> getPartitionNames();

    /**
     * Get partition columns
     */
    abstract List<Column> getPartitionColumns();

    /**
     * Get partition range map with the specified partition column and expression
     *
     * @apiNote it must be a range-partitioned table
     */
    abstract Map<String, Range<PartitionKey>> getPartitionKeyRange(Column partitionColumn, Expr partitionExpr)
            throws AnalysisException;

    /**
     * Get the list-map with specified partition column and expression
     *
     * @apiNote it must be a list-partitioned table
     */
    abstract Map<String, List<List<String>>> getPartitionList(Column partitionColumn) throws AnalysisException;

    public abstract Map<String, PartitionInfo> getPartitionNameWithPartitionInfo();

    /**
     * The max of refresh ts for all partitions
     */
    public abstract Optional<Long> maxPartitionRefreshTs();

    /**
     * Get updated partitions based on current snapshot, to implement incremental refresh
     */
    public abstract Set<String> getUpdatedPartitionNames(List<BaseTableInfo> baseTables,
                                                         MaterializedView.AsyncRefreshContext context);

    // ========================================= Implementations ==============================================

    public abstract static class DefaultTraits extends ConnectorPartitionTraits {

        @Override
        public boolean supportPartitionRefresh() {
            return false;
        }

        @Override
        public PartitionKey createPartitionKeyWithType(List<String> values, List<Type> types) throws AnalysisException {
            Preconditions.checkState(values.size() == types.size(),
                    "columns size is %s, but values size is %s", types.size(), values.size());

            PartitionKey partitionKey = createEmptyKey();

            // change string value to LiteralExpr,
            for (int i = 0; i < values.size(); i++) {
                String rawValue = values.get(i);
                Type type = types.get(i);
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
        public PartitionKey createPartitionKey(List<String> partitionValues, List<Column> partitionColumns)
                throws AnalysisException {
            return createPartitionKeyWithType(partitionValues,
                    partitionColumns.stream().map(Column::getType).collect(Collectors.toList()));
        }

        protected String getTableName() {
            return table.getName();
        }

        @Override
        public List<String> getPartitionNames() {
            if (table.isUnPartitioned()) {
                return Lists.newArrayList(table.getName());
            }
            return GlobalStateMgr.getCurrentState().getMetadataMgr().listPartitionNames(
                    table.getCatalogName(), getDbName(), getTableName());
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
        public Map<String, List<List<String>>> getPartitionList(Column partitionColumn) throws AnalysisException {
            return PartitionUtil.getMVPartitionNameWithList(table, partitionColumn, getPartitionNames());
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

        protected List<PartitionInfo> getPartitions(List<String> names) {
            throw new NotImplementedException("Only support hive/jdbc");
        }

        @Override
        public Optional<Long> maxPartitionRefreshTs() {
            throw new NotImplementedException("Not support maxPartitionRefreshTs");
        }

        @Override
        public Set<String> getUpdatedPartitionNames(List<BaseTableInfo> baseTables,
                                                    MaterializedView.AsyncRefreshContext context) {
            Table baseTable = table;
            Set<String> result = Sets.newHashSet();
            Map<String, com.starrocks.connector.PartitionInfo> latestPartitionInfo =
                    getPartitionNameWithPartitionInfo();

            for (BaseTableInfo baseTableInfo : baseTables) {
                if (!baseTableInfo.getTableIdentifier().equalsIgnoreCase(baseTable.getTableIdentifier())) {
                    continue;
                }
                Map<String, MaterializedView.BasePartitionInfo> versionMap =
                        context.getBaseTableRefreshInfo(baseTableInfo);

                // check whether there are partitions added
                for (Map.Entry<String, com.starrocks.connector.PartitionInfo> entry : latestPartitionInfo.entrySet()) {
                    if (!versionMap.containsKey(entry.getKey())) {
                        result.add(entry.getKey());
                    }
                }

                for (Map.Entry<String, MaterializedView.BasePartitionInfo> versionEntry : versionMap.entrySet()) {
                    String basePartitionName = versionEntry.getKey();
                    if (!latestPartitionInfo.containsKey(basePartitionName)) {
                        // partitions deleted
                        return latestPartitionInfo.keySet();
                    }
                    long basePartitionVersion = latestPartitionInfo.get(basePartitionName).getModifiedTime();

                    MaterializedView.BasePartitionInfo basePartitionInfo = versionEntry.getValue();
                    if (basePartitionInfo == null || basePartitionVersion != basePartitionInfo.getVersion()) {
                        result.add(basePartitionName);
                    }
                }
            }
            return result;
        }
    }

    // ========================================= Specific Implementations ======================================

    static class OlapPartitionTraits extends DefaultTraits {

        @Override
        PartitionKey createEmptyKey() {
            throw new NotImplementedException("not support olap table");
        }

        @Override
        String getDbName() {
            throw new NotImplementedException("not support olap table");
        }

        @Override
        public boolean supportPartitionRefresh() {
            // TODO: check partition types
            return true;
        }

        @Override
        public Map<String, Range<PartitionKey>> getPartitionKeyRange(Column partitionColumn, Expr partitionExpr) {
            // TODO: check partition type
            return ((OlapTable) table).getRangePartitionMap();
        }

        @Override
        public Map<String, List<List<String>>> getPartitionList(Column partitionColumn) {
            // TODO: check partition type
            return ((OlapTable) table).getListPartitionMap();
        }

        @Override
        public Optional<Long> maxPartitionRefreshTs() {
            OlapTable olapTable = (OlapTable) table;
            return olapTable.getPhysicalPartitions().stream().map(PhysicalPartition::getVisibleVersionTime).max(Long::compareTo);
        }

        @Override
        public Set<String> getUpdatedPartitionNames(List<BaseTableInfo> baseTables,
                                                    MaterializedView.AsyncRefreshContext context) {
            OlapTable baseTable = (OlapTable) table;
            Map<String, MaterializedView.BasePartitionInfo> mvBaseTableVisibleVersionMap =
                    context.getBaseTableVisibleVersionMap()
                            .computeIfAbsent(baseTable.getId(), k -> Maps.newHashMap());
            Set<String> result = Sets.newHashSet();

            // If there are new added partitions, add it into refresh result.
            for (String partitionName : baseTable.getVisiblePartitionNames()) {
                if (!mvBaseTableVisibleVersionMap.containsKey(partitionName)) {
                    Partition partition = baseTable.getPartition(partitionName);
                    // TODO: use `mvBaseTableVisibleVersionMap` to check whether base table has been refreshed or not instead of
                    //  checking its version, remove this later.
                    if (partition.getVisibleVersion() != 1) {
                        result.add(partitionName);
                    }
                }
            }

            for (Map.Entry<String, MaterializedView.BasePartitionInfo> versionEntry : mvBaseTableVisibleVersionMap.entrySet()) {
                String basePartitionName = versionEntry.getKey();
                Partition basePartition = baseTable.getPartition(basePartitionName);
                if (basePartition == null) {
                    // Once there is a partition deleted, refresh all partitions.
                    return baseTable.getVisiblePartitionNames();
                }
                MaterializedView.BasePartitionInfo mvRefreshedPartitionInfo = versionEntry.getValue();
                if (mvRefreshedPartitionInfo == null) {
                    result.add(basePartitionName);
                } else {
                    // Ignore partitions if mv's partition is the same with the basic table.
                    if (mvRefreshedPartitionInfo.getId() == basePartition.getId()
                            && basePartition.getVisibleVersion() == mvRefreshedPartitionInfo.getVersion()) {
                        continue;
                    }

                    // others will add into the result.
                    result.add(basePartitionName);
                }
            }
            return result;
        }

        public List<Column> getPartitionColumns() {
            return ((OlapTable) table).getPartitionInfo().getPartitionColumns();
        }
    }

    static class HivePartitionTraits extends DefaultTraits {

        @Override
        public boolean supportPartitionRefresh() {
            return true;
        }

        @Override
        public String getDbName() {
            return ((HiveMetaStoreTable) table).getDbName();
        }

        @Override
        public PartitionKey createEmptyKey() {
            return new HivePartitionKey();
        }

        @Override
        public List<PartitionInfo> getPartitions(List<String> partitionNames) {
            HiveTable hiveTable = (HiveTable) table;
            return GlobalStateMgr.getCurrentState().getMetadataMgr().
                    getPartitions(hiveTable.getCatalogName(), table, partitionNames);
        }

        @Override
        public Optional<Long> maxPartitionRefreshTs() {
            Map<String, com.starrocks.connector.PartitionInfo> partitionNameWithPartition =
                    getPartitionNameWithPartitionInfo();
            return
                    partitionNameWithPartition.values().stream()
                            .map(com.starrocks.connector.PartitionInfo::getModifiedTime)
                            .max(Long::compareTo);
        }
    }

    static class HudiPartitionTraits extends DefaultTraits {

        @Override
        public String getDbName() {
            return ((HiveMetaStoreTable) table).getDbName();
        }

        @Override
        PartitionKey createEmptyKey() {
            return new HudiPartitionKey();
        }

        @Override
        public Set<String> getUpdatedPartitionNames(List<BaseTableInfo> baseTables,
                                                    MaterializedView.AsyncRefreshContext context) {
            // TODO: implement
            return null;
        }
    }

    static class IcebergPartitionTraits extends DefaultTraits {

        @Override
        public boolean supportPartitionRefresh() {
            // TODO: refine the check
            return true;
        }

        @Override
        public String getDbName() {
            return ((IcebergTable) table).getRemoteDbName();
        }

        @Override
        public String getTableName() {
            return ((IcebergTable) table).getRemoteTableName();
        }

        @Override
        PartitionKey createEmptyKey() {
            return new IcebergPartitionKey();
        }

        @Override
        public Optional<Long> maxPartitionRefreshTs() {
            IcebergTable icebergTable = (IcebergTable) table;
            return Optional.of(icebergTable.getRefreshSnapshotTime());
        }

        @Override
        public List<PartitionInfo> getPartitions(List<String> partitionNames) {
            IcebergTable icebergTable = (IcebergTable) table;
            return GlobalStateMgr.getCurrentState().getMetadataMgr().
                    getPartitions(icebergTable.getCatalogName(), table, partitionNames);
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
    }

    static class PaimonPartitionTraits extends DefaultTraits {

        @Override
        public boolean supportPartitionRefresh() {
            // TODO: refine the check
            return true;
        }

        @Override
        public String getDbName() {
            return ((PaimonTable) table).getDbName();
        }

        @Override
        PartitionKey createEmptyKey() {
            return new PaimonPartitionKey();
        }

        @Override
        public Set<String> getUpdatedPartitionNames(List<BaseTableInfo> baseTables,
                                                    MaterializedView.AsyncRefreshContext context) {
            PaimonTable baseTable = (PaimonTable) table;
            Set<String> result = Sets.newHashSet();
            for (BaseTableInfo baseTableInfo : baseTables) {
                if (!baseTableInfo.getTableIdentifier().equalsIgnoreCase(baseTable.getTableIdentifier())) {
                    continue;
                }
                Optional<ConnectorMetadata> connectorMetadata = GlobalStateMgr.getCurrentState().getMetadataMgr().
                        getOptionalMetadata(baseTable.getCatalogName());
                if (!connectorMetadata.isPresent()) {
                    LOG.error("Get paimon connectorMetadata failed : {}", baseTable.getCatalogName());
                    throw new RuntimeException("Get paimon connectorMetadata failed :" + baseTable.getCatalogName());
                }
                Map<String, MaterializedView.BasePartitionInfo> partitionVersionMap =
                        context.getBaseTableRefreshInfo(baseTableInfo);
                Set<String> partitions = Sets.newHashSet(getPartitionNames());
                long mvLatestSnapShotID = Long.MIN_VALUE;
                for (Map.Entry<String, MaterializedView.BasePartitionInfo> entry : partitionVersionMap.entrySet()) {
                    if (entry.getValue() != null) {
                        mvLatestSnapShotID = Math.max(mvLatestSnapShotID, entry.getValue().getVersion());
                    }
                    // If there are partitions deleted, return all latest partitions.
                    if (!partitions.contains(entry.getKey())) {
                        result.addAll(partitions);
                        LOG.info("Get paimon updated partition names {}, partition {} has been deleted, return all.",
                                baseTables, entry.getKey());
                        return result;
                    }
                }
                for (String part : partitions) {
                    if (!partitionVersionMap.containsKey(part)) {
                        result.add(part);
                    }
                }
                ConnectorMetadata metadata = connectorMetadata.get();
                List<PartitionInfo> changedPartitionInfo = metadata.getChangedPartitionInfo(baseTable,
                        mvLatestSnapShotID);
                for (PartitionInfo partitionInfo : changedPartitionInfo) {
                    com.starrocks.connector.paimon.Partition info =
                            (com.starrocks.connector.paimon.Partition) partitionInfo;
                    // Change log record partition which has been deleted.
                    if (!partitions.contains(info.getPartitionName())) {
                        continue;
                    }
                    result.add(info.getPartitionName());
                }
            }
            LOG.debug("Get updated partition name of paimon table result: {}", result);
            return result;
        }
    }

    static class OdpsPartitionTraits extends DefaultTraits {

        @Override
        public String getDbName() {
            return ((OdpsTable) table).getDbName();
        }

        @Override
        PartitionKey createEmptyKey() {
            return new OdpsPartitionKey();
        }
    }

    static class JDBCPartitionTraits extends DefaultTraits {

        @Override
        public boolean supportPartitionRefresh() {
            // TODO: refine check
            return true;
        }

        @Override
        public String getDbName() {
            return ((JDBCTable) table).getDbName();
        }

        @Override
        public String getTableName() {
            return ((JDBCTable) table).getJdbcTable();
        }

        @Override
        public List<PartitionInfo> getPartitions(List<String> partitionNames) {
            JDBCTable jdbcTable = (JDBCTable) table;
            return GlobalStateMgr.getCurrentState().getMetadataMgr().
                    getPartitions(jdbcTable.getCatalogName(), table, partitionNames);
        }

        @Override
        PartitionKey createEmptyKey() {
            return new JDBCPartitionKey();
        }

        @Override
        public Optional<Long> maxPartitionRefreshTs() {
            Map<String, com.starrocks.connector.PartitionInfo> partitionNameWithPartition =
                    getPartitionNameWithPartitionInfo();
            return partitionNameWithPartition.values().stream()
                    .map(com.starrocks.connector.PartitionInfo::getModifiedTime)
                    .max(Long::compareTo);
        }
    }

    static class DeltaLakePartitionTraits extends DefaultTraits {

        @Override
        PartitionKey createEmptyKey() {
            return new DeltaLakePartitionKey();
        }

        @Override
        String getDbName() {
            return ((DeltaLakeTable) table).getDbName();
        }

        @Override
        public Set<String> getUpdatedPartitionNames(List<BaseTableInfo> baseTables,
                                                    MaterializedView.AsyncRefreshContext context) {
            // TODO: implement
            return null;
        }
    }
}
