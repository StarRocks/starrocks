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
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
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
import com.starrocks.catalog.NullablePartitionKey;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PaimonPartitionKey;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.server.GlobalStateMgr;
import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Abstract the partition-related interfaces for different connectors, including Iceberg/Hive/....
 */
public abstract class ConnectorPartitionTraits {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectorPartitionTraits.class);
    private static final Map<Table.TableType, Supplier<ConnectorPartitionTraits>> TRAITS_TABLE =
            ImmutableMap.<Table.TableType, Supplier<ConnectorPartitionTraits>>builder()
                    // Consider all native table as OLA
                    .put(Table.TableType.OLAP, OlapPartitionTraits::new)
                    .put(Table.TableType.MATERIALIZED_VIEW, OlapPartitionTraits::new)
                    .put(Table.TableType.CLOUD_NATIVE, OlapPartitionTraits::new)
                    .put(Table.TableType.CLOUD_NATIVE_MATERIALIZED_VIEW, OlapPartitionTraits::new)

                    // external tables
                    .put(Table.TableType.HIVE, HivePartitionTraits::new)
                    .put(Table.TableType.HUDI, HudiPartitionTraits::new)
                    .put(Table.TableType.ICEBERG, IcebergPartitionTraits::new)
                    .put(Table.TableType.PAIMON, PaimonPartitionTraits::new)
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

    abstract PartitionKey createEmptyKey();

    abstract String getDbName();

    abstract PartitionKey createPartitionKey(List<String> values, List<Column> columns) throws AnalysisException;

    abstract List<String> getPartitionNames();

    abstract List<Column> getPartitionColumns();

    abstract Map<String, Range<PartitionKey>> getPartitionKeyRange(Column partitionColumn, Expr partitionExpr)
            throws AnalysisException;

    abstract Map<String, List<List<String>>> getPartitionList(Column partitionColumn) throws AnalysisException;

    abstract Map<String, PartitionInfo> getPartitionNameWithPartitionInfo();

    // ========================================= Implementations ==============================================

    abstract static class DefaultTraits extends ConnectorPartitionTraits {

        @Override
        public PartitionKey createPartitionKey(List<String> values, List<Column> columns) throws AnalysisException {
            Preconditions.checkState(values.size() == columns.size(),
                    "columns size is %s, but values size is %s", columns.size(), values.size());

            PartitionKey partitionKey = createEmptyKey();

            // change string value to LiteralExpr,
            for (int i = 0; i < values.size(); i++) {
                String rawValue = values.get(i);
                Type type = columns.get(i).getType();
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
        public Map<String, Range<PartitionKey>> getPartitionKeyRange(Column partitionColumn, Expr partitionExpr) {
            // TODO: check partition type
            return ((OlapTable) table).getRangePartitionMap();
        }

        @Override
        public Map<String, List<List<String>>> getPartitionList(Column partitionColumn) {
            // TODO: check partition type
            return ((OlapTable) table).getListPartitionMap();
        }

    }

    static class HivePartitionTraits extends DefaultTraits {

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
    }

    static class IcebergPartitionTraits extends DefaultTraits {

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
    }

    static class PaimonPartitionTraits extends DefaultTraits {

        @Override
        public String getDbName() {
            return ((PaimonTable) table).getDbName();
        }

        @Override
        PartitionKey createEmptyKey() {
            return new PaimonPartitionKey();
        }
    }

    static class JDBCPartitionTraits extends DefaultTraits {
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
    }
}
