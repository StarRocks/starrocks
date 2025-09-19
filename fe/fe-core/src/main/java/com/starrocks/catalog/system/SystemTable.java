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

package com.starrocks.catalog.system;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.system.information.BeConfigsSystemTable;
import com.starrocks.catalog.system.information.BeTabletsSystemTable;
import com.starrocks.catalog.system.information.FeTabletSchedulesSystemTable;
import com.starrocks.catalog.system.information.LoadTrackingLogsSystemTable;
import com.starrocks.catalog.system.information.LoadsSystemTable;
import com.starrocks.catalog.system.information.MaterializedViewsSystemTable;
import com.starrocks.catalog.system.information.PartitionsMetaSystemTable;
import com.starrocks.catalog.system.information.PipesSystemTable;
import com.starrocks.catalog.system.information.RoutineLoadJobsSystemTable;
import com.starrocks.catalog.system.information.StreamLoadsSystemTable;
import com.starrocks.catalog.system.information.TablesConfigSystemTable;
import com.starrocks.catalog.system.information.TaskRunsSystemTable;
import com.starrocks.catalog.system.information.TasksSystemTable;
import com.starrocks.catalog.system.information.TemporaryTablesTable;
import com.starrocks.catalog.system.information.ViewsSystemTable;
import com.starrocks.catalog.system.information.WarehouseMetricsSystemTable;
import com.starrocks.catalog.system.information.WarehouseQueriesSystemTable;
import com.starrocks.common.util.DateUtils;
import com.starrocks.planner.DescriptorTable.ReferencedPartitionInfo;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.thrift.TSchemaTable;
import com.starrocks.thrift.TSchemaTableType;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.thrift.protocol.TType;

import java.time.ZoneId;
import java.util.List;
import java.util.Set;

import static com.starrocks.catalog.InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;

/**
 * representation of MySQL information schema table metadata,
 */
public class SystemTable extends Table {
    public static final int FN_REFLEN = 512;
    public static final int NAME_CHAR_LEN = 2048;
    public static final int MAX_FIELD_VARCHAR_LENGTH = 65535;

    // some metadata may be inaccurate in the follower fe, because they may be not persisted in leader fe,
    // such as routine load job state changed from NEED_SCHEDULE to RUNNING.
    private static final ImmutableSortedSet<String> QUERY_FROM_LEADER_TABLES =
            ImmutableSortedSet.orderedBy(String.CASE_INSENSITIVE_ORDER)
                    .add(FeTabletSchedulesSystemTable.NAME)
                    .add(LoadTrackingLogsSystemTable.NAME)
                    .add(LoadsSystemTable.NAME)
                    .add(MaterializedViewsSystemTable.NAME)
                    .add(PartitionsMetaSystemTable.NAME)
                    .add(PipesSystemTable.NAME)
                    .add(RoutineLoadJobsSystemTable.NAME)
                    .add(StreamLoadsSystemTable.NAME)
                    .add(TablesConfigSystemTable.NAME)
                    .add(TaskRunsSystemTable.NAME)
                    .add(TasksSystemTable.NAME)
                    .add(TemporaryTablesTable.NAME)
                    .add(ViewsSystemTable.NAME)
                    .add(WarehouseMetricsSystemTable.NAME)
                    .add(WarehouseQueriesSystemTable.NAME)
                    .build();

    private static final ImmutableMap<Byte, Type> THRIFT_TO_SCALAR_TYPE_MAPPING =
            ImmutableMap.<Byte, Type>builder()
                    .put(TType.I16, Type.SMALLINT)
                    .put(TType.I32, Type.INT)
                    .put(TType.I64, Type.BIGINT)
                    .put(TType.STRING, Type.STRING)
                    .put(TType.BOOL, Type.BOOLEAN)
                    .build();

    private final TSchemaTableType schemaTableType;

    private final String catalogName;

    public SystemTable(long id, String name, TableType type, List<Column> baseSchema,
                       TSchemaTableType schemaTableType) {
        this(DEFAULT_INTERNAL_CATALOG_NAME, id, name, type, baseSchema, schemaTableType);
    }

    public SystemTable(String catalogName, long id, String name, TableType type, List<Column> baseSchema,
                       TSchemaTableType schemaTableType) {
        super(id, name, type, baseSchema);
        this.catalogName = catalogName;
        this.schemaTableType = schemaTableType;
    }

    public static boolean isBeSchemaTable(String name) {
        return name.startsWith("be_");
    }

    public static boolean isFeSchemaTable(String name) {
        // currently, it only stands for single FE leader, because only FE leader has related info
        return name.startsWith("fe_");
    }

    public boolean requireOperatePrivilege() {
        return (SystemTable.isBeSchemaTable(getName()) || SystemTable.isFeSchemaTable(getName())) &&
                !getName().equals(BeTabletsSystemTable.NAME) && !getName().equals(FeTabletSchedulesSystemTable.NAME);
    }

    @Override
    public boolean supportsUpdate() {
        return name.equals(BeConfigsSystemTable.NAME);
    }






    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    public static class Builder {
        List<Column> columns;

        public Builder() {
            columns = Lists.newArrayList();
        }

        public Builder column(String name, Type type) {
            return column(name, type, true);
        }

        public Builder column(String name, Type type, String comment) {
            columns.add(new Column(name, type, false, null, true, null, comment));
            return this;
        }

        public Builder column(String name, Type type, boolean nullable) {
            columns.add(new Column(name, type, false, null, nullable, null, ""));
            return this;
        }

        public List<Column> build() {
            return columns;
        }
    }

    @Override
    public TTableDescriptor toThrift(List<ReferencedPartitionInfo> partitions) {
        TSchemaTable tSchemaTable = new TSchemaTable(schemaTableType);
        TTableDescriptor tTableDescriptor =
                new TTableDescriptor(getId(), TTableType.SCHEMA_TABLE, getBaseSchema().size(), 0, this.name, "");
        tTableDescriptor.setSchemaTable(tSchemaTable);
        return tTableDescriptor;
    }

    @Override
    public boolean isSupported() {
        return true;
    }


    /**
     * The thrift type may differ from schema-type, for example user a LONG timestamp in thrift, but return a
     * DATETIME in the schema table.
     */
    protected static ConstantOperator mayCast(ConstantOperator value, Type schemaType) {
        if (value.getType().equals(schemaType)) {
            return value;
        }
        if (value.getType().isStringType() && schemaType.isStringType()) {
            return value;
        }
        // From timestamp to DATETIME
        if (value.getType().isBigint() && schemaType.isDatetime()) {
            return ConstantOperator.createDatetime(DateUtils.fromEpochMillis(value.getBigint() * 1000, ZoneId.systemDefault()));
        }
        return value.castTo(schemaType)
                .orElseThrow(() -> new NotImplementedException(String.format("unsupported type cast from %s to %s",
                        value.getType(), schemaType)));
    }

    protected static Type thriftToScalarType(byte type) {
        Type valueType = THRIFT_TO_SCALAR_TYPE_MAPPING.get(type);
        if (valueType == null) {
            throw new NotImplementedException("not supported type: " + type);
        }
        return valueType;
    }

    /**
     * Check if the conjuncts only contains column equal constant operations, eg: c1=v1 AND c2=v2
     * @param conjuncts: the conjuncts to check
     * @return: true if all conjuncts are not empty and column equal constant operations
     */
    protected boolean isOnlyEqualConstantOps(List<ScalarOperator> conjuncts) {
        return CollectionUtils.isNotEmpty(conjuncts) &&
                conjuncts.stream().allMatch(ScalarOperator::isColumnEqualConstant);
    }

    /**
     * Check if the conjuncts is empty or only contains column equal constant operations.
     * @param conjuncts: the conjuncts to check
     * @return: true if conjuncts is empty or all conjuncts are column equal constant operations
     */
    protected boolean isEmptyOrOnlyEqualConstantOps(List<ScalarOperator> conjuncts) {
        return CollectionUtils.isEmpty(conjuncts) ||
                conjuncts.stream().allMatch(ScalarOperator::isColumnEqualConstant);
    }

    /**
     * Check if the equal predicate columns are supported by this system table.
     * @param conjuncts: the conjuncts to check
     * @param supportedColumns: the set of supported columns by this system table
     * @return: true if all equal predicate columns are supported
     */
    protected boolean isSupportedEqualPredicateColumn(List<ScalarOperator> conjuncts,
                                                      Set<String> supportedColumns) {
        return conjuncts.stream()
                .allMatch(conjunct -> {
                    if (!(conjunct instanceof BinaryPredicateOperator)) {
                        return false;
                    }
                    BinaryPredicateOperator binary = (BinaryPredicateOperator) conjunct;
                    ColumnRefOperator columnRef = binary.getChild(0).cast();
                    String name = columnRef.getName().toUpperCase();
                    return supportedColumns.contains(name);
                });
    }

    /**
     * Whether this system table supports evaluation in FE
     *
     * @return true if it's supported
     */
    public boolean supportFeEvaluation(ScalarOperator predicate) {
        return false;
    }

    /**
     * Evaluate the system table query with specified predicate
     *
     * @param predicate can only be conjuncts
     * @return All columns and rows according to the schema of this table
     */
    public List<List<ScalarOperator>> evaluate(ScalarOperator predicate) {
        throw new NotImplementedException("not supported");
    }

    public static boolean needQueryFromLeader(String tableName) {
        return QUERY_FROM_LEADER_TABLES.contains(tableName);
    }

    public static ScalarType createNameType() {
        return ScalarType.createVarchar(NAME_CHAR_LEN);
    }
}
