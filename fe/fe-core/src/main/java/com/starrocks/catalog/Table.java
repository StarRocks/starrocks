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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/Table.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.DescriptorTable.ReferencedPartitionInfo;
import com.starrocks.catalog.constraint.ForeignKeyConstraint;
import com.starrocks.catalog.constraint.UniqueConstraint;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TTableDescriptor;
import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Internal representation of table-related metadata. A table contains several partitions.
 */
public class Table extends MetaObject implements Writable, GsonPostProcessable, BasicTable {
    private static final Logger LOG = LogManager.getLogger(Table.class);

    // 1. Native table:
    //   1.1 Local: OLAP, MATERIALIZED_VIEW
    //   1.2 Cloud native: LAKE, LAKE_MATERIALIZED_VIEW
    // 2. System table: SCHEMA
    // 3. View: INLINE_VIEW, VIEW
    // 4. External table: MYSQL, OLAP_EXTERNAL, BROKER, ELASTICSEARCH, HIVE, ICEBERG, HUDI, ODBC, JDBC
    public enum TableType {
        @SerializedName("MYSQL")
        MYSQL,
        @SerializedName("OLAP")
        OLAP,
        @SerializedName("OLAP_EXTERNAL")
        OLAP_EXTERNAL,
        @SerializedName("SCHEMA")
        SCHEMA,
        @SerializedName("INLINE_VIEW")
        INLINE_VIEW,
        @SerializedName("VIEW")
        VIEW,
        @SerializedName("BROKER")
        BROKER,
        @SerializedName("ELASTICSEARCH")
        ELASTICSEARCH,
        @SerializedName("HIVE")
        HIVE,
        @SerializedName("ICEBERG")
        ICEBERG,
        @SerializedName("HUDI")
        HUDI,
        @SerializedName("JDBC")
        JDBC,
        @SerializedName("MATERIALIZED_VIEW")
        MATERIALIZED_VIEW,
        @SerializedName("LAKE") // for backward and rollback compatibility
        CLOUD_NATIVE,
        @SerializedName("DELTALAKE")
        DELTALAKE,
        @SerializedName("FILE")
        FILE,
        @SerializedName("LAKE_MATERIALIZED_VIEW") // for backward and rollback compatibility
        CLOUD_NATIVE_MATERIALIZED_VIEW,
        @SerializedName("TABLE_FUNCTION")
        TABLE_FUNCTION,
        @SerializedName("PAIMON")
        PAIMON,
        @SerializedName("ODPS")
        ODPS,
        @SerializedName("BLACKHOLE")
        BLACKHOLE,
        @SerializedName("METADATA")
        METADATA,
        @SerializedName("KUDU")
        KUDU,
        @SerializedName("HIVE_VIEW")
        HIVE_VIEW,
        @SerializedName("ICEBERG_VIEW")
        ICEBERG_VIEW;

        public static String serialize(TableType type) {
            if (type == CLOUD_NATIVE) {
                return "LAKE"; // for rollback compatibility
            }
            if (type == CLOUD_NATIVE_MATERIALIZED_VIEW) {
                return "LAKE_MATERIALIZED_VIEW"; // for rollback compatibility
            }
            return type.name();
        }

        public static TableType deserialize(String serializedName) {
            if ("LAKE".equals(serializedName)) {
                return CLOUD_NATIVE; // for backward compatibility
            }
            if ("LAKE_MATERIALIZED_VIEW".equals(serializedName)) {
                return CLOUD_NATIVE_MATERIALIZED_VIEW; // for backward compatibility
            }
            return TableType.valueOf(serializedName);
        }
    }

    public static final ImmutableSet<TableType> IS_ANALYZABLE_EXTERNAL_TABLE =
            new ImmutableSet.Builder<TableType>()
                    .add(TableType.HIVE)
                    .add(TableType.ICEBERG)
                    .add(TableType.HUDI)
                    .add(TableType.ODPS)
                    .add(TableType.DELTALAKE)
                    .build();

    @SerializedName(value = "id")
    protected long id;
    @SerializedName(value = "name")
    protected String name;
    @SerializedName(value = "type")
    protected TableType type;
    @SerializedName(value = "createTime")
    protected long createTime;

    /**
     * For OlapTable:
     * The fullSchema of OlapTable includes the base columns and the SHADOW_NAME_PREFIX columns.
     * The properties of base columns in fullSchema are same as properties in baseIndex.
     * For example:
     * Table (c1 int, c2 int, c3 int)
     * Schema change (c3 to bigint)
     * When OlapTable is changing schema, the fullSchema is (c1 int, c2 int, c3 int, SHADOW_NAME_PREFIX_c3 bigint)
     * The fullSchema of OlapTable is mainly used by Scanner of Load job.
     * NOTICE: The columns of baseIndex is placed before the SHADOW_NAME_PREFIX columns
     *
     * If you want to get all visible columns, you should call getBaseSchema() method, which is override in
     * subclasses.
     * If you want to get the mv columns, you should call getIndexToSchema in Subclass OlapTable.
     *
     * If we are simultaneously executing multiple light schema change tasks, there may be occasional concurrent
     * read-write operations between these tasks with a relatively low probability.
     * Therefore, we choose to use a CopyOnWriteArrayList.
     */
    @SerializedName(value = "fullSchema")
    protected List<Column> fullSchema = new CopyOnWriteArrayList<>();

    /**
     * nameToColumn and idToColumn are both indexes of fullSchema.
     * nameToColumn is the index of column name, idToColumn is the index of column id,
     * column names can change, but the column ID of a specific column will never change.
     * Use case-insensitive tree map, because the column name is case-insensitive in the system.
     */
    protected Map<String, Column> nameToColumn;
    protected Map<ColumnId, Column> idToColumn;

    // table(view)'s comment
    @SerializedName(value = "comment")
    protected String comment = "";

    // not serialized field
    // record all materialized views based on this Table
    @SerializedName(value = "mvs")
    protected Set<MvId> relatedMaterializedViews;

    // unique constraints for mv rewrite
    // a table may have multi unique constraints
    protected List<UniqueConstraint> uniqueConstraints;

    // foreign key constraint for mv rewrite
    protected List<ForeignKeyConstraint> foreignKeyConstraints;

    public Table(TableType type) {
        this.type = type;
        this.fullSchema = Lists.newArrayList();
        updateSchemaIndex();
        this.relatedMaterializedViews = Sets.newConcurrentHashSet();
    }

    public Table(long id, String tableName, TableType type, List<Column> fullSchema) {
        this.id = id;
        this.name = tableName;
        this.type = type;
        // must copy the list, it should not be the same object as in indexIdToSchema
        if (fullSchema != null) {
            this.fullSchema = Lists.newArrayList(fullSchema);
        }
        updateSchemaIndex();
        this.createTime = Instant.now().getEpochSecond();
        this.relatedMaterializedViews = Sets.newConcurrentHashSet();
    }

    public long getId() {
        return id;
    }

    /**
     * Get the unique id of table in string format, since we already ensure
     * the uniqueness of id for internal table, we just convert it to string
     * and return, for external table it's up to the implementation of connector.
     * Note: for external table, we use table name as the privilege entry
     * id, not the uuid returned by this interface.
     *
     * @return unique id of table in string format
     */
    public String getUUID() {
        return Long.toString(id);
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getCatalogName() {
        return InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTableIdentifier() {
        return name;
    }

    public void setType(TableType type) {
        this.type = type;
    }

    public TableType getType() {
        return type;
    }

    public boolean isOlapTable() {
        return type == TableType.OLAP;
    }

    public boolean isOlapExternalTable() {
        return type == TableType.OLAP_EXTERNAL;
    }

    public boolean isOlapMaterializedView() {
        return type == TableType.MATERIALIZED_VIEW;
    }

    public boolean isOlapView() {
        return type == TableType.VIEW;
    }

    public boolean isHiveView() {
        return type == TableType.HIVE_VIEW;
    }

    public boolean isIcebergView() {
        return type == TableType.ICEBERG_VIEW;
    }

    public boolean isMetadataTable() {
        return type == TableType.METADATA;
    }

    public boolean isAnalyzableExternalTable() {
        return IS_ANALYZABLE_EXTERNAL_TABLE.contains(type);
    }

    public boolean isView() {
        return isOlapView() || isConnectorView();
    }

    public boolean isConnectorView() {
        return isHiveView() || isIcebergView();
    }

    public boolean isOlapTableOrMaterializedView() {
        return isOlapTable() || isOlapMaterializedView();
    }

    public boolean isCloudNativeTable() {
        return type == TableType.CLOUD_NATIVE;
    }

    public boolean isCloudNativeMaterializedView() {
        return type == TableType.CLOUD_NATIVE_MATERIALIZED_VIEW;
    }

    public boolean isCloudNativeTableOrMaterializedView() {
        return isCloudNativeTable() || isCloudNativeMaterializedView();
    }

    public boolean isMaterializedView() {
        return isOlapMaterializedView() || isCloudNativeMaterializedView();
    }

    public boolean isNativeTableOrMaterializedView() {
        return isOlapTableOrMaterializedView() || isCloudNativeTableOrMaterializedView();
    }

    public boolean isNativeTable() {
        return isOlapTable() || isCloudNativeTable();
    }

    public boolean isExternalTableWithFileSystem() {
        return isHiveTable() || isIcebergTable() || isHudiTable() || isDeltalakeTable() || isPaimonTable() || isKuduTable();
    }

    public boolean isHiveTable() {
        return type == TableType.HIVE;
    }

    public boolean isHudiTable() {
        return type == TableType.HUDI;
    }

    public boolean isIcebergTable() {
        return type == TableType.ICEBERG;
    }

    public boolean isDeltalakeTable() {
        return type == TableType.DELTALAKE;
    }

    public boolean isPaimonTable() {
        return type == TableType.PAIMON;
    }

    public boolean isOdpsTable() {
        return type == TableType.ODPS;
    }

    public boolean isJDBCTable() {
        return type == TableType.JDBC;
    }

    public boolean isTableFunctionTable() {
        return type == TableType.TABLE_FUNCTION;
    }

    public boolean isBlackHoleTable() {
        return type == TableType.BLACKHOLE;
    }

    public boolean isKuduTable() {
        return type == TableType.KUDU;
    }

    // for create table
    public boolean isOlapOrCloudNativeTable() {
        return isOlapTable() || isCloudNativeTable();
    }

    public boolean isExprPartitionTable() {
        if (this instanceof OlapTable) {
            OlapTable olapTable = (OlapTable) this;
            if (olapTable.getPartitionInfo().getType() == PartitionType.EXPR_RANGE_V2) {
                PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                return partitionInfo instanceof ExpressionRangePartitionInfoV2;
            }
        }
        return false;
    }

    public boolean isTemporaryTable() {
        return false;
    }

    public List<Column> getFullSchema() {
        return fullSchema;
    }

    // should override in subclass if necessary
    public List<Column> getBaseSchema() {
        return fullSchema;
    }

    public Map<ColumnId, Column> getIdToColumn() {
        return idToColumn;
    }

    public void setNewFullSchema(List<Column> newSchema) {
        this.fullSchema = newSchema;
        updateSchemaIndex();
    }

    protected void updateSchemaIndex() {
        Map<String, Column> newNameToColumn = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        Map<ColumnId, Column> newIdToColumn = Maps.newTreeMap(ColumnId.CASE_INSENSITIVE_ORDER);
        for (Column column : this.fullSchema) {
            newNameToColumn.put(column.getName(), column);
            newIdToColumn.put(column.getColumnId(), column);
        }
        this.nameToColumn = newNameToColumn;
        this.idToColumn = newIdToColumn;
    }

    public Column getColumn(String name) {
        return nameToColumn.get(name);
    }

    public Column getColumn(ColumnId columnId) {
        return nameToColumn.get(columnId.getId());
    }

    public boolean containColumn(String columnName) {
        return nameToColumn.containsKey(columnName);
    }

    public List<Column> getColumns() {
        return new ArrayList<>(nameToColumn.values());
    }

    public void addColumn(Column column) {
        fullSchema.add(column);
        nameToColumn.put(column.getName(), column);
    }

    public long getCreateTime() {
        return createTime;
    }

    public String getTableLocation() {
        String msg = "The getTableLocation() method needs to be implemented.";
        throw new NotImplementedException(msg);
    }

    public Map<String, Column> getNameToColumn() {
        return nameToColumn;
    }

    public TTableDescriptor toThrift(List<ReferencedPartitionInfo> partitions) {
        return null;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Override
    public void gsonPostProcess() throws IOException {
        updateSchemaIndex();
        relatedMaterializedViews = Sets.newConcurrentHashSet();
    }

    @Override
    public int hashCode() {
        return Long.hashCode(id);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Table)) {
            return false;
        }
        Table otherTable = (Table) other;
        return id == otherTable.id;
    }

    // return if this table is partitioned.
    // For OlapTable ture when is partitioned, or distributed by hash when no partition
    public boolean isPartitioned() {
        return false;
    }

    public Partition getPartition(String partitionName) {
        return null;
    }

    public Partition getPartition(String partitionName, boolean isTempPartition) {
        return null;
    }

    public Partition getPartition(long partitionId) {
        return null;
    }

    public Collection<Partition> getPartitions() {
        return Collections.emptyList();
    }

    public PhysicalPartition getPhysicalPartition(long partitionId) {
        return null;
    }

    public Set<String> getDistributionColumnNames() {
        return Collections.emptySet();
    }

    public String getEngine() {
        if (this instanceof OlapTable) {
            return "StarRocks";
        } else if (this instanceof MysqlTable) {
            return "MySQL";
        } else if (this instanceof SystemTable) {
            return "MEMORY";
        } else if (this instanceof HiveTable) {
            return "Hive";
        } else if (this instanceof HudiTable) {
            return "Hudi";
        } else if (this instanceof IcebergTable) {
            return "Iceberg";
        } else if (this instanceof DeltaLakeTable) {
            return "DeltaLake";
        } else if (this instanceof EsTable) {
            return "Elasticsearch";
        } else if (this instanceof JDBCTable) {
            return "JDBC";
        } else if (this instanceof FileTable) {
            return "File";
        } else {
            return null;
        }
    }

    public String getMysqlType() {
        switch (type) {
            case INLINE_VIEW:
            case VIEW:
            case MATERIALIZED_VIEW:
            case CLOUD_NATIVE_MATERIALIZED_VIEW:
                return "VIEW";
            case SCHEMA:
                return "SYSTEM VIEW";
            default:
                // external table also returns "BASE TABLE" for BI compatibility
                return "BASE TABLE";
        }
    }

    public String getComment() {
        if (!Strings.isNullOrEmpty(comment)) {
            return comment;
        }
        return "";
    }

    // Attention: cause the remove escape character in parser phase, when you want to print the
    // comment, you need add the escape character back
    public String getDisplayComment() {
        if (!Strings.isNullOrEmpty(comment)) {
            return CatalogUtils.addEscapeCharacter(comment);
        }
        return "";
    }

    public void setComment(String comment) {
        this.comment = Strings.nullToEmpty(comment);
    }

    @Override
    public int getSignature(int signatureVersion) {
        throw new NotImplementedException();
    }

    @Override
    public String toString() {
        return "Table [id=" + id + ", name=" + name + ", type=" + type + "]";
    }

    /*
     * 1. Only schedule OLAP table.
     * 2. If table is colocate with other table,
     *   2.1 If is clone between bes or group is not stable, table can not be scheduled.
     *   2.2 If is local balance and group is stable, table can be scheduled.
     * 3. (deprecated). if table's state is ROLLUP or SCHEMA_CHANGE, but alter job's state is FINISHING, we should also
     *      schedule the tablet to repair it(only for VERSION_INCOMPLETE case, this will be checked in
     *      TabletScheduler).
     * 4. Even if table's state is ROLLUP or SCHEMA_CHANGE, check it. Because we can repair the tablet of base index.
     * 5. PRIMARY_KEYS table does not support local balance.
     */
    public boolean needSchedule(boolean isLocalBalance) {
        if (!isOlapTableOrMaterializedView()) {
            return false;
        }

        ColocateTableIndex colocateIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        if (colocateIndex.isColocateTable(getId())) {
            boolean isGroupUnstable = colocateIndex.isGroupUnstable(colocateIndex.getGroup(getId()));
            if (!isLocalBalance || isGroupUnstable) {
                LOG.debug(
                        "table {} is a colocate table, skip tablet checker. " +
                                "is local migration: {}, is group unstable: {}",
                        name, isLocalBalance, isGroupUnstable);
                return false;
            }
        }

        return true;
    }

    public boolean hasAutoIncrementColumn() {
        List<Column> columns = this.getFullSchema();
        for (Column col : columns) {
            if (col.isAutoIncrement()) {
                return true;
            }
        }
        return false;
    }

    /**
     * onCreate is called when this table is created
     */
    public void onReload() {
        // Do nothing by default.
    }

    public void onCreate(Database database) {
        onReload();
    }

    /**
     * This method is called right after the calling of {@link Database#dropTable(String)}, with the
     * protection of the database's writer lock.
     * <p>
     * If {@code force} is false, this table can be recovered later, so the implementation should not
     * delete any real data otherwise there will be data loss after the table been recovered.
     * <p>
     * To avoid holding the database lock for a long time, do NOT perform time-consuming operations in this
     * method, such as deleting data, sending RPC requests, etc. Instead, you should put these operations
     * into {@link Table#delete(boolean)}.
     *
     * @param db     the owner database of the table
     * @param force  is this a force drop
     * @param replay is this is a log replay operation
     */
    public void onDrop(Database db, boolean force, boolean replay) {
        // Do nothing by default.
    }

    /**
     * Delete this table permanently. Implementations can perform necessary cleanup work.
     *
     * @param dbId ID of the database to which the table belongs
     * @param replay is this a log replay operation.
     * @return Returns true if the deletion task was performed successfully, false otherwise.
     */
    public boolean delete(long dbId, boolean replay) {
        return true;
    }

    /**
     * Delete thie table from {@link CatalogRecycleBin}
     * @param replay is this a log relay operation.
     * @return Returns true if the deletion task was performed successfully, false otherwise.
     */
    public boolean deleteFromRecycleBin(long dbId, boolean replay) {
        return delete(dbId, replay);
    }

    /**
     * Whether the delete table operation supports retry on failure
     *
     * @return true if retry is supported on delete table failure, false if retry is not supported.
     */
    public boolean isDeleteRetryable() {
        return false;
    }

    public boolean isSupported() {
        return false;
    }

    public Map<String, String> getProperties() {
        throw new NotImplementedException();
    }

    // should call this when create materialized view
    public void addRelatedMaterializedView(MvId mvId) {
        relatedMaterializedViews.add(mvId);
    }

    // should call this when drop materialized view
    public void removeRelatedMaterializedView(MvId mvId) {
        relatedMaterializedViews.remove(mvId);
    }

    public Set<MvId> getRelatedMaterializedViews() {
        return relatedMaterializedViews;
    }

    public boolean isUnPartitioned() {
        return true;
    }

    public List<Column> getPartitionColumns() {
        throw new NotImplementedException();
    }

    public List<String> getPartitionColumnNames() {
        return Lists.newArrayList();
    }

    public boolean supportsUpdate() {
        return false;
    }

    public boolean supportInsert() {
        return false;
    }

    public boolean supportPreCollectMetadata() {
        return false;
    }

    public boolean isTemporal() {
        return false;
    }

    public boolean hasUniqueConstraints() {
        List<UniqueConstraint> uniqueConstraint = getUniqueConstraints();
        return uniqueConstraint != null;
    }

    public void setUniqueConstraints(List<UniqueConstraint> uniqueConstraints) {
        this.uniqueConstraints = uniqueConstraints;
    }

    public List<UniqueConstraint> getUniqueConstraints() {
        return this.uniqueConstraints;
    }

    public void setForeignKeyConstraints(List<ForeignKeyConstraint> foreignKeyConstraints) {
        this.foreignKeyConstraints = foreignKeyConstraints;
    }

    /**
     * Get foreign key constraints of this table. Caller should not change the returned list.
     */
    public List<ForeignKeyConstraint> getForeignKeyConstraints() {
        return this.foreignKeyConstraints;
    }

    public boolean hasForeignKeyConstraints() {
        return this.foreignKeyConstraints != null && !this.foreignKeyConstraints.isEmpty();
    }

    public boolean isTable() {
        return !type.equals(TableType.MATERIALIZED_VIEW) &&
                !type.equals(TableType.CLOUD_NATIVE_MATERIALIZED_VIEW) &&
                !type.equals(TableType.VIEW) &&
                !isConnectorView();
    }
}
