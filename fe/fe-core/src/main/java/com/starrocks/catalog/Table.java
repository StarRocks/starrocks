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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.DescriptorTable.ReferencedPartitionInfo;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.lake.LakeTable;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.thrift.TTableDescriptor;
import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Internal representation of table-related metadata. A table contains several partitions.
 */
public class Table extends MetaObject implements Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(Table.class);

    // 1. Native table:
    //   1.1 Local: OLAP, MATERIALIZED_VIEW
    //   1.2 Lake: LAKE
    // 2. System table: SCHEMA
    // 3. View: INLINE_VIEW, VIEW
    // 4. External table: MYSQL, OLAP_EXTERNAL, BROKER, ELASTICSEARCH, HIVE, ICEBERG, HUDI, ODBC, JDBC
    public enum TableType {
        MYSQL,
        OLAP,
        OLAP_EXTERNAL,
        SCHEMA,
        INLINE_VIEW,
        VIEW,
        BROKER,
        ELASTICSEARCH,
        HIVE,
        ICEBERG,
        HUDI,
        JDBC,
        MATERIALIZED_VIEW,
        LAKE,
        DELTALAKE,
        FILE
    }

    @SerializedName(value = "id")
    protected long id;
    @SerializedName(value = "name")
    protected String name;
    @SerializedName(value = "type")
    protected TableType type;
    @SerializedName(value = "createTime")
    protected long createTime;
    /*
     *  fullSchema and nameToColumn should contain all columns, both visible and shadow.
     *  eg. for OlapTable, when doing schema change, there will be some shadow columns which are not visible
     *      to query but visible to load process.
     *  If you want to get all visible columns, you should call getBaseSchema() method, which is override in
     *  sub classes.
     *
     *  NOTICE: the order of this fullSchema is meaningless to OlapTable
     */
    /**
     * The fullSchema of OlapTable includes the base columns and the SHADOW_NAME_PRFIX columns.
     * The properties of base columns in fullSchema are same as properties in baseIndex.
     * For example:
     * Table (c1 int, c2 int, c3 int)
     * Schema change (c3 to bigint)
     * When OlapTable is changing schema, the fullSchema is (c1 int, c2 int, c3 int, SHADOW_NAME_PRFIX_c3 bigint)
     * The fullSchema of OlapTable is mainly used by Scanner of Load job.
     * <p>
     * If you want to get the mv columns, you should call getIndexToSchema in Subclass OlapTable.
     */
    @SerializedName(value = "fullSchema")
    protected List<Column> fullSchema;
    // tree map for case-insensitive lookup.
    /**
     * The nameToColumn of OlapTable includes the base columns and the SHADOW_NAME_PRFIX columns.
     */
    protected Map<String, Column> nameToColumn;

    // DO NOT persist this variable.
    protected boolean isTypeRead = false;
    // table(view)'s comment
    @SerializedName(value = "comment")
    protected String comment = "";

    // not serialized field
    // record all materialized views based on this Table
    @SerializedName(value = "mvs")
    private Set<MvId> relatedMaterializedViews;

    public Table(TableType type) {
        this.type = type;
        this.fullSchema = Lists.newArrayList();
        this.nameToColumn = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
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
        this.nameToColumn = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        if (this.fullSchema != null) {
            for (Column col : this.fullSchema) {
                nameToColumn.put(col.getName(), col);
            }
        } else {
            // Only view in with-clause have null base
            Preconditions.checkArgument(type == TableType.VIEW, "Table has no columns");
        }
        this.createTime = Instant.now().getEpochSecond();
        this.relatedMaterializedViews = Sets.newConcurrentHashSet();
    }

    public void setTypeRead(boolean isTypeRead) {
        this.isTypeRead = isTypeRead;
    }

    public long getId() {
        return id;
    }

    /**
     * Get the unique id of table in string format, since we already ensure
     * the uniqueness of id for internal table, we just convert it to string
     * and return, for external table it's up to the implementation of connector.
     *
     * @return unique id of table in string format
     */
    public String getUUID() {
        return Long.toString(id);
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
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

    public boolean isMaterializedView() {
        return type == TableType.MATERIALIZED_VIEW;
    }

    public boolean isLakeTable() {
        return type == TableType.LAKE;
    }

    public boolean isLocalTable() {
        return isOlapTable() || isMaterializedView();
    }

    public boolean isNativeTable() {
        return isLocalTable() || isLakeTable();
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

    // for create table
    public boolean isOlapOrLakeTable() {
        return isOlapTable() || isLakeTable();
    }

    public List<Column> getFullSchema() {
        return fullSchema;
    }

    // should override in subclass if necessary
    public List<Column> getBaseSchema() {
        return fullSchema;
    }

    public void setNewFullSchema(List<Column> newSchema) {
        this.fullSchema = newSchema;
        this.nameToColumn.clear();
        for (Column col : fullSchema) {
            nameToColumn.put(col.getName(), col);
        }
    }

    public Column getColumn(String name) {
        return nameToColumn.get(name);
    }

    public List<Column> getColumns() {
        return new ArrayList<>(nameToColumn.values());
    }

    public long getCreateTime() {
        return createTime;
    }

    public TTableDescriptor toThrift(List<ReferencedPartitionInfo> partitions) {
        return null;
    }

    public static Table read(DataInput in) throws IOException {
        Table table = null;
        TableType type = TableType.valueOf(Text.readString(in));
        if (type == TableType.OLAP) {
            table = new OlapTable();
        } else if (type == TableType.MYSQL) {
            table = new MysqlTable();
        } else if (type == TableType.VIEW) {
            table = new View();
        } else if (type == TableType.BROKER) {
            table = new BrokerTable();
        } else if (type == TableType.ELASTICSEARCH) {
            table = new EsTable();
        } else if (type == TableType.HIVE) {
            table = new HiveTable();
        } else if (type == TableType.FILE) {
            table = new FileTable();
        } else if (type == TableType.HUDI) {
            table = new HudiTable();
        } else if (type == TableType.OLAP_EXTERNAL) {
            table = new ExternalOlapTable();
        } else if (type == TableType.ICEBERG) {
            table = new IcebergTable();
        } else if (type == TableType.JDBC) {
            table = new JDBCTable();
        } else if (type == TableType.MATERIALIZED_VIEW) {
            table = MaterializedView.read(in);
            table.setTypeRead(true);
            return table;
        } else if (type == TableType.LAKE) {
            table = LakeTable.read(in);
            table.setTypeRead(true);
            return table;
        } else {
            throw new IOException("Unknown table type: " + type.name());
        }

        table.setTypeRead(true);
        table.readFields(in);
        return table;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // ATTN: must write type first
        Text.writeString(out, type.name());

        // write last check time
        super.write(out);

        out.writeLong(id);
        Text.writeString(out, name);

        // base schema
        int columnCount = fullSchema.size();
        out.writeInt(columnCount);
        for (Column column : fullSchema) {
            column.write(out);
        }

        Text.writeString(out, comment);

        // write create time
        out.writeLong(createTime);
    }

    public void readFields(DataInput in) throws IOException {
        if (!isTypeRead) {
            type = TableType.valueOf(Text.readString(in));
            isTypeRead = true;
        }

        super.readFields(in);

        this.id = in.readLong();
        this.name = Text.readString(in);

        // base schema
        int columnCount = in.readInt();
        for (int i = 0; i < columnCount; i++) {
            Column column = Column.read(in);
            this.fullSchema.add(column);
            this.nameToColumn.put(column.getName(), column);
        }

        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_63) {
            comment = Text.readString(in);
        } else {
            comment = "";
        }

        // read create time
        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_64) {
            this.createTime = in.readLong();
        } else {
            this.createTime = -1L;
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
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

    public Partition getPartition(long partitionId) {
        return null;
    }

    public Collection<Partition> getPartitions() {
        return Collections.emptyList();
    }

    public Set<String> getDistributionColumnNames() {
        return Collections.emptySet();
    }

    public String getEngine() {
        if (this instanceof OlapTable) {
            return "StarRocks";
        } else if (this instanceof MysqlTable) {
            return "MySQL";
        } else if (this instanceof SchemaTable) {
            return "MEMORY";
        } else {
            return null;
        }
    }

    public String getMysqlType() {
        if (this instanceof View) {
            return "VIEW";
        }
        if (this instanceof MaterializedView) {
            return "VIEW";
        }
        return "BASE TABLE";
    }

    public String getComment() {
        if (!Strings.isNullOrEmpty(comment)) {
            return comment;
        }
        return type.name();
    }

    public void setComment(String comment) {
        this.comment = Strings.nullToEmpty(comment);
    }

    public CreateTableStmt toCreateTableStmt(String dbName) {
        throw new NotImplementedException();
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
     *      schedule the tablet to repair it(only for VERSION_IMCOMPLETE case, this will be checked in
     *      TabletScheduler).
     * 4. Even if table's state is ROLLUP or SCHEMA_CHANGE, check it. Because we can repair the tablet of base index.
     * 5. PRIMARY_KEYS table does not support local balance.
     */
    public boolean needSchedule(boolean isLocalBalance) {
        if (!isLocalTable()) {
            return false;
        }

        ColocateTableIndex colocateIndex = GlobalStateMgr.getCurrentColocateIndex();
        if (colocateIndex.isColocateTable(getId())) {
            boolean isGroupUnstable = colocateIndex.isGroupUnstable(colocateIndex.getGroup(getId()));
            if (!isLocalBalance || isGroupUnstable) {
                LOG.debug(
                        "table {} is a colocate table, skip tablet checker. is local migration: {}, is group unstable: {}",
                        name, isLocalBalance, isGroupUnstable);
                return false;
            }
        }

        OlapTable olapTable = (OlapTable) this;
        if (isLocalBalance && olapTable.getKeysType() == KeysType.PRIMARY_KEYS) {
            return false;
        }

        return true;
    }

    /**
     * onCreate is called when this table is created
     */
    public void onCreate() {
        // Do nothing by default.
    }

    /**
     * This method is called right before the calling of {@link Database#dropTable(String)}, with the protection of the
     * database's writer lock.
     * <p>
     * If {@code force} is false, this table will be placed into the {@link CatalogRecycleBin} and may be
     * recovered later, so the implementation should not delete any real data otherwise there will be
     * data loss after the table been recovered.
     *
     * @param db     the owner database of the table
     * @param force  is this a force drop
     * @param replay is this is a log replay operation
     */
    public void onDrop(Database db, boolean force, boolean replay) {
        // Do nothing by default.
    }

    /**
     * Delete this table. this method is called with the protection of the database's writer lock.
     *
     * @param replay is this a log replay operation.
     * @return a {@link Runnable} object that will be invoked after the table has been deleted from
     * catalog, or null if no action need to be performed.
     */
    @Nullable
    public Runnable delete(boolean replay) {
        return null;
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

    public List<String> getPartitionColumnNames() {
        return Lists.newArrayList();
    }
}
