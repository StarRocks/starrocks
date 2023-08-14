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


package com.starrocks.catalog;

import com.google.common.base.Strings;
import com.google.common.collect.Range;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.IndexDef;
import com.starrocks.catalog.DistributionInfo.DistributionInfoType;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.Replica.ReplicaState;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.HashDistributionDesc;
import com.starrocks.system.Backend;
import com.starrocks.system.Backend.BackendState;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TBackendMeta;
import com.starrocks.thrift.TColumnMeta;
import com.starrocks.thrift.TDataProperty;
import com.starrocks.thrift.THashDistributionInfo;
import com.starrocks.thrift.TIndexInfo;
import com.starrocks.thrift.TIndexMeta;
import com.starrocks.thrift.TPartitionInfo;
import com.starrocks.thrift.TPartitionMeta;
import com.starrocks.thrift.TRange;
import com.starrocks.thrift.TRangePartitionDesc;
import com.starrocks.thrift.TReplicaMeta;
import com.starrocks.thrift.TSinglePartitionDesc;
import com.starrocks.thrift.TTableMeta;
import com.starrocks.thrift.TTabletMeta;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ExternalOlapTable extends OlapTable {
    private static final Logger LOG = LogManager.getLogger(ExternalOlapTable.class);

    private static final String JSON_KEY_HOST = "host";
    private static final String JSON_KEY_PORT = "port";
    private static final String JSON_KEY_USER = "user";
    private static final String JSON_KEY_PASSWORD = "password";
    private static final String JSON_KEY_TABLE_NAME = "table_name";
    private static final String JSON_KEY_DB_ID = "db_id";
    private static final String JSON_KEY_TABLE_ID = "table_id";
    private static final String JSON_KEY_SOURCE_DB_NAME = "source_db_name";
    private static final String JSON_KEY_SOURCE_DB_ID = "source_db_id";
    private static final String JSON_KEY_SOURCE_TABLE_ID = "source_table_id";
    private static final String JSON_KEY_SOURCE_TABLE_NAME = "source_table_name";
    private static final String JSON_KEY_SOURCE_TABLE_TYPE = "source_table_type";

    public class ExternalTableInfo {
        // remote doris cluster fe addr
        @SerializedName("ht")
        private String host;
        @SerializedName("pt")
        private int port;
        // access credential
        @SerializedName("us")
        private String user;
        @SerializedName("pw")
        private String password;

        // source table info
        @SerializedName("db")
        private String dbName;
        @SerializedName("tb")
        private String tableName;

        @SerializedName("dbi")
        private long dbId;
        @SerializedName("tbi")
        private long tableId;
        @SerializedName("tbt")
        private TableType tableType;

        public ExternalTableInfo() {
            this.host = "";
            this.port = 0;
            this.user = "";
            this.password = "";
            this.dbName = "";
            this.tableName = "";
            this.dbId = -1;
            this.tableId = -1;
            this.tableType = TableType.OLAP;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        public String getUser() {
            return user;
        }

        public String getPassword() {
            return password;
        }

        public String getDbName() {
            return dbName;
        }

        public String getTableName() {
            return tableName;
        }

        public long getDbId() {
            return dbId;
        }

        public void setDbId(long dbId) {
            this.dbId = dbId;
        }

        public long getTableId() {
            return tableId;
        }

        public TableType getTableType() {
            return tableType;
        }

        public void setTableId(long tableId) {
            this.tableId = tableId;
        }

        public void setTableType(TableType tableType) {
            this.tableType = tableType;
        }

        public void toJsonObj(JsonObject obj) {
            obj.addProperty(JSON_KEY_HOST, host);
            obj.addProperty(JSON_KEY_PORT, port);
            obj.addProperty(JSON_KEY_USER, user);
            obj.addProperty(JSON_KEY_PASSWORD, password);
            obj.addProperty(JSON_KEY_SOURCE_DB_NAME, dbName);
            obj.addProperty(JSON_KEY_SOURCE_DB_ID, dbId);
            obj.addProperty(JSON_KEY_SOURCE_TABLE_NAME, tableName);
            obj.addProperty(JSON_KEY_SOURCE_TABLE_ID, tableId);
            obj.addProperty(JSON_KEY_SOURCE_TABLE_TYPE, TableType.serialize(tableType));
        }

        public void fromJsonObj(JsonObject obj) {
            host = obj.getAsJsonPrimitive(JSON_KEY_HOST).getAsString();
            port = obj.getAsJsonPrimitive(JSON_KEY_PORT).getAsInt();
            user = obj.getAsJsonPrimitive(JSON_KEY_USER).getAsString();
            password = obj.getAsJsonPrimitive(JSON_KEY_PASSWORD).getAsString();
            dbName = obj.getAsJsonPrimitive(JSON_KEY_SOURCE_DB_NAME).getAsString();
            dbId = obj.getAsJsonPrimitive(JSON_KEY_SOURCE_DB_ID).getAsLong();
            tableName = obj.getAsJsonPrimitive(JSON_KEY_SOURCE_TABLE_NAME).getAsString();
            tableId = obj.getAsJsonPrimitive(JSON_KEY_SOURCE_TABLE_ID).getAsLong();
            JsonPrimitive tableTypeJson = obj.getAsJsonPrimitive(JSON_KEY_SOURCE_TABLE_TYPE);
            if (tableTypeJson != null) {
                tableType = TableType.deserialize(tableTypeJson.getAsString());
            }
        }

        public void parseFromProperties(Map<String, String> properties) throws DdlException {
            if (properties == null) {
                throw new DdlException("miss properties for external table, "
                        + "they are: host, port, user, password, database and table");
            }

            host = properties.get("host");
            if (Strings.isNullOrEmpty(host)) {
                throw new DdlException("Host of external table is null. "
                        + "Please add properties('host'='xxx.xxx.xxx.xxx') when create table");
            }

            String portStr = properties.get("port");
            if (Strings.isNullOrEmpty(portStr)) {
                // Maybe null pointer or number convert
                throw new DdlException("miss port of external table is null. "
                        + "Please add properties('port'='3306') when create table");
            }
            try {
                port = Integer.valueOf(portStr);
            } catch (Exception e) {
                throw new DdlException("port of external table must be a number."
                        + "Please add properties('port'='3306') when create table");
            }

            user = properties.get("user");
            if (Strings.isNullOrEmpty(user)) {
                throw new DdlException("User of external table is null. "
                        + "Please add properties('user'='root') when create table");
            }

            password = properties.get("password");
            if (password == null) {
                throw new DdlException("Password of external table is null. "
                        + "Please add properties('password'='xxxx') when create table");
            }

            dbName = properties.get("database");
            if (Strings.isNullOrEmpty(dbName)) {
                throw new DdlException("Database of external table is null. "
                        + "Please add properties('database'='xxxx') when create table");
            }

            tableName = properties.get("table");
            if (Strings.isNullOrEmpty(tableName)) {
                throw new DdlException("external table name missing."
                        + "Please add properties('table'='xxxx') when create table");
            }
        }
    }


    private long dbId;
    private TTableMeta lastExternalMeta;

    @SerializedName(value = "ef")
    private ExternalTableInfo externalTableInfo;

    public ExternalOlapTable() {
        super();
        setType(TableType.OLAP_EXTERNAL);
        dbId = -1;
        // sourceDbId = -1;
        lastExternalMeta = null;
        externalTableInfo = null;
    }

    public ExternalOlapTable(long dbId, long tableId, String tableName, List<Column> baseSchema, KeysType keysType,
                             PartitionInfo partitionInfo, DistributionInfo defaultDistributionInfo,
                             TableIndexes indexes, Map<String, String> properties)
            throws DdlException {
        super(tableId, tableName, baseSchema, keysType, partitionInfo, defaultDistributionInfo, indexes);
        setType(TableType.OLAP_EXTERNAL);
        this.dbId = dbId;
        lastExternalMeta = null;

        externalTableInfo = new ExternalTableInfo();
        externalTableInfo.parseFromProperties(properties);
    }

    public long getDbId() {
        return dbId;
    }

    public long getSourceTableDbId() {
        return externalTableInfo.getDbId();
    }

    public String getSourceTableDbName() {
        return externalTableInfo.getDbName();
    }

    public long getSourceTableId() {
        return externalTableInfo.getTableId();
    }

    public String getSourceTableName() {
        return externalTableInfo.getTableName();
    }

    public TableType getSourceTableType() {
        return externalTableInfo.getTableType();
    }

    public String getSourceTableHost() {
        return externalTableInfo.getHost();
    }

    public int getSourceTablePort() {
        return externalTableInfo.getPort();
    }

    public String getSourceTableUser() {
        return externalTableInfo.getUser();
    }

    public String getSourceTablePassword() {
        return externalTableInfo.getPassword();
    }

    public boolean isSourceTableCloudNativeTable() {
        return externalTableInfo.getTableType() == TableType.CLOUD_NATIVE;
    }

    public boolean isSourceTableCloudNativeMaterializedView() {
        return externalTableInfo.getTableType() == TableType.CLOUD_NATIVE_MATERIALIZED_VIEW;
    }

    public boolean isSourceTableCloudNativeTableOrMaterializedView() {
        return isSourceTableCloudNativeTable() || isSourceTableCloudNativeMaterializedView();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        JsonObject obj = new JsonObject();
        obj.addProperty(JSON_KEY_TABLE_ID, id);
        obj.addProperty(JSON_KEY_TABLE_NAME, name);
        obj.addProperty(JSON_KEY_DB_ID, dbId);
        externalTableInfo.toJsonObj(obj);
        Text.writeString(out, obj.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        String jsonStr = Text.readString(in);
        JsonObject obj = JsonParser.parseString(jsonStr).getAsJsonObject();
        id = obj.getAsJsonPrimitive(JSON_KEY_TABLE_ID).getAsLong();
        name = obj.getAsJsonPrimitive(JSON_KEY_TABLE_NAME).getAsString();
        dbId = obj.getAsJsonPrimitive(JSON_KEY_DB_ID).getAsLong();
        externalTableInfo = new ExternalTableInfo();
        externalTableInfo.fromJsonObj(obj);
    }

    @Override
    public void copyOnlyForQuery(OlapTable olapTable) {
        super.copyOnlyForQuery(olapTable);
        ExternalOlapTable externalOlapTable = (ExternalOlapTable) olapTable;
        externalOlapTable.externalTableInfo = this.externalTableInfo;
    }

    public void updateMeta(String dbName, TTableMeta meta, List<TBackendMeta> backendMetas)
            throws DdlException, IOException {
        // no meta changed since last time, do nothing
        if (lastExternalMeta != null && meta.compareTo(lastExternalMeta) == 0) {
            return;
        }

        clusterId = meta.getCluster_id();
        externalTableInfo.setDbId(meta.getDb_id());
        externalTableInfo.setTableId(meta.getTable_id());
        if (meta.isSetTable_type()) {
            externalTableInfo.setTableType(TableType.deserialize(meta.getTable_type()));
        }
        long start = System.currentTimeMillis();

        state = OlapTableState.valueOf(meta.getState());
        baseIndexId = meta.getBase_index_id();
        colocateGroup = meta.getColocate_group();
        bfFpp = meta.getBloomfilter_fpp();

        keysType = KeysType.valueOf(meta.getKey_type());
        tableProperty = new TableProperty(meta.getProperties());
        tableProperty.buildReplicationNum();
        tableProperty.buildStorageVolume();
        tableProperty.buildInMemory();
        tableProperty.buildDynamicProperty();
        tableProperty.buildWriteQuorum();
        tableProperty.buildReplicatedStorage();

        indexes = null;
        if (meta.isSetIndex_infos()) {
            List<Index> indexList = new ArrayList<>();
            for (TIndexInfo indexInfo : meta.getIndex_infos()) {
                Index index = new Index(indexInfo.getIndex_name(), indexInfo.getColumns(),
                        IndexDef.IndexType.valueOf(indexInfo.getIndex_type()), indexInfo.getComment());
                indexList.add(index);
            }
            indexes = new TableIndexes(indexList);
        }

        TPartitionInfo tPartitionInfo = meta.getPartition_info();
        PartitionType partitionType = PartitionType.fromThrift(tPartitionInfo.getType());
        switch (partitionType) {
            case RANGE:
            case EXPR_RANGE:
            case EXPR_RANGE_V2:
                TRangePartitionDesc rangePartitionDesc = tPartitionInfo.getRange_partition_desc();
                List<Column> columns = new ArrayList<Column>();
                for (TColumnMeta columnMeta : rangePartitionDesc.getColumns()) {
                    Type type = Type.fromThrift(columnMeta.getColumnType());
                    Column column = new Column(columnMeta.getColumnName(), type);
                    if (columnMeta.isSetKey()) {
                        column.setIsKey(columnMeta.isKey());
                    }
                    if (columnMeta.isSetAggregationType()) {
                        column.setAggregationType(AggregateType.valueOf(columnMeta.getAggregationType()), false);
                    }
                    if (columnMeta.isSetComment()) {
                        column.setComment(columnMeta.getComment());
                    }
                    if (columnMeta.isSetDefaultValue()) {
                        column.setDefaultValue(columnMeta.getDefaultValue());
                    }
                    columns.add(column);
                }
                partitionInfo = new RangePartitionInfo(columns);

                for (Map.Entry<Long, TRange> entry : rangePartitionDesc.getRanges().entrySet()) {
                    TRange tRange = entry.getValue();
                    long partitionId = tRange.getPartition_id();
                    ByteArrayInputStream stream = new ByteArrayInputStream(tRange.getStart_key());
                    DataInputStream input = new DataInputStream(stream);
                    PartitionKey startKey = PartitionKey.read(input);
                    stream = new ByteArrayInputStream(tRange.getEnd_key());
                    input = new DataInputStream(stream);
                    PartitionKey endKey = PartitionKey.read(input);
                    Range<PartitionKey> range = Range.closedOpen(startKey, endKey);
                    short replicaNum = tRange.getBase_desc().getReplica_num_map().get(partitionId);
                    boolean inMemory = tRange.getBase_desc().getIn_memory_map().get(partitionId);
                    TDataProperty thriftDataProperty = tRange.getBase_desc().getData_property().get(partitionId);
                    DataProperty dataProperty = new DataProperty(thriftDataProperty.getStorage_medium(),
                            thriftDataProperty.getCold_time());
                    // TODO: confirm false is ok
                    RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                    rangePartitionInfo.addPartition(partitionId, tRange.isSetIs_temp() && tRange.isIs_temp(),
                            range, dataProperty, replicaNum, inMemory);
                }
                break;
            case UNPARTITIONED:
                partitionInfo = new SinglePartitionInfo();
                TSinglePartitionDesc singePartitionDesc = tPartitionInfo.getSingle_partition_desc();
                for (Map.Entry<Long, Short> entry : singePartitionDesc.getBase_desc().getReplica_num_map()
                        .entrySet()) {
                    long partitionId = entry.getKey();
                    short replicaNum = singePartitionDesc.getBase_desc().getReplica_num_map().get(partitionId);
                    boolean inMemory = singePartitionDesc.getBase_desc().getIn_memory_map().get(partitionId);
                    TDataProperty thriftDataProperty =
                            singePartitionDesc.getBase_desc().getData_property().get(partitionId);
                    DataProperty dataProperty = new DataProperty(thriftDataProperty.getStorage_medium(),
                            thriftDataProperty.getCold_time());
                    partitionInfo.addPartition(partitionId, dataProperty, replicaNum, inMemory);
                }
                break;
            default:
                LOG.error("invalid partition type: {}", partitionType);
                return;
        }
        long endOfPartitionBuild = System.currentTimeMillis();

        indexIdToMeta.clear();
        indexNameToId.clear();

        for (TIndexMeta indexMeta : meta.getIndexes()) {
            List<Column> columns = new ArrayList();
            for (TColumnMeta columnMeta : indexMeta.getSchema_meta().getColumns()) {
                Type type = Type.fromThrift(columnMeta.getColumnType());
                Column column = new Column(columnMeta.getColumnName(), type, columnMeta.isAllowNull());
                if (columnMeta.isSetKey()) {
                    column.setIsKey(columnMeta.isKey());
                }
                if (columnMeta.isSetAggregationType()) {
                    column.setAggregationType(AggregateType.valueOf(columnMeta.getAggregationType()), false);
                }
                if (columnMeta.isSetComment()) {
                    column.setComment(columnMeta.getComment());
                }
                if (columnMeta.isSetDefaultValue()) {
                    column.setDefaultValue(columnMeta.getDefaultValue());
                }
                columns.add(column);
            }
            MaterializedIndexMeta index = new MaterializedIndexMeta(indexMeta.getIndex_id(), columns,
                    indexMeta.getSchema_meta().getSchema_version(),
                    indexMeta.getSchema_meta().getSchema_hash(),
                    indexMeta.getSchema_meta().getShort_key_col_count(),
                    indexMeta.getSchema_meta().getStorage_type(),
                    KeysType.valueOf(indexMeta.getSchema_meta()
                            .getKeys_type()),
                    null);
            indexIdToMeta.put(index.getIndexId(), index);
            // TODO(wulei)
            // indexNameToId.put(indexMeta.getIndex_name(), index.getIndexId());
        }
        long endOfIndexMetaBuild = System.currentTimeMillis();

        rebuildFullSchema();
        long endOfSchemaRebuild = System.currentTimeMillis();

        idToPartition.clear();
        nameToPartition.clear();

        DistributionInfoType type =
                DistributionInfoType.valueOf(meta.getDistribution_desc().getDistribution_type());
        if (type == DistributionInfoType.HASH) {
            THashDistributionInfo hashDist = meta.getDistribution_desc().getHash_distribution();
            DistributionDesc distributionDesc = new HashDistributionDesc(hashDist.getBucket_num(),
                    hashDist.getDistribution_columns());
            defaultDistributionInfo = distributionDesc.toDistributionInfo(getBaseSchema());
        }

        for (TPartitionMeta partitionMeta : meta.getPartitions()) {
            Partition partition = new Partition(partitionMeta.getPartition_id(),
                    partitionMeta.getPartition_name(),
                    null, // TODO(wulei): fix it
                    defaultDistributionInfo);
            partition.setNextVersion(partitionMeta.getNext_version());
            partition.updateVisibleVersion(partitionMeta.getVisible_version(),
                    partitionMeta.getVisible_time());
            for (TIndexMeta indexMeta : meta.getIndexes()) {
                MaterializedIndex index = new MaterializedIndex(indexMeta.getIndex_id(),
                        IndexState.fromThrift(indexMeta.getIndex_state()));
                index.setRowCount(indexMeta.getRow_count());
                for (TTabletMeta tTabletMeta : indexMeta.getTablets()) {
                    LocalTablet tablet = new LocalTablet(tTabletMeta.getTablet_id());
                    tablet.setCheckedVersion(tTabletMeta.getChecked_version());
                    tablet.setIsConsistent(tTabletMeta.isConsistent());
                    for (TReplicaMeta replicaMeta : tTabletMeta.getReplicas()) {
                        Replica replica = new Replica(replicaMeta.getReplica_id(), replicaMeta.getBackend_id(),
                                replicaMeta.getVersion(),
                                replicaMeta.getSchema_hash(), replicaMeta.getData_size(),
                                replicaMeta.getRow_count(), ReplicaState.valueOf(replicaMeta.getState()),
                                replicaMeta.getLast_failed_version(),
                                replicaMeta.getLast_success_version());
                        replica.setLastFailedTime(replicaMeta.getLast_failed_time());
                        // forbidden repair for external table
                        replica.setNeedFurtherRepair(false);
                        tablet.addReplica(replica, false);
                    }
                    TabletMeta tabletMeta = new TabletMeta(tTabletMeta.getDb_id(), tTabletMeta.getTable_id(),
                            tTabletMeta.getPartition_id(), tTabletMeta.getIndex_id(),
                            tTabletMeta.getOld_schema_hash(), tTabletMeta.getStorage_medium());
                    index.addTablet(tablet, tabletMeta, false);
                }
                if (indexMeta.getPartition_id() == partition.getId()) {
                    if (index.getId() != baseIndexId) {
                        partition.createRollupIndex(index);
                    } else {
                        partition.setBaseIndex(index);
                    }
                }
            }
            if (partitionMeta.isSetIs_temp() && partitionMeta.isIs_temp()) {
                addTempPartition(partition);
            } else {
                addPartition(partition);
            }
        }
        long endOfTabletMetaBuild = System.currentTimeMillis();

        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getOrCreateSystemInfo(clusterId);
        for (TBackendMeta backendMeta : backendMetas) {
            Backend backend = systemInfoService.getBackend(backendMeta.getBackend_id());
            if (backend == null) {
                backend = new Backend();
                backend.setId(backendMeta.getBackend_id());
                backend.setHost(backendMeta.getHost());
                backend.setBePort(backendMeta.getBe_port());
                backend.setHttpPort(backendMeta.getHttp_port());
                backend.setBrpcPort(backendMeta.getRpc_port());
                backend.setAlive(backendMeta.isAlive());
                backend.setBackendState(BackendState.values()[backendMeta.getState()]);
                systemInfoService.addBackend(backend);
            } else {
                backend.setId(backendMeta.getBackend_id());
                backend.setBePort(backendMeta.getBe_port());
                backend.setHttpPort(backendMeta.getHttp_port());
                backend.setBrpcPort(backendMeta.getRpc_port());
                backend.setAlive(backendMeta.isAlive());
                backend.setBackendState(BackendState.values()[backendMeta.getState()]);
            }
        }

        lastExternalMeta = meta;
        LOG.info("TableMetaSyncer finish meta update. partition build cost: {}ms, " +
                        "index meta build cost: {}ms, schema rebuild cost: {}ms, " +
                        "tablet meta build cost: {}ms, total cost: {}ms",
                endOfPartitionBuild - start, endOfIndexMetaBuild - endOfPartitionBuild,
                endOfSchemaRebuild - endOfIndexMetaBuild, endOfTabletMetaBuild - endOfSchemaRebuild,
                System.currentTimeMillis() - start);
    }
}
