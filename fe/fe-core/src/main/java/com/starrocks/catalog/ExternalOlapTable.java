// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.catalog;

import com.google.common.base.Strings;
import com.starrocks.analysis.DistributionDesc;
import com.starrocks.analysis.HashDistributionDesc;
import com.starrocks.analysis.IndexDef;
import com.starrocks.catalog.DistributionInfo.DistributionInfoType;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.Replica.ReplicaState;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.system.Backend;
import com.starrocks.system.Backend.BackendState;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TBackendMeta;
import com.starrocks.thrift.TColumnMeta;
import com.starrocks.thrift.THashDistributionInfo;
import com.starrocks.thrift.TIndexInfo;
import com.starrocks.thrift.TIndexMeta;
import com.starrocks.thrift.TPartitionInfo;
import com.starrocks.thrift.TPartitionMeta;
import com.starrocks.thrift.TReplicaMeta;
import com.starrocks.thrift.TTableMeta;
import com.starrocks.thrift.TTabletMeta;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ExternalOlapTable extends OlapTable {
    private static final Logger LOG = LogManager.getLogger(ExternalOlapTable.class);

    public class ExternalTableInfo {
        // remote doris cluster fe addr
        private String host;
        private int port;
        // access credential
        private String user;
        private String password;

        // source table info
        private String dbName;
        private String tableName;

        public ExternalTableInfo() {
            this.host = "";
            this.port = 0;
            this.user = "";
            this.password = "";
            this.dbName = "";
            this.tableName = "";
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

        public void write(DataOutput out) throws IOException {
            Text.writeString(out, host);
            out.writeInt(port);
            Text.writeString(out, user);
            Text.writeString(out, password);
            Text.writeString(out, dbName);
            Text.writeString(out, tableName);
        }

        public void read(DataInput in) throws IOException {
            host = Text.readString(in);
            port = in.readInt();
            user = Text.readString(in);
            password = Text.readString(in);
            dbName = Text.readString(in);
            tableName = Text.readString(in);
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
    private ExternalTableInfo externalTableInfo;

    public ExternalOlapTable() {
        super();
        setType(TableType.OLAP_EXTERNAL);
        dbId = -1;
        lastExternalMeta = null;
        externalTableInfo = null;
    }

    public ExternalOlapTable(long tableId, String tableName, List<Column> baseSchema, KeysType keysType,
                             PartitionInfo partitionInfo, DistributionInfo defaultDistributionInfo,
                             TableIndexes indexes, Map<String, String> properties)
        throws DdlException {
        super(tableId, tableName, baseSchema, keysType, partitionInfo, defaultDistributionInfo, indexes);
        setType(TableType.OLAP_EXTERNAL);
        dbId = -1;
        lastExternalMeta = null;

        externalTableInfo = new ExternalTableInfo();
        externalTableInfo.parseFromProperties(properties);
    }

    public long getDbId() {
        return dbId;
    }

    public ExternalTableInfo getExternalInfo() {
        return externalTableInfo;
    }

    @Override
    public void onCreate() {
        Catalog.getCurrentCatalog().getStarRocksRepository().registerTable(this);
    }

    @Override
    public void onDrop() {
        Catalog.getCurrentCatalog().getStarRocksRepository().deRegisterTable(this);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, type.name());
        out.writeLong(id);
        Text.writeString(out, name);
        externalTableInfo.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readLong();
        this.name = Text.readString(in);

        externalTableInfo = new ExternalTableInfo();
        externalTableInfo.read(in);
    }

    public void updateMeta(String dbName, TTableMeta meta, List<TBackendMeta> backendMetas) throws DdlException {
        // no meta changed since last time, do nothing
        if (lastExternalMeta != null && meta.compareTo(lastExternalMeta) == 0) {
            LOG.info("no meta changed since last time, do nothing");
            return;
        }

        clusterId = meta.getCluster_id();
        dbId = meta.getDb_id();

        String fullDbName = ClusterNamespace.getFullName(SystemInfoService.DEFAULT_CLUSTER, dbName);
        Database db = Catalog.getCurrentCatalog().getDb(fullDbName);
        if (db == null) {
            throw new DdlException("database " + dbName + " does not exist");
        }
        db.writeLock();

        try {
            lastExternalMeta = meta;

            state = OlapTableState.valueOf(meta.getState());
            baseIndexId = meta.getBase_index_id();
            colocateGroup = meta.getColocate_group();
            bfFpp = meta.getBloomfilter_fpp();

            keysType = KeysType.valueOf(meta.getKey_type());
            tableProperty = new TableProperty(meta.getProperties());
            tableProperty.buildReplicationNum();
            tableProperty.buildStorageFormat();
            tableProperty.buildInMemory();
            tableProperty.buildDynamicProperty();

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
            partitionInfo = new PartitionInfo(PartitionType.valueOf(tPartitionInfo.getType()));
            for (Map.Entry<Long, Short> entry : tPartitionInfo.getReplica_num_map().entrySet()) {
                partitionInfo.setReplicationNum(entry.getKey(), entry.getValue());
            }
            for (Map.Entry<Long, Boolean> entry : tPartitionInfo.getIn_memory_map().entrySet()) {
                partitionInfo.setIsInMemory(entry.getKey(), entry.getValue());
            }

            indexIdToMeta.clear();
            indexNameToId.clear();

            for (TIndexMeta indexMeta : meta.getIndexes()) {
                List<Column> columns = new ArrayList();
                for (TColumnMeta columnMeta : indexMeta.getSchema_meta().getColumns()) {
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

            rebuildFullSchema();

            idToPartition.clear();
            nameToPartition.clear();

            DistributionInfoType type = DistributionInfoType.valueOf(meta.getDistribution_desc().getDistribution_type());
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
                partition.setNextVersionHash(partitionMeta.getNext_version_hash(), partitionMeta.getCommit_version_hash());
                partition.updateVisibleVersionAndVersionHash(partitionMeta.getVisible_version(),
                                                            partitionMeta.getVisible_version_hash(),
                                                            partitionMeta.getVisible_time());
                for (TIndexMeta indexMeta : meta.getIndexes()) {
                    MaterializedIndex index = new MaterializedIndex(indexMeta.getIndex_id(),
                                                                    IndexState.valueOf(indexMeta.getIndex_state()));
                    index.setRowCount(indexMeta.getRow_count());
                    index.setRollupIndexInfo(indexMeta.getRollup_index_id(), indexMeta.getRollup_finished_version());
                    for (TTabletMeta tTabletMeta : indexMeta.getTablets()) {
                        Tablet tablet = new Tablet(tTabletMeta.getTablet_id());
                        tablet.setCheckedVersion(tTabletMeta.getChecked_version(), tTabletMeta.getChecked_version_hash());
                        tablet.setIsConsistent(tTabletMeta.isConsistent());
                        for (TReplicaMeta replicaMeta : tTabletMeta.getReplicas()) {
                            Replica replica = new Replica(replicaMeta.getReplica_id(), replicaMeta.getBackend_id(),
                                                        replicaMeta.getVersion(), replicaMeta.getVersion_hash(),
                                                        replicaMeta.getSchema_hash(), replicaMeta.getData_size(),
                                                        replicaMeta.getRow_count(), ReplicaState.valueOf(replicaMeta.getState()),
                                                        replicaMeta.getLast_failed_version(),
                                                        replicaMeta.getLast_failed_version_hash(),
                                                        replicaMeta.getLast_success_version(),
                                                        replicaMeta.getLast_success_version_hash());
                            replica.setLastFailedTime(replicaMeta.getLast_failed_time());
                            // forbidden repair for external table
                            replica.setNeedFurtherRepair(false);
                            tablet.addReplica(replica, true);
                        }
                        TabletMeta tabletMeta = new TabletMeta(tTabletMeta.getDb_id(), tTabletMeta.getTable_id(),
                                                            tTabletMeta.getPartition_id(), tTabletMeta.getIndex_id(),
                                                            tTabletMeta.getOld_schema_hash(), tTabletMeta.getStorage_medium());
                        index.addTablet(tablet, tabletMeta);
                    }
                    if (index.getId() != baseIndexId) {
                        partition.createRollupIndex(index);
                    } else {
                        partition.setBaseIndex(index);
                    }
                }
                addPartition(partition);
            }

            SystemInfoService systemInfoService = Catalog.getCurrentCatalog().getOrCreateSystemInfo(clusterId);
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
        } finally {
            db.writeUnlock();
        }
    }
}
