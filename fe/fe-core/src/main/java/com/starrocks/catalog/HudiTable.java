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

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hudi.HudiRemoteFileDesc;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TColumn;
import com.starrocks.thrift.THdfsPartition;
import com.starrocks.thrift.THdfsPartitionLocation;
import com.starrocks.thrift.THudiTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.getResourceMappingCatalogName;
import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog;

/**
 * Currently, we depend on Hive metastore to obtain table/partition path and statistics.
 * This logic should be decoupled from metastore when the related interfaces are ready.
 */
public class HudiTable extends Table {
    private static final Logger LOG = LogManager.getLogger(HudiTable.class);

    private static final String JSON_KEY_HUDI_DB = "database";
    private static final String JSON_KEY_HUDI_TABLE = "table";
    private static final String JSON_KEY_RESOURCE_NAME = "resource";
    private static final String JSON_KEY_PART_COLUMN_NAMES = "partColumnNames";
    private static final String JSON_KEY_DATA_COLUMN_NAMES = "dataColumnNames";
    private static final String JSON_KEY_HUDI_PROPERTIES = "hudiProperties";

    public static final String HUDI_TABLE_TYPE = "hudi.table.type";
    public static final String HUDI_HMS_TABLE_TYPE = "hudi.hms.table.type";
    public static final String HUDI_BASE_PATH = "hudi.table.base.path";
    public static final String HUDI_TABLE_SERDE_LIB = "hudi.table.serde.lib";
    public static final String HUDI_TABLE_INPUT_FOAMT = "hudi.table.input.format";
    public static final String HUDI_TABLE_COLUMN_NAMES = "hudi.table.column.names";
    public static final String HUDI_TABLE_COLUMN_TYPES = "hudi.table.column.types";

    public static final String COW_INPUT_FORMAT = "org.apache.hudi.hadoop.HoodieParquetInputFormat";
    public static final String COW_INPUT_FORMAT_LEGACY = "com.uber.hoodie.hadoop.HoodieInputFormat";
    public static final String MOR_RO_INPUT_FORMAT = "org.apache.hudi.hadoop.HoodieParquetInputFormat";
    public static final String MOR_RO_INPUT_FORMAT_LEGACY = "com.uber.hoodie.hadoop.HoodieInputFormat";
    public static final String MOR_RT_INPUT_FORMAT = "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat";
    public static final String MOR_RT_INPUT_FORMAT_LEGACY = "com.uber.hoodie.hadoop.realtime.HoodieRealtimeInputFormat";

    public enum HudiTableType {
        COW, MOR, UNKNOWN
    }

    private String catalogName;
    @SerializedName(value = "dn")
    private String hiveDbName;
    @SerializedName(value = "tn")
    private String hiveTableName;
    @SerializedName(value = "rn")
    private String resourceName;
    @SerializedName(value = "pcn")
    private List<String> partColumnNames = Lists.newArrayList();
    // dataColumnNames stores all the non-partition columns of the hudi table,
    // consistent with the order defined in the hudi table
    @SerializedName(value = "dcn")
    private List<String> dataColumnNames = Lists.newArrayList();
    @SerializedName(value = "prop")
    private Map<String, String> hudiProperties = Maps.newHashMap();

    private HudiTableType tableType;

    public HudiTable() {
        super(TableType.HUDI);
    }

    public HudiTable(long id, String name, String catalogName, String hiveDbName, String hiveTableName,
                     String resourceName, String comment, List<Column> schema, List<String> dataColumnNames,
                     List<String> partColumnNames, long createTime, Map<String, String> properties,
                     HudiTableType type) {
        super(id, name, TableType.HUDI, schema);
        this.catalogName = catalogName;
        this.hiveDbName = hiveDbName;
        this.hiveTableName = hiveTableName;
        this.resourceName = resourceName;
        this.dataColumnNames = dataColumnNames;
        this.partColumnNames = partColumnNames;
        this.createTime = createTime;
        this.hudiProperties = properties;
        this.comment = comment;
        this.tableType = type;
    }

    @Override
    public String getCatalogDBName() {
        return hiveDbName;
    }

    @Override
    public String getResourceName() {
        return resourceName;
    }

    @Override
    public String getCatalogName() {
        return catalogName == null ? getResourceMappingCatalogName(resourceName, "hudi") : catalogName;
    }

    public HoodieTableType getTableType() {
        return HoodieTableType.valueOf(hudiProperties.get(HUDI_TABLE_TYPE));
    }

    @Override
    public String getTableLocation() {
        return hudiProperties.get(HUDI_BASE_PATH);
    }

    public String getHudiInputFormat() {
        return hudiProperties.get(HUDI_TABLE_INPUT_FOAMT);
    }

    @Override
    public String getCatalogTableName() {
        return hiveTableName;
    }

    @Override
    public String getUUID() {
        if (CatalogMgr.isExternalCatalog(catalogName)) {
            return String.join(".", catalogName, hiveDbName, hiveTableName, Long.toString(createTime));
        } else {
            return Long.toString(id);
        }
    }

    @Override
    public List<Column> getPartitionColumns() {
        return partColumnNames.stream()
                .map(name -> nameToColumn.get(name))
                .collect(Collectors.toList());
    }

    @Override
    public List<String> getPartitionColumnNames() {
        return partColumnNames;
    }

    @Override
    public List<String> getDataColumnNames() {
        return dataColumnNames;
    }

    @Override
    public boolean isUnPartitioned() {
        return partColumnNames.size() == 0;
    }

    @Override
    public String getTableIdentifier() {
        return Joiner.on(":").join(name, createTime);
    }

    public static HudiTableType fromInputFormat(String inputFormat) {
        switch (inputFormat) {
            case COW_INPUT_FORMAT:
            case COW_INPUT_FORMAT_LEGACY:
                return HudiTableType.COW;
            case MOR_RT_INPUT_FORMAT:
            case MOR_RT_INPUT_FORMAT_LEGACY:
                return HudiTableType.MOR;
            default:
                return HudiTableType.UNKNOWN;
        }
    }

    public Map<String, String> getProperties() {
        return hudiProperties;
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        Preconditions.checkNotNull(partitions);

        THudiTable tHudiTable = new THudiTable();
        tHudiTable.setLocation(getTableLocation());

        // columns and partition columns
        Set<String> partitionColumnNames = Sets.newHashSet();
        List<TColumn> tPartitionColumns = Lists.newArrayList();
        List<TColumn> tColumns = Lists.newArrayList();

        for (Column column : getPartitionColumns()) {
            tPartitionColumns.add(column.toThrift());
            partitionColumnNames.add(column.getName());
        }
        for (Column column : getBaseSchema()) {
            if (partitionColumnNames.contains(column.getName())) {
                continue;
            }
            tColumns.add(column.toThrift());
        }
        tHudiTable.setColumns(tColumns);
        if (!tPartitionColumns.isEmpty()) {
            tHudiTable.setPartition_columns(tPartitionColumns);
        }

        // partitions
        List<PartitionKey> partitionKeys = Lists.newArrayList();
        for (DescriptorTable.ReferencedPartitionInfo partition : partitions) {
            partitionKeys.add(partition.getKey());
        }
        List<RemoteFileInfo> hudiPartitions;
        try {
            GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder().setPartitionKeys(partitionKeys).build();
            hudiPartitions = GlobalStateMgr.getCurrentState().getMetadataMgr()
                    .getRemoteFiles(this, params);
        } catch (StarRocksConnectorException e) {
            LOG.warn("Table {} gets partition info failed.", name, e);
            return null;
        }

        HoodieInstant lastInstant = null;
        for (int i = 0; i < hudiPartitions.size(); i++) {
            DescriptorTable.ReferencedPartitionInfo info = partitions.get(i);
            PartitionKey key = info.getKey();
            long partitionId = info.getId();

            THdfsPartition tPartition = new THdfsPartition();
            tPartition.setFile_format(hudiPartitions.get(i).getFormat().toThrift());

            List<LiteralExpr> keys = key.getKeys();
            tPartition.setPartition_key_exprs(keys.stream().map(Expr::treeToThrift).collect(Collectors.toList()));

            THdfsPartitionLocation tPartitionLocation = new THdfsPartitionLocation();
            tPartitionLocation.setPrefix_index(-1);
            tPartitionLocation.setSuffix(hudiPartitions.get(i).getFullPath());
            tPartition.setLocation(tPartitionLocation);
            tHudiTable.putToPartitions(partitionId, tPartition);

            // update lastInstant according to remote file info.
            {
                RemoteFileInfo fileInfo = hudiPartitions.get(i);
                for (RemoteFileDesc desc : fileInfo.getFiles()) {
                    HudiRemoteFileDesc hudiDesc = (HudiRemoteFileDesc) desc;
                    HoodieInstant instant = hudiDesc.getHudiInstant();
                    if (instant == null) {
                        continue;
                    }
                    if (lastInstant == null || instant.compareTo(lastInstant) > 0) {
                        lastInstant = instant;
                    }
                }
            }
        }

        Configuration conf = new Configuration();
        if (!Strings.isNullOrEmpty(catalogName)) {
            GlobalStateMgr.getCurrentState()
                    .getConnectorMgr()
                    .getConnector(catalogName)
                    .getMetadata()
                    .getCloudConfiguration()
                    .applyToConfiguration(conf);
        }

        if (tableType == HudiTableType.MOR) {
            tHudiTable.setInstant_time(lastInstant == null ? "" : lastInstant.getTimestamp());
        }

        tHudiTable.setHive_column_names(hudiProperties.get(HUDI_TABLE_COLUMN_NAMES));
        tHudiTable.setHive_column_types(hudiProperties.get(HUDI_TABLE_COLUMN_TYPES));
        tHudiTable.setInput_format(hudiProperties.get(HUDI_TABLE_INPUT_FOAMT));
        tHudiTable.setSerde_lib(hudiProperties.get(HUDI_TABLE_SERDE_LIB));
        tHudiTable.setTime_zone(TimeUtils.getSessionTimeZone());

        TTableDescriptor tTableDescriptor =
                new TTableDescriptor(id, TTableType.HUDI_TABLE, fullSchema.size(), 0, hiveTableName, hiveDbName);
        tTableDescriptor.setHudiTable(tHudiTable);
        return tTableDescriptor;
    }

    @Override
    public void onDrop(Database db, boolean force, boolean replay) {
        if (isResourceMappingCatalog(getCatalogName())) {
            GlobalStateMgr.getCurrentState().getMetadataMgr().dropTable(getCatalogName(), db.getFullName(), name);
        }
    }

    @Override
    public boolean isSupported() {
        return true;
    }

    @Override
    public boolean isHMSExternalTable() {
        return hudiProperties.get(HUDI_HMS_TABLE_TYPE).equals("EXTERNAL_TABLE");
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HudiTable{");
        sb.append("resourceName='").append(resourceName).append('\'');
        sb.append(", catalogName='").append(catalogName).append('\'');
        sb.append(", hiveDbName='").append(hiveDbName).append('\'');
        sb.append(", hiveTableName='").append(hiveTableName).append('\'');
        sb.append(", id=").append(id);
        sb.append(", name='").append(name).append('\'');
        sb.append(", type=").append(type);
        sb.append(", createTime=").append(createTime);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getCatalogName(), hiveDbName, getTableIdentifier());
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof HudiTable)) {
            return false;
        }

        HudiTable otherTable = (HudiTable) other;
        String catalogName = getCatalogName();
        String tableIdentifier = getTableIdentifier();
        return Objects.equal(catalogName, otherTable.getCatalogName()) &&
                Objects.equal(hiveDbName, otherTable.hiveDbName) &&
                Objects.equal(tableIdentifier, otherTable.getTableIdentifier());
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private long id;
        private String tableName;
        private String catalogName;
        private String hiveDbName;
        private String hiveTableName;
        private String resourceName;

        private String comment;
        private long createTime;
        private List<Column> fullSchema;
        private List<String> partitionColNames = Lists.newArrayList();
        private List<String> dataColNames = Lists.newArrayList();
        private Map<String, String> hudiProperties = Maps.newHashMap();

        private HudiTableType tableType;

        public Builder() {
        }

        public Builder setId(long id) {
            this.id = id;
            return this;
        }

        public Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder setCatalogName(String catalogName) {
            this.catalogName = catalogName;
            return this;
        }

        public Builder setHiveDbName(String hiveDbName) {
            this.hiveDbName = hiveDbName;
            return this;
        }

        public Builder setHiveTableName(String hiveTableName) {
            this.hiveTableName = hiveTableName;
            return this;
        }

        public Builder setResourceName(String resourceName) {
            this.resourceName = resourceName;
            return this;
        }

        public Builder setComment(String comment) {
            this.comment = comment;
            return this;
        }

        public Builder setCreateTime(long createTime) {
            this.createTime = createTime;
            return this;
        }

        public Builder setFullSchema(List<Column> fullSchema) {
            this.fullSchema = fullSchema;
            return this;
        }

        public Builder setPartitionColNames(List<String> partitionColNames) {
            this.partitionColNames = partitionColNames;
            return this;
        }

        public Builder setDataColNames(List<String> dataColNames) {
            this.dataColNames = dataColNames;
            return this;
        }

        public Builder setHudiProperties(Map<String, String> hudiProperties) {
            this.hudiProperties = hudiProperties;
            return this;
        }

        public Builder setTableType(HudiTableType tableType) {
            this.tableType = tableType;
            return this;
        }

        public HudiTable build() {
            return new HudiTable(id, tableName, catalogName, hiveDbName, hiveTableName, resourceName, comment,
                    fullSchema, dataColNames, partitionColNames, createTime, hudiProperties, tableType);
        }
    }
}
