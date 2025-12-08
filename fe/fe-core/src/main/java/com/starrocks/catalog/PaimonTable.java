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

import com.aliyun.datalake.catalog.CatalogClient;
import com.aliyun.datalake.common.DlfDataToken;
import com.aliyun.datalake.common.impl.Base64Util;
import com.aliyun.datalake.core.DlfAuthContext;
import com.aliyun.datalake.credential.SimpleStsCredentialsProvider;
import com.aliyun.datalake.paimon.fs.DlfPaimonFileIO;
import com.aliyun.datalake.paimon.table.DlfPaimonTable;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.common.util.DlfUtil;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.CatalogConnectorMetadata;
import com.starrocks.connector.paimon.PaimonMetadata;
import com.starrocks.connector.paimon.PaimonUtils;
import com.starrocks.planner.PaimonScanNode;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TColumn;
import com.starrocks.thrift.THdfsPartition;
import com.starrocks.thrift.THdfsPartitionLocation;
import com.starrocks.thrift.TPaimonTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.table.DataTable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.ALIYUN_OSS_ACCESS_KEY;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.ALIYUN_OSS_SECRET_KEY;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.ALIYUN_OSS_STS_TOKEN;

public class PaimonTable extends Table {
    private static final Logger LOG = LogManager.getLogger(PaimonTable.class);
    public static final String FILE_FORMAT = "file.format";

    private static final Set<String> NATIVE_SUPPORT_FILE_FORMAT = ImmutableSet.of("parquet", "orc", "aliorc");
    private static final Set<String> SUPPORT_INSERT_FORMAT = ImmutableSet.of("parquet", "orc", "avro", "aliorc");

    private String catalogName;
    private String databaseName;
    private String tableName;
    private org.apache.paimon.table.Table paimonNativeTable;
    private String uuid;
    private List<String> partColumnNames;
    private List<String> paimonFieldNames;
    private Map<String, String> properties;
    private final AtomicLong partitionIdGen = new AtomicLong(0L);

    public PaimonTable() {
        super(TableType.PAIMON);
    }

    public PaimonTable(String catalogName, String dbName, String tblName, List<Column> schema,
                       org.apache.paimon.table.Table paimonNativeTable) {
        super(CONNECTOR_ID_GENERATOR.getNextId().asInt(), tblName, TableType.PAIMON, schema);
        this.catalogName = catalogName;
        this.databaseName = dbName;
        this.tableName = tblName;
        this.paimonNativeTable = paimonNativeTable;
        this.partColumnNames = paimonNativeTable.partitionKeys();
        this.paimonFieldNames = paimonNativeTable.rowType().getFieldNames();
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public String getCatalogDBName() {
        return databaseName;
    }

    @Override
    public String getCatalogTableName() {
        return tableName;
    }

    public org.apache.paimon.table.Table getNativeTable() {
        return paimonNativeTable;
    }

    // For refresh table only
    public void setPaimonNativeTable(org.apache.paimon.table.Table paimonNativeTable) {
        this.paimonNativeTable = paimonNativeTable;
    }

    @Override
    public String getUUID() {
        if (Strings.isNullOrEmpty(this.uuid)) {
            this.uuid = String.join(".", catalogName, databaseName, tableName, paimonNativeTable.uuid().replace(".", "_"));
        }
        return this.uuid;
    }

    @Override
    public String getTableLocation() {
        if (paimonNativeTable instanceof DataTable) {
            return ((DataTable) paimonNativeTable).location().toString();
        } else {
            return paimonNativeTable.name().toString();
        }
    }

    @Override
    public Map<String, String> getProperties() {
        if (properties == null) {
            this.properties = new HashMap<>();
            if (!paimonNativeTable.primaryKeys().isEmpty()) {
                properties.put("primary-key", String.join(",", paimonNativeTable.primaryKeys()));
            }
            this.properties.putAll(paimonNativeTable.options());
        }
        return properties;
    }

    @Override
    public List<String> getPartitionColumnNames() {
        return partColumnNames;
    }

    @Override
    public List<Column> getPartitionColumns() {
        List<Column> partitionColumns = new ArrayList<>();
        if (!partColumnNames.isEmpty()) {
            partitionColumns = partColumnNames.stream().map(this::getColumn)
                    .collect(Collectors.toList());
        }
        return partitionColumns;
    }

    public List<Integer> partitionColumnIndexes() {
        List<Column> partitionCols = getPartitionColumns();
        return partitionCols.stream().map(col -> fullSchema.indexOf(col)).collect(Collectors.toList());
    }

    public List<String> getFieldNames() {
        return paimonFieldNames;
    }

    @Override
    public boolean isUnPartitioned() {
        return partColumnNames.isEmpty();
    }

    @Override
    public boolean isSupported() {
        return true;
    }

    public List<String> getBucketKey() {
        String bucketKey = paimonNativeTable.options().get("bucket-key");
        if (bucketKey == null) {
            return new ArrayList<>();
        } else {
            return Arrays.stream(bucketKey.split(",")).map(String::trim).collect(Collectors.toList());
        }
    }

    public int getBucketNum() {
        String bucketNum = paimonNativeTable.options().get("bucket");
        if (Strings.isNullOrEmpty(bucketNum)) {
            return CoreOptions.BUCKET.defaultValue();
        } else {
            return Integer.parseInt(bucketNum);
        }
    }

    public boolean supportInsert() {
        String format = paimonNativeTable.options().get(FILE_FORMAT);
        if (Strings.isNullOrEmpty(format)) {
            format = CoreOptions.FILE_FORMAT.defaultValue();
        }
        return SUPPORT_INSERT_FORMAT.contains(format.toLowerCase());
    }

    public boolean supportNativeWriter() {
        String format = paimonNativeTable.options().get(FILE_FORMAT);
        if (Strings.isNullOrEmpty(format)) {
            format = CoreOptions.FILE_FORMAT.defaultValue();
        }
        return NATIVE_SUPPORT_FILE_FORMAT.contains(format.toLowerCase());
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        TPaimonTable tPaimonTable = new TPaimonTable();

        FileIO paimonFileIO = paimonNativeTable.fileIO();
        if (paimonFileIO instanceof DlfPaimonFileIO) {
            try {
                CatalogConnector connector = GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalogName);
                PaimonMetadata paimonMetadata = (PaimonMetadata) ((CatalogConnectorMetadata) connector.getMetadata())
                        .metadataOfDb(databaseName);
                paimonMetadata.refreshDlfDataToken(databaseName, tableName);
            } catch (Exception e) {
                LOG.error("Failed to refresh DLF data token of table {}.{}", databaseName, tableName, e);
            }
            CatalogClient client = DlfUtil.getFieldValue(paimonFileIO, "client");
            DlfAuthContext dlfAuthContext = DlfUtil.getFieldValue(paimonFileIO, "dlfAuthContext");
            Map<String, String> options = ((DlfPaimonFileIO) paimonFileIO).dlsFileSystemOptions(false);
            Properties properties = new Properties();
            for (Map.Entry<String, String> entry : options.entrySet()) {
                properties.setProperty(entry.getKey(), entry.getValue());
            }
            String dlfAuthConfigPrefix = DlfUtil.getFieldValue(paimonFileIO, "dlfAuthConfigPrefix");
            org.apache.paimon.fs.Path basePath = DlfUtil.getFieldValue(paimonFileIO, "basePath");

            String dataTokenPath = DlfUtil.getDataTokenPath(getTableLocation());
            dataTokenPath = "/secret/DLF/data/" + Base64Util.encodeBase64WithoutPadding(dataTokenPath);
            File dataTokenFile = new File(dataTokenPath);
            Map<String, String> dataToken = new HashMap<>();
            if (dataTokenFile.exists()) {
                try {
                    dataToken = DlfUtil.setDataToken(dataTokenFile);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                SimpleStsCredentialsProvider<DlfDataToken> provider = new SimpleStsCredentialsProvider<>();
                // Ignore fs.dlf since there is no longer this kind of scheme
                properties.setProperty("fs.oss.credentials.provider",
                        "com.aliyun.jindodata.oss.auth.CommonCredentialsProvider");
                properties.remove("aliyun.oss.provider.url");
                properties.put("fs.oss.accessKeyId", dataToken.get(ALIYUN_OSS_ACCESS_KEY));
                properties.put("fs.oss.accessKeySecret", dataToken.get(ALIYUN_OSS_SECRET_KEY));
                properties.put("fs.oss.securityToken", dataToken.get(ALIYUN_OSS_STS_TOKEN));

                properties.setProperty("fs.dls.credentials.provider",
                        "com.aliyun.jindodata.dls.auth.CommonCredentialsProvider");
                properties.remove("aliyun.dls.provider.url");
                properties.put("fs.dls.accessKeyId", dataToken.get(ALIYUN_OSS_ACCESS_KEY));
                properties.put("fs.dls.accessKeySecret", dataToken.get(ALIYUN_OSS_SECRET_KEY));
                properties.put("fs.dls.securityToken", dataToken.get(ALIYUN_OSS_STS_TOKEN));
                // DLF 2.3 version updated
                properties.put("dlf.tokenCache.data.accessKeyId", dataToken.get(ALIYUN_OSS_ACCESS_KEY));
                properties.put("dlf.tokenCache.data.accessKeySecret", dataToken.get(ALIYUN_OSS_SECRET_KEY));
                properties.put("dlf.tokenCache.data.securityToken", dataToken.get(ALIYUN_OSS_STS_TOKEN));
                provider.init(properties, "", DlfDataToken.class);
                paimonFileIO = new DlfPaimonFileIO(client,
                        provider,
                        dlfAuthContext,
                        properties,
                        dlfAuthConfigPrefix,
                        basePath);
                paimonNativeTable = new DlfPaimonTable(paimonFileIO,
                        ((DlfPaimonTable) paimonNativeTable).location(),
                        ((DlfPaimonTable) paimonNativeTable).schema(),
                        DlfUtil.getFieldValue(paimonNativeTable, "catalogEnvironment"),
                        DlfUtil.getFieldValue(paimonNativeTable, "checker"));
            } else {
                LOG.warn("Cannot find data token file " + dataTokenPath);
            }
        }
        Set<String> partitionColumnNames = Sets.newHashSet();
        List<TColumn> tPartitionColumns = Lists.newArrayList();
        for (Column column : getPartitionColumns()) {
            tPartitionColumns.add(column.toThrift());
            partitionColumnNames.add(column.getName());
        }
        if (!tPartitionColumns.isEmpty()) {
            tPaimonTable.setPartition_columns(tPartitionColumns);
        }

        for (DescriptorTable.ReferencedPartitionInfo info : partitions) {
            PartitionKey key = info.getKey();
            long partitionId = info.getId();

            THdfsPartition tPartition = new THdfsPartition();

            List<LiteralExpr> keys = key.getKeys();
            tPartition.setPartition_key_exprs(keys.stream().map(Expr::treeToThrift).collect(Collectors.toList()));

            THdfsPartitionLocation tPartitionLocation = new THdfsPartitionLocation();
            tPartitionLocation.setPrefix_index(-1);
            tPartitionLocation.setSuffix(info.getPath());
            tPartition.setLocation(tPartitionLocation);
            tPaimonTable.putToPartitions(partitionId, tPartition);
        }

        String encodedTable = PaimonScanNode.encodeObjectToString(paimonNativeTable);

        Map<String, String> originalOptions = new HashMap<>(paimonNativeTable.options());
        originalOptions.putIfAbsent(CoreOptions.FILE_FORMAT.key(), CoreOptions.FILE_FORMAT.defaultValue());
        originalOptions.putIfAbsent(CoreOptions.MANIFEST_FORMAT.key(), CoreOptions.MANIFEST_FORMAT.defaultValue());

        tPaimonTable.setPaimon_options(originalOptions);
        tPaimonTable.setPaimon_native_table(encodedTable);
        tPaimonTable.setTime_zone(TimeUtils.getSessionTimeZone());

        tPaimonTable.setPaimon_schema(PaimonUtils.getTPaimonSchema(this.paimonNativeTable.rowType()));

        tPaimonTable.setPrimary_keys(paimonNativeTable.primaryKeys());
        tPaimonTable.setPartition_keys(paimonNativeTable.partitionKeys());
        tPaimonTable.setBucket_num(getBucketNum());
        tPaimonTable.setBucket_keys(getBucketKey());
        
        TTableDescriptor tTableDescriptor = new TTableDescriptor(id, TTableType.PAIMON_TABLE,
                fullSchema.size(), 0, tableName, databaseName);
        tTableDescriptor.setPaimonTable(tPaimonTable);
        return tTableDescriptor;
    }

    @Override
    public String getTableIdentifier() {
        String uuid = getUUID();
        return Joiner.on(":").join(name, uuid == null ? "" : uuid);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PaimonTable that = (PaimonTable) o;
        return catalogName.equals(that.catalogName) &&
                databaseName.equals(that.databaseName) &&
                tableName.equals(that.tableName) &&
                Objects.equals(getTableIdentifier(), that.getTableIdentifier());
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogName, databaseName, tableName, getTableIdentifier());
    }

    public long nextPartitionId() {
        return partitionIdGen.getAndIncrement();
    }
}
