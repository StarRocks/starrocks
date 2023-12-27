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

package com.starrocks.connector.odps;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Partition;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Project;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.utils.StringUtils;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OdpsTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.aliyun.AliyunCloudConfiguration;
import com.starrocks.credential.aliyun.AliyunCloudCredential;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static com.google.common.cache.CacheLoader.asyncReloading;
import static java.util.concurrent.TimeUnit.HOURS;

public class OdpsMetadata implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(OdpsMetadata.class);
    public static final long NEVER_CACHE = 0;
    private final Odps odps;
    private final String catalogName;
    private final EnvironmentSettings settings;
    private final AliyunCloudCredential aliyunCloudCredential;
    private final OdpsProperties properties;

    private String catalogOwner;
    private LoadingCache<String, Set<String>> tableNameCache;
    private LoadingCache<OdpsTableName, OdpsTable> tableCache;
    private LoadingCache<OdpsTableName, List<Partition>> partitionCache;

    public OdpsMetadata(Odps odps, String catalogName, AliyunCloudCredential aliyunCloudCredential,
                        OdpsProperties properties) {
        this.odps = odps;
        this.catalogName = catalogName;
        this.aliyunCloudCredential = aliyunCloudCredential;
        this.properties = properties;
        EnvironmentSettings.Builder settingsBuilder =
                EnvironmentSettings.newBuilder().withServiceEndpoint(odps.getEndpoint())
                        .withCredentials(Credentials.newBuilder().withAccount(odps.getAccount()).build());
        if (!StringUtils.isNullOrEmpty(properties.get(OdpsProperties.TUNNEL_ENDPOINT))) {
            settingsBuilder.withTunnelEndpoint(properties.get(OdpsProperties.TUNNEL_ENDPOINT));
        }
        if (!StringUtils.isNullOrEmpty(properties.get(OdpsProperties.TUNNEL_QUOTA))) {
            settingsBuilder.withQuotaName(properties.get(OdpsProperties.TUNNEL_QUOTA));
        }
        settings = settingsBuilder.build();
        initMetaCache();
    }

    private void initMetaCache() {
        Executor executor = MoreExecutors.newDirectExecutorService();
        if (Boolean.parseBoolean(properties.get(OdpsProperties.ENABLE_TABLE_NAME_CACHE))) {
            tableNameCache =
                    newCacheBuilder(Long.parseLong(properties.get(OdpsProperties.TABLE_NAME_CACHE_EXPIRE_TIME)),
                            Long.parseLong(properties.get(OdpsProperties.PROJECT_CACHE_SIZE)))
                            .build(asyncReloading(CacheLoader.from(this::loadProjects), executor));
        } else {
            tableNameCache = newCacheBuilder(NEVER_CACHE, NEVER_CACHE)
                    .build(CacheLoader.from(this::loadProjects));
        }
        if (Boolean.parseBoolean(properties.get(OdpsProperties.ENABLE_TABLE_CACHE))) {
            tableCache = newCacheBuilder(Long.parseLong(properties.get(OdpsProperties.TABLE_CACHE_EXPIRE_TIME)),
                    Long.parseLong(properties.get(OdpsProperties.TABLE_CACHE_SIZE)))
                    .build(asyncReloading(CacheLoader.from(this::loadTable), executor));
        } else {
            tableCache = newCacheBuilder(NEVER_CACHE, NEVER_CACHE)
                    .build(asyncReloading(CacheLoader.from(this::loadTable), executor));
        }
        if (Boolean.parseBoolean(properties.get(OdpsProperties.ENABLE_PARTITION_CACHE))) {
            partitionCache = newCacheBuilder(Long.parseLong(properties.get(OdpsProperties.PARTITION_CACHE_EXPIRE_TIME)),
                    Long.parseLong(properties.get(OdpsProperties.PARTITION_CACHE_SIZE)))
                    .build(asyncReloading(CacheLoader.from(this::loadPartitions), executor));
        } else {
            partitionCache = newCacheBuilder(NEVER_CACHE, NEVER_CACHE)
                    .build(asyncReloading(CacheLoader.from(this::loadPartitions), executor));
        }
    }

    @Override
    public List<String> listDbNames() {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        try {
            if (StringUtils.isNullOrEmpty(catalogOwner)) {
                SecurityManager sm = odps.projects().get().getSecurityManager();
                String result = sm.runQuery("whoami", false);
                JsonObject js = JsonParser.parseString(result).getAsJsonObject();
                catalogOwner = js.get("DisplayName").getAsString();
            }
            Iterator<Project> iterator = odps.projects().iterator(catalogOwner);
            while (iterator.hasNext()) {
                Project project = iterator.next();
                builder.add(project.getName());
            }
        } catch (OdpsException e) {
            e.printStackTrace();
            throw new StarRocksConnectorException("fail to list project names", e);
        }
        ImmutableList<String> databases = builder.build();
        if (databases.isEmpty()) {
            return ImmutableList.of(odps.getDefaultProject());
        }
        return databases;
    }

    @Override
    public Database getDb(String name) {
        try {
            return new Database(ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt(), name);
        } catch (StarRocksConnectorException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public List<String> listTableNames(String dbName) {
        try {
            return new ArrayList<>(tableNameCache.get(dbName));
        } catch (ExecutionException e) {
            LOG.error("listTableNames error", e);
            return Collections.emptyList();
        }
    }

    private Set<String> loadProjects(String dbName) {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        Iterator<com.aliyun.odps.Table> iterator = odps.tables().iterator(dbName);
        while (iterator.hasNext()) {
            builder.add(iterator.next().getName());
        }
        return builder.build();
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        return get(tableCache, OdpsTableName.of(dbName, tblName));
    }

    private OdpsTable loadTable(OdpsTableName odpsTableName) {
        com.aliyun.odps.Table table = odps.tables().get(odpsTableName.getDatabaseName(), odpsTableName.getTableName());
        try {
            table.reload();
        } catch (OdpsException e) {
            return null;
        }
        return new OdpsTable(catalogName, table);
    }

    @Override
    public List<String> listPartitionNames(String databaseName, String tableName) {
        OdpsTableName odpsTableName = OdpsTableName.of(databaseName, tableName);
        // TODO: perhaps not good to support users to fetch whole tables?
        List<Partition> partitions = get(partitionCache, odpsTableName);
        if (partitions == null || partitions.isEmpty()) {
            return Collections.emptyList();
        }
        return partitions.stream().map(Partition::getPartitionSpec)
                .map(p -> p.toString(false, true)).collect(
                        Collectors.toList());
    }

    @Override
    public List<String> listPartitionNamesByValue(String databaseName, String tableName,
                                                  List<Optional<String>> partitionValues) {
        List<Partition> partitions = get(partitionCache, OdpsTableName.of(databaseName, tableName));
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        if (partitions == null || partitions.isEmpty()) {
            return builder.build();
        }
        List<PartitionSpec> partitionSpecs =
                partitions.stream().map(Partition::getPartitionSpec).collect(Collectors.toList());
        List<String> keys = new ArrayList<>(partitionSpecs.get(0).keys());
        for (PartitionSpec partitionSpec : partitionSpecs) {
            boolean present = true;
            for (int index = 0; index < keys.size(); index++) {
                String value = keys.get(index);
                if (partitionValues.get(index).isPresent() && partitionSpec.get(value) != null) {
                    if (!partitionSpec.get(value).equals(partitionValues.get(index).get())) {
                        present = false;
                        break;
                    }
                }
            }
            if (present) {
                builder.add(partitionSpec.toString(false, true));
            }
        }
        return builder.build();
    }

    @Override
    public Statistics getTableStatistics(OptimizerContext session,
                                         Table table,
                                         Map<ColumnRefOperator, Column> columns,
                                         List<PartitionKey> partitionKeys,
                                         ScalarOperator predicate,
                                         long limit) {
        Statistics.Builder builder = Statistics.builder();
        for (ColumnRefOperator columnRefOperator : columns.keySet()) {
            builder.addColumnStatistic(columnRefOperator, ColumnStatistic.unknown());
        }
        // cause we don't know the real schema in file，just use the default Row Count now
        builder.setOutputRowCount(1);
        return builder.build();
    }

    private List<Partition> loadPartitions(OdpsTableName odpsTableName) {
        com.aliyun.odps.Table odpsTable =
                odps.tables().get(odpsTableName.getDatabaseName(), odpsTableName.getTableName());
        return odpsTable.getPartitions();
    }

    @Override
    public List<PartitionInfo> getPartitions(Table table, List<String> partitionNames) {
        if (partitionNames == null || partitionNames.isEmpty()) {
            return Collections.emptyList();
        }
        OdpsTable odpsTable = (OdpsTable) table;
        List<Partition> partitions = get(partitionCache,
                OdpsTableName.of(odpsTable.getDbName(), odpsTable.getTableName()));
        if (partitions == null || partitions.isEmpty()) {
            return Collections.emptyList();
        }
        Set<String> filter = new HashSet<>(partitionNames);
        return partitions.stream()
                .filter(partition -> filter.contains(partition.getPartitionSpec().toString(false, true)))
                .map(OdpsPartition::new).collect(Collectors.toList());
    }

    @Override
    public void refreshTable(String srDbName, Table table, List<String> partitionNames, boolean onlyCachedPartitions) {
        OdpsTableName odpsTableName = OdpsTableName.of(srDbName, table.getName());
        tableCache.invalidate(odpsTableName);
        get(tableCache, odpsTableName);
        if (!table.isUnPartitioned()) {
            partitionCache.invalidate(odpsTableName);
            get(partitionCache, odpsTableName);
        }
    }

    @Override
    public CloudConfiguration getCloudConfiguration() {
        AliyunCloudConfiguration configuration = new AliyunCloudConfiguration(aliyunCloudCredential);
        configuration.loadCommonFields(new HashMap<>(0));
        return configuration;
    }

    private static CacheBuilder<Object, Object> newCacheBuilder(long expiresAfterWriteSec, long maximumSize) {
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        if (expiresAfterWriteSec >= 0) {
            cacheBuilder.expireAfterWrite(expiresAfterWriteSec, HOURS);
        }
        cacheBuilder.maximumSize(maximumSize);
        return cacheBuilder;
    }

    private static <K, V> V get(LoadingCache<K, V> cache, K key) {
        try {
            return cache.get(key);
        } catch (Exception e) {
            return null;
        }
    }
}
