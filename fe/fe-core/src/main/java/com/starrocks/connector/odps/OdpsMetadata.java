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
import com.aliyun.odps.Partition;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.SplitOptions;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.TableReadSessionBuilder;
import com.aliyun.odps.table.read.split.InputSplitAssigner;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OdpsTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.aliyun.AliyunCloudConfiguration;
import com.starrocks.credential.aliyun.AliyunCloudCredential;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static com.starrocks.connector.PartitionUtil.toHivePartitionName;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class OdpsMetadata implements ConnectorMetadata {

    private static final Logger LOG = LogManager.getLogger(OdpsMetadata.class);
    private static final long DEFAULT_EXPIRES_TIME = 24;
    private static final long DEFAULT_REFRESH_TIME = 0;
    private static final long DEFAULT_CACHE_SIZE = 1000;

    private final Odps odps;
    private final String catalogName;
    private final EnvironmentSettings settings;
    private final AliyunCloudCredential aliyunCloudCredential;

    private final LoadingCache<String, Set<String>> tableNamesCache;
    private final LoadingCache<OdpsTableName, OdpsTable> tableCache;
    private final LoadingCache<OdpsTableName, List<PartitionSpec>> partitionCache;

    public OdpsMetadata(Odps odps, String catalogName, AliyunCloudCredential aliyunCloudCredential) {
        this.odps = odps;
        this.catalogName = catalogName;
        this.aliyunCloudCredential = aliyunCloudCredential;
        settings = EnvironmentSettings.newBuilder().withServiceEndpoint(odps.getEndpoint())
                .withCredentials(Credentials.newBuilder().withAccount(odps.getAccount()).build()).build();
        Executor executor = MoreExecutors.newDirectExecutorService();
        // TODO: enable user set cache time
        tableNamesCache = newCacheBuilder(DEFAULT_EXPIRES_TIME, DEFAULT_REFRESH_TIME, DEFAULT_CACHE_SIZE)
                .build(asyncReloading(CacheLoader.from(this::loadProjects), executor));
        tableCache = newCacheBuilder(DEFAULT_EXPIRES_TIME, DEFAULT_REFRESH_TIME, DEFAULT_CACHE_SIZE)
                .build(asyncReloading(CacheLoader.from(this::loadTable), executor));
        partitionCache = newCacheBuilder(DEFAULT_EXPIRES_TIME, DEFAULT_REFRESH_TIME, DEFAULT_CACHE_SIZE)
                .build(asyncReloading(CacheLoader.from(this::loadPartitions), executor));
    }

    @Override
    public List<String> listDbNames() {
        return ImmutableList.of(odps.getDefaultProject());
    }

    @Override
    public Database getDb(String name) {
        try {
            return new Database(0, name);
        } catch (StarRocksConnectorException e) {
            return null;
        }
    }

    @Override
    public List<String> listTableNames(String dbName) {
        try {
            return new ArrayList<>(tableNamesCache.get(dbName));
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
        return new OdpsTable(catalogName, table);
    }

    @Override
    public List<String> listPartitionNames(String databaseName, String tableName) {
        OdpsTableName odpsTableName = OdpsTableName.of(databaseName, tableName);
        // TODO: perhaps not good to support users to fetch whole tables?
        return get(partitionCache, odpsTableName).stream()
                .map(p -> p.toString(false, true)).collect(
                        Collectors.toList());
    }

    @Override
    public List<String> listPartitionNamesByValue(String databaseName, String tableName,
                                                  List<Optional<String>> partitionValues) {
        List<PartitionSpec> partitionSpecs = get(partitionCache, OdpsTableName.of(databaseName, tableName));
        List<String> keys = new ArrayList<>(partitionSpecs.get(0).keys());
        ImmutableList.Builder<String> builder = ImmutableList.builder();
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

    private List<PartitionSpec> loadPartitions(OdpsTableName odpsTableName) {
        com.aliyun.odps.Table odpsTable = odps.tables().get(odpsTableName.getDatabaseName(), odpsTableName.getTableName());
        List<Partition> partitions =
                odpsTable.getPartitions();
        return partitions.stream().map(Partition::getPartitionSpec).collect(Collectors.toList());
    }

    @Override
    public List<PartitionInfo> getPartitions(Table table, List<String> partitionNames) {
        OdpsTable odpsTable = (OdpsTable) table;
        ImmutableList.Builder<PartitionInfo> builder = ImmutableList.builder();
        odps.tables().get(odpsTable.getProjectName(), odpsTable.getTableName()).getPartitions()
                .forEach(p -> builder.add(new OdpsPartition(p)));
        return builder.build();
    }

    @Override
    public void refreshTable(String srDbName, Table table, List<String> partitionNames, boolean onlyCachedPartitions) {
        OdpsTableName odpsTableName = OdpsTableName.of(srDbName, table.getName());
        tableCache.invalidate(odpsTableName);
        get(tableCache, odpsTableName);
        if(!table.isUnPartitioned()) {
            partitionCache.invalidate(odpsTableName);
            get(partitionCache, odpsTableName);
        }
    }

    @Override
    public List<RemoteFileInfo> getRemoteFileInfos(Table table, List<PartitionKey> partitionKeys,
                                                   long snapshotId, ScalarOperator predicate,
                                                   List<String> columnNames, long limit) {
        RemoteFileInfo remoteFileInfo = new RemoteFileInfo();
        OdpsTable odpsTable = (OdpsTable) table;
        TableReadSessionBuilder scanBuilder = new TableReadSessionBuilder();
        Set<String> set = new HashSet<>(columnNames);
        List<String> orderedColumnNames = new ArrayList<>();
        for (Column column : odpsTable.getFullSchema()) {
            if (set.contains(column.getName())) {
                orderedColumnNames.add(column.getName());
            }
        }
        List<PartitionSpec> partitionSpecs = new ArrayList<>();
        if (partitionKeys != null) {
            for (PartitionKey partitionKey : partitionKeys) {
                String hivePartitionName = toHivePartitionName(odpsTable.getPartitionColumnNames(), partitionKey);
                if (!hivePartitionName.isEmpty()) {
                    partitionSpecs.add(new PartitionSpec(hivePartitionName));
                }
            }
        }
        try {
            LOG.info("get remote file infos, project:{}, table:{}, columns:{}", odpsTable.getProjectName(),
                    odpsTable.getTableName(), columnNames);
            TableBatchReadSession
                    scan =
                    scanBuilder.identifier(TableIdentifier.of(odpsTable.getProjectName(), odpsTable.getTableName()))
                            .withSettings(settings)
                            .requiredDataColumns(orderedColumnNames)
                            .requiredPartitions(partitionSpecs)
                            .withSplitOptions(SplitOptions.createDefault())
                            .buildBatchReadSession();
            InputSplitAssigner assigner = scan.getInputSplitAssigner();
            OdpsSplitsInfo odpsSplitsInfo = new OdpsSplitsInfo(Arrays.asList(assigner.getAllSplits()), scan);
            RemoteFileDesc odpsRemoteFileDesc = RemoteFileDesc.createOdpsRemoteFileDesc(odpsSplitsInfo);
            List<RemoteFileDesc> remoteFileDescs = ImmutableList.of(odpsRemoteFileDesc);
            remoteFileInfo.setFiles(remoteFileDescs);
            return Lists.newArrayList(remoteFileInfo);
        } catch (Exception e) {
            LOG.error("getRemoteFileInfos error", e);
        }
        return Collections.emptyList();
    }

    @Override
    public CloudConfiguration getCloudConfiguration() {
        AliyunCloudConfiguration configuration = new AliyunCloudConfiguration(aliyunCloudCredential);
        configuration.loadCommonFields(new HashMap<>(0));
        return configuration;
    }

    private static CacheBuilder<Object, Object> newCacheBuilder(long expiresAfterWriteSec, long refreshSec,
                                                                long maximumSize) {
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        if (expiresAfterWriteSec >= 0) {
            cacheBuilder.expireAfterWrite(expiresAfterWriteSec, HOURS);
        }

        if (refreshSec > 0 && expiresAfterWriteSec > refreshSec) {
            cacheBuilder.refreshAfterWrite(refreshSec, SECONDS);
        }

        cacheBuilder.maximumSize(maximumSize);
        return cacheBuilder;
    }

    private static <K, V> V get(LoadingCache<K, V> cache, K key) {
        try {
            return cache.getUnchecked(key);
        } catch (UncheckedExecutionException e) {
            LOG.error("Error occurred when loading cache", e);
            throwIfInstanceOf(e.getCause(), StarRocksConnectorException.class);
            throw e;
        }
    }
}
