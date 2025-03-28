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

package com.starrocks.connector.delta;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.common.Pair;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.metastore.IMetastore;
import com.starrocks.connector.metastore.MetastoreTable;
import com.starrocks.sql.analyzer.SemanticException;
import io.delta.kernel.Scan;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Table;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.util.SizeEstimator;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.starrocks.common.profile.Tracers.Module.EXTERNAL;
import static com.starrocks.connector.PartitionUtil.toHivePartitionName;

public abstract class DeltaLakeMetastore implements IDeltaLakeMetastore {
    private static final Logger LOG = LogManager.getLogger(DeltaLakeMetastore.class);
    private static final int MEMORY_META_SAMPLES = 10;
    protected final String catalogName;
    protected final IMetastore delegate;
    protected final Configuration hdfsConfiguration;
    protected final DeltaLakeCatalogProperties properties;

    private final LoadingCache<Pair<DeltaLakeFileStatus, StructType>, List<ColumnarBatch>> checkpointCache;
    private final LoadingCache<DeltaLakeFileStatus, List<JsonNode>> jsonCache;

    public DeltaLakeMetastore(String catalogName, IMetastore metastore, Configuration hdfsConfiguration,
                              DeltaLakeCatalogProperties properties) {
        this.catalogName = catalogName;
        this.delegate = metastore;
        this.hdfsConfiguration = hdfsConfiguration;
        this.properties = properties;
        long checkpointCacheSize = Math.round(Runtime.getRuntime().maxMemory() *
                properties.getDeltaLakeCheckpointMetaCacheMemoryUsageRatio());
        long jsonCacheSize = Math.round(Runtime.getRuntime().maxMemory() *
                properties.getDeltaLakeJsonMetaCacheMemoryUsageRatio());

        this.checkpointCache = CacheBuilder.newBuilder()
                .expireAfterWrite(properties.getDeltaLakeCheckpointMetaCacheTtlSec(), TimeUnit.SECONDS)
                .weigher((key, value) -> Math.toIntExact(SizeEstimator.estimate(key) + SizeEstimator.estimate(value)))
                .maximumWeight(checkpointCacheSize)
                .build(new CacheLoader<>() {
                    @NotNull
                    @Override
                    public List<ColumnarBatch> load(@NotNull Pair<DeltaLakeFileStatus, StructType> pair) {
                        return DeltaLakeParquetHandler.readParquetFile(pair.first.getPath(), pair.second, hdfsConfiguration);
                    }
                });

        this.jsonCache = CacheBuilder.newBuilder()
                .expireAfterWrite(properties.getDeltaLakeJsonMetaCacheTtlSec(), TimeUnit.SECONDS)
                .weigher((key, value) ->
                        Math.toIntExact(SizeEstimator.estimate(key) + SizeEstimator.estimate(value)))
                .maximumWeight(jsonCacheSize)
                .build(new CacheLoader<>() {
                    @NotNull
                    @Override
                    public List<JsonNode> load(@NotNull DeltaLakeFileStatus fileStatus) throws IOException {
                        return DeltaLakeJsonHandler.readJsonFile(fileStatus.getPath(), hdfsConfiguration);
                    }
                });
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public List<String> getAllDatabaseNames() {
        return delegate.getAllDatabaseNames();
    }

    @Override
    public List<String> getAllTableNames(String dbName) {
        return delegate.getAllTableNames(dbName);
    }

    @Override
    public Database getDb(String dbName) {
        return delegate.getDb(dbName);
    }

    @Override
    public DeltaLakeSnapshot getLatestSnapshot(String dbName, String tableName) {
        MetastoreTable metastoreTable = getMetastoreTable(dbName, tableName);
        if (metastoreTable == null) {
            LOG.error("get metastore table failed. dbName: {}, tableName: {}", dbName, tableName);
            return null;
        }

        String path = metastoreTable.getTableLocation();
        long createTime = metastoreTable.getCreateTime();
        DeltaLakeEngine deltaLakeEngine = DeltaLakeEngine.create(hdfsConfiguration, properties, checkpointCache, jsonCache);
        SnapshotImpl snapshot;

        try (Timer ignored = Tracers.watchScope(EXTERNAL, "DeltaLake.getSnapshot")) {
            Table deltaTable = Table.forPath(deltaLakeEngine, path);
            snapshot = (SnapshotImpl) deltaTable.getLatestSnapshot(deltaLakeEngine);
        } catch (TableNotFoundException e) {
            LOG.error("Failed to find Delta table for {}.{}.{}, {}", catalogName, dbName, tableName, e.getMessage());
            throw new SemanticException("Failed to find Delta table for " + catalogName + "." + dbName + "." + tableName);
        } catch (Exception e) {
            LOG.error("Failed to get latest snapshot for {}.{}.{}, {}", catalogName, dbName, tableName, e.getMessage());
            throw new SemanticException("Failed to get latest snapshot for " + catalogName + "." + dbName + "." + tableName);
        }
        return new DeltaLakeSnapshot(dbName, tableName, deltaLakeEngine, snapshot, createTime, path);
    }

    @Override
    public DeltaLakeTable getTable(String dbName, String tableName) {
        DeltaLakeSnapshot snapshot = getLatestSnapshot(dbName, tableName);
        return DeltaUtils.convertDeltaSnapshotToSRTable(catalogName, snapshot);
    }

    @Override
    public List<String> getPartitionKeys(String dbName, String tableName) {
        DeltaLakeTable deltaLakeTable = getTable(dbName, tableName);
        if (deltaLakeTable == null) {
            LOG.error("Table {}.{}.{} doesn't exist", catalogName, dbName, tableName);
            return Lists.newArrayList();
        }

        List<String> partitionKeys = Lists.newArrayList();
        Engine deltaEngine = deltaLakeTable.getDeltaEngine();
        List<String> partitionColumnNames = deltaLakeTable.getPartitionColumnNames();

        ScanBuilder scanBuilder = deltaLakeTable.getDeltaSnapshot().getScanBuilder(deltaEngine);
        Scan scan = scanBuilder.build();
        try (CloseableIterator<FilteredColumnarBatch> scanFilesAsBatches = scan.getScanFiles(deltaEngine)) {
            while (scanFilesAsBatches.hasNext()) {
                FilteredColumnarBatch scanFileBatch = scanFilesAsBatches.next();

                try (CloseableIterator<Row> scanFileRows = scanFileBatch.getRows()) {
                    while (scanFileRows.hasNext()) {
                        Row scanFileRow = scanFileRows.next();
                        Map<String, String> partitionValueMap = InternalScanFileUtils.getPartitionValues(scanFileRow);
                        List<String> partitionValues =
                                partitionColumnNames.stream().map(partitionValueMap::get).collect(
                                        Collectors.toList());
                        String partitionName = toHivePartitionName(partitionColumnNames, partitionValues);
                        partitionKeys.add(partitionName);
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to get partition keys for table {}.{}.{}", catalogName, dbName, tableName, e);
            throw new StarRocksConnectorException(String.format("Failed to get partition keys for table %s.%s.%s",
                    catalogName, dbName, tableName), e);
        }

        return partitionKeys;
    }

    @Override
    public boolean tableExists(String dbName, String tableName) {
        return delegate.tableExists(dbName, tableName);
    }

    public void invalidateAll() {
        checkpointCache.invalidateAll();
        jsonCache.invalidateAll();
    }

    @Override
    public Map<String, Long> estimateCount() {
        return Map.of("checkpointCache", checkpointCache.size(), "jsonCache", jsonCache.size());
    }

    @Override
    public List<Pair<List<Object>, Long>> getSamples() {
        List<Object> jsonSamples = jsonCache.asMap().values()
                .stream()
                .limit(MEMORY_META_SAMPLES)
                .collect(Collectors.toList());

        List<Object> checkpointSamples = checkpointCache.asMap().values()
                .stream()
                .limit(MEMORY_META_SAMPLES)
                .collect(Collectors.toList());

        return Lists.newArrayList(Pair.create(jsonSamples, jsonCache.size()),
                Pair.create(checkpointSamples, checkpointCache.size()));
    }
}
