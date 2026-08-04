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

package com.starrocks.http.rest;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.hash.Hashing;
import com.google.gson.Gson;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksHttpException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.connector.starrocks.StarRocksRemoteTableStats;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.MaxLiteral;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.statistic.BasicStatsMeta;
import com.starrocks.statistic.HistogramStatsMeta;
import com.starrocks.statistic.StatisticExecutor;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.thrift.TStatisticData;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.connector.starrocks.StarRocksRemoteTableStats.ANALYZE_TYPE_NONE;

/**
 * Catalog statistics endpoints consumed by a peer StarRocks cluster's
 * StarRocks catalog (the local FE of that cluster optimizes queries against
 * tables served by this FE). Served over HTTP because the thrift port is
 * internal-facing and typically not exposed across cluster boundaries.
 *
 * <p>Endpoint A (snapshot, epoch-gated conditional fetch):
 * GET /api/{db}/{table}/_sr_catalog_stats_snapshot
 *     ?cached_list_epoch=&cached_data_epoch=&cached_analyze_epoch=
 *
 * <p>Endpoint B (batch per-partition column stats):
 * POST /api/{db}/{table}/_sr_catalog_partition_stats
 *     body: {"partition_ids": [...], "columns": [...]}
 *
 * <p>All partition ids are logical {@link Partition} ids. Epochs are opaque
 * equality-only tokens minted from persisted metadata: a partition-set hash
 * (list), the sum of physical partitions' visibleVersion (data), and the max
 * of BasicStatsMeta / HistogramStatsMeta updateTime + analyze type (analyze).
 */
public class StarRocksCatalogStatsAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(StarRocksCatalogStatsAction.class);

    public static final String STATS_SNAPSHOT_PATH = "_sr_catalog_stats_snapshot";
    public static final String PARTITION_STATS_PATH = "_sr_catalog_partition_stats";

    private static final String PARAM_CACHED_LIST_EPOCH = "cached_list_epoch";
    private static final String PARAM_CACHED_DATA_EPOCH = "cached_data_epoch";
    private static final String PARAM_CACHED_ANALYZE_EPOCH = "cached_analyze_epoch";

    private static final Gson GSON = new Gson();

    private enum Mode {
        SNAPSHOT,
        PARTITION_STATS
    }

    private final Mode mode;

    private StarRocksCatalogStatsAction(ActionController controller, Mode mode) {
        super(controller);
        this.mode = mode;
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET,
                "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/" + STATS_SNAPSHOT_PATH,
                new StarRocksCatalogStatsAction(controller, Mode.SNAPSHOT));
        controller.registerHandler(HttpMethod.POST,
                "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/" + PARTITION_STATS_PATH,
                new StarRocksCatalogStatsAction(controller, Mode.PARTITION_STATS));
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response)
            throws DdlException, AccessDeniedException {
        String dbName = request.getSingleParameter(DB_KEY);
        String tableName = request.getSingleParameter(TABLE_KEY);
        Object result;
        HttpResponseStatus responseStatus = HttpResponseStatus.OK;
        try {
            if (Strings.isNullOrEmpty(dbName) || Strings.isNullOrEmpty(tableName)) {
                throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST, "{database}/{table} must be specified");
            }
            Authorizer.checkTableAction(ConnectContext.get(), dbName, tableName, PrivilegeType.SELECT);

            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
            if (db == null) {
                throw new StarRocksHttpException(HttpResponseStatus.NOT_FOUND,
                        "Database [" + dbName + "] does not exist");
            }
            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName);
            if (table == null) {
                throw new StarRocksHttpException(HttpResponseStatus.NOT_FOUND,
                        "Table [" + tableName + "] does not exist");
            }
            if (!table.isNativeTableOrMaterializedView()) {
                throw new StarRocksHttpException(HttpResponseStatus.FORBIDDEN,
                        "catalog statistics only support native tables");
            }
            if (mode == Mode.SNAPSHOT) {
                result = buildSnapshot(request, db, (OlapTable) table);
            } else {
                result = buildPartitionStats(request, db, (OlapTable) table);
            }
        } catch (StarRocksHttpException e) {
            responseStatus = e.getCode();
            result = errorBody(e.getCode().code(), e.getMessage());
        } catch (AccessDeniedException e) {
            throw e;
        } catch (Exception e) {
            LOG.warn("failed to serve catalog stats for {}.{}", dbName, tableName, e);
            responseStatus = HttpResponseStatus.INTERNAL_SERVER_ERROR;
            result = errorBody(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), e.getMessage());
        }

        response.setContentType(JSON_CONTENT_TYPE);
        response.getContent().append(GSON.toJson(result));
        sendResult(request, response, responseStatus);
    }

    @VisibleForTesting
    static Map<String, Object> errorBody(int code, String message) {
        Map<String, Object> body = new HashMap<>(2);
        body.put("status", code);
        body.put("exception", message == null ? "" : message);
        return body;
    }

    private StarRocksRemoteTableStats.Snapshot buildSnapshot(BaseRequest request, Database db, OlapTable table) {
        String cachedListEpoch = request.getSingleParameter(PARAM_CACHED_LIST_EPOCH);
        String cachedDataEpoch = request.getSingleParameter(PARAM_CACHED_DATA_EPOCH);
        String cachedAnalyzeEpoch = request.getSingleParameter(PARAM_CACHED_ANALYZE_EPOCH);

        StarRocksRemoteTableStats.Snapshot snapshot = new StarRocksRemoteTableStats.Snapshot();
        snapshot.status = HttpResponseStatus.OK.code();

        List<String> partitionColumnNames = new ArrayList<>();
        List<StarRocksRemoteTableStats.PartitionMeta> partitions = new ArrayList<>();
        String partitionType;
        long tableRowCount = 0;
        long dataEpochSum = 0;
        List<Long> sortedPartitionIds = new ArrayList<>();

        Locker locker = new Locker();
        locker.lockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.READ);
        try {
            PartitionInfo partitionInfo = table.getPartitionInfo();
            partitionType = resolvePartitionType(table, partitionInfo, partitionColumnNames);

            Map<Long, List<List<String>>> listValuesById = new HashMap<>();
            if (StarRocksRemoteTableStats.PARTITION_TYPE_LIST.equals(partitionType)) {
                listValuesById = serializeListValues((ListPartitionInfo) partitionInfo);
            }
            RangePartitionInfo rangePartitionInfo =
                    StarRocksRemoteTableStats.PARTITION_TYPE_RANGE.equals(partitionType)
                            ? (RangePartitionInfo) partitionInfo : null;

            for (Partition partition : table.getPartitions()) {
                StarRocksRemoteTableStats.PartitionMeta meta = new StarRocksRemoteTableStats.PartitionMeta();
                meta.id = partition.getId();
                meta.name = partition.getName();
                // Count base-index rows only, the same caliber as OlapTable.getRowCount() /
                // the get_table metadata: Partition.getRowCount() sums ALL latest visible
                // indices, double-counting rollup / synchronous-MV rows for the CBO.
                long partitionRowCount = 0;
                for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                    partitionRowCount += physicalPartition.getLatestBaseIndex().getRowCount();
                    dataEpochSum += physicalPartition.getVisibleVersion();
                }
                meta.rowCount = partitionRowCount;
                tableRowCount += meta.rowCount;
                sortedPartitionIds.add(partition.getId());
                if (rangePartitionInfo != null) {
                    com.google.common.collect.Range<PartitionKey> range = rangePartitionInfo.getRange(partition.getId());
                    if (range != null) {
                        meta.rangeLower = serializeBound(range.lowerEndpoint());
                        meta.rangeUpper = serializeBound(range.upperEndpoint());
                    }
                }
                if (listValuesById.containsKey(partition.getId())) {
                    meta.listValues = listValuesById.get(partition.getId());
                }
                partitions.add(meta);
            }
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.READ);
        }

        sortedPartitionIds.sort(Long::compareTo);
        String listEpoch = Hashing.murmur3_128()
                .hashString(sortedPartitionIds.stream().map(String::valueOf).collect(Collectors.joining(",")),
                        StandardCharsets.UTF_8)
                .toString();
        String dataEpoch = String.valueOf(dataEpochSum);

        BasicStatsMeta basicStatsMeta =
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().getTableBasicStatsMeta(table.getId());
        String analyzeType = basicStatsMeta == null ? ANALYZE_TYPE_NONE : basicStatsMeta.getType().name();
        // The snapshot's column stats embed histograms, whose meta lives apart from
        // BasicStatsMeta, so a histogram-only ANALYZE over an already-analyzed table has to move
        // this epoch too or the client's conditional refresh would keep the stale histograms
        // forever. A table with no BasicStatsMeta at all reports analyze type NONE and ships no
        // column stats (hence no histograms) below, so its epoch stays the constant sentinel.
        String analyzeEpoch = basicStatsMeta == null ? "0:" + ANALYZE_TYPE_NONE :
                Math.max(toEpochMilli(basicStatsMeta.getUpdateTime()), maxHistogramUpdateMilli(table))
                        + ":" + analyzeType;

        snapshot.epochs = new StarRocksRemoteTableStats.Epochs(listEpoch, dataEpoch, analyzeEpoch);
        // Shipping decisions only: the client re-derives the same verdicts by
        // comparing the response epochs against its cached ones (no wire flags).
        boolean listChanged = !listEpoch.equals(cachedListEpoch);
        boolean dataChanged = !dataEpoch.equals(cachedDataEpoch);
        boolean analyzeChanged = !analyzeEpoch.equals(cachedAnalyzeEpoch);

        // Immutable partitioning shape: fixed at CREATE TABLE, always shipped.
        snapshot.partitionType = partitionType;
        snapshot.partitionColumns = partitionColumnNames;
        // partitions embed per-partition row counts, so a moved partition set (list)
        // OR moved data versions (data) re-ship them together with tableRowCount.
        if (listChanged || dataChanged) {
            snapshot.tableRowCount = tableRowCount;
            snapshot.partitions = partitions;
        }
        if (analyzeChanged) {
            snapshot.analyzeType = analyzeType;
            if (!ANALYZE_TYPE_NONE.equals(analyzeType)) {
                snapshot.columnStats = collectTableColumnStats(db, table);
            } else {
                snapshot.columnStats = new ArrayList<>();
            }
        }
        return snapshot;
    }

    private static String resolvePartitionType(OlapTable table, PartitionInfo partitionInfo,
                                               List<String> partitionColumnNamesOut) {
        if (!partitionInfo.isPartitioned()) {
            return StarRocksRemoteTableStats.PARTITION_TYPE_UNPARTITIONED;
        }
        List<Column> partitionColumns = partitionInfo.getPartitionColumns(table.getIdToColumn());
        Set<String> baseSchemaNames = table.getBaseSchema().stream()
                .map(column -> column.getName().toLowerCase(Locale.ROOT))
                .collect(Collectors.toSet());
        for (Column column : partitionColumns) {
            if (!baseSchemaNames.contains(column.getName().toLowerCase(Locale.ROOT))) {
                // Partition columns the consumer cannot resolve against the
                // user-visible schema (e.g. generated columns): row counts stay
                // usable, but the local side must not attempt pruning.
                return StarRocksRemoteTableStats.PARTITION_TYPE_UNSUPPORTED;
            }
        }
        partitionColumns.forEach(column -> partitionColumnNamesOut.add(column.getName()));
        if (partitionInfo.isRangePartition()) {
            return StarRocksRemoteTableStats.PARTITION_TYPE_RANGE;
        }
        if (partitionInfo.isListPartition()) {
            return StarRocksRemoteTableStats.PARTITION_TYPE_LIST;
        }
        return StarRocksRemoteTableStats.PARTITION_TYPE_UNSUPPORTED;
    }

    private static Map<Long, List<List<String>>> serializeListValues(ListPartitionInfo listPartitionInfo) {
        Map<Long, List<List<String>>> result = new HashMap<>();
        for (Map.Entry<Long, List<LiteralExpr>> entry : listPartitionInfo.getLiteralExprValues().entrySet()) {
            List<List<String>> tuples = new ArrayList<>();
            for (LiteralExpr literal : entry.getValue()) {
                List<String> tuple = new ArrayList<>(1);
                tuple.add(serializeLiteral(literal));
                tuples.add(tuple);
            }
            result.put(entry.getKey(), tuples);
        }
        for (Map.Entry<Long, List<List<LiteralExpr>>> entry :
                listPartitionInfo.getMultiLiteralExprValues().entrySet()) {
            List<List<String>> tuples = new ArrayList<>();
            for (List<LiteralExpr> multiValue : entry.getValue()) {
                tuples.add(multiValue.stream().map(StarRocksCatalogStatsAction::serializeLiteral)
                        .collect(Collectors.toList()));
            }
            result.put(entry.getKey(), tuples);
        }
        return result;
    }

    @VisibleForTesting
    static String serializeLiteral(LiteralExpr literal) {
        if (literal == null || literal instanceof NullLiteral) {
            return null;
        }
        return literal.getStringValue();
    }

    @VisibleForTesting
    static StarRocksRemoteTableStats.RangeBound serializeBound(PartitionKey key) {
        StarRocksRemoteTableStats.RangeBound bound = new StarRocksRemoteTableStats.RangeBound();
        if (key.isMinValue()) {
            bound.infiniteMin = true;
            return bound;
        }
        if (key.isMaxValue()) {
            bound.infiniteMax = true;
            return bound;
        }
        bound.values = new ArrayList<>(key.getKeys().size());
        for (LiteralExpr literal : key.getKeys()) {
            bound.values.add(literal instanceof MaxLiteral ? "MAXVALUE" : literal.getStringValue());
        }
        return bound;
    }

    private static List<StarRocksRemoteTableStats.ColumnStats> collectTableColumnStats(Database db, OlapTable table) {
        List<String> columnNames = table.getBaseSchema().stream().map(Column::getName).collect(Collectors.toList());
        List<StarRocksRemoteTableStats.ColumnStats> result = new ArrayList<>();
        ConnectContext previous = ConnectContext.get();
        ConnectContext statsContext = StatisticUtils.buildConnectContext();
        statsContext.setThreadLocalInfo();
        try {
            StatisticExecutor executor = new StatisticExecutor();
            List<TStatisticData> statsData =
                    executor.queryStatisticSync(statsContext, db.getId(), table.getId(), columnNames);
            Map<String, String> histogramJsonByColumn = collectHistograms(statsContext, executor, table);
            for (TStatisticData data : statsData) {
                if (data.getColumnName() == null) {
                    continue;
                }
                StarRocksRemoteTableStats.ColumnStats columnStats = new StarRocksRemoteTableStats.ColumnStats();
                columnStats.column = data.getColumnName();
                columnStats.rowCount = data.getRowCount();
                columnStats.dataSize = data.getDataSize();
                columnStats.ndv = data.getCountDistinct();
                columnStats.nullCount = data.getNullCount();
                columnStats.max = data.getMax();
                columnStats.min = data.getMin();
                columnStats.collectionSize = data.isSetCollectionSize() ? data.getCollectionSize() : -1;
                columnStats.histogram = histogramJsonByColumn.get(data.getColumnName());
                result.add(columnStats);
            }
        } catch (Exception e) {
            LOG.warn("failed to collect table column statistics for {}.{}", db.getFullName(), table.getName(), e);
        } finally {
            if (previous != null) {
                previous.setThreadLocalInfo();
            } else {
                ConnectContext.remove();
            }
        }
        return result;
    }

    @VisibleForTesting
    static long toEpochMilli(LocalDateTime time) {
        return time.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    private static long maxHistogramUpdateMilli(OlapTable table) {
        long max = 0;
        for (Map.Entry<Pair<Long, String>, HistogramStatsMeta> entry :
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().getHistogramStatsMetaMap().entrySet()) {
            if (entry.getKey().first == table.getId() && entry.getValue().getUpdateTime() != null) {
                max = Math.max(max, toEpochMilli(entry.getValue().getUpdateTime()));
            }
        }
        return max;
    }

    private static Map<String, String> collectHistograms(ConnectContext statsContext, StatisticExecutor executor,
                                                         OlapTable table) {
        Map<String, String> result = new HashMap<>();
        try {
            Set<String> histogramColumns = new HashSet<>();
            for (Pair<Long, String> key :
                    GlobalStateMgr.getCurrentState().getAnalyzeMgr().getHistogramStatsMetaMap().keySet()) {
                if (key.first == table.getId()) {
                    histogramColumns.add(key.second);
                }
            }
            if (histogramColumns.isEmpty()) {
                return result;
            }
            List<TStatisticData> histogramData =
                    executor.queryHistogram(statsContext, table.getId(), new ArrayList<>(histogramColumns));
            for (TStatisticData data : histogramData) {
                if (data.getColumnName() != null && data.getHistogram() != null) {
                    result.put(data.getColumnName(), data.getHistogram());
                }
            }
        } catch (Exception e) {
            LOG.warn("failed to collect histograms for table {}", table.getName(), e);
        }
        return result;
    }

    private StarRocksRemoteTableStats.PartitionStatsResponse buildPartitionStats(
            BaseRequest request, Database db, OlapTable table) throws DdlException {
        String body = request.getContent();
        if (Strings.isNullOrEmpty(body)) {
            throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST,
                    "POST body must contain partition_ids and columns");
        }
        StarRocksRemoteTableStats.PartitionStatsRequest statsRequest;
        try {
            statsRequest = GSON.fromJson(body, StarRocksRemoteTableStats.PartitionStatsRequest.class);
        } catch (Exception e) {
            throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST, "malformed json [" + body + "]");
        }
        if (statsRequest == null || statsRequest.partitionIds == null || statsRequest.partitionIds.isEmpty()
                || statsRequest.columns == null || statsRequest.columns.isEmpty()) {
            throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST,
                    "POST body must contain non-empty partition_ids and columns");
        }

        StarRocksRemoteTableStats.PartitionStatsResponse statsResponse =
                new StarRocksRemoteTableStats.PartitionStatsResponse();
        statsResponse.status = HttpResponseStatus.OK.code();

        BasicStatsMeta basicStatsMeta =
                GlobalStateMgr.getCurrentState().getAnalyzeMgr().getTableBasicStatsMeta(table.getId());
        String analyzeType = basicStatsMeta == null ? ANALYZE_TYPE_NONE : basicStatsMeta.getType().name();
        statsResponse.partitionStats = new ArrayList<>();
        if (ANALYZE_TYPE_NONE.equals(analyzeType)) {
            return statsResponse;
        }

        ConnectContext previous = ConnectContext.get();
        ConnectContext statsContext = StatisticUtils.buildConnectContext();
        statsContext.setThreadLocalInfo();
        try {
            List<TStatisticData> statsData = new StatisticExecutor().queryPartitionLevelColumnNDV(
                    statsContext, table.getId(), statsRequest.partitionIds, statsRequest.columns);
            for (TStatisticData data : statsData) {
                if (data.getColumnName() == null) {
                    continue;
                }
                StarRocksRemoteTableStats.PartitionColumnStats partitionStats =
                        new StarRocksRemoteTableStats.PartitionColumnStats();
                partitionStats.partitionId = data.getPartitionId();
                partitionStats.column = data.getColumnName();
                partitionStats.ndv = data.getCountDistinct();
                partitionStats.nullCount = data.getNullCount();
                partitionStats.rowCount = data.getRowCount();
                statsResponse.partitionStats.add(partitionStats);
            }
        } catch (Exception e) {
            // Do NOT report a partial/empty success here: the client cannot tell it apart from
            // "this table genuinely has no collected statistics" and would cache the empty result.
            LOG.warn("failed to collect partition statistics for {}.{}", db.getFullName(), table.getName(), e);
            throw new StarRocksHttpException(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    "failed to collect partition statistics: " + e.getMessage());
        } finally {
            if (previous != null) {
                previous.setThreadLocalInfo();
            } else {
                ConnectContext.remove();
            }
        }
        return statsResponse;
    }
}
