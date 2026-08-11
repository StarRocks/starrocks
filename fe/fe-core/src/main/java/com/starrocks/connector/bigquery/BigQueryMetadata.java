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

package com.starrocks.connector.bigquery;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.BigQueryTable;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.statistics.ConnectorNdvEstimator;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.gcp.GCPCloudConfiguration;
import com.starrocks.credential.gcp.GCPCloudCredential;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.SECONDS;

public class BigQueryMetadata implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(BigQueryMetadata.class);

    private final BigQuery bigQuery;
    private final BigQueryReadClient readClient;
    private final GoogleCredentials credentials;
    private final String catalogName;
    private final BigQueryProperties properties;
    private final String projectId;

    private LoadingCache<String, Set<String>> tableNameCache;
    private LoadingCache<BigQueryTableName, BigQueryTable> tableCache;

    public BigQueryMetadata(BigQuery bigQuery, BigQueryReadClient readClient, GoogleCredentials credentials,
                            String catalogName, BigQueryProperties properties) {
        this.bigQuery = bigQuery;
        this.readClient = readClient;
        this.credentials = credentials;
        this.catalogName = catalogName;
        this.properties = properties;
        this.projectId = properties.get(BigQueryProperties.PROJECT_ID);
        initMetaCache();
    }

    private void initMetaCache() {
        long tableExpireSecs = properties.getLong(BigQueryProperties.TABLE_CACHE_EXPIRE_TIME);
        long tableSize = properties.getLong(BigQueryProperties.TABLE_CACHE_SIZE);
        long datasetExpireSecs = properties.getLong(BigQueryProperties.DATASET_CACHE_EXPIRE_TIME);
        long datasetSize = properties.getLong(BigQueryProperties.DATASET_CACHE_SIZE);

        boolean tableEnabled = properties.getBoolean(BigQueryProperties.ENABLE_TABLE_CACHE);
        boolean datasetEnabled = properties.getBoolean(BigQueryProperties.ENABLE_DATASET_CACHE);

        tableNameCache = CacheBuilder.newBuilder()
                .expireAfterWrite(datasetEnabled ? datasetExpireSecs : 0, SECONDS)
                .maximumSize(datasetEnabled ? datasetSize : 0)
                .build(CacheLoader.from(this::loadTableNames));

        tableCache = CacheBuilder.newBuilder()
                .expireAfterWrite(tableEnabled ? tableExpireSecs : 0, SECONDS)
                .maximumSize(tableEnabled ? tableSize : 0)
                .build(CacheLoader.from(this::loadTable));
    }

    @Override
    public Table.TableType getTableType() {
        return Table.TableType.BIGQUERY;
    }

    @Override
    public List<String> listDbNames(ConnectContext context) {
        String filterStr = properties.get(BigQueryProperties.DATASET_FILTER);
        Set<String> filter = (filterStr != null && !filterStr.isEmpty())
                ? new HashSet<>(Arrays.asList(filterStr.split(",")))
                : Collections.emptySet();

        ImmutableList.Builder<String> names = ImmutableList.builder();
        try {
            for (com.google.cloud.bigquery.Dataset dataset : bigQuery.listDatasets(projectId).iterateAll()) {
                String datasetId = dataset.getDatasetId().getDataset();
                if (filter.isEmpty() || filter.contains(datasetId)) {
                    names.add(datasetId);
                }
            }
        } catch (Exception e) {
            throw new StarRocksConnectorException("Failed to list BigQuery datasets: " + e.getMessage(), e);
        }
        return names.build();
    }

    @Override
    public Database getDb(ConnectContext context, String name) {
        return new Database(ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asLong(), name);
    }

    @Override
    public List<String> listTableNames(ConnectContext context, String dbName) {
        try {
            return new ArrayList<>(tableNameCache.get(dbName));
        } catch (ExecutionException e) {
            LOG.error("listTableNames error for dataset '{}'", dbName, e);
            return Collections.emptyList();
        }
    }

    private Set<String> loadTableNames(String datasetId) {
        boolean viewEnabled = properties.getBoolean(BigQueryProperties.VIEW_ENABLED);
        Set<String> names = new HashSet<>();
        for (com.google.cloud.bigquery.Table table :
                bigQuery.listTables(DatasetId.of(projectId, datasetId)).iterateAll()) {
            TableDefinition.Type type = table.getDefinition().getType();
            if (type == TableDefinition.Type.TABLE
                    || type == TableDefinition.Type.MATERIALIZED_VIEW
                    || (viewEnabled && type == TableDefinition.Type.VIEW)) {
                names.add(table.getTableId().getTable());
            }
        }
        return names;
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        return get(tableCache, BigQueryTableName.of(dbName, tblName));
    }

    private BigQueryTable loadTable(BigQueryTableName key) {
        TableInfo info = bigQuery.getTable(TableId.of(projectId, key.getDatasetId(), key.getTableName()));
        if (info == null) {
            return null;
        }
        TableDefinition.Type type = info.getDefinition().getType();
        boolean viewEnabled = properties.getBoolean(BigQueryProperties.VIEW_ENABLED);

        if (type == TableDefinition.Type.VIEW && !viewEnabled) {
            return null;
        }
        if (type != TableDefinition.Type.TABLE
                && type != TableDefinition.Type.MATERIALIZED_VIEW
                && type != TableDefinition.Type.VIEW) {
            return null;
        }

        com.google.cloud.bigquery.Schema bqSchema = info.getDefinition().getSchema();
        if (bqSchema == null) {
            LOG.warn("BigQuery table {}.{}.{} has no schema", projectId, key.getDatasetId(), key.getTableName());
            return null;
        }

        List<Column> columns = BigQuerySchemaUtils.toStarRocksColumns(bqSchema);
        long createTime = info.getCreationTime() != null ? info.getCreationTime() : 0L;
        boolean isView = type == TableDefinition.Type.VIEW;

        return new BigQueryTable(catalogName, key.getDatasetId(), key.getTableName(),
                columns, createTime, isView);
    }

    @Override
    public List<RemoteFileInfo> getRemoteFiles(Table table, GetRemoteFilesParams params) {
        BigQueryTable bqTable = (BigQueryTable) table;

        // For regular tables / materialised views: use the table directly.
        // For views: materialise into a temp table via a query job first.
        TableId readTableId;
        boolean usingTempTable = false;

        if (bqTable.isView()) {
            readTableId = materializeView(bqTable, params);
            usingTempTable = true;
        } else {
            readTableId = TableId.of(projectId, bqTable.getCatalogDBName(), bqTable.getCatalogTableName());
        }

        String tableProject = readTableId.getProject() != null ? readTableId.getProject() : projectId;
        String tablePath = String.format("projects/%s/datasets/%s/tables/%s",
                tableProject, readTableId.getDataset(), readTableId.getTable());

        // Determine projected columns in schema order.
        // getFieldNames() can return null when the caller does not set a projection.
        List<String> paramFieldNames = params.getFieldNames() != null
                ? params.getFieldNames() : Collections.emptyList();
        Set<String> requestedFields = new HashSet<>(paramFieldNames);
        List<String> orderedFields = bqTable.getFullSchema().stream()
                .map(Column::getName)
                .filter(requestedFields::contains)
                .collect(Collectors.toList());
        if (orderedFields.isEmpty()) {
            // No specific fields requested — project all columns.
            orderedFields = bqTable.getFullSchema().stream()
                    .map(Column::getName)
                    .collect(Collectors.toList());
        }

        int preferredMinStreams = properties.getInt(BigQueryProperties.MAX_STREAMS);

        ReadSession.Builder sessionBuilder = ReadSession.newBuilder()
                .setTable(tablePath)
                .setDataFormat(DataFormat.ARROW)
                .setReadOptions(ReadSession.TableReadOptions.newBuilder()
                        .addAllSelectedFields(orderedFields)
                        .build());

        CreateReadSessionRequest.Builder requestBuilder = CreateReadSessionRequest.newBuilder()
                .setParent("projects/" + projectId)
                .setReadSession(sessionBuilder);
        if (preferredMinStreams > 0) {
            requestBuilder.setPreferredMinStreamCount(preferredMinStreams);
        }

        ReadSession session = readClient.createReadSession(requestBuilder.build());

        if (session.getStreamsList().isEmpty()) {
            LOG.info("BigQuery read session returned 0 streams for table {}", tablePath);
            return Collections.emptyList();
        }

        String credentialsBase64 = serializeCredentials();

        // Serialize the Arrow schema from the ReadSession so the JNI scanner can build
        // VectorSchemaRoot without needing a separate API call.
        String arrowSchemaBase64 = "";
        if (session.hasArrowSchema()) {
            arrowSchemaBase64 = Base64.getEncoder().encodeToString(
                    session.getArrowSchema().getSerializedSchema().toByteArray());
        }

        // Build a common params map attached to the RemoteFileInfo; the scan node reads it.
        Map<String, String> commonParams = new HashMap<>();
        commonParams.put("project_id", projectId);
        commonParams.put("dataset_id", bqTable.getCatalogDBName());
        commonParams.put("table_id", bqTable.getCatalogTableName());
        commonParams.put("required_fields", String.join(",", orderedFields));
        commonParams.put("credentials_base64", credentialsBase64);
        commonParams.put("read_session_name", session.getName());
        commonParams.put("arrow_schema_base64", arrowSchemaBase64);

        List<RemoteFileDesc> fileDescs = new ArrayList<>();
        for (int i = 0; i < session.getStreamsList().size(); i++) {
            String streamName = session.getStreamsList().get(i).getName();
            fileDescs.add(BigQueryRemoteFileDesc.createBigQueryRemoteFileDesc(
                    session.getName(), streamName, i, usingTempTable));
        }

        RemoteFileInfo remoteFileInfo = new RemoteFileInfo();
        remoteFileInfo.setFiles(fileDescs);
        remoteFileInfo.setAttachment(commonParams);

        return Collections.singletonList(remoteFileInfo);
    }

    /**
     * Materialise a BigQuery VIEW into a temp table by running a query job.
     * Returns the {@link TableId} of the destination temp table.
     */
    private TableId materializeView(BigQueryTable view, GetRemoteFilesParams params) {
        String materializeProject = properties.get(BigQueryProperties.VIEW_MATERIALIZE_PROJECT);
        if (materializeProject == null || materializeProject.isEmpty()) {
            materializeProject = projectId;
        }
        String materializeDataset = properties.get(BigQueryProperties.VIEW_MATERIALIZE_DATASET);
        long timeoutSeconds = properties.getLong(BigQueryProperties.VIEW_JOB_TIMEOUT_SECONDS);

        String tempTableName = String.format("_sr_view_%s_%s_%s",
                view.getCatalogDBName(), view.getCatalogTableName(),
                UUID.randomUUID().toString().replace("-", ""));
        TableId destTableId = TableId.of(materializeProject, materializeDataset, tempTableName);

        List<String> fields = (params.getFieldNames() == null || params.getFieldNames().isEmpty())
                ? view.getFullSchema().stream().map(Column::getName).collect(Collectors.toList())
                : params.getFieldNames();
        String fieldList = fields.stream().map(f -> "`" + f + "`").collect(Collectors.joining(", "));
        String sql = String.format("SELECT %s FROM `%s.%s.%s`",
                fieldList, projectId, view.getCatalogDBName(), view.getCatalogTableName());

        QueryJobConfiguration jobConfig = QueryJobConfiguration.newBuilder(sql)
                .setDestinationTable(destTableId)
                .setCreateDisposition(QueryJobConfiguration.CreateDisposition.CREATE_IF_NEEDED)
                .setWriteDisposition(QueryJobConfiguration.WriteDisposition.WRITE_TRUNCATE)
                .build();

        String jobId = "sr-view-" + UUID.randomUUID();
        Job job = bigQuery.create(JobInfo.newBuilder(jobConfig).setJobId(JobId.of(jobId)).build());

        LOG.info("Materialising BigQuery view {}.{} into temp table {}.{}.{}, jobId={}",
                view.getCatalogDBName(), view.getCatalogTableName(),
                materializeProject, materializeDataset, tempTableName, jobId);

        long startMs = System.currentTimeMillis();
        try {
            Job completed = job.waitFor();
            if (completed == null) {
                throw new StarRocksConnectorException(
                        "BigQuery view materialisation job disappeared unexpectedly, jobId=" + jobId);
            }
            if (completed.getStatus().getError() != null) {
                throw new StarRocksConnectorException(
                        "BigQuery view materialisation job failed: " + completed.getStatus().getError());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StarRocksConnectorException("Interrupted waiting for BigQuery view materialisation", e);
        }

        long elapsed = System.currentTimeMillis() - startMs;
        if (elapsed > 10_000) {
            LOG.warn("BigQuery view materialisation for {}.{} took {}ms",
                    view.getCatalogDBName(), view.getCatalogTableName(), elapsed);
        }

        // Set the temp table to expire in 1 hour.
        try {
            TableInfo tempInfo = bigQuery.getTable(destTableId);
            if (tempInfo != null) {
                bigQuery.update(tempInfo.toBuilder()
                        .setExpirationTime(System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1))
                        .build());
            }
        } catch (Exception e) {
            LOG.warn("Failed to set expiry on temp table {}.{}.{}: {}",
                    materializeProject, materializeDataset, tempTableName, e.getMessage());
        }

        return destTableId;
    }

    /**
     * Obtain a short-lived access token from the connector credentials and encode it as base64.
     * The JNI scanner uses this token to authenticate with the BigQuery Storage Read API.
     * Falls back to an empty string when the token cannot be obtained — in that case
     * the JNI scanner will attempt Application Default Credentials itself.
     */
    private String serializeCredentials() {
        try {
            credentials.refreshIfExpired();
            AccessToken token = credentials.getAccessToken();
            if (token != null && token.getTokenValue() != null) {
                return Base64.getEncoder().encodeToString(
                        ("access_token:" + token.getTokenValue()).getBytes());
            }
        } catch (IOException e) {
            LOG.warn("Could not refresh credentials for BigQuery JNI scanner: {}", e.getMessage());
        }
        return "";
    }

    @Override
    public Statistics getTableStatistics(OptimizerContext session,
                                          Table table,
                                          Map<ColumnRefOperator, Column> columns,
                                          List<PartitionKey> partitionKeys,
                                          ScalarOperator predicate,
                                          long limit,
                                          TvrVersionRange version) {
        BigQueryTable bqTable = (BigQueryTable) table;
        double rowCount = 10000.0;
        try {
            TableInfo info = bigQuery.getTable(
                    TableId.of(projectId, bqTable.getCatalogDBName(), bqTable.getCatalogTableName()));
            if (info != null && info.getNumRows() != null) {
                rowCount = info.getNumRows().doubleValue();
            }
        } catch (Exception e) {
            LOG.warn("Could not fetch row count for BigQuery table {}.{}: {}",
                    bqTable.getCatalogDBName(), bqTable.getCatalogTableName(), e.getMessage());
        }

        Statistics.Builder builder = Statistics.builder().setOutputRowCount(rowCount);
        for (Map.Entry<ColumnRefOperator, Column> entry : columns.entrySet()) {
            ConnectorNdvEstimator.TypeCategory cat =
                    ConnectorNdvEstimator.fromStarRocksType(entry.getValue().getType());
            double ndv = Math.max(1.0, Math.min(ConnectorNdvEstimator.typeNdv(cat, rowCount), rowCount));
            builder.addColumnStatistic(entry.getKey(), ColumnStatistic.builder()
                    .setDistinctValuesCount(ndv)
                    .setAverageRowSize(entry.getValue().getType().getTypeSize())
                    .setNullsFraction(0)
                    .setType(ColumnStatistic.StatisticType.ESTIMATE)
                    .build());
        }
        return builder.build();
    }

    @Override
    public CloudConfiguration getCloudConfiguration() {
        // Provide a GCP cloud configuration for cases where the BE needs cloud context.
        // The actual BigQuery Storage Read API auth is handled by the JNI scanner using
        // the access token serialised in bigquery_split_infos.
        GCPCloudCredential gcpCredential = new GCPCloudCredential(
                "", true, "", "", "", "", "", "");
        GCPCloudConfiguration conf = new GCPCloudConfiguration(gcpCredential);
        conf.loadCommonFields(new HashMap<>(0));
        return conf;
    }

    @Override
    public void refreshTable(String srDbName, Table table, List<String> partitionNames,
                             boolean onlyCachedPartitions) {
        BigQueryTableName key = BigQueryTableName.of(srDbName, table.getName());
        tableCache.invalidate(key);
        get(tableCache, key);
    }

    // ---- Cache helpers ----

    private <K, V> V get(LoadingCache<K, V> cache, K key) {
        try {
            return cache.get(key);
        } catch (ExecutionException e) {
            LOG.error("Cache load error for key '{}': {}", key, e.getMessage());
            return null;
        }
    }
}
