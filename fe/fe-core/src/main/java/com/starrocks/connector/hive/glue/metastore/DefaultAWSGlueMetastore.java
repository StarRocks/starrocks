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


package com.starrocks.connector.hive.glue.metastore;

<<<<<<< HEAD
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.BatchCreatePartitionRequest;
import com.amazonaws.services.glue.model.BatchGetPartitionRequest;
import com.amazonaws.services.glue.model.BatchGetPartitionResult;
import com.amazonaws.services.glue.model.BinaryColumnStatisticsData;
import com.amazonaws.services.glue.model.BooleanColumnStatisticsData;
import com.amazonaws.services.glue.model.ColumnStatistics;
import com.amazonaws.services.glue.model.ColumnStatisticsData;
import com.amazonaws.services.glue.model.ColumnStatisticsType;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.CreateUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.DateColumnStatisticsData;
import com.amazonaws.services.glue.model.DecimalColumnStatisticsData;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.DeletePartitionRequest;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.DeleteUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.DoubleColumnStatisticsData;
import com.amazonaws.services.glue.model.GetColumnStatisticsForPartitionRequest;
import com.amazonaws.services.glue.model.GetColumnStatisticsForPartitionResult;
import com.amazonaws.services.glue.model.GetColumnStatisticsForTableRequest;
import com.amazonaws.services.glue.model.GetColumnStatisticsForTableResult;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import com.amazonaws.services.glue.model.GetDatabaseResult;
import com.amazonaws.services.glue.model.GetDatabasesRequest;
import com.amazonaws.services.glue.model.GetDatabasesResult;
import com.amazonaws.services.glue.model.GetPartitionRequest;
import com.amazonaws.services.glue.model.GetPartitionsRequest;
import com.amazonaws.services.glue.model.GetPartitionsResult;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.GetTableResult;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionsRequest;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionsResult;
import com.amazonaws.services.glue.model.LongColumnStatisticsData;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.PartitionError;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.PartitionValueList;
import com.amazonaws.services.glue.model.Segment;
import com.amazonaws.services.glue.model.StringColumnStatisticsData;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UpdateDatabaseRequest;
import com.amazonaws.services.glue.model.UpdatePartitionRequest;
import com.amazonaws.services.glue.model.UpdateTableRequest;
import com.amazonaws.services.glue.model.UpdateUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.UserDefinedFunction;
import com.amazonaws.services.glue.model.UserDefinedFunctionInput;
=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.hive.glue.util.MetastoreClientUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.thrift.TException;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.MutableDateTime;
<<<<<<< HEAD
=======
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.BatchCreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchGetPartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchGetPartitionResponse;
import software.amazon.awssdk.services.glue.model.BinaryColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.BooleanColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.ColumnStatistics;
import software.amazon.awssdk.services.glue.model.ColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.ColumnStatisticsType;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.CreateUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.DateColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.DecimalColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DeletePartitionRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.DoubleColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.GetColumnStatisticsForPartitionRequest;
import software.amazon.awssdk.services.glue.model.GetColumnStatisticsForPartitionResponse;
import software.amazon.awssdk.services.glue.model.GetColumnStatisticsForTableRequest;
import software.amazon.awssdk.services.glue.model.GetColumnStatisticsForTableResponse;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GetPartitionRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionsRequest;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionsResponse;
import software.amazon.awssdk.services.glue.model.LongColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.Partition;
import software.amazon.awssdk.services.glue.model.PartitionError;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.PartitionValueList;
import software.amazon.awssdk.services.glue.model.Segment;
import software.amazon.awssdk.services.glue.model.StringColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.UpdatePartitionRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.glue.model.UpdateUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.UserDefinedFunction;
import software.amazon.awssdk.services.glue.model.UserDefinedFunctionInput;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.starrocks.connector.hive.glue.util.AWSGlueConfig.CUSTOM_EXECUTOR_FACTORY_CONF;
import static com.starrocks.connector.hive.glue.util.AWSGlueConfig.NUM_PARTITION_SEGMENTS_CONF;
import static java.util.concurrent.CompletableFuture.supplyAsync;

public class DefaultAWSGlueMetastore implements AWSGlueMetastore {

    public static final int BATCH_GET_PARTITIONS_MAX_REQUEST_SIZE = 1000;
    /**
     * Based on the maxResults parameter at https://docs.aws.amazon.com/glue/latest/webapi/API_GetPartitions.html
     */
    public static final int GET_PARTITIONS_MAX_SIZE = 1000;
    /**
     * Maximum number of Glue Segments. A segment defines a non-overlapping region of a table's partitions,
     * allowing multiple requests to be executed in parallel.
     */
    public static final int DEFAULT_NUM_PARTITION_SEGMENTS = 5;
    /**
     * Currently the upper limit allowed by Glue is 10.
     * https://docs.aws.amazon.com/glue/latest/webapi/API_Segment.html
     */
    public static final int MAX_NUM_PARTITION_SEGMENTS = 10;

    // There are read limit for AWS Glue API : GetColumnStatisticsForPartition
    // https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html#aws-glue-api-catalog-partitions-GetColumnStatisticsForPartition
    public static final int GLUE_COLUMN_STATS_SEGMENT_SIZE = 100;

    private final HiveConf conf;
<<<<<<< HEAD
    private final AWSGlue glueClient;
=======
    private final GlueClient glueClient;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    private final String catalogId;
    private final ExecutorService executorService;
    private final int numPartitionSegments;

    protected ExecutorService getExecutorService(HiveConf hiveConf) {
        Class<? extends ExecutorServiceFactory> executorFactoryClass = hiveConf
                .getClass(CUSTOM_EXECUTOR_FACTORY_CONF,
                        DefaultExecutorServiceFactory.class).asSubclass(
                        ExecutorServiceFactory.class);
        ExecutorServiceFactory factory = ReflectionUtils.newInstance(
                executorFactoryClass, hiveConf);
        return factory.getExecutorService(hiveConf);
    }

<<<<<<< HEAD
    public DefaultAWSGlueMetastore(HiveConf conf, AWSGlue glueClient) {
=======
    public DefaultAWSGlueMetastore(HiveConf conf, GlueClient glueClient) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        checkNotNull(conf, "Hive Config cannot be null");
        checkNotNull(glueClient, "glueClient cannot be null");
        this.numPartitionSegments = conf.getInt(NUM_PARTITION_SEGMENTS_CONF, DEFAULT_NUM_PARTITION_SEGMENTS);
        checkArgument(numPartitionSegments <= MAX_NUM_PARTITION_SEGMENTS,
                String.format("Hive Config [%s] can't exceed %d", NUM_PARTITION_SEGMENTS_CONF, MAX_NUM_PARTITION_SEGMENTS));
        this.conf = conf;
        this.glueClient = glueClient;
        this.catalogId = MetastoreClientUtils.getCatalogId(conf);
        this.executorService = getExecutorService(conf);
    }

    // ======================= Database =======================

    @Override
    public void createDatabase(DatabaseInput databaseInput) {
<<<<<<< HEAD
        CreateDatabaseRequest createDatabaseRequest = new CreateDatabaseRequest().withDatabaseInput(databaseInput)
                .withCatalogId(catalogId);
        glueClient.createDatabase(createDatabaseRequest);
=======
        CreateDatabaseRequest.Builder createDatabaseRequest =
                CreateDatabaseRequest.builder().databaseInput(databaseInput)
                        .catalogId(catalogId);
        glueClient.createDatabase(createDatabaseRequest.build());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    @Override
    public Database getDatabase(String dbName) {
<<<<<<< HEAD
        GetDatabaseRequest getDatabaseRequest = new GetDatabaseRequest().withCatalogId(catalogId).withName(dbName);
        GetDatabaseResult result = glueClient.getDatabase(getDatabaseRequest);
        return result.getDatabase();
=======
        GetDatabaseRequest.Builder getDatabaseRequest = GetDatabaseRequest.builder().catalogId(catalogId).name(dbName);
        GetDatabaseResponse result = glueClient.getDatabase(getDatabaseRequest.build());
        return result.database();
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    @Override
    public List<Database> getAllDatabases() {
        List<Database> ret = Lists.newArrayList();
        String nextToken = null;
        do {
<<<<<<< HEAD
            GetDatabasesRequest getDatabasesRequest = new GetDatabasesRequest().withNextToken(nextToken).withCatalogId(
                    catalogId);
            GetDatabasesResult result = glueClient.getDatabases(getDatabasesRequest);
            nextToken = result.getNextToken();
            ret.addAll(result.getDatabaseList());
=======
            GetDatabasesRequest.Builder getDatabasesRequest =
                    GetDatabasesRequest.builder().nextToken(nextToken).catalogId(
                    catalogId);
            GetDatabasesResponse result = glueClient.getDatabases(getDatabasesRequest.build());
            nextToken = result.nextToken();
            ret.addAll(result.databaseList());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        } while (nextToken != null);
        return ret;
    }

    @Override
    public void updateDatabase(String databaseName, DatabaseInput databaseInput) {
<<<<<<< HEAD
        UpdateDatabaseRequest updateDatabaseRequest = new UpdateDatabaseRequest().withName(databaseName)
                .withDatabaseInput(databaseInput).withCatalogId(catalogId);
        glueClient.updateDatabase(updateDatabaseRequest);
=======
        UpdateDatabaseRequest.Builder updateDatabaseRequest = UpdateDatabaseRequest.builder().name(databaseName)
                .databaseInput(databaseInput).catalogId(catalogId);
        glueClient.updateDatabase(updateDatabaseRequest.build());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    @Override
    public void deleteDatabase(String dbName) {
<<<<<<< HEAD
        DeleteDatabaseRequest deleteDatabaseRequest = new DeleteDatabaseRequest().withName(dbName).withCatalogId(
                catalogId);
        glueClient.deleteDatabase(deleteDatabaseRequest);
=======
        DeleteDatabaseRequest.Builder deleteDatabaseRequest = DeleteDatabaseRequest.builder().name(dbName).catalogId(
                catalogId);
        glueClient.deleteDatabase(deleteDatabaseRequest.build());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    // ======================== Table ========================

    @Override
    public void createTable(String dbName, TableInput tableInput) {
<<<<<<< HEAD
        CreateTableRequest createTableRequest = new CreateTableRequest().withTableInput(tableInput)
                .withDatabaseName(dbName).withCatalogId(catalogId);
        glueClient.createTable(createTableRequest);
=======
        CreateTableRequest.Builder createTableRequest = CreateTableRequest.builder().tableInput(tableInput)
                .databaseName(dbName).catalogId(catalogId);
        glueClient.createTable(createTableRequest.build());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    @Override
    public Table getTable(String dbName, String tableName) {
<<<<<<< HEAD
        GetTableRequest getTableRequest = new GetTableRequest().withDatabaseName(dbName).withName(tableName)
                .withCatalogId(catalogId);
        GetTableResult result = glueClient.getTable(getTableRequest);
        return result.getTable();
=======
        GetTableRequest.Builder getTableRequest = GetTableRequest.builder().databaseName(dbName).name(tableName)
                .catalogId(catalogId);
        GetTableResponse result = glueClient.getTable(getTableRequest.build());
        return result.table();
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    @Override
    public List<Table> getTables(String dbname, String tablePattern) {
        List<Table> ret = new ArrayList<>();
        String nextToken = null;
        do {
<<<<<<< HEAD
            GetTablesRequest getTablesRequest = new GetTablesRequest().withDatabaseName(dbname)
                    .withExpression(tablePattern).withNextToken(nextToken).withCatalogId(catalogId);
            GetTablesResult result = glueClient.getTables(getTablesRequest);
            ret.addAll(result.getTableList());
            nextToken = result.getNextToken();
=======
            GetTablesRequest.Builder getTablesRequest = GetTablesRequest.builder().databaseName(dbname)
                    .expression(tablePattern).nextToken(nextToken).catalogId(catalogId);
            GetTablesResponse result = glueClient.getTables(getTablesRequest.build());
            ret.addAll(result.tableList());
            nextToken = result.nextToken();
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        } while (nextToken != null);
        return ret;
    }

    @Override
    public void updateTable(String dbName, TableInput tableInput) {
<<<<<<< HEAD
        UpdateTableRequest updateTableRequest = new UpdateTableRequest().withDatabaseName(dbName)
                .withTableInput(tableInput).withCatalogId(catalogId);
        glueClient.updateTable(updateTableRequest);
=======
        UpdateTableRequest.Builder updateTableRequest = UpdateTableRequest.builder().databaseName(dbName)
                .tableInput(tableInput).catalogId(catalogId);
        glueClient.updateTable(updateTableRequest.build());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    @Override
    public void deleteTable(String dbName, String tableName) {
<<<<<<< HEAD
        DeleteTableRequest deleteTableRequest = new DeleteTableRequest().withDatabaseName(dbName).withName(tableName)
                .withCatalogId(catalogId);
        glueClient.deleteTable(deleteTableRequest);
=======
        DeleteTableRequest.Builder deleteTableRequest =
                DeleteTableRequest.builder().databaseName(dbName).name(tableName)
                        .catalogId(catalogId);
        glueClient.deleteTable(deleteTableRequest.build());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    // =========================== Partition ===========================

    @Override
    public Partition getPartition(String dbName, String tableName, List<String> partitionValues) {
<<<<<<< HEAD
        GetPartitionRequest request = new GetPartitionRequest()
                .withDatabaseName(dbName)
                .withTableName(tableName)
                .withPartitionValues(partitionValues)
                .withCatalogId(catalogId);
        return glueClient.getPartition(request).getPartition();
=======
        GetPartitionRequest.Builder request = GetPartitionRequest.builder()
                .databaseName(dbName)
                .tableName(tableName)
                .partitionValues(partitionValues)
                .catalogId(catalogId);
        return glueClient.getPartition(request.build()).partition();
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    @Override
    public List<Partition> getPartitionsByNames(String dbName, String tableName,
                                                List<PartitionValueList> partitionsToGet) {

        List<List<PartitionValueList>> batchedPartitionsToGet = Lists.partition(partitionsToGet,
                BATCH_GET_PARTITIONS_MAX_REQUEST_SIZE);
<<<<<<< HEAD
        List<Future<BatchGetPartitionResult>> batchGetPartitionFutures = Lists.newArrayList();

        for (List<PartitionValueList> batch : batchedPartitionsToGet) {
            final BatchGetPartitionRequest request = new BatchGetPartitionRequest()
                    .withDatabaseName(dbName)
                    .withTableName(tableName)
                    .withPartitionsToGet(batch)
                    .withCatalogId(catalogId);
            batchGetPartitionFutures.add(this.executorService.submit(new Callable<BatchGetPartitionResult>() {
                @Override
                public BatchGetPartitionResult call() throws Exception {
                    return glueClient.batchGetPartition(request);
=======
        List<Future<BatchGetPartitionResponse>> batchGetPartitionFutures = Lists.newArrayList();

        for (List<PartitionValueList> batch : batchedPartitionsToGet) {
            final BatchGetPartitionRequest.Builder request = BatchGetPartitionRequest.builder()
                    .databaseName(dbName)
                    .tableName(tableName)
                    .partitionsToGet(batch)
                    .catalogId(catalogId);
            batchGetPartitionFutures.add(this.executorService.submit(new Callable<BatchGetPartitionResponse>() {
                @Override
                public BatchGetPartitionResponse call() throws Exception {
                    return glueClient.batchGetPartition(request.build());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                }
            }));
        }

        List<Partition> result = Lists.newArrayList();
        try {
<<<<<<< HEAD
            for (Future<BatchGetPartitionResult> future : batchGetPartitionFutures) {
                result.addAll(future.get().getPartitions());
            }
        } catch (ExecutionException e) {
            Throwables.throwIfInstanceOf(e.getCause(), AmazonServiceException.class);
=======
            for (Future<BatchGetPartitionResponse> future : batchGetPartitionFutures) {
                result.addAll(future.get().partitions());
            }
        } catch (ExecutionException e) {
            Throwables.throwIfInstanceOf(e.getCause(), SdkException.class);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            Throwables.throwIfUnchecked(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return result;
    }

    @Override
    public List<Partition> getPartitions(String dbName, String tableName, String expression,
                                         long max) throws TException {
        if (max == 0) {
            return Collections.emptyList();
        }
        if (max < 0 || max > GET_PARTITIONS_MAX_SIZE) {
            return getPartitionsParallel(dbName, tableName, expression, max);
        } else {
            // We don't need to get too many partitions, so just do it serially.
            return getCatalogPartitions(dbName, tableName, expression, max, null);
        }
    }

    private List<Partition> getPartitionsParallel(
            final String databaseName,
            final String tableName,
            final String expression,
            final long max) throws TException {
        // Prepare the segments
        List<Segment> segments = Lists.newArrayList();
        for (int i = 0; i < numPartitionSegments; i++) {
<<<<<<< HEAD
            segments.add(new Segment()
                    .withSegmentNumber(i)
                    .withTotalSegments(numPartitionSegments));
=======
            segments.add(Segment.builder()
                    .segmentNumber(i)
                    .totalSegments(numPartitionSegments).build());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }
        // Submit Glue API calls in parallel using the thread pool.
        // We could convert this into a parallelStream after upgrading to JDK 8 compiler base.
        List<Future<List<Partition>>> futures = Lists.newArrayList();
        for (final Segment segment : segments) {
            futures.add(this.executorService.submit(new Callable<List<Partition>>() {
                @Override
                public List<Partition> call() throws Exception {
                    return getCatalogPartitions(databaseName, tableName, expression, max, segment);
                }
            }));
        }

        // Get the results
        List<Partition> partitions = Lists.newArrayList();
        try {
            for (Future<List<Partition>> future : futures) {
                List<Partition> segmentPartitions = future.get();
                if (partitions.size() + segmentPartitions.size() >= max && max > 0) {
                    // Extract the required number of partitions from the segment and we're done.
                    long remaining = max - partitions.size();
                    partitions.addAll(segmentPartitions.subList(0, (int) remaining));
                    break;
                } else {
                    partitions.addAll(segmentPartitions);
                }
            }
        } catch (ExecutionException e) {
<<<<<<< HEAD
            Throwables.throwIfInstanceOf(e.getCause(), AmazonServiceException.class);
=======
            Throwables.throwIfInstanceOf(e.getCause(), SdkException.class);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            Throwables.throwIfUnchecked(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return partitions;
    }


    private List<Partition> getCatalogPartitions(String databaseName, String tableName, String expression,
                                                 long max, Segment segment) {
        List<Partition> partitions = Lists.newArrayList();
        String nextToken = null;
        do {
<<<<<<< HEAD
            GetPartitionsRequest request = new GetPartitionsRequest()
                    .withDatabaseName(databaseName)
                    .withTableName(tableName)
                    .withExpression(expression)
                    .withNextToken(nextToken)
                    .withCatalogId(catalogId)
                    .withSegment(segment);
            GetPartitionsResult res = glueClient.getPartitions(request);
            List<Partition> list = res.getPartitions();
=======
            GetPartitionsRequest.Builder request = GetPartitionsRequest.builder()
                    .databaseName(databaseName)
                    .tableName(tableName)
                    .expression(expression)
                    .nextToken(nextToken)
                    .catalogId(catalogId)
                    .segment(segment);
            GetPartitionsResponse res = glueClient.getPartitions(request.build());
            List<Partition> list = res.partitions();
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            if ((partitions.size() + list.size()) >= max && max > 0) {
                long remaining = max - partitions.size();
                partitions.addAll(list.subList(0, (int) remaining));
                break;
            }
            partitions.addAll(list);
<<<<<<< HEAD
            nextToken = res.getNextToken();
=======
            nextToken = res.nextToken();
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        } while (nextToken != null);
        return partitions;
    }

    @Override
    public void updatePartition(String dbName, String tableName, List<String> partitionValues,
                                PartitionInput partitionInput) {
<<<<<<< HEAD
        UpdatePartitionRequest updatePartitionRequest = new UpdatePartitionRequest().withDatabaseName(dbName)
                .withTableName(tableName).withPartitionValueList(partitionValues)
                .withPartitionInput(partitionInput).withCatalogId(catalogId);
        glueClient.updatePartition(updatePartitionRequest);
=======
        UpdatePartitionRequest.Builder updatePartitionRequest = UpdatePartitionRequest.builder().databaseName(dbName)
                .tableName(tableName).partitionValueList(partitionValues)
                .partitionInput(partitionInput).catalogId(catalogId);
        glueClient.updatePartition(updatePartitionRequest.build());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    @Override
    public void deletePartition(String dbName, String tableName, List<String> partitionValues) {
<<<<<<< HEAD
        DeletePartitionRequest request = new DeletePartitionRequest()
                .withDatabaseName(dbName)
                .withTableName(tableName)
                .withPartitionValues(partitionValues)
                .withCatalogId(catalogId);
        glueClient.deletePartition(request);
=======
        DeletePartitionRequest.Builder request = DeletePartitionRequest.builder()
                .databaseName(dbName)
                .tableName(tableName)
                .partitionValues(partitionValues)
                .catalogId(catalogId);
        glueClient.deletePartition(request.build());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    @Override
    public List<PartitionError> createPartitions(String dbName, String tableName,
                                                 List<PartitionInput> partitionInputs) {
<<<<<<< HEAD
        BatchCreatePartitionRequest request =
                new BatchCreatePartitionRequest().withDatabaseName(dbName)
                .withTableName(tableName).withCatalogId(catalogId)
                .withPartitionInputList(partitionInputs);
        return glueClient.batchCreatePartition(request).getErrors();
=======
        BatchCreatePartitionRequest.Builder request =
                BatchCreatePartitionRequest.builder().databaseName(dbName)
                        .tableName(tableName).catalogId(catalogId)
                        .partitionInputList(partitionInputs);
        return glueClient.batchCreatePartition(request.build()).errors();
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    // ====================== User Defined Function ======================

    @Override
    public void createUserDefinedFunction(String dbName, UserDefinedFunctionInput functionInput) {
<<<<<<< HEAD
        CreateUserDefinedFunctionRequest createUserDefinedFunctionRequest = new CreateUserDefinedFunctionRequest()
                .withDatabaseName(dbName).withFunctionInput(functionInput).withCatalogId(catalogId);
        glueClient.createUserDefinedFunction(createUserDefinedFunctionRequest);
=======
        CreateUserDefinedFunctionRequest.Builder createUserDefinedFunctionRequest =
                CreateUserDefinedFunctionRequest.builder()
                        .databaseName(dbName).functionInput(functionInput).catalogId(catalogId);
        glueClient.createUserDefinedFunction(createUserDefinedFunctionRequest.build());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    @Override
    public UserDefinedFunction getUserDefinedFunction(String dbName, String functionName) {
<<<<<<< HEAD
        GetUserDefinedFunctionRequest getUserDefinedFunctionRequest = new GetUserDefinedFunctionRequest()
                .withDatabaseName(dbName).withFunctionName(functionName).withCatalogId(catalogId);
        return glueClient.getUserDefinedFunction(getUserDefinedFunctionRequest).getUserDefinedFunction();
=======
        GetUserDefinedFunctionRequest.Builder getUserDefinedFunctionRequest = GetUserDefinedFunctionRequest.builder()
                .databaseName(dbName).functionName(functionName).catalogId(catalogId);
        return glueClient.getUserDefinedFunction(getUserDefinedFunctionRequest.build()).userDefinedFunction();
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    @Override
    public List<UserDefinedFunction> getUserDefinedFunctions(String dbName, String pattern) {
        List<UserDefinedFunction> ret = Lists.newArrayList();
        String nextToken = null;
        do {
<<<<<<< HEAD
            GetUserDefinedFunctionsRequest getUserDefinedFunctionsRequest = new GetUserDefinedFunctionsRequest()
                    .withDatabaseName(dbName).withPattern(pattern).withNextToken(nextToken).withCatalogId(catalogId);
            GetUserDefinedFunctionsResult result = glueClient.getUserDefinedFunctions(getUserDefinedFunctionsRequest);
            nextToken = result.getNextToken();
            ret.addAll(result.getUserDefinedFunctions());
=======
            GetUserDefinedFunctionsRequest.Builder getUserDefinedFunctionsRequest =
                    GetUserDefinedFunctionsRequest.builder()
                            .databaseName(dbName).pattern(pattern).nextToken(nextToken).catalogId(catalogId);
            GetUserDefinedFunctionsResponse result =
                    glueClient.getUserDefinedFunctions(getUserDefinedFunctionsRequest.build());
            nextToken = result.nextToken();
            ret.addAll(result.userDefinedFunctions());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        } while (nextToken != null);
        return ret;
    }

    @Override
    public void deleteUserDefinedFunction(String dbName, String functionName) {
<<<<<<< HEAD
        DeleteUserDefinedFunctionRequest deleteUserDefinedFunctionRequest = new DeleteUserDefinedFunctionRequest()
                .withDatabaseName(dbName).withFunctionName(functionName).withCatalogId(catalogId);
        glueClient.deleteUserDefinedFunction(deleteUserDefinedFunctionRequest);
=======
        DeleteUserDefinedFunctionRequest.Builder deleteUserDefinedFunctionRequest =
                DeleteUserDefinedFunctionRequest.builder()
                        .databaseName(dbName).functionName(functionName).catalogId(catalogId);
        glueClient.deleteUserDefinedFunction(deleteUserDefinedFunctionRequest.build());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    @Override
    public void updateUserDefinedFunction(String dbName, String functionName, UserDefinedFunctionInput functionInput) {
<<<<<<< HEAD
        UpdateUserDefinedFunctionRequest updateUserDefinedFunctionRequest = new UpdateUserDefinedFunctionRequest()
                .withDatabaseName(dbName).withFunctionName(functionName).withFunctionInput(functionInput)
                .withCatalogId(catalogId);
        glueClient.updateUserDefinedFunction(updateUserDefinedFunctionRequest);
=======
        UpdateUserDefinedFunctionRequest.Builder updateUserDefinedFunctionRequest =
                UpdateUserDefinedFunctionRequest.builder()
                        .databaseName(dbName).functionName(functionName).functionInput(functionInput)
                        .catalogId(catalogId);
        glueClient.updateUserDefinedFunction(updateUserDefinedFunctionRequest.build());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName, List<String> colNames) {
        // Split colNames into column chunks
        List<List<String>> columnChunks = Lists.partition(colNames, GLUE_COLUMN_STATS_SEGMENT_SIZE);

        // Submit Glue API calls in parallel using the thread pool.
<<<<<<< HEAD
        List<CompletableFuture<GetColumnStatisticsForTableResult>> futures = columnChunks.stream().
                map(columnChunk -> supplyAsync(() -> {
                    GetColumnStatisticsForTableRequest request = new GetColumnStatisticsForTableRequest()
                            .withDatabaseName(dbName)
                            .withTableName(tableName)
                            .withColumnNames(columnChunk);
                    return glueClient.getColumnStatisticsForTable(request);
=======
        List<CompletableFuture<GetColumnStatisticsForTableResponse>> futures = columnChunks.stream().
                map(columnChunk -> supplyAsync(() -> {
                    GetColumnStatisticsForTableRequest.Builder request = GetColumnStatisticsForTableRequest.builder()
                            .databaseName(dbName)
                            .tableName(tableName)
                            .columnNames(columnChunk);
                    return glueClient.getColumnStatisticsForTable(request.build());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                }, this.executorService)).collect(Collectors.toList());

        // Get the column statistics
        Map<String, ColumnStatisticsObj> columnStatisticsMap = Maps.newHashMap();
        try {
<<<<<<< HEAD
            for (CompletableFuture<GetColumnStatisticsForTableResult> future : futures) {
                GetColumnStatisticsForTableResult columnStatisticsForTableResult = future.get();
                for (ColumnStatistics columnStatistics : columnStatisticsForTableResult.getColumnStatisticsList()) {
                    columnStatisticsMap.put(columnStatistics.getColumnName(),
=======
            for (CompletableFuture<GetColumnStatisticsForTableResponse> future : futures) {
                GetColumnStatisticsForTableResponse columnStatisticsForTableResult = future.get();
                for (ColumnStatistics columnStatistics : columnStatisticsForTableResult.columnStatisticsList()) {
                    columnStatisticsMap.put(columnStatistics.columnName(),
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                            toHiveColumnStatistic(columnStatistics));
                }
            }
        } catch (ExecutionException e) {
<<<<<<< HEAD
            Throwables.throwIfInstanceOf(e.getCause(), AmazonServiceException.class);
=======
            Throwables.throwIfInstanceOf(e.getCause(), SdkException.class);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            Throwables.throwIfUnchecked(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return new ArrayList<>(columnStatisticsMap.values());
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String dbName, String tableName,
                                                                               List<String> partitionNames,
                                                                               List<String> colNames) {
<<<<<<< HEAD
        Map<String, List<CompletableFuture<GetColumnStatisticsForPartitionResult>>> futureForPartition =
=======
        Map<String, List<CompletableFuture<GetColumnStatisticsForPartitionResponse>>> futureForPartition =
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                Maps.newHashMap();
        for (String partitionName : partitionNames) {
            // get column statistics for each partition
            List<String> partitionValue = PartitionUtil.toPartitionValues(partitionName);
            // Split colNames into column chunks
            List<List<String>> columnChunks = Lists.partition(colNames, GLUE_COLUMN_STATS_SEGMENT_SIZE);
            futureForPartition.put(partitionName, columnChunks.stream().map(columnChunk -> supplyAsync(() -> {
<<<<<<< HEAD
                GetColumnStatisticsForPartitionRequest request = new GetColumnStatisticsForPartitionRequest()
                        .withDatabaseName(dbName)
                        .withTableName(tableName)
                        .withColumnNames(columnChunk)
                        .withPartitionValues(partitionValue);
                return glueClient.getColumnStatisticsForPartition(request);
=======
                GetColumnStatisticsForPartitionRequest.Builder request =
                        GetColumnStatisticsForPartitionRequest.builder()
                                .databaseName(dbName)
                                .tableName(tableName)
                                .columnNames(columnChunk)
                                .partitionValues(partitionValue);
                return glueClient.getColumnStatisticsForPartition(request.build());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            }, this.executorService)).collect(Collectors.toList()));
        }

        Map<String, List<ColumnStatisticsObj>> partitionColumnStats = Maps.newHashMap();
        futureForPartition.forEach((partitionName, futures) -> {
            List<ColumnStatisticsObj> columnStatisticsObjs = Lists.newArrayList();
<<<<<<< HEAD
            for (CompletableFuture<GetColumnStatisticsForPartitionResult> future : futures) {
                try {
                    GetColumnStatisticsForPartitionResult result = future.get();
                    result.getColumnStatisticsList().forEach(columnStatistics ->
=======
            for (CompletableFuture<GetColumnStatisticsForPartitionResponse> future : futures) {
                try {
                    GetColumnStatisticsForPartitionResponse result = future.get();
                    result.columnStatisticsList().forEach(columnStatistics ->
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                            columnStatisticsObjs.add(toHiveColumnStatistic(columnStatistics)));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
<<<<<<< HEAD
                    Throwables.throwIfInstanceOf(e.getCause(), AmazonServiceException.class);
=======
                    Throwables.throwIfInstanceOf(e.getCause(), SdkException.class);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                    Throwables.throwIfUnchecked(e.getCause());
                }
            }
            partitionColumnStats.put(partitionName, columnStatisticsObjs);
        });

        return partitionColumnStats;
    }

    private ColumnStatisticsObj toHiveColumnStatistic(ColumnStatistics columnStatistics) {
<<<<<<< HEAD
        ColumnStatisticsData columnStatisticsData = columnStatistics.getStatisticsData();
        ColumnStatisticsType type = ColumnStatisticsType.fromValue(columnStatisticsData.getType());
        String columnName = columnStatistics.getColumnName();
        switch (type) {
            case BINARY: {
                BinaryColumnStatisticsData data = columnStatisticsData.getBinaryColumnStatisticsData();
                org.apache.hadoop.hive.metastore.api.ColumnStatisticsData hiveColumnStatisticsData =
                        org.apache.hadoop.hive.metastore.api.ColumnStatisticsData.binaryStats(
                                new BinaryColumnStatsData(data.getMaximumLength(), data.getAverageLength(),
                                        data.getNumberOfNulls()));
                return new ColumnStatisticsObj(columnName, "BINARY", hiveColumnStatisticsData);
            }
            case BOOLEAN: {
                BooleanColumnStatisticsData data = columnStatisticsData.getBooleanColumnStatisticsData();
                org.apache.hadoop.hive.metastore.api.ColumnStatisticsData hiveColumnStatisticsData =
                        org.apache.hadoop.hive.metastore.api.ColumnStatisticsData.booleanStats(
                                new BooleanColumnStatsData(data.getNumberOfTrues(), data.getNumberOfFalses(),
                                        data.getNumberOfNulls()));
                return new ColumnStatisticsObj(columnName, "BOOLEAN", hiveColumnStatisticsData);
            }
            case DATE: {
                DateColumnStatisticsData data = columnStatisticsData.getDateColumnStatisticsData();
                DateColumnStatsData dateColumnStatsData = new DateColumnStatsData(data.getNumberOfNulls(),
                        data.getNumberOfDistinctValues());
                MutableDateTime epoch = new MutableDateTime();
                epoch.setDate(0); //Set to Epoch time
                dateColumnStatsData.setHighValue(new Date(Days.daysBetween(epoch,
                        new DateTime(data.getMaximumValue().getTime())).getDays()));
                dateColumnStatsData.setLowValue(new Date(Days.daysBetween(epoch,
                        new DateTime(data.getMinimumValue().getTime())).getDays()));
=======
        ColumnStatisticsData columnStatisticsData = columnStatistics.statisticsData();
        ColumnStatisticsType type = columnStatisticsData.type();
        String columnName = columnStatistics.columnName();
        switch (type) {
            case BINARY: {
                BinaryColumnStatisticsData data = columnStatisticsData.binaryColumnStatisticsData();
                org.apache.hadoop.hive.metastore.api.ColumnStatisticsData hiveColumnStatisticsData =
                        org.apache.hadoop.hive.metastore.api.ColumnStatisticsData.binaryStats(
                                new BinaryColumnStatsData(data.maximumLength(), data.averageLength(),
                                        data.numberOfNulls()));
                return new ColumnStatisticsObj(columnName, "BINARY", hiveColumnStatisticsData);
            }
            case BOOLEAN: {
                BooleanColumnStatisticsData data = columnStatisticsData.booleanColumnStatisticsData();
                org.apache.hadoop.hive.metastore.api.ColumnStatisticsData hiveColumnStatisticsData =
                        org.apache.hadoop.hive.metastore.api.ColumnStatisticsData.booleanStats(
                                new BooleanColumnStatsData(data.numberOfTrues(), data.numberOfFalses(),
                                        data.numberOfNulls()));
                return new ColumnStatisticsObj(columnName, "BOOLEAN", hiveColumnStatisticsData);
            }
            case DATE: {
                DateColumnStatisticsData data = columnStatisticsData.dateColumnStatisticsData();
                DateColumnStatsData dateColumnStatsData = new DateColumnStatsData(data.numberOfNulls(),
                        data.numberOfDistinctValues());
                MutableDateTime epoch = new MutableDateTime();
                epoch.setDate(0); //Set to Epoch time
                dateColumnStatsData.setHighValue(new Date(Days.daysBetween(epoch,
                        new DateTime(data.maximumValue().getEpochSecond() * 1000)).getDays()));
                dateColumnStatsData.setLowValue(new Date(Days.daysBetween(epoch,
                        new DateTime(data.minimumValue().getEpochSecond() * 1000)).getDays()));
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

                org.apache.hadoop.hive.metastore.api.ColumnStatisticsData hiveColumnStatisticsData =
                        org.apache.hadoop.hive.metastore.api.ColumnStatisticsData.dateStats(dateColumnStatsData);
                return new ColumnStatisticsObj(columnName, "DATE", hiveColumnStatisticsData);
            }
            case DECIMAL: {
<<<<<<< HEAD
                DecimalColumnStatisticsData data = columnStatisticsData.getDecimalColumnStatisticsData();
                DecimalColumnStatsData decimalColumnStatsData = new DecimalColumnStatsData(data.getNumberOfNulls(),
                        data.getNumberOfDistinctValues());
                decimalColumnStatsData.setHighValue(new Decimal(data.getMaximumValue().getScale().shortValue(),
                        data.getMaximumValue().getUnscaledValue()));
                decimalColumnStatsData.setLowValue(new Decimal(data.getMinimumValue().getScale().shortValue(),
                        data.getMinimumValue().getUnscaledValue()));
=======
                DecimalColumnStatisticsData data = columnStatisticsData.decimalColumnStatisticsData();
                DecimalColumnStatsData decimalColumnStatsData = new DecimalColumnStatsData(data.numberOfNulls(),
                        data.numberOfDistinctValues());
                decimalColumnStatsData.setHighValue(new Decimal(data.maximumValue().scale().shortValue(),
                        data.maximumValue().unscaledValue().asByteBuffer()));
                decimalColumnStatsData.setLowValue(new Decimal(data.minimumValue().scale().shortValue(),
                        data.minimumValue().unscaledValue().asByteBuffer()));
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

                org.apache.hadoop.hive.metastore.api.ColumnStatisticsData hiveColumnStatisticsData =
                        org.apache.hadoop.hive.metastore.api.ColumnStatisticsData.decimalStats(decimalColumnStatsData);
                return new ColumnStatisticsObj(columnName, "DECIMAL", hiveColumnStatisticsData);
            }
            case DOUBLE: {
<<<<<<< HEAD
                DoubleColumnStatisticsData data = columnStatisticsData.getDoubleColumnStatisticsData();
                DoubleColumnStatsData doubleColumnStatsData = new DoubleColumnStatsData(data.getNumberOfNulls(),
                        data.getNumberOfDistinctValues());
                doubleColumnStatsData.setHighValue(data.getMaximumValue());
                doubleColumnStatsData.setLowValue(data.getMinimumValue());
=======
                DoubleColumnStatisticsData data = columnStatisticsData.doubleColumnStatisticsData();
                DoubleColumnStatsData doubleColumnStatsData = new DoubleColumnStatsData(data.numberOfNulls(),
                        data.numberOfDistinctValues());
                doubleColumnStatsData.setHighValue(data.maximumValue());
                doubleColumnStatsData.setLowValue(data.minimumValue());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

                org.apache.hadoop.hive.metastore.api.ColumnStatisticsData hiveColumnStatisticsData =
                        org.apache.hadoop.hive.metastore.api.ColumnStatisticsData.doubleStats(doubleColumnStatsData);
                return new ColumnStatisticsObj(columnName, "DOUBLE", hiveColumnStatisticsData);
            }
            case LONG: {
<<<<<<< HEAD
                LongColumnStatisticsData data = columnStatisticsData.getLongColumnStatisticsData();
                LongColumnStatsData longColumnStatsData = new LongColumnStatsData(data.getNumberOfNulls(),
                        data.getNumberOfDistinctValues());
                longColumnStatsData.setHighValue(data.getMaximumValue());
                longColumnStatsData.setLowValue(data.getMinimumValue());
=======
                LongColumnStatisticsData data = columnStatisticsData.longColumnStatisticsData();
                LongColumnStatsData longColumnStatsData = new LongColumnStatsData(data.numberOfNulls(),
                        data.numberOfDistinctValues());
                longColumnStatsData.setHighValue(data.maximumValue());
                longColumnStatsData.setLowValue(data.minimumValue());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

                org.apache.hadoop.hive.metastore.api.ColumnStatisticsData hiveColumnStatisticsData =
                        org.apache.hadoop.hive.metastore.api.ColumnStatisticsData.longStats(longColumnStatsData);
                return new ColumnStatisticsObj(columnName, "LONG", hiveColumnStatisticsData);
            }
            case STRING: {
<<<<<<< HEAD
                StringColumnStatisticsData data = columnStatisticsData.getStringColumnStatisticsData();
                StringColumnStatsData stringColumnStatsData = new StringColumnStatsData(data.getMaximumLength(),
                        data.getAverageLength(), data.getNumberOfNulls(), data.getNumberOfDistinctValues());
=======
                StringColumnStatisticsData data = columnStatisticsData.stringColumnStatisticsData();
                StringColumnStatsData stringColumnStatsData = new StringColumnStatsData(data.maximumLength(),
                        data.averageLength(), data.numberOfNulls(), data.numberOfDistinctValues());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

                org.apache.hadoop.hive.metastore.api.ColumnStatisticsData hiveColumnStatisticsData =
                        org.apache.hadoop.hive.metastore.api.ColumnStatisticsData.stringStats(stringColumnStatsData);
                return new ColumnStatisticsObj(columnName, "STRING", hiveColumnStatisticsData);
            }
        }

        throw new RuntimeException("Invalid glue column statistics : " + columnStatistics);
    }
}