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

package com.starrocks.connector.iceberg.procedure;

import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergTableOperation;
import com.starrocks.connector.iceberg.TestTables;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterTableOperationClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RewriteManifestsProcedureTest {

    public static final HdfsEnvironment HDFS_ENVIRONMENT = new HdfsEnvironment();

    private static final Schema SCHEMA =
            new Schema(required(1, "k1", Types.IntegerType.get()), required(2, "k2", Types.IntegerType.get()));
    // Identity partition on an int column, so partition values have a natural order to cluster by.
    private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).identity("k2").build();

    @TempDir
    public File warehouse;

    @AfterEach
    public void clearNativeTables() {
        TestTables.clearTables();
    }

    @Test
    void testProcedureCreation() {
        RewriteManifestsProcedure procedure = RewriteManifestsProcedure.getInstance();

        assertNotNull(procedure);
        assertEquals("rewrite_manifests", procedure.getProcedureName());
        assertEquals(Collections.emptyList(), procedure.getArguments());
        assertEquals(IcebergTableOperation.REWRITE_MANIFESTS, procedure.getOperation());
    }

    @Test
    void testExecuteWithInvalidArgs() {
        RewriteManifestsProcedure procedure = RewriteManifestsProcedure.getInstance();
        Table table = Mockito.mock(Table.class);
        IcebergTableProcedureContext context = createContext(table, Mockito.mock(Transaction.class));

        Map<String, ConstantOperator> args = new HashMap<>();
        args.put("some_arg", ConstantOperator.createVarchar("value"));

        StarRocksConnectorException ex = assertThrows(StarRocksConnectorException.class,
                () -> procedure.execute(context, args));

        assertTrue(ex.getMessage().contains("invalid args"));
        assertTrue(ex.getMessage().contains("rewrite_manifests"));
        assertTrue(ex.getMessage().contains("does not support any arguments"));
    }

    @Test
    void testExecuteWithNoSnapshot() {
        RewriteManifestsProcedure procedure = RewriteManifestsProcedure.getInstance();
        Table table = Mockito.mock(Table.class);
        when(table.currentSnapshot()).thenReturn(null);
        IcebergTableProcedureContext context = createContext(table, Mockito.mock(Transaction.class));

        assertDoesNotThrow(() -> procedure.execute(context, Collections.emptyMap()));
        verify(table, never()).io();
    }

    @Test
    void testExecuteWithEmptyManifests() {
        RewriteManifestsProcedure procedure = RewriteManifestsProcedure.getInstance();
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        when(snapshot.dataManifests(any())).thenReturn(Collections.emptyList());

        Table table = Mockito.mock(Table.class);
        when(table.currentSnapshot()).thenReturn(snapshot);
        IcebergTableProcedureContext context = createContext(table, Mockito.mock(Transaction.class));

        assertDoesNotThrow(() -> procedure.execute(context, Collections.emptyMap()));

        Transaction txn = context.transaction();
        verify(txn, never()).rewriteManifests();
    }

    @Test
    void testExecuteWithSingleSmallManifest() {
        RewriteManifestsProcedure procedure = RewriteManifestsProcedure.getInstance();
        ManifestFile smallManifest = Mockito.mock(ManifestFile.class);
        when(smallManifest.length()).thenReturn(100L);

        Snapshot snapshot = Mockito.mock(Snapshot.class);
        when(snapshot.dataManifests(any())).thenReturn(List.of(smallManifest));

        Table table = Mockito.mock(Table.class);
        when(table.currentSnapshot()).thenReturn(snapshot);
        when(table.properties()).thenReturn(Collections.emptyMap());
        when(table.spec()).thenReturn(PartitionSpec.unpartitioned());

        Transaction txn = Mockito.mock(Transaction.class);
        IcebergTableProcedureContext context = createContext(table, txn);

        assertDoesNotThrow(() -> procedure.execute(context, Collections.emptyMap()));
        verify(txn, never()).rewriteManifests();
    }

    @Test
    void testExecuteRewriteSuccess() {
        RewriteManifestsProcedure procedure = RewriteManifestsProcedure.getInstance();

        ManifestFile m1 = Mockito.mock(ManifestFile.class);
        ManifestFile m2 = Mockito.mock(ManifestFile.class);
        when(m1.length()).thenReturn(10 * 1024 * 1024L);
        when(m2.length()).thenReturn(10 * 1024 * 1024L);

        Snapshot snapshot = Mockito.mock(Snapshot.class);
        when(snapshot.dataManifests(any())).thenReturn(List.of(m1, m2));

        Table table = Mockito.mock(Table.class);
        when(table.currentSnapshot()).thenReturn(snapshot);
        when(table.properties()).thenReturn(Collections.emptyMap());
        when(table.spec()).thenReturn(PartitionSpec.unpartitioned());

        RewriteManifests rewriteManifests = Mockito.mock(RewriteManifests.class);
        when(rewriteManifests.clusterBy(any())).thenReturn(rewriteManifests);
        doNothing().when(rewriteManifests).commit();

        Transaction txn = Mockito.mock(Transaction.class);
        when(txn.rewriteManifests()).thenReturn(rewriteManifests);

        IcebergTableProcedureContext context = createContext(table, txn);

        assertDoesNotThrow(() -> procedure.execute(context, Collections.emptyMap()));

        verify(txn).rewriteManifests();
        verify(rewriteManifests).clusterBy(any());
        verify(rewriteManifests).commit();
    }

    @Test
    void testExecuteWithSingleLargeManifestTriggersRewrite() {
        RewriteManifestsProcedure procedure = RewriteManifestsProcedure.getInstance();
        // One manifest larger than default target (8MB) still triggers rewrite
        ManifestFile largeManifest = Mockito.mock(ManifestFile.class);
        when(largeManifest.length()).thenReturn(10 * 1024 * 1024L);

        Snapshot snapshot = Mockito.mock(Snapshot.class);
        when(snapshot.dataManifests(any())).thenReturn(List.of(largeManifest));

        Table table = Mockito.mock(Table.class);
        when(table.currentSnapshot()).thenReturn(snapshot);
        when(table.properties()).thenReturn(Collections.emptyMap());
        when(table.spec()).thenReturn(PartitionSpec.unpartitioned());

        RewriteManifests rewriteManifests = Mockito.mock(RewriteManifests.class);
        when(rewriteManifests.clusterBy(any())).thenReturn(rewriteManifests);
        doNothing().when(rewriteManifests).commit();

        Transaction txn = Mockito.mock(Transaction.class);
        when(txn.rewriteManifests()).thenReturn(rewriteManifests);

        IcebergTableProcedureContext context = createContext(table, txn);

        assertDoesNotThrow(() -> procedure.execute(context, Collections.emptyMap()));

        verify(txn).rewriteManifests();
        verify(rewriteManifests).clusterBy(any());
        verify(rewriteManifests).commit();
    }

    @Test
    void testPartitionedManifestsClusteredByOrderPreservingRange() throws Exception {
        // 200 distinct partitions, cluster count capped at MAX_MANIFEST_CLUSTERS(=100),
        // so the order-preserving assignment is bucket = rank * 100 / 200 = rank / 2.
        int numPartitions = 200;
        int expectedClusters = 100;

        Function<DataFile, Object> clusterFn = runAndCaptureClusterFunction(numPartitions);

        int[] buckets = new int[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            Object bucket = clusterFn.apply(dataFileForPartition(i));
            assertNotNull(bucket);
            buckets[i] = ((Number) bucket).intValue();
        }

        for (int i = 0; i < numPartitions; i++) {
            // partition value equals its rank here, so the expected bucket is a pure function of i
            assertEquals(i / 2, buckets[i], "partition k2=" + i + " landed in an unexpected bucket");
            assertTrue(buckets[i] >= 0 && buckets[i] < expectedClusters, "bucket out of range: " + buckets[i]);
            if (i > 0) {
                // monotonic non-decreasing in partition-value order: the property a hash would break
                assertTrue(buckets[i] >= buckets[i - 1], "bucketing is not order-preserving");
            }
        }
        // grouping actually happened and stayed within the cap
        assertEquals(expectedClusters, (int) Arrays.stream(buckets).distinct().count());
    }

    @Test
    void testUnmappedPartitionValueSlotsIntoNeighborBucket() throws Exception {
        int numPartitions = 10;
        Function<DataFile, Object> clusterFn = runAndCaptureClusterFunction(numPartitions);
        // A value not seen at planning time (e.g. a new partition from a concurrent write to the same
        // spec) must not abort and must not be hash-scattered: an above-max value slots into the last
        // known partition's bucket, preserving the order-preserving clustering.
        Object maxBucket = clusterFn.apply(dataFileForPartition(numPartitions - 1));
        Object unmappedBucket = clusterFn.apply(dataFileForPartition(9999));
        assertNotNull(unmappedBucket);
        assertEquals(maxBucket, unmappedBucket);
    }

    @Test
    void testUnknownSpecAbortsRewrite() throws Exception {
        Function<DataFile, Object> clusterFn = runAndCaptureClusterFunction(10);
        // A file under a spec not seen at planning time (concurrent partition-spec evolution) aborts,
        // deferring to the next maintenance run rather than guessing an order.
        DataFile unknownSpecFile = Mockito.mock(DataFile.class);
        when(unknownSpecFile.specId()).thenReturn(999);
        assertThrows(StarRocksConnectorException.class, () -> clusterFn.apply(unknownSpecFile));
    }

    // Builds a real partitioned table with `numPartitions` single-file partitions, runs the
    // procedure against it, and returns the clustering Function it hands to RewriteManifests.
    // The manifest target size is forced to 1 byte so the cluster count is pinned to the cap.
    private Function<DataFile, Object> runAndCaptureClusterFunction(int numPartitions) throws Exception {
        TestTables.TestTable table = TestTables.create(warehouse, "t_range_" + numPartitions, SCHEMA, SPEC, 2);
        AppendFiles append = table.newAppend();
        for (int i = 0; i < numPartitions; i++) {
            append.appendFile(dataFileForPartition(i));
        }
        append.commit();
        table.updateProperties().set(TableProperties.MANIFEST_TARGET_SIZE_BYTES, "1").commit();

        RewriteManifests rewriteManifests = Mockito.mock(RewriteManifests.class);
        when(rewriteManifests.clusterBy(any())).thenReturn(rewriteManifests);
        doNothing().when(rewriteManifests).commit();
        Transaction txn = Mockito.mock(Transaction.class);
        when(txn.rewriteManifests()).thenReturn(rewriteManifests);

        IcebergTableProcedureContext context = createContext(table, txn);
        RewriteManifestsProcedure procedure = RewriteManifestsProcedure.getInstance();
        assertDoesNotThrow(() -> procedure.execute(context, Collections.emptyMap()));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Function<DataFile, Object>> captor = ArgumentCaptor.forClass(Function.class);
        verify(rewriteManifests).clusterBy(captor.capture());
        verify(rewriteManifests).commit();
        return captor.getValue();
    }

    private static DataFile dataFileForPartition(int k2) {
        return DataFiles.builder(SPEC)
                .withPath("/path/to/data-k2-" + k2 + ".parquet")
                .withFileSizeInBytes(10)
                .withPartitionPath("k2=" + k2)
                .withRecordCount(1)
                .build();
    }

    private IcebergTableProcedureContext createContext(Table table, Transaction transaction) {
        IcebergHiveCatalog catalog = Mockito.mock(IcebergHiveCatalog.class);
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        AlterTableStmt stmt = Mockito.mock(AlterTableStmt.class);
        AlterTableOperationClause clause = Mockito.mock(AlterTableOperationClause.class);
        return new IcebergTableProcedureContext(catalog, table, ctx, transaction, HDFS_ENVIRONMENT, stmt, clause);
    }
}
