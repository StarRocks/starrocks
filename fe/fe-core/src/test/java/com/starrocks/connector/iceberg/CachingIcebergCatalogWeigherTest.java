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

package com.starrocks.connector.iceberg;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Policy;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.iceberg.CachingIcebergCatalog.IcebergTableName;
import com.starrocks.memory.estimate.Estimator;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.starrocks.connector.iceberg.IcebergCatalogProperties.HIVE_METASTORE_URIS;
import static com.starrocks.connector.iceberg.IcebergCatalogProperties.ICEBERG_CATALOG_TYPE;

/**
 * Guards the accounting boundary of the iceberg table cache weigher.
 *
 * <p>A cached {@link BaseTable} reaches catalog-level singletons through its {@link TableOperations}
 * -- the Hadoop configuration, the metastore client pool, the {@code FileIO}, the metrics reporter.
 * Those exist once per catalog, so charging them to every entry makes the Caffeine weight ledger
 * grow linearly with the entry count while the heap does not. The cache then declares itself full
 * while holding a fraction of its budget, and evicts continuously.
 *
 * <p>Two fixture decisions matter, both about keeping the reflective walk bounded and controlled:
 * <ul>
 *   <li>{@link TableOperations} and {@link TableMetadata} are real, not Mockito mocks. The weigher
 *       walks the object graph reflectively, so a mock would have it measure the proxy's
 *       interceptor and invocation containers instead of a realistic table.</li>
 *   <li>The stand-in for catalog-level state holds a primitive array rather than a real Hadoop
 *       {@code Configuration}. A {@code Configuration} drags in a class loader, resource lists and
 *       a logger, whose reachable graph is neither bounded nor stable; a {@code byte[]} is sized
 *       exactly and costs the estimator a single arithmetic step. What is under test is the
 *       accounting boundary, not any particular shared class.</li>
 * </ul>
 */
public class CachingIcebergCatalogWeigherTest {
    private static final String CATALOG_NAME = "iceberg_catalog";
    private static final IcebergCatalogProperties CATALOG_PROPERTIES;

    private static final int SMALL_SHARED_BYTES = 1024;
    private static final int LARGE_SHARED_BYTES = 4 << 20;

    /** Reporting is irrelevant here; keep it out of the measured graph. */
    private static final MetricsReporter NO_OP_REPORTER = report -> {
    };

    static {
        Map<String, String> config = new HashMap<>();
        // non-exist ip, prevent to connect local service
        config.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732");
        config.put(ICEBERG_CATALOG_TYPE, "hive");
        CATALOG_PROPERTIES = new IcebergCatalogProperties(config);
    }

    /** Stands in for whatever the catalog shares across all of its tables. */
    private static final class SharedCatalogState {
        private final byte[] payload;

        private SharedCatalogState(int bytes) {
            this.payload = new byte[bytes];
        }
    }

    /**
     * A real {@link TableOperations} holding exactly the two things that matter: one catalog-level
     * shared object, and the entry-private metadata.
     */
    private static final class SharedStateTableOperations implements TableOperations {
        private final SharedCatalogState sharedState;
        private final TableMetadata privateMetadata;

        private SharedStateTableOperations(SharedCatalogState sharedState, TableMetadata privateMetadata) {
            this.sharedState = sharedState;
            this.privateMetadata = privateMetadata;
        }

        @Override
        public TableMetadata current() {
            return privateMetadata;
        }

        @Override
        public TableMetadata refresh() {
            return privateMetadata;
        }

        @Override
        public void commit(TableMetadata base, TableMetadata metadata) {
            throw new UnsupportedOperationException("read-only fixture");
        }

        @Override
        public FileIO io() {
            return null;
        }

        @Override
        public String metadataFileLocation(String fileName) {
            return privateMetadata.location() + "/metadata/" + fileName;
        }

        @Override
        public LocationProvider locationProvider() {
            throw new UnsupportedOperationException("read-only fixture");
        }
    }

    private static TableMetadata newTableMetadata() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "ts", Types.TimestampType.withZone()));
        return TableMetadata.newTableMetadata(
                schema, PartitionSpec.unpartitioned(), "/tmp/weigher-test/tbl", Map.of());
    }

    private static Table tableSharing(SharedCatalogState shared, TableMetadata metadata, String name) {
        return new BaseTable(new SharedStateTableOperations(shared, metadata), name, NO_OP_REPORTER);
    }

    private static CachingIcebergCatalog newCatalog(ExecutorService executor) {
        return new CachingIcebergCatalog(
                CATALOG_NAME, Mockito.mock(IcebergCatalog.class), CATALOG_PROPERTIES, executor);
    }

    private static LoadingCache<IcebergTableName, Table> tableCacheOf(CachingIcebergCatalog catalog) {
        return Deencapsulation.getField(catalog, "tables");
    }

    private static Policy.Eviction<IcebergTableName, Table> evictionOf(
            LoadingCache<IcebergTableName, Table> tables) {
        return tables.policy().eviction()
                .orElseThrow(() -> new AssertionError("table cache must be weight-bounded"));
    }

    /** Weight the production weigher assigns to one table whose operations hold {@code shared}. */
    private static int weighTableSharing(SharedCatalogState shared, TableMetadata metadata) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            LoadingCache<IcebergTableName, Table> tables = tableCacheOf(newCatalog(executor));
            IcebergTableName key = new IcebergTableName("db", "tbl");
            tables.put(key, tableSharing(shared, metadata, "db.tbl"));
            tables.cleanUp();
            return evictionOf(tables).weightOf(key)
                    .orElseThrow(() -> new AssertionError("entry must be present and weighed"));
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Two tables that are identical except for the size of the catalog-level object their operations
     * point at must be weighed the same. Anything else means per-catalog state is charged per entry.
     *
     * <p>Intentionally asserts only that relationship. Asserting a concrete weight, or a delta
     * derived from the weigher's formula, would pin the test to today's implementation -- which is
     * why the previous weigher guard had to be deleted rather than updated when the estimation
     * switched from hand-rolled constants to reflective walking.
     */
    @Test
    public void tableWeightMustNotIncludeCatalogLevelSharedState() {
        SharedCatalogState smallShared = new SharedCatalogState(SMALL_SHARED_BYTES);
        SharedCatalogState largeShared = new SharedCatalogState(LARGE_SHARED_BYTES);

        long sharedSizeDelta = Estimator.estimate(largeShared) - Estimator.estimate(smallShared);

        // Precondition: the two shared objects must differ enough that charging them per entry
        // would be unmistakable. Without this the assertion below could pass for the wrong reason.
        Assertions.assertTrue(sharedSizeDelta > 1024L * 1024L,
                "fixture is not discriminating: shared state size delta is only "
                        + sharedSizeDelta + " bytes");

        // One metadata instance for both, so the entry-private part is held constant by
        // construction and the shared object is the only variable.
        TableMetadata metadata = newTableMetadata();
        int weightWithSmallShared = weighTableSharing(smallShared, metadata);
        int weightWithLargeShared = weighTableSharing(largeShared, metadata);

        long weightDelta = Math.abs((long) weightWithLargeShared - weightWithSmallShared);
        Assertions.assertTrue(weightDelta < sharedSizeDelta / 10,
                "entry weight tracks catalog-level shared state: weight went from "
                        + weightWithSmallShared + " to " + weightWithLargeShared + " (delta "
                        + weightDelta + ") while only the shared state grew by "
                        + sharedSizeDelta + " bytes");
    }

    /**
     * The ledger must scale with the number of entries, not with how many times the shared state is
     * re-counted. N tables sharing one catalog-level object should not be charged for N copies of it.
     */
    @Test
    public void ledgerMustScaleWithEntryCountNotSharedState() {
        SharedCatalogState shared = new SharedCatalogState(LARGE_SHARED_BYTES);
        TableMetadata metadata = newTableMetadata();
        long sharedSize = Estimator.estimate(shared);

        int tableCount = 8;
        ExecutorService executor = Executors.newSingleThreadExecutor();
        long ledger;
        try {
            LoadingCache<IcebergTableName, Table> tables = tableCacheOf(newCatalog(executor));
            for (int i = 0; i < tableCount; i++) {
                tables.put(new IcebergTableName("db", "tbl" + i),
                        tableSharing(shared, metadata, "db.tbl" + i));
            }
            tables.cleanUp();
            ledger = evictionOf(tables).weightedSize()
                    .orElseThrow(() -> new AssertionError("weighted size must be available"));
        } finally {
            executor.shutdownNow();
        }

        // Each table's own metadata is orders of magnitude smaller than the shared state, so if the
        // shared state were charged per entry the ledger would be dominated by it. Allowing one full
        // copy of slack keeps this insensitive to how precisely the private part is measured.
        Assertions.assertTrue(ledger < sharedSize,
                "ledger for " + tableCount + " tables sharing one " + sharedSize
                        + "-byte catalog object is " + ledger
                        + ", i.e. the shared object is being charged more than once");
    }
}
