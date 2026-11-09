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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.cache.Cache;
import com.google.common.collect.Maps;
import com.starrocks.common.Pair;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.connector.hive.HiveMetastore;
import com.starrocks.connector.hive.HiveMetastoreTest;
import com.starrocks.connector.hive.IHiveMetastore;
import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.openjdk.jol.info.GraphLayout;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * Checks the Delta Lake checkpoint and json caches' memory accounting -- the Guava weigher ledger that eviction
 * is bounded by, and {@link DeltaLakeMetastore#estimateSize()} that feeds {@code sys.fe_memory_usage} --
 * against the heap the cached objects retain, as measured by JOL. Each case prints a {@code MEMACC} line;
 * the assertions keep both numbers in a band.
 */
public class DeltaLakeCacheMemoryAccuracyTest {
    static {
        // delta kernel vectors hold lambdas; JOL needs this to lay out hidden classes. Each test class runs in
        // its own JVM (reuseForks=false), and JOL reads the property when it first initializes.
        System.setProperty("jol.magicFieldOffset", "true");
    }

    private static final double LOW = 0.90;
    private static final double HIGH = 1.15;
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int COLUMNS = 30;

    // Every ratio of a test is printed before any is asserted, so one run shows the whole picture.
    private final List<String> outOfBand = new ArrayList<>();

    private void check(String label, String what, long estimated, long actual) {
        double ratio = estimated * 1.0 / actual;
        System.out.printf("MEMACC %s | %s estimated=%d actual=%d ratio=%.3f%n", label, what, estimated, actual, ratio);
        if (ratio < LOW || ratio > HIGH) {
            outOfBand.add(String.format("%s %s: estimated %d vs actual %d (%.3f) is outside [%.2f, %.2f]",
                    label, what, estimated, actual, ratio, LOW, HIGH));
        }
    }

    @AfterEach
    public void assertInBand() {
        Assertions.assertTrue(outOfBand.isEmpty(), String.join("\n", outOfBand));
    }

    private static String schemaString() throws Exception {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("type", "struct");
        ArrayNode fields = root.putArray("fields");
        for (int i = 0; i < COLUMNS; i++) {
            ObjectNode f = fields.addObject();
            f.put("name", "column_name_" + i);
            f.put("type", i % 2 == 0 ? "string" : "long");
            f.put("nullable", true);
            f.putObject("metadata");
        }
        ObjectNode dt = fields.addObject();
        dt.put("name", "dt");
        dt.put("type", "string");
        dt.put("nullable", true);
        dt.putObject("metadata");
        return MAPPER.writeValueAsString(root);
    }

    private static String rnd(Random r, int len) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            sb.append((char) ('a' + r.nextInt(26)));
        }
        return sb.toString();
    }

    private static String stats(Random r) throws Exception {
        ObjectNode s = MAPPER.createObjectNode();
        s.put("numRecords", 1000 + r.nextInt(100000));
        ObjectNode min = s.putObject("minValues");
        ObjectNode max = s.putObject("maxValues");
        ObjectNode nulls = s.putObject("nullCount");
        for (int i = 0; i < COLUMNS; i++) {
            String c = "column_name_" + i;
            if (i % 2 == 0) {
                min.put(c, rnd(r, 8 + r.nextInt(24)));
                max.put(c, rnd(r, 8 + r.nextInt(24)));
            } else {
                min.put(c, r.nextInt(1 << 30));
                max.put(c, (1L << 31) + r.nextInt(1 << 30));
            }
            nulls.put(c, r.nextInt(10));
        }
        return MAPPER.writeValueAsString(s);
    }

    private static ObjectNode add(Random r, String path, String dt) throws Exception {
        ObjectNode a = MAPPER.createObjectNode();
        ObjectNode add = a.putObject("add");
        add.put("path", path);
        add.putObject("partitionValues").put("dt", dt);
        add.put("size", 1_000_000 + r.nextInt(100_000_000));
        add.put("modificationTime", System.currentTimeMillis());
        add.put("dataChange", true);
        add.put("stats", stats(r));
        return a;
    }

    private static ObjectNode remove(String path, String dt) {
        ObjectNode a = MAPPER.createObjectNode();
        ObjectNode rm = a.putObject("remove");
        rm.put("path", path);
        rm.put("deletionTimestamp", System.currentTimeMillis());
        rm.put("dataChange", true);
        rm.put("extendedFileMetadata", true);
        rm.putObject("partitionValues").put("dt", dt);
        rm.put("size", 1_000_000);
        return a;
    }

    private static void writeCommit(File log, long version, List<ObjectNode> actions) throws Exception {
        StringBuilder sb = new StringBuilder();
        for (ObjectNode a : actions) {
            sb.append(MAPPER.writeValueAsString(a)).append('\n');
        }
        Files.writeString(new File(log, String.format("%020d.json", version)).toPath(), sb.toString(),
                StandardCharsets.UTF_8);
    }

    // Builds a table with `commits` commits of `filesPerCommit` adds, optionally removes `removeRatio` of them
    // before the checkpoint, checkpoints, then appends `tailCommits` json commits on top.
    private static String buildTable(int commits, int filesPerCommit, double removeRatio, int tailCommits)
            throws Exception {
        File dir = Files.createTempDirectory("delta_memgap").toFile();
        File log = new File(dir, "_delta_log");
        log.mkdirs();
        Random r = new Random(17);
        List<ObjectNode> first = new ArrayList<>();
        ObjectNode protocol = MAPPER.createObjectNode();
        protocol.putObject("protocol").put("minReaderVersion", 1).put("minWriterVersion", 2);
        first.add(protocol);
        ObjectNode meta = MAPPER.createObjectNode();
        ObjectNode md = meta.putObject("metaData");
        md.put("id", UUID.randomUUID().toString());
        md.putObject("format").put("provider", "parquet").putObject("options");
        md.put("schemaString", schemaString());
        md.putArray("partitionColumns").add("dt");
        md.putObject("configuration");
        md.put("createdTime", System.currentTimeMillis());
        first.add(meta);
        writeCommit(log, 0, first);
        long version = 0;
        List<String[]> live = new ArrayList<>();
        for (int c = 0; c < commits; c++) {
            List<ObjectNode> actions = new ArrayList<>();
            String dt = "2025-01-" + String.format("%02d", 1 + c % 28);
            for (int i = 0; i < filesPerCommit; i++) {
                String path = "dt=" + dt + "/part-" + String.format("%05d", i) + "-" + UUID.randomUUID() +
                        ".c000.snappy.parquet";
                actions.add(add(r, path, dt));
                live.add(new String[] {path, dt});
            }
            writeCommit(log, ++version, actions);
        }
        if (removeRatio > 0) {
            List<ObjectNode> actions = new ArrayList<>();
            int n = (int) (live.size() * removeRatio);
            for (int i = 0; i < n; i++) {
                // every other file, so removes interleave with adds the way compaction leaves them
                String[] f = live.get((i * 2) % live.size());
                actions.add(remove(f[0], f[1]));
            }
            writeCommit(log, ++version, actions);
        }
        DefaultEngine engine = DefaultEngine.create(new Configuration());
        Table.forPath(engine, dir.getAbsolutePath()).checkpoint(engine, version);
        for (int t = 0; t < tailCommits; t++) {
            List<ObjectNode> actions = new ArrayList<>();
            for (int i = 0; i < filesPerCommit; i++) {
                actions.add(add(r, "dt=2025-02-01/part-tail-" + t + "-" + i + "-" + UUID.randomUUID() +
                        ".c000.snappy.parquet", "2025-02-01"));
            }
            writeCommit(log, ++version, actions);
        }
        return dir.getAbsolutePath();
    }

    @SuppressWarnings("unchecked")
    private static <T> T field(Object o, Class<?> c, String name) throws Exception {
        Field f = c.getDeclaredField(name);
        f.setAccessible(true);
        return (T) f.get(o);
    }

    @Test
    public void testAddsOnly() throws Exception {
        measure("delta adds_only", 10, 1000, 0.0, 3);
    }

    @Test
    public void testHalfRemoved() throws Exception {
        // the removes leave one checkpoint batch half add rows, half remove rows: null slots in every column
        measure("delta half_removed", 10, 1000, 0.5, 3);
    }

    private void measure(String label, int commits, int filesPerCommit, double removeRatio, int tail)
            throws Exception {
        String path = buildTable(commits, filesPerCommit, removeRatio, tail);
        HiveMetaClient client = new HiveMetastoreTest.MockedHiveMetaClient();
        IHiveMetastore hiveMetastore = new HiveMetastore(client, "delta0", MetastoreType.HMS);
        DeltaLakeCatalogProperties properties = new DeltaLakeCatalogProperties(Maps.newHashMap());
        DeltaLakeMetastore metastore =
                new HMSBackedDeltaMetastore("delta0", hiveMetastore, new Configuration(), properties);
        Cache<Pair<DeltaLakeFileStatus, StructType>, List<ColumnarBatch>> checkpointCache =
                field(metastore, DeltaLakeMetastore.class, "checkpointCache");
        Cache<DeltaLakeFileStatus, List<JsonNode>> jsonCache = field(metastore, DeltaLakeMetastore.class, "jsonCache");
        DeltaLakeEngine engine = DeltaLakeEngine.create(new Configuration(), properties, checkpointCache, jsonCache);
        Snapshot snapshot = Table.forPath(engine, path).getLatestSnapshot(engine);
        Scan scan = snapshot.getScanBuilder().build();
        try (CloseableIterator<FilteredColumnarBatch> it = scan.getScanFiles(engine)) {
            while (it.hasNext()) {
                try (CloseableIterator<Row> rows = it.next().getRows()) {
                    rows.forEachRemaining(row -> { });
                }
            }
        }
        Assertions.assertEquals(1, checkpointCache.size());
        Assertions.assertEquals(tail, jsonCache.size());

        // The read schema in the checkpoint key is the kernel's, shared by every read; it is not counted.
        Method weighCheckpoint =
                DeltaLakeMetastore.class.getDeclaredMethod("weighCheckpointEntry", Pair.class, List.class);
        weighCheckpoint.setAccessible(true);
        long checkpointLedger = 0;
        List<Object> checkpointRoots = new ArrayList<>();
        for (Map.Entry<Pair<DeltaLakeFileStatus, StructType>, List<ColumnarBatch>> e :
                checkpointCache.asMap().entrySet()) {
            checkpointLedger += (Integer) weighCheckpoint.invoke(metastore, e.getKey(), e.getValue());
            checkpointRoots.add(e.getKey().first);
            checkpointRoots.add(e.getValue());
        }
        long checkpointActual = GraphLayout.parseInstance(checkpointRoots.toArray()).totalSize();
        check(label, "checkpoint ledger", checkpointLedger, checkpointActual);

        Method weighJson = DeltaLakeMetastore.class.getDeclaredMethod("weighJsonEntry", DeltaLakeFileStatus.class,
                List.class);
        weighJson.setAccessible(true);
        long jsonLedger = 0;
        List<Object> jsonRoots = new ArrayList<>();
        for (Map.Entry<DeltaLakeFileStatus, List<JsonNode>> e : jsonCache.asMap().entrySet()) {
            jsonLedger += (Integer) weighJson.invoke(metastore, e.getKey(), e.getValue());
            jsonRoots.add(e.getKey());
            jsonRoots.add(e.getValue());
        }
        long jsonActual = GraphLayout.parseInstance(jsonRoots.toArray()).totalSize();
        check(label, "json ledger", jsonLedger, jsonActual);

        check(label, "estimateSize()", metastore.estimateSize(), checkpointActual + jsonActual);
    }
}
