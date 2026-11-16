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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.common.Pair;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.connector.hive.HiveMetastore;
import com.starrocks.connector.hive.HiveMetastoreTest;
import com.starrocks.connector.metastore.MetastoreTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import io.delta.kernel.Table;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.openjdk.jol.info.GraphLayout;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * FE planning micro-benchmark on a synthetic, metadata-heavy Delta table (no data files: planning only).
 * Skipped unless DELTA_PLANNING_BENCH_FILES is set, e.g.
 * DELTA_PLANNING_BENCH_FILES=200000 mvn test -Dtest=DeltaLakePlanningBench ...
 * Prints one BENCH line per phase.
 */
public class DeltaLakePlanningBench {
    static {
        System.setProperty("jol.magicFieldOffset", "true");
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int COLUMNS = 16;

    private static int env(String name, int def) {
        String v = System.getenv(name);
        return v == null || v.isEmpty() ? def : Integer.parseInt(v);
    }

    private static String schemaString() throws Exception {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("type", "struct");
        ArrayNode fields = root.putArray("fields");
        for (int i = 0; i < COLUMNS; i++) {
            ObjectNode f = fields.addObject();
            f.put("name", "c" + i);
            f.put("type", i % 4 == 0 ? "string" : "long");
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

    private static String stats(Random r) throws Exception {
        ObjectNode s = MAPPER.createObjectNode();
        s.put("numRecords", 1000 + r.nextInt(100000));
        ObjectNode min = s.putObject("minValues");
        ObjectNode max = s.putObject("maxValues");
        ObjectNode nulls = s.putObject("nullCount");
        for (int i = 0; i < COLUMNS; i++) {
            String c = "c" + i;
            if (i % 4 == 0) {
                min.put(c, "aaaa" + r.nextInt(1000));
                max.put(c, "zzzz" + r.nextInt(1000));
            } else {
                min.put(c, r.nextInt(1 << 30));
                max.put(c, (1L << 31) + r.nextInt(1 << 30));
            }
            nulls.put(c, r.nextInt(10));
        }
        return MAPPER.writeValueAsString(s);
    }

    private static void writeCommit(File log, long version, List<ObjectNode> actions) throws Exception {
        StringBuilder sb = new StringBuilder();
        for (ObjectNode a : actions) {
            sb.append(MAPPER.writeValueAsString(a)).append('\n');
        }
        Files.writeString(new File(log, String.format("%020d.json", version)).toPath(), sb.toString(),
                StandardCharsets.UTF_8);
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

    // `files` adds spread over `partitions` dt values, `filesPerCommit` per commit, then one checkpoint,
    // then `tailCommits` small json commits on top.
    static String buildTable(int files, int partitions, int filesPerCommit, int tailCommits)
            throws Exception {
        File dir = Files.createTempDirectory("delta_plan_bench").toFile();
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
        int written = 0;
        while (written < files) {
            List<ObjectNode> actions = new ArrayList<>();
            for (int i = 0; i < filesPerCommit && written < files; i++, written++) {
                String dt = String.format("2025-%04d", written % partitions);
                actions.add(add(r, "dt=" + dt + "/part-" + written + "-" + UUID.randomUUID() + ".parquet", dt));
            }
            writeCommit(log, ++version, actions);
        }
        DefaultEngine engine = DefaultEngine.create(new Configuration());
        Table.forPath(engine, dir.getAbsolutePath()).checkpoint(engine, version);
        for (int t = 0; t < tailCommits; t++) {
            List<ObjectNode> actions = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                String dt = String.format("2025-%04d", i % partitions);
                actions.add(add(r, "dt=" + dt + "/part-tail-" + t + "-" + i + "-" + UUID.randomUUID() + ".parquet",
                        dt));
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

    static final class Env {
        DeltaLakeEngine engine;
        DeltaLakeTable table;
    }

    static Env openTable(String path) throws Exception {
        HiveMetaClient client = new HiveMetastoreTest.MockedHiveMetaClient();
        DeltaLakeCatalogProperties properties = new DeltaLakeCatalogProperties(Maps.newHashMap());
        DeltaLakeMetastore metastore = new HMSBackedDeltaMetastore("delta0",
                new HiveMetastore(client, "delta0", MetastoreType.HMS), new Configuration(), properties);
        Cache<Pair<DeltaLakeFileStatus, StructType>, List<ColumnarBatch>> checkpointCache =
                field(metastore, DeltaLakeMetastore.class, "checkpointCache");
        Cache<DeltaLakeFileStatus, List<JsonNode>> jsonCache = field(metastore, DeltaLakeMetastore.class, "jsonCache");
        Env env = new Env();
        env.engine = DeltaLakeEngine.create(new Configuration(), properties, checkpointCache, jsonCache);
        SnapshotImpl snapshot = (SnapshotImpl) Table.forPath(env.engine, path).getLatestSnapshot(env.engine);
        env.table = DeltaUtils.convertDeltaSnapshotToSRTable("delta0", new DeltaLakeSnapshot("db", "t", env.engine,
                snapshot, new MetastoreTable("db", "t", path, 123)));
        return env;
    }

    static DeltaLakeMetadata newMetadata() {
        return new DeltaLakeMetadata(new HdfsEnvironment(Maps.newHashMap()), "delta0", null, null,
                new ConnectorProperties(ConnectorType.DELTALAKE));
    }

    private static void bench(String label, long startNs, String extra) {
        System.out.printf("BENCH %-34s %8.1f ms %s%n", label, (System.nanoTime() - startNs) / 1e6, extra);
    }

    private static void round(String label, Env env, OptimizerContext oc,
                              Map<ColumnRefOperator, Column> columns) throws Exception {
        long t = System.nanoTime();
        long rows = 0;
        try (CloseableIterator<FilteredColumnarBatch> it =
                     env.table.getDeltaSnapshot().getScanBuilder().build().getScanFiles(env.engine)) {
            while (it.hasNext()) {
                try (CloseableIterator<Row> rs = it.next().getRows()) {
                    while (rs.hasNext()) {
                        rs.next();
                        rows++;
                    }
                }
            }
        }
        bench(label + " kernel replay", t, "files=" + rows);

        t = System.nanoTime();
        rows = 0;
        try (CloseableIterator<FilteredColumnarBatch> it = ((io.delta.kernel.internal.ScanImpl)
                env.table.getDeltaSnapshot().getScanBuilder().build()).getScanFiles(env.engine, true)) {
            while (it.hasNext()) {
                try (CloseableIterator<Row> rs = it.next().getRows()) {
                    while (rs.hasNext()) {
                        rs.next();
                        rows++;
                    }
                }
            }
        }
        bench(label + " kernel replay +stats", t, "files=" + rows);

        for (boolean retain : new boolean[] {false, true}) {
            List<FileScanTask> kept = new ArrayList<>();
            t = System.nanoTime();
            rows = 0;
            try (CloseableIterator<Pair<FileScanTask, DeltaLakeAddFileStatsSerDe>> it = newMetadata()
                    .buildFileScanTaskIterator(env.table, DeltaScanContext.of(env.table), null, false)) {
                while (it.hasNext()) {
                    FileScanTask task = it.next().first;
                    rows++;
                    if (retain) {
                        kept.add(task);
                    }
                }
            }
            bench(label + " convert to FileScanTask retain=" + retain, t, "files=" + rows + " kept=" + kept.size());
        }

        // the per-file bookkeeping collectDeltaLakePlanFiles adds on top of the conversion
        for (int variant = 0; variant < 3; variant++) {
            DeltaStatisticProvider provider = new DeltaStatisticProvider();
            com.starrocks.connector.PredicateSearchKey key = com.starrocks.connector.PredicateSearchKey.of("db", "t",
                    GetRemoteFilesParams.newBuilder().setTableVersionRange(
                            TvrTableSnapshot.of(env.table.getDeltaSnapshot().getVersion())).build());
            java.util.Set<String> none = new java.util.HashSet<>();
            java.util.Set<String> part = java.util.Set.of("dt");
            t = System.nanoTime();
            try (CloseableIterator<Pair<FileScanTask, DeltaLakeAddFileStatsSerDe>> it = newMetadata()
                    .buildFileScanTaskIterator(env.table, DeltaScanContext.of(env.table), null, false)) {
                while (it.hasNext()) {
                    Pair<FileScanTask, DeltaLakeAddFileStatsSerDe> p = it.next();
                    if (variant == 1) {
                        provider.updateFileStats(env.table, key, p.first, p.second, none, part);
                    } else if (variant == 2) {
                        try (com.starrocks.common.profile.Timer ignored = com.starrocks.common.profile.Tracers.watchScope(
                                com.starrocks.common.profile.Tracers.Module.EXTERNAL, "DELTA_LAKE.x")) {
                            provider.updateFileStats(env.table, key, p.first, p.second, none, part);
                        }
                    }
                }
            }
            bench(label + " convert + bookkeeping v" + variant, t, "");
        }

        for (boolean colStats : new boolean[] {false, true}) {
            oc.getSessionVariable().setEnableDeltaLakeColumnStatistics(colStats);
            DeltaLakeMetadata metadata = newMetadata();
            t = System.nanoTime();
            Statistics stats = metadata.getTableStatistics(oc, env.table, columns, null, null, -1,
                    TvrTableSnapshot.of(env.table.getDeltaSnapshot().getVersion()));
            long elapsed = System.nanoTime() - t;
            Map<?, ?> splitTasks = field(metadata, DeltaLakeMetadata.class, "splitTasks");
            // the JOL walk is slow; it runs after the timing is taken
            long retained = GraphLayout.parseInstance(splitTasks).totalSize();
            System.out.printf("BENCH %-34s %8.1f ms rows=%.0f retainedSplitTasks=%dMB%n",
                    label + " getTableStatistics colStats=" + colStats, elapsed / 1e6, stats.getOutputRowCount(),
                    retained >> 20);

            if (!colStats) {
                t = System.nanoTime();
                long n = 0;
                RemoteFileInfoSource source = metadata.getRemoteFilesAsync(env.table,
                        GetRemoteFilesParams.newBuilder().setTableVersionRange(
                                TvrTableSnapshot.of(env.table.getDeltaSnapshot().getVersion())).build());
                while (source.hasMoreOutput()) {
                    source.getOutput();
                    n++;
                }
                source.close();
                bench(label + " scan enumerate", t, "files=" + n);
            }
        }
    }

    @Test
    public void bench() throws Exception {
        int files = env("DELTA_PLANNING_BENCH_FILES", 0);
        Assumptions.assumeTrue(files > 0, "set DELTA_PLANNING_BENCH_FILES to run");
        int partitions = env("DELTA_PLANNING_BENCH_PARTITIONS", 1000);
        int filesPerCommit = env("DELTA_PLANNING_BENCH_FILES_PER_COMMIT", 5000);

        long t = System.nanoTime();
        String path = buildTable(files, partitions, filesPerCommit, 3);
        bench("build table", t, "files=" + files + " partitions=" + partitions + " path=" + path);

        ConnectContext ctx = new ConnectContext();
        ctx.setThreadLocalInfo();
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext oc = OptimizerFactory.mockContext(ctx, factory);

        Env cold = openTable(path);
        Map<ColumnRefOperator, Column> columns = new HashMap<>();
        for (Column c : cold.table.getFullSchema()) {
            columns.put(factory.create(c.getName(), c.getType(), c.isAllowNull()), c);
        }
        round("cold", cold, oc, columns);
        // same engine: checkpoint/json caches are warm now
        round("warm", cold, oc, columns);
    }
}
