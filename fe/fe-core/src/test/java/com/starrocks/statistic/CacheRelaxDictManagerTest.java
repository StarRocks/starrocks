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


package com.starrocks.statistic;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.connector.statistics.ConnectorTableColumnKey;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.CacheRelaxDictManager;
import com.starrocks.sql.optimizer.statistics.ColumnDict;
import com.starrocks.sql.optimizer.statistics.IRelaxDictManager;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TGlobalDict;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class CacheRelaxDictManagerTest {
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();
    private static ConnectContext ctx;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        ConnectorPlanTestBase.mockAllCatalogs(ctx, temp.newFolder().toURI().toString());
    }

    @After
    public void tearDown() {}

    private TResultBatch generateDictResult(int size) throws TException {
        TStatisticData sd = new TStatisticData();
        TGlobalDict dict = new TGlobalDict();
        TreeSet<ByteBuffer> orderSet = new TreeSet<>();
        for (int i = 0; i < size; i++) {
            orderSet.add(ByteBuffer.wrap(Integer.toString(i).getBytes(StandardCharsets.UTF_8)));
        }

        List<Integer> ids = new ArrayList<>();
        List<ByteBuffer> values = new ArrayList<>();
        int index = 1;
        for (ByteBuffer v : orderSet) {
            ids.add(index);
            values.add(v);
            index++;
        }
        dict.setIds(ids);
        dict.setStrings(values);
        sd.setDict(dict);

        TResultBatch resultBatch = new TResultBatch();
        resultBatch.setStatistic_version(StatsConstants.STATISTIC_DICT_VERSION);
        TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
        ByteBuffer bb = ByteBuffer.wrap(serializer.serialize(sd));
        resultBatch.addToRows(bb);

        return resultBatch;
    }

    @Test
    public void  testLoader() throws ExecutionException, InterruptedException {
        AsyncLoadingCache<ConnectorTableColumnKey, Optional<ColumnDict>> dictStatistics = Caffeine.newBuilder()
                .maximumSize(Config.statistic_dict_columns)
                .buildAsync(new CacheRelaxDictManager.DictLoader());
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable("hive0",
                "tpch", "part");
        String tableUUID = table.getUUID();
        ConnectorTableColumnKey key = new ConnectorTableColumnKey(tableUUID, "p_mfgr");
        new MockUp<StmtExecutor>() {
            @Mock
            public Pair<List<TResultBatch>, Status> executeStmtWithExecPlan(ConnectContext context, ExecPlan plan) {
                return new Pair<>(new ArrayList<>(), new Status(TStatusCode.OK, "ok"));
            }
        };
        CompletableFuture<Optional<ColumnDict>> future = dictStatistics.get(key);
        Optional<ColumnDict> optionalColumnDict = future.get();
        Assert.assertTrue(optionalColumnDict.isEmpty());
    }

    @Test
    public void  testLoaderError() throws ExecutionException, InterruptedException {
        AsyncLoadingCache<ConnectorTableColumnKey, Optional<ColumnDict>> dictStatistics = Caffeine.newBuilder()
                .maximumSize(Config.statistic_dict_columns)
                .buildAsync(new CacheRelaxDictManager.DictLoader());
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable("hive0",
                "tpch", "part");
        String tableUUID = table.getUUID();
        ConnectorTableColumnKey key = new ConnectorTableColumnKey(tableUUID, "p_mfgr");
        new MockUp<StmtExecutor>() {
            @Mock
            public Pair<List<TResultBatch>, Status> executeStmtWithExecPlan(ConnectContext context, ExecPlan plan) {
                return new Pair<>(new ArrayList<>(), new Status(TStatusCode.INTERNAL_ERROR, "some error"));
            }
        };
        CompletableFuture<Optional<ColumnDict>> future = dictStatistics.get(key);
        Optional<ColumnDict> optionalColumnDict = future.get();
        Assert.assertTrue(optionalColumnDict.isEmpty());
    }

    @Test
    public void  testLoaderDeserialize() throws ExecutionException, InterruptedException {
        AsyncLoadingCache<ConnectorTableColumnKey, Optional<ColumnDict>> dictStatistics = Caffeine.newBuilder()
                .maximumSize(Config.statistic_dict_columns)
                .refreshAfterWrite(1, TimeUnit.SECONDS)
                .buildAsync(new CacheRelaxDictManager.DictLoader());
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable("hive0",
                "tpch", "part");
        String tableUUID = table.getUUID();
        ConnectorTableColumnKey key = new ConnectorTableColumnKey(tableUUID, "p_mfgr");
        int size = Config.low_cardinality_threshold / 2;
        new MockUp<StmtExecutor>() {
            @Mock
            public Pair<List<TResultBatch>, Status> executeStmtWithExecPlan(ConnectContext context, ExecPlan plan)
                    throws TException {
                return new Pair<>(List.of(generateDictResult(size)), new Status(TStatusCode.OK, "ok"));
            }
        };
        CompletableFuture<Optional<ColumnDict>> future = dictStatistics.get(key);
        Optional<ColumnDict> optionalColumnDict = future.get();
        Assert.assertFalse(optionalColumnDict.isEmpty());
        Assert.assertEquals(size, optionalColumnDict.get().getDictSize());
        Thread.sleep(2000);
        Assert.assertTrue(dictStatistics.get(key).get().isPresent());
    }

    @Test
    public void  testLoaderOversize() throws ExecutionException, InterruptedException {
        AsyncLoadingCache<ConnectorTableColumnKey, Optional<ColumnDict>> dictStatistics = Caffeine.newBuilder()
                .maximumSize(Config.statistic_dict_columns)
                .buildAsync(new CacheRelaxDictManager.DictLoader());
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable("hive0",
                "tpch", "part");
        String tableUUID = table.getUUID();
        ConnectorTableColumnKey key = new ConnectorTableColumnKey(tableUUID, "p_mfgr");
        new MockUp<StmtExecutor>() {
            @Mock
            public Pair<List<TResultBatch>, Status> executeStmtWithExecPlan(ConnectContext context, ExecPlan plan)
                    throws TException {
                int size = Config.low_cardinality_threshold + 2;
                return new Pair<>(List.of(generateDictResult(size)), new Status(TStatusCode.OK, "ok"));
            }
        };
        CompletableFuture<Optional<ColumnDict>> future = dictStatistics.get(key);
        Optional<ColumnDict> optionalColumnDict = future.get();
        Assert.assertTrue(optionalColumnDict.isEmpty());
    }

    @Test
    public void testHasGlobalDict() {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable("hive0",
                "tpch", "part");
        String tableUUID = table.getUUID();
        // clear
        IRelaxDictManager.getInstance().removeGlobalDict(tableUUID, "p_mfgr");
        Assert.assertTrue(IRelaxDictManager.getInstance().hasGlobalDict(tableUUID, "p_mfgr"));
        IRelaxDictManager.getInstance().invalidTemporarily(tableUUID, "p_mfgr");
        Assert.assertFalse(IRelaxDictManager.getInstance().hasGlobalDict(tableUUID, "p_mfgr"));
        IRelaxDictManager.getInstance().removeTemporaryInvalid(tableUUID, "p_mfgr");
        Assert.assertTrue(IRelaxDictManager.getInstance().hasGlobalDict(tableUUID, "p_mfgr"));
    }

    @Test
    public void testGlobalDict() throws InterruptedException, TException {
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable("hive0",
                "tpch", "part");
        String tableUUID = table.getUUID();
        // clear
        String columnName = "p_mfgr";
        IRelaxDictManager manager = IRelaxDictManager.getInstance();
        manager.removeGlobalDict(tableUUID, columnName);
        Assert.assertTrue(manager.hasGlobalDict(tableUUID, columnName));
        int size = Config.low_cardinality_threshold / 2;
        new MockUp<StmtExecutor>() {
            @Mock
            public Pair<List<TResultBatch>, Status> executeStmtWithExecPlan(ConnectContext context, ExecPlan plan)
                    throws TException {
                return new Pair<>(List.of(generateDictResult(size)), new Status(TStatusCode.OK, "ok"));
            }
        };
        int retry = 5;
        // wait for loading
        while (manager.getGlobalDict(tableUUID, columnName).isEmpty() && retry > 0) {
            retry--;
            Thread.sleep(1000);
        }
        if (manager.getGlobalDict(tableUUID, columnName).isPresent()) {
            ColumnDict columnDict = manager.getGlobalDict(tableUUID, columnName).get();
            Assert.assertEquals(size, columnDict.getDictSize());
            TResultBatch resultBatch = generateDictResult(size + 1);

            TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
            ByteBuffer buffer = resultBatch.rows.get(0);
            TStatisticData sd = new TStatisticData();
            byte[] bytes = new byte[buffer.limit() - buffer.position()];
            buffer.get(bytes);
            deserializer.deserialize(sd, bytes);

            manager.updateGlobalDict(tableUUID, columnName, Optional.of(sd));
            Optional<ColumnDict> optional = manager.getGlobalDict(tableUUID, columnName);
            Assert.assertTrue(optional.isPresent());
            Assert.assertEquals(size + 1, optional.get().getDictSize());
            TStatisticData nullDict = new TStatisticData();
            manager.updateGlobalDict(tableUUID, columnName, Optional.of(nullDict));
            optional = manager.getGlobalDict(tableUUID, columnName);
            Assert.assertTrue(optional.isPresent());
            Assert.assertEquals(size + 1, optional.get().getDictSize());
            Assert.assertEquals(1,
                    ((CacheRelaxDictManager) manager).estimateCount().get("ExternalTableColumnDict").intValue());
            Assert.assertEquals(1, ((CacheRelaxDictManager) manager).getSamples().get(0).first.size());

            manager.removeGlobalDict(tableUUID, columnName);
        }
    }

}
