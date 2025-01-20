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
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class CacheRelaxDictManagerTest {
    private static ConnectContext ctx;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        ConnectorPlanTestBase.mockHiveCatalog(ctx);
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
    public void  testLoaderDeserialize() throws ExecutionException, InterruptedException {
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
                return new Pair<>(List.of(generateDictResult(100)), new Status(TStatusCode.OK, "ok"));
            }
        };
        CompletableFuture<Optional<ColumnDict>> future = dictStatistics.get(key);
        Optional<ColumnDict> optionalColumnDict = future.get();
        Assert.assertFalse(optionalColumnDict.isEmpty());
        Assert.assertEquals(100, optionalColumnDict.get().getDictSize());
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
                return new Pair<>(List.of(generateDictResult(1000)), new Status(TStatusCode.OK, "ok"));
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

}
