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

package com.starrocks.load.routineload;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.PulsarUtil;
import com.starrocks.proto.PPulsarMetaProxyResult;
import com.starrocks.proto.PPulsarProxyRequest;
import com.starrocks.proto.PPulsarProxyResult;
import com.starrocks.proto.StatusPB;
import com.starrocks.qe.ConnectContext;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.when;

public class PulsarRoutineLoadJobTest {

    @Mocked
    private SystemInfoService systemInfoService;
    @Mocked
    BackendServiceClient client;

    @Test
    public void testGetStatistic() {
        RoutineLoadJob job = new PulsarRoutineLoadJob(1L, "routine_load", 1L, 1L,
                "127.0.0.1:9020", "topic1", "");
        Deencapsulation.setField(job, "receivedBytes", 10);
        Deencapsulation.setField(job, "totalRows", 20);
        Deencapsulation.setField(job, "errorRows", 2);
        Deencapsulation.setField(job, "unselectedRows", 2);
        Deencapsulation.setField(job, "totalTaskExcutionTimeMs", 1000);
        String statistic = job.getStatistic();
        Assertions.assertTrue(statistic.contains("\"receivedBytesRate\":10"));
        Assertions.assertTrue(statistic.contains("\"loadRowsRate\":16"));
    }


    @Test
    public void testGetSourceLagString() {
        RoutineLoadJob job = new PulsarRoutineLoadJob(1L, "routine_load", 1L, 1L,
                "127.0.0.1:9020", "topic1", "");
        String sourceLagString = job.getSourceLagString(null);
        Assertions.assertTrue(sourceLagString.equals(""));
    }

    @Test
    public void getAllPulsarPartitions_returnsPartitionsSuccessfully() throws StarRocksException, RpcException {
        PulsarRoutineLoadJob job = new PulsarRoutineLoadJob(1L, "routine_load", 1L, 1L,
                "http://pulsar-service", "topic1", "sub1");
        Deencapsulation.setField(job, "convertedCustomProperties", ImmutableMap.of("key1", "value1"));

        List<String> partitions = Lists.newArrayList("partition1", "partition2");
        new MockUp<PulsarUtil>() {
            @Mock
            public List<String> getAllPulsarPartitions(String serviceUrl, String topic, String subscription,
                                                       Map<String, String> properties, String computeResource) {
                return partitions;
            }
        };

        List<Long> beIds = Lists.newArrayList(1L, 2L, 3L, 4L, 5L);
        new Expectations() {
            {
                systemInfoService.getBackendIds(true);
                minTimes = 0;
                result = beIds;
            }
        };
        StatusPB status = new com.starrocks.proto.StatusPB();
        status.setStatusCode(0);
        PPulsarProxyResult proxyResult = new PPulsarProxyResult();
        proxyResult.setStatus(status);
        PPulsarMetaProxyResult pulsarMetaResult = new PPulsarMetaProxyResult();
        pulsarMetaResult.setPartitions(partitions);
        proxyResult.setPulsarMetaResult(pulsarMetaResult);
        new Expectations() {
            {
                client.getPulsarInfo((TNetworkAddress) any, (PPulsarProxyRequest) any);
                result = CompletableFuture.completedFuture(proxyResult);
            }
        };

        List<String> result = job.getAllPulsarPartitions();
        Assertions.assertEquals(partitions, result);
    }

    @Test
    public void getAllPulsarPartitions_throwsExceptionOnError() {
        PulsarRoutineLoadJob job = new PulsarRoutineLoadJob(1L, "routine_load", 1L, 1L,
                "http://pulsar-service", "topic1", "sub1");
        Deencapsulation.setField(job, "convertedCustomProperties", ImmutableMap.of("key1", "value1"));

        new MockUp<PulsarUtil>() {
            @Mock
            public List<String> getAllPulsarPartitions(String serviceUrl,
                                                       String topic,
                                                       String subscription,
                                                       Map<String, String> properties,
                                                       String computeResource) throws StarRocksException {
                throw new StarRocksException("Error fetching partitions");
            }
        };

        Assertions.assertThrows(StarRocksException.class, () -> {
            job.getAllPulsarPartitions();
        });
    }

    @Test
    public void fromCreateStmt_createsJobSuccessfully() throws StarRocksException {
        CreateRoutineLoadStmt stmt = Mockito.mock(CreateRoutineLoadStmt.class);
        when(stmt.getDBName()).thenReturn("test_db");
        when(stmt.getTableName()).thenReturn("test_table");
        when(stmt.getPulsarServiceUrl()).thenReturn("http://pulsar-service");
        when(stmt.getPulsarTopic()).thenReturn("topic1");
        when(stmt.getPulsarSubscription()).thenReturn("sub1");

        Table table = Mockito.mock(OlapTable.class);
        when(table.getName()).thenReturn("test_table");
        when(table.getId()).thenReturn(1L);
        when(table.isOlapOrCloudNativeTable()).thenReturn(true);

        Database db = Mockito.mock(Database.class);
        when(db.getId()).thenReturn(1L);

        when(db.getTable("test_table")).thenReturn(null);

        new MockUp<LocalMetastore>() {
            @Mock
            public Database getDb(String dbName) {
                return db;
            }

            @Mock
            public Table getTable(String dbName, String tableName) {
                return table;
            }
        };
        new MockUp<GlobalStateMgr>() {
            @Mock
            public long getNextId() {
                return 1L;
            }
        };
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        PulsarRoutineLoadJob job = PulsarRoutineLoadJob.fromCreateStmt(stmt);
        Assertions.assertNotNull(job);
        Assertions.assertEquals("http://pulsar-service", job.getServiceUrl());
        Assertions.assertEquals("topic1", job.getTopic());
        Assertions.assertEquals("sub1", job.getSubscription());
    }

    @Test
    public void fromCreateStmt_throwsExceptionForInvalidDb() {
        CreateRoutineLoadStmt stmt = Mockito.mock(CreateRoutineLoadStmt.class);
        when(stmt.getDBName()).thenReturn("invalid_db");

        new MockUp<GlobalStateMgr>() {
            @Mock
            public Database getDb(String dbName) {
                return null;
            }
        };
        Assertions.assertThrows(StarRocksException.class, () -> {
            PulsarRoutineLoadJob.fromCreateStmt(stmt);
        });
    }
}
