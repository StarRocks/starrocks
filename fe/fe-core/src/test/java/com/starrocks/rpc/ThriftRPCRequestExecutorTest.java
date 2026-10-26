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

package com.starrocks.rpc;

import com.starrocks.metric.LongCounterMetric;
import com.starrocks.metric.Metric;
import com.starrocks.metric.MetricRepo;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransportException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ThriftRPCRequestExecutorTest {
    private static final TNetworkAddress ADDRESS = new TNetworkAddress("127.0.0.1", 9020);
    private static final int TIMEOUT_MS = 1000;

    private final ThriftRPCRequestExecutor.Sleeper savedSleeper = ThriftRPCRequestExecutor.sleeper;
    private final List<Long> sleeps = new ArrayList<>();
    private boolean savedMetricHasInit;
    private LongCounterMetric savedRetriesCounter;

    private ThriftConnectionPool<TServiceClient> pool;
    private TServiceClient client;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        ThriftRPCRequestExecutor.sleeper = sleeps::add;
        // MetricRepo.init() is not run in this test; wire a fresh counter so each test can read it.
        savedMetricHasInit = MetricRepo.hasInit;
        savedRetriesCounter = MetricRepo.COUNTER_THRIFT_CLIENT_RETRIES;
        MetricRepo.COUNTER_THRIFT_CLIENT_RETRIES =
                new LongCounterMetric("thrift_client_retries_total", Metric.MetricUnit.REQUESTS, "test");
        MetricRepo.hasInit = true;
        pool = Mockito.mock(ThriftConnectionPool.class);
        client = Mockito.mock(TServiceClient.class);
        Mockito.when(pool.borrowObject(ADDRESS, TIMEOUT_MS)).thenReturn(client);
        Mockito.when(pool.reopen(client, TIMEOUT_MS)).thenReturn(true);
    }

    @AfterEach
    public void tearDown() {
        ThriftRPCRequestExecutor.sleeper = savedSleeper;
        MetricRepo.hasInit = savedMetricHasInit;
        MetricRepo.COUNTER_THRIFT_CLIENT_RETRIES = savedRetriesCounter;
        // Clear any interrupt status a test left behind, so it cannot leak into the next test.
        Thread.interrupted();
    }

    private static long retriesCounted() {
        return MetricRepo.COUNTER_THRIFT_CLIENT_RETRIES.getValue();
    }

    private static TTransportException brokenConnection() {
        return new TTransportException(new SocketException("Broken pipe"));
    }

    private static TTransportException timedOut() {
        return new TTransportException(new SocketTimeoutException("Read timed out"));
    }

    @Test
    public void testBackoffBoundGrowsExponentiallyUpToTheCap() {
        Assertions.assertEquals(200L, ThriftRPCRequestExecutor.retryBackoffBoundMs(0));
        Assertions.assertEquals(400L, ThriftRPCRequestExecutor.retryBackoffBoundMs(1));
        Assertions.assertEquals(800L, ThriftRPCRequestExecutor.retryBackoffBoundMs(2));
        Assertions.assertEquals(1000L, ThriftRPCRequestExecutor.retryBackoffBoundMs(3));
        Assertions.assertEquals(1000L, ThriftRPCRequestExecutor.retryBackoffBoundMs(4));
        // A large retry count must neither overflow the shift nor pass the cap.
        Assertions.assertEquals(1000L, ThriftRPCRequestExecutor.retryBackoffBoundMs(63));
        Assertions.assertEquals(1000L, ThriftRPCRequestExecutor.retryBackoffBoundMs(Integer.MAX_VALUE));
    }

    @Test
    public void testBrokenConnectionIsRetriedAfterJitteredBackoff() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        String result = ThriftRPCRequestExecutor.call(pool, ADDRESS, TIMEOUT_MS, 3, c -> {
            if (calls.getAndIncrement() < 2) {
                throw brokenConnection();
            }
            return "ok";
        });

        Assertions.assertEquals("ok", result);
        Assertions.assertEquals(3, calls.get());
        Assertions.assertEquals(2, sleeps.size());
        // Each wait is drawn from the upper half of its bound: 100-200 ms, then 200-400 ms.
        for (int retry = 0; retry < sleeps.size(); retry++) {
            long sleepMs = sleeps.get(retry);
            long boundMs = ThriftRPCRequestExecutor.retryBackoffBoundMs(retry);
            Assertions.assertTrue(sleepMs >= boundMs / 2 && sleepMs <= boundMs,
                    "retry " + retry + " slept " + sleepMs + " ms");
        }
        Assertions.assertEquals(2L, retriesCounted());
        Mockito.verify(pool, Mockito.times(2)).reopen(client, TIMEOUT_MS);
        Mockito.verify(pool).returnObject(ADDRESS, client);
        Mockito.verify(pool, Mockito.never()).invalidateObject(ADDRESS, client);
    }

    @Test
    public void testEveryBackoffStaysInTheUpperHalfOfItsBound() throws Exception {
        Assertions.assertThrows(TTransportException.class,
                () -> ThriftRPCRequestExecutor.call(pool, ADDRESS, TIMEOUT_MS, 40, c -> {
                    throw brokenConnection();
                }));

        Assertions.assertEquals(39, sleeps.size());
        for (int retry = 0; retry < sleeps.size(); retry++) {
            long sleepMs = sleeps.get(retry);
            long boundMs = ThriftRPCRequestExecutor.retryBackoffBoundMs(retry);
            Assertions.assertTrue(sleepMs >= boundMs / 2 && sleepMs <= boundMs,
                    "retry " + retry + " slept " + sleepMs + " ms");
        }
        // Past the cap every wait is 500-1000 ms.
        Assertions.assertTrue(sleeps.subList(3, sleeps.size()).stream().allMatch(ms -> ms >= 500 && ms <= 1000));
    }

    @Test
    public void testBackoffHappensBeforeReconnect() throws Exception {
        List<String> events = new ArrayList<>();
        ThriftRPCRequestExecutor.sleeper = millis -> events.add("sleep");
        Mockito.when(pool.reopen(client, TIMEOUT_MS)).thenAnswer(invocation -> {
            events.add("reopen");
            return true;
        });
        AtomicInteger calls = new AtomicInteger();

        ThriftRPCRequestExecutor.call(pool, ADDRESS, TIMEOUT_MS, 3, c -> {
            events.add("call");
            if (calls.getAndIncrement() == 0) {
                throw brokenConnection();
            }
            return "ok";
        });

        Assertions.assertEquals(List.of("call", "sleep", "reopen", "call"), events);
    }

    @Test
    public void testTimeoutIsNotRetriedAndDoesNotBackOff() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        Assertions.assertThrows(TTransportException.class,
                () -> ThriftRPCRequestExecutor.call(pool, ADDRESS, TIMEOUT_MS, 3, c -> {
                    calls.incrementAndGet();
                    throw timedOut();
                }));

        Assertions.assertEquals(1, calls.get());
        Assertions.assertTrue(sleeps.isEmpty());
        Assertions.assertEquals(0L, retriesCounted());
        // As before, the conn is reopened so a healthy one goes back to the pool.
        Mockito.verify(pool).reopen(client, TIMEOUT_MS);
        Mockito.verify(pool).returnObject(ADDRESS, client);
    }

    @Test
    public void testLastAttemptDoesNotBackOff() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        Assertions.assertThrows(TTransportException.class,
                () -> ThriftRPCRequestExecutor.call(pool, ADDRESS, TIMEOUT_MS, 3, c -> {
                    calls.incrementAndGet();
                    throw brokenConnection();
                }));

        Assertions.assertEquals(3, calls.get());
        Assertions.assertEquals(2, sleeps.size());
        Assertions.assertEquals(2L, retriesCounted());
        Mockito.verify(pool, Mockito.times(3)).reopen(client, TIMEOUT_MS);
    }

    @Test
    public void testNoRetryCallDoesNotBackOff() throws Exception {
        Mockito.when(pool.getDefaultTimeoutMs()).thenReturn(TIMEOUT_MS);
        AtomicInteger calls = new AtomicInteger();
        Assertions.assertThrows(TTransportException.class,
                () -> ThriftRPCRequestExecutor.callNoRetry(pool, ADDRESS, c -> {
                    calls.incrementAndGet();
                    throw brokenConnection();
                }));

        Assertions.assertEquals(1, calls.get());
        Assertions.assertTrue(sleeps.isEmpty());
        Assertions.assertEquals(0L, retriesCounted());
    }

    @Test
    public void testFailedReopenAfterBackoffGivesUp() throws Exception {
        Mockito.when(pool.reopen(client, TIMEOUT_MS)).thenReturn(false);
        AtomicInteger calls = new AtomicInteger();
        TTransportException failure = brokenConnection();
        TTransportException thrown = Assertions.assertThrows(TTransportException.class,
                () -> ThriftRPCRequestExecutor.call(pool, ADDRESS, TIMEOUT_MS, 3, c -> {
                    calls.incrementAndGet();
                    throw failure;
                }));

        Assertions.assertSame(failure, thrown);
        Assertions.assertEquals(1, calls.get());
        Assertions.assertEquals(1, sleeps.size());
        // Backed off, but nothing was resent.
        Assertions.assertEquals(0L, retriesCounted());
        Mockito.verify(pool).invalidateObject(ADDRESS, client);
        Mockito.verify(pool, Mockito.never()).returnObject(ADDRESS, client);
    }

    @Test
    public void testInterruptedBackoffInvalidatesTheConnection() throws Exception {
        // The first retry reopens fine, which marks the conn valid. The second backoff is
        // interrupted before the reopen, so the conn is still broken and must not go back to the pool.
        InterruptedException interrupt = new InterruptedException("stop");
        ThriftRPCRequestExecutor.sleeper = millis -> {
            sleeps.add(millis);
            if (sleeps.size() == 2) {
                throw interrupt;
            }
        };
        AtomicInteger calls = new AtomicInteger();
        TTransportException secondFailure = brokenConnection();
        TTransportException thrown = Assertions.assertThrows(TTransportException.class,
                () -> ThriftRPCRequestExecutor.call(pool, ADDRESS, TIMEOUT_MS, 5, c -> {
                    if (calls.getAndIncrement() == 0) {
                        throw brokenConnection();
                    }
                    throw secondFailure;
                }));

        Assertions.assertSame(secondFailure, thrown);
        Assertions.assertSame(interrupt, thrown.getSuppressed()[0]);
        Assertions.assertTrue(Thread.currentThread().isInterrupted());
        Assertions.assertEquals(2, calls.get());
        // Only the first retry was resent; the interrupted one was not.
        Assertions.assertEquals(1L, retriesCounted());
        Mockito.verify(pool, Mockito.times(1)).reopen(client, TIMEOUT_MS);
        Mockito.verify(pool).invalidateObject(ADDRESS, client);
        Mockito.verify(pool, Mockito.never()).returnObject(ADDRESS, client);
    }
}
