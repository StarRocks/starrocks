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

package com.starrocks.common;

import com.starrocks.metric.MetricRepo;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SRTThreadPoolServerTest {
    private static boolean metricRepoLoaded;

    private final long savedQueueTimeoutMs = Config.thrift_server_queue_timeout_ms;

    @BeforeAll
    public static void loadMetricRepoClass() {
        // The rejection path reads MetricRepo.hasInit, and that first read forces MetricRepo's class
        // initialization, dragging in ThreadPoolManager, Config and a few hundred metric objects.
        // Do it here so the burst is not charged to the timed test below.
        metricRepoLoaded = MetricRepo.hasInit;
    }

    @AfterEach
    public void restoreConfig() {
        Config.thrift_server_queue_timeout_ms = savedQueueTimeoutMs;
    }

    @Test
    public void testRejectedConnectionsAreClosedWithoutRetry() throws Exception {
        TServerTransport serverTransport = Mockito.mock(TServerTransport.class);
        TTransport firstClient = Mockito.mock(TTransport.class);
        TTransport secondClient = Mockito.mock(TTransport.class);
        ExecutorService executor = Mockito.mock(ExecutorService.class);
        Mockito.doThrow(new RejectedExecutionException("saturated"))
                .when(executor).execute(Mockito.any(Runnable.class));

        SRTThreadPoolServer server = new SRTThreadPoolServer(
                new SRTThreadPoolServer.Args(serverTransport)
                        .processor(Mockito.mock(TProcessor.class))
                        .executorService(executor));
        AtomicInteger acceptCount = new AtomicInteger();
        Mockito.when(serverTransport.accept()).thenAnswer(invocation -> {
            int current = acceptCount.getAndIncrement();
            if (current == 0) {
                return firstClient;
            }
            if (current == 1) {
                return secondClient;
            }
            server.stop();
            throw new TTransportException("stop test server");
        });

        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(5), server::execute);

        Mockito.verify(executor, Mockito.times(2)).execute(Mockito.any(Runnable.class));
        Mockito.verify(firstClient).close();
        Mockito.verify(secondClient).close();
    }

    @Test
    public void testAcceptorStallUsesMonotonicTime() {
        AtomicLong nanoTime = new AtomicLong(TimeUnit.SECONDS.toNanos(10));
        SRTThreadPoolServer server = newServer(nanoTime);

        nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(2345));
        Assertions.assertEquals(2345, server.getAcceptorStallTimeMs());

        server.markAcceptorProgress();
        Assertions.assertEquals(0, server.getAcceptorStallTimeMs());
    }

    @Test
    public void testRejectionWarningReportsEveryConnectionInTheWindow() {
        AtomicLong nanoTime = new AtomicLong(TimeUnit.SECONDS.toNanos(10));
        SRTThreadPoolServer server = newServer(nanoTime);
        TTransport client = Mockito.mock(TTransport.class);

        // nothing has been rejected, so there is nothing to report
        Assertions.assertEquals(0, server.reportRejectedConnections(false));

        // a burst inside one window stays quiet, but every connection is still accounted for
        for (int i = 0; i < 10000; i++) {
            server.recordRejectedConnection(client);
        }
        Assertions.assertEquals(0, server.reportRejectedConnections(false));

        // when the window closes the warning carries the whole burst, not just its first connection
        nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(10));
        Assertions.assertEquals(10000, server.reportRejectedConnections(false));

        // and the window resets, so the next burst is counted from scratch
        Assertions.assertEquals(0, server.reportRejectedConnections(false));
        server.recordRejectedConnection(client);
        Assertions.assertEquals(0, server.reportRejectedConnections(false));

        // a burst that ends before its window does is still reported, not stranded
        Assertions.assertEquals(1, server.reportRejectedConnections(true));
        Assertions.assertEquals(0, server.reportRejectedConnections(true));
    }

    @Test
    public void testReportedSpanCoversTheBurstNotTheDelayBeforeReporting() {
        AtomicLong nanoTime = new AtomicLong(TimeUnit.SECONDS.toNanos(10));
        SRTThreadPoolServer server = newServer(nanoTime);
        TTransport client = Mockito.mock(TTransport.class);

        // two rejections two seconds apart, then an hour of silence before a connection arrives
        server.recordRejectedConnection(client);
        nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(2));
        server.recordRejectedConnection(client);
        nanoTime.addAndGet(TimeUnit.HOURS.toNanos(1));

        Assertions.assertEquals(2, server.reportRejectedConnections(false));

        // the warning must describe the burst, not the hour spent waiting to report it
        Assertions.assertEquals(2000, server.getLastReportedSpanMs());
    }

    @Test
    public void testStallGaugeIgnoresAnAcceptThatNeverReturnsAConnection() throws Exception {
        // an accept() that always throws is the "FE accepts nothing" outage the gauge exists to show
        TServerTransport serverTransport = Mockito.mock(TServerTransport.class);
        AtomicLong nanoTime = new AtomicLong(TimeUnit.SECONDS.toNanos(10));
        SRTThreadPoolServer server = new SRTThreadPoolServer(
                new SRTThreadPoolServer.Args(serverTransport)
                        .processor(Mockito.mock(TProcessor.class))
                        .executorService(Mockito.mock(ExecutorService.class)),
                nanoTime::get);

        AtomicInteger spins = new AtomicInteger();
        Mockito.when(serverTransport.accept()).thenAnswer(invocation -> {
            nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(1));
            if (spins.incrementAndGet() >= 5) {
                server.stop();
            }
            throw new TTransportException("Too many open files");
        });

        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(5), server::execute);

        // the loop spun five seconds without serving anyone, and the gauge says so
        Assertions.assertEquals(5000, server.getAcceptorStallTimeMs());
    }

    @Test
    public void testPeerAddressComesFromTheTransportInterface() {
        AtomicLong nanoTime = new AtomicLong(TimeUnit.SECONDS.toNanos(10));
        SRTThreadPoolServer server = newServer(nanoTime);
        TSocket client = Mockito.mock(TSocket.class);
        Mockito.when(client.getRemoteSocketAddress()).thenReturn(new InetSocketAddress("10.0.0.7", 51234));

        server.recordRejectedConnection(client);

        Assertions.assertEquals(1, server.reportRejectedConnections(true));
        Mockito.verify(client).getRemoteSocketAddress();
    }

    @Test
    public void testConnectionOlderThanTheQueueTimeoutIsClosedBeforeItIsServed() throws Exception {
        Config.thrift_server_queue_timeout_ms = 2000;
        AtomicLong nanoTime = new AtomicLong(TimeUnit.SECONDS.toNanos(10));
        TProcessor processor = Mockito.mock(TProcessor.class);
        TTransport client = Mockito.mock(TTransport.class);
        Runnable queued = enqueue(nanoTime, processor, client);

        nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(2001));
        queued.run();

        Mockito.verify(client).close();
        // nothing was read off the wire: the connection never reached a processor
        Mockito.verifyNoInteractions(processor);
    }

    @Test
    public void testConnectionWithinTheQueueTimeoutIsServed() throws Exception {
        Config.thrift_server_queue_timeout_ms = 2000;
        AtomicLong nanoTime = new AtomicLong(TimeUnit.SECONDS.toNanos(10));
        TProcessor processor = servedOnceProcessor();
        Runnable queued = enqueue(nanoTime, processor, Mockito.mock(TTransport.class));

        nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(2000));
        queued.run();

        Mockito.verify(processor).process(Mockito.any(), Mockito.any());
    }

    @Test
    public void testQueueTimeoutIsDisarmedByDefault() throws Exception {
        // No fixed timeout is safe for every caller -- a statement forwarded from a follower waits
        // getExecTimeout() + thrift_rpc_timeout_ms, minutes by default and hours for an INSERT -- so
        // the shipped default must serve a queued connection however long it waited.
        Assertions.assertEquals(0, Config.thrift_server_queue_timeout_ms);
        AtomicLong nanoTime = new AtomicLong(TimeUnit.SECONDS.toNanos(10));
        TProcessor processor = servedOnceProcessor();
        Runnable queued = enqueue(nanoTime, processor, Mockito.mock(TTransport.class));

        nanoTime.addAndGet(TimeUnit.HOURS.toNanos(1));
        queued.run();

        Mockito.verify(processor).process(Mockito.any(), Mockito.any());
    }

    @Test
    public void testExpiryWarningReportsEveryConnectionInTheWindow() {
        AtomicLong nanoTime = new AtomicLong(TimeUnit.SECONDS.toNanos(10));
        SRTThreadPoolServer server = newServer(nanoTime);

        expire(server, 3);
        // the window is still open, so nothing is said yet
        Assertions.assertEquals(0, server.reportExpiredConnections(false));

        nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(10) - 1);
        Assertions.assertEquals(0, server.reportExpiredConnections(false));

        // once it closes, one warning covers the whole burst
        nanoTime.addAndGet(1);
        Assertions.assertEquals(3, server.reportExpiredConnections(false));

        // and the window is spent, so a repeat report says nothing
        Assertions.assertEquals(0, server.reportExpiredConnections(false));
    }

    @Test
    public void testForcedReportFlushesAWindowThatNeverGotAnotherExpiry() {
        AtomicLong nanoTime = new AtomicLong(TimeUnit.SECONDS.toNanos(10));
        SRTThreadPoolServer server = newServer(nanoTime);

        expire(server, 4);

        // A burst that ends leaves its tail in an open window. Nothing else expires, so only the
        // shutdown flush can report it -- without one those four closures are never warned about.
        Assertions.assertEquals(4, server.reportExpiredConnections(true));
        Assertions.assertEquals(0, server.reportExpiredConnections(true));
    }

    @Test
    public void testExpiredConnectionsCountedFromManyWorkersAreAllReported() throws Exception {
        int threads = 8;
        int perThread = 2000;
        AtomicLong nanoTime = new AtomicLong(TimeUnit.SECONDS.toNanos(10));
        SRTThreadPoolServer server = newServer(nanoTime);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    expire(server, perThread);
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }

            // every worker's expiries land in the one window, and none is double counted or lost
            Assertions.assertEquals((long) threads * perThread, server.reportExpiredConnections(true));
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    public void testExpiryWarningNamesThePeerOfTheLastExpiredConnection() {
        AtomicLong nanoTime = new AtomicLong(TimeUnit.SECONDS.toNanos(10));
        SRTThreadPoolServer server = newServer(nanoTime);
        TSocket client = Mockito.mock(TSocket.class);
        Mockito.when(client.getRemoteSocketAddress()).thenReturn(new InetSocketAddress("10.0.0.9", 51235));

        // resolved when the connection expires, not when the warning fires, because by then the
        // socket has been closed
        server.recordExpiredConnection(client, 2001, 2000);
        Mockito.verify(client).getRemoteSocketAddress();

        Assertions.assertEquals(1, server.reportExpiredConnections(true));
    }

    /** Runs the expiry bookkeeping a worker does, without needing a real queued connection. */
    private void expire(SRTThreadPoolServer server, int count) {
        TTransport client = Mockito.mock(TTransport.class);
        for (int i = 0; i < count; i++) {
            server.recordExpiredConnection(client, 2001, 2000);
        }
    }

    /**
     * Submits a client through the acceptor path and returns the work item the executor queued,
     * so that a test can advance the clock between enqueue and execution the way a saturated
     * pool does.
     */
    private Runnable enqueue(AtomicLong nanoTime, TProcessor processor, TTransport client) {
        ExecutorService executor = Mockito.mock(ExecutorService.class);
        List<Runnable> queue = new ArrayList<>();
        Mockito.doAnswer(invocation -> queue.add(invocation.getArgument(0)))
                .when(executor).execute(Mockito.any(Runnable.class));

        SRTThreadPoolServer server = new SRTThreadPoolServer(
                new SRTThreadPoolServer.Args(Mockito.mock(TServerTransport.class))
                        .processor(processor)
                        .executorService(executor),
                nanoTime::get);
        server.submitClient(client);

        Assertions.assertEquals(1, queue.size());
        return queue.get(0);
    }

    /**
     * A processor that ends the connection the way a client hangup does, so that the worker's serve
     * loop runs exactly one round instead of spinning forever on a mock.
     */
    private TProcessor servedOnceProcessor() throws TException {
        TProcessor processor = Mockito.mock(TProcessor.class);
        Mockito.doThrow(new TTransportException(TTransportException.END_OF_FILE))
                .when(processor).process(Mockito.any(), Mockito.any());
        return processor;
    }

    private SRTThreadPoolServer newServer(AtomicLong nanoTime) {
        return new SRTThreadPoolServer(
                new SRTThreadPoolServer.Args(Mockito.mock(TServerTransport.class))
                        .processor(Mockito.mock(TProcessor.class))
                        .executorService(Mockito.mock(ExecutorService.class)),
                nanoTime::get);
    }
}
