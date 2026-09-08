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
import org.apache.thrift.TProcessor;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SRTThreadPoolServerTest {
    private static boolean metricRepoLoaded;

    @BeforeAll
    public static void loadMetricRepoClass() {
        // The rejection path reads MetricRepo.hasInit, and that first read forces MetricRepo's class
        // initialization, dragging in ThreadPoolManager, Config and a few hundred metric objects.
        // Do it here so the burst is not charged to the timed test below.
        metricRepoLoaded = MetricRepo.hasInit;
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

    private SRTThreadPoolServer newServer(AtomicLong nanoTime) {
        return new SRTThreadPoolServer(
                new SRTThreadPoolServer.Args(Mockito.mock(TServerTransport.class))
                        .processor(Mockito.mock(TProcessor.class))
                        .executorService(Mockito.mock(ExecutorService.class)),
                nanoTime::get);
    }
}
