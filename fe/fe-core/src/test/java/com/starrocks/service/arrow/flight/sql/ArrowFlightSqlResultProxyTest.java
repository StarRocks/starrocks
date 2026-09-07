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

package com.starrocks.service.arrow.flight.sql;

import io.grpc.Context;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelOption;
import org.apache.arrow.flight.BackpressureStrategy.CallbackBackpressureStrategy;
import org.apache.arrow.flight.BackpressureStrategy.WaitResult;
import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightGrpcUtils;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ArrowFlightSqlResultProxyTest {
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final ServerStreamListener listener = mock(ServerStreamListener.class);
    private final FlightStream stream = mock(FlightStream.class);
    private final AtomicBoolean ready = new AtomicBoolean();
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final CountDownLatch closed = new CountDownLatch(1);
    private final CountDownLatch waiting = new CountDownLatch(1);
    private final AtomicReference<Runnable> onReady = new AtomicReference<>();
    private final AtomicReference<Runnable> onCancel = new AtomicReference<>();

    @BeforeEach
    void setup() throws Exception {
        when(listener.isReady()).thenAnswer(invocation -> {
            if (!ready.get()) {
                waiting.countDown();
            }
            return ready.get();
        });
        when(listener.isCancelled()).thenAnswer(invocation -> cancelled.get());
        doAnswer(invocation -> {
            onReady.set(invocation.getArgument(0));
            return null;
        }).when(listener).setOnReadyHandler(any());
        doAnswer(invocation -> {
            onCancel.set(invocation.getArgument(0));
            return null;
        }).when(listener).setOnCancelHandler(any());
        when(stream.getRoot()).thenReturn(mock(VectorSchemaRoot.class));
        doAnswer(invocation -> {
            closed.countDown();
            return null;
        }).when(stream).close();
    }

    @AfterEach
    void shutdown() throws Exception {
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    private void start() {
        ArrowFlightSqlResultProxy.start(executor, () -> stream, listener, "BE", "localhost:9400", 300_000);
        assertTrue(onReady.get() != null);
        assertTrue(onCancel.get() != null);
    }

    private void cancel() {
        cancelled.set(true);
        onCancel.get().run();
    }

    @Test
    void waitsBeforeReadingAndResumes() throws Exception {
        when(stream.next()).thenReturn(true, false);
        start();
        assertTrue(waiting.await(5, TimeUnit.SECONDS));
        verify(stream, never()).next();
        verify(listener, never()).putNext();
        ready.set(true);
        onReady.get().run();
        assertTrue(closed.await(5, TimeUnit.SECONDS));
        verify(listener).putNext();
        verify(listener).completed();
        verify(stream).close();
    }

    @Test
    void rechecksReadinessAfterUpstreamRead() throws Exception {
        ready.set(true);
        when(stream.next()).thenAnswer(invocation -> {
            ready.set(false);
            return true;
        }).thenReturn(false);
        start();
        assertTrue(waiting.await(5, TimeUnit.SECONDS));
        verify(stream).next();
        verify(listener, never()).putNext();
        ready.set(true);
        onReady.get().run();
        assertTrue(closed.await(5, TimeUnit.SECONDS));
        verify(listener).putNext();
        verify(listener).completed();
    }

    @Test
    void cancelsWhileWaiting() throws Exception {
        start();
        assertTrue(waiting.await(5, TimeUnit.SECONDS));
        cancel();
        assertTrue(closed.await(5, TimeUnit.SECONDS));
        verify(stream).cancel("Client cancelled request", null);
        verify(stream, never()).next();
        verify(listener, never()).completed();
        verify(listener, never()).error(any());
    }

    @Test
    void cancelsBlockedUpstreamRead() throws Exception {
        ready.set(true);
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch released = new CountDownLatch(1);
        when(stream.next()).thenAnswer(invocation -> {
            reading.countDown();
            assertTrue(released.await(5, TimeUnit.SECONDS));
            return true;
        });
        doAnswer(invocation -> {
            released.countDown();
            return null;
        }).when(stream).cancel("Client cancelled request", null);
        start();
        assertTrue(reading.await(5, TimeUnit.SECONDS));
        cancel();
        assertTrue(closed.await(5, TimeUnit.SECONDS));
        verify(listener, never()).putNext();
        verify(listener, never()).completed();
    }

    @Test
    void cancelsBlockedUpstreamSchemaRead() throws Exception {
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch released = new CountDownLatch(1);
        when(stream.getRoot()).thenAnswer(invocation -> {
            reading.countDown();
            assertTrue(released.await(5, TimeUnit.SECONDS));
            throw new RuntimeException("schema read cancelled");
        });
        doAnswer(invocation -> {
            released.countDown();
            return null;
        }).when(stream).cancel("Client cancelled request", null);
        start();
        assertTrue(reading.await(5, TimeUnit.SECONDS));
        cancel();
        assertTrue(closed.await(5, TimeUnit.SECONDS));
        verify(stream).cancel("Client cancelled request", null);
        verify(listener, never()).start(any());
        verify(listener, never()).putNext();
        verify(listener, never()).completed();
        verify(listener, never()).error(any());
    }

    @Test
    void cancellationBeforeStreamPublicationIsNotLost() throws Exception {
        CountDownLatch opening = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ArrowFlightSqlResultProxy.start(executor, () -> {
            opening.countDown();
            assertTrue(release.await(5, TimeUnit.SECONDS));
            return stream;
        }, listener, "FE", "localhost:9408", 300_000);
        assertTrue(opening.await(5, TimeUnit.SECONDS));
        cancel();
        release.countDown();
        assertTrue(closed.await(5, TimeUnit.SECONDS));
        verify(stream).cancel("Client cancelled request", null);
        verify(stream, never()).getRoot();
    }

    @Test
    void reportsErrorsAndCloses() throws Exception {
        ready.set(true);
        RuntimeException failure = new RuntimeException("upstream failed");
        when(stream.next()).thenThrow(failure);
        start();
        assertTrue(closed.await(5, TimeUnit.SECONDS));
        verify(stream).cancel("Error during streaming", failure);
        verify(listener).error(any());
        verify(listener, never()).completed();
    }

    @Test
    void downstreamFailureCancelsAndCloses() throws Exception {
        ready.set(true);
        when(stream.next()).thenReturn(true);
        RuntimeException failure = new RuntimeException("write failed");
        doThrow(failure).when(listener).putNext();
        start();
        assertTrue(closed.await(5, TimeUnit.SECONDS));
        verify(stream).cancel("Error during streaming", failure);
        verify(listener).error(any());
        verify(listener, never()).completed();
    }

    @Test
    void cancelledBeforeWorkerStartsDoesNotOpenUpstream() throws Exception {
        cancelled.set(true);
        AtomicBoolean opened = new AtomicBoolean();
        ArrowFlightSqlResultProxy.start(executor, () -> {
            opened.set(true);
            return stream;
        }, listener, "BE", "localhost:9400", 300_000);
        executor.submit(() -> {}).get(5, TimeUnit.SECONDS);
        assertFalse(opened.get());
        verify(listener, never()).completed();
        verify(listener, never()).error(any());
    }

    @Test
    void emptyStreamPreservesSchemaAndCompletes() throws Exception {
        ready.set(true);
        start();
        assertTrue(closed.await(5, TimeUnit.SECONDS));
        verify(listener).start(stream.getRoot());
        verify(listener).completed();
        verify(listener, never()).putNext();
    }

    @Test
    void interruptionCancelsAndCloses() throws Exception {
        doAnswer(invocation -> {
            assertFalse(Thread.currentThread().isInterrupted());
            closed.countDown();
            return null;
        }).when(stream).close();
        start();
        assertTrue(waiting.await(5, TimeUnit.SECONDS));
        executor.shutdownNow();
        assertTrue(closed.await(5, TimeUnit.SECONDS));
        verify(stream).cancel(any(), any(InterruptedException.class));
        verify(listener).error(any());
    }

    @Test
    void nonpositiveTimeoutRejectsWithoutOpeningStream() {
        for (int timeout : new int[] {0, -1}) {
            ServerStreamListener rejected = mock(ServerStreamListener.class);
            ArrowFlightSqlResultProxy.start(executor, () -> {
                throw new AssertionError("Invalid timeout must not open upstream");
            }, rejected, "BE", "localhost", timeout);
            verify(rejected).error(any());
        }
    }

    @Test
    void continuousNotReadyWaitTimesOutDespiteSpuriousCallbacks() throws Exception {
        ArrowFlightSqlResultProxy.start(executor, () -> stream, listener, "BE", "localhost", 100);
        assertTrue(waiting.await(5, TimeUnit.SECONDS));
        for (int i = 0; i < 100; i++) {
            onReady.get().run();
        }
        assertTrue(closed.await(5, TimeUnit.SECONDS));
        ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
        verify(listener).error(error.capture());
        assertEquals(FlightStatusCode.TIMED_OUT, ((FlightRuntimeException) error.getValue()).status().code());
        verify(stream).cancel(any(), any(TimeoutException.class));
        verify(listener, never()).completed();
    }

    @Test
    @Timeout(15)
    void interruptedRealStreamReleasesAllocatedBuffersAndRestoresInterrupt() throws Exception {
        AtomicReference<Thread> worker = new AtomicReference<>();
        AtomicBoolean restored = new AtomicBoolean();
        CountDownLatch finished = new CountDownLatch(1);
        UnpooledByteBufAllocator transportAllocator = new UnpooledByteBufAllocator(true);
        try (RootAllocator allocator = new RootAllocator(128L * 1024 * 1024);
                BufferAllocator clientAllocator = allocator.newChildAllocator("interrupted-client", 0, allocator.getLimit())) {
            NoOpFlightProducer source = new NoOpFlightProducer() {
                @Override
                public void getStream(CallContext context, Ticket ticket, ServerStreamListener output) {
                    try (IntVector vector = new IntVector("value", allocator);
                            VectorSchemaRoot root = VectorSchemaRoot.of(vector)) {
                        vector.allocateNew(1024);
                        vector.set(0, 42);
                        root.setRowCount(1024);
                        output.start(root);
                        output.putNext();
                        output.completed();
                    }
                }
            };
            try (FlightServer server = newServer(allocator, transportAllocator, source);
                    FlightClient client = newClient(clientAllocator, transportAllocator, server)) {
                ArrowFlightSqlResultProxy.start(task -> executor.execute(() -> {
                    worker.set(Thread.currentThread());
                    try {
                        task.run();
                    } finally {
                        restored.set(Thread.currentThread().isInterrupted());
                        finished.countDown();
                    }
                }), () -> {
                    FlightStream actual = client.getStream(new Ticket(new byte[0]));
                    assertTrue(actual.next());
                    assertEquals(42, ((IntVector) actual.getRoot().getVector(0)).get(0));
                    return actual;
                }, listener, "BE", "localhost", 300_000);
                assertTrue(waiting.await(5, TimeUnit.SECONDS));
                assertTrue(clientAllocator.getAllocatedMemory() > 0);
                worker.get().interrupt();
                assertTrue(finished.await(5, TimeUnit.SECONDS));
                assertTrue(restored.get());
                assertEquals(0, clientAllocator.getAllocatedMemory());
                verify(listener, never()).completed();
            }
        }
    }

    @Test
    void rejectsInsteadOfLeavingAnUnfinishedCall() {
        AtomicBoolean opened = new AtomicBoolean();
        ArrowFlightSqlResultProxy.start(task -> {
            throw new RejectedExecutionException("full");
        }, () -> {
            opened.set(true);
            return stream;
        }, listener, "BE", "localhost:9400", 300_000);
        assertFalse(opened.get());
        verify(listener).error(any());
    }

    @Test
    void propagatesGrpcContext() throws Exception {
        Context.Key<String> key = Context.key("proxy-test");
        Context context = Context.current().withValue(key, "value");
        AtomicReference<String> actual = new AtomicReference<>();
        ready.set(true);
        context.run(() -> ArrowFlightSqlResultProxy.start(executor, () -> {
            actual.set(key.get());
            return stream;
        }, listener, "BE", "localhost:9400", 300_000));
        assertTrue(closed.await(5, TimeUnit.SECONDS));
        assertSame("value", actual.get());
    }

    @Test
    @Timeout(30)
    void realFlightSlowConsumerResumesWithoutDrainingUpstream() throws Exception {
        transferThroughRealFlight(TransferTermination.COMPLETE, 1);
    }

    @Test
    @Timeout(30)
    void realFlightCancellationClosesBothHops() throws Exception {
        transferThroughRealFlight(TransferTermination.CANCEL, 1);
    }

    @Test
    @Timeout(30)
    void realFlightMultiFeSlowConsumerResumesWithoutDrainingUpstream() throws Exception {
        transferThroughRealFlight(TransferTermination.COMPLETE, 2);
    }

    @Test
    @Timeout(30)
    void realFlightMultiFeCancellationClosesAllThreeHops() throws Exception {
        transferThroughRealFlight(TransferTermination.CANCEL, 2);
    }

    @Test
    @Timeout(30)
    void realFlightStallTimeoutReleasesWorkerForSubsequentStream() throws Exception {
        transferThroughRealFlight(TransferTermination.BACKPRESSURE_TIMEOUT, 1);
    }

    @Test
    @Timeout(30)
    void realFlightRpcDeadlineClosesAllThreeHops() throws Exception {
        transferThroughRealFlight(TransferTermination.RPC_DEADLINE, 2);
    }

    private enum TransferTermination {
        COMPLETE, CANCEL, BACKPRESSURE_TIMEOUT, RPC_DEADLINE
    }

    private void transferThroughRealFlight(TransferTermination termination, int proxyCount) throws Exception {
        int batchCount = 256;
        int rowsPerBatch = 256 * 1024;
        // Keep transport copies in direct memory, but do not retain gRPC's process-wide pooled chunks
        // between transfers. This tests outstanding stream data, not the shared allocator's cache size.
        UnpooledByteBufAllocator transportAllocator = new UnpooledByteBufAllocator(true);
        CountDownLatch backpressured = new CountDownLatch(proxyCount);
        CountDownLatch sourceClosed = new CountDownLatch(1);
        AtomicInteger[] forwarded = new AtomicInteger[proxyCount];
        for (int hop = 0; hop < proxyCount; hop++) {
            forwarded[hop] = new AtomicInteger();
        }
        ExecutorService sourceExecutor = Executors.newSingleThreadExecutor();
        try (RootAllocator allocator = new RootAllocator(128L * 1024 * 1024)) {
            NoOpFlightProducer source = new NoOpFlightProducer() {
                @Override
                public void getStream(CallContext context, Ticket ticket, ServerStreamListener output) {
                    CallbackBackpressureStrategy strategy = new CallbackBackpressureStrategy();
                    strategy.register(output);
                    sourceExecutor.execute(() -> {
                        try (IntVector vector = new IntVector("value", allocator);
                                VectorSchemaRoot root = VectorSchemaRoot.of(vector)) {
                            vector.allocateNew(rowsPerBatch);
                            root.setRowCount(rowsPerBatch);
                            output.start(root);
                            for (int batch = 0; batch < batchCount; batch++) {
                                if (strategy.waitForListener(0) != WaitResult.READY || output.isCancelled()) {
                                    return;
                                }
                                vector.set(0, batch);
                                output.putNext();
                            }
                            output.completed();
                        } catch (Exception e) {
                            output.error(e);
                        } finally {
                            sourceClosed.countDown();
                        }
                    });
                }
            };
            try (FlightServer upstreamServer = newServer(allocator, transportAllocator, source);
                    FlightClient upstreamClient = newClient(allocator, transportAllocator, upstreamServer)) {
                transferThroughProxy(allocator, upstreamClient, 0, forwarded, backpressured,
                        sourceClosed, termination, batchCount, rowsPerBatch, transportAllocator);
            }
            sourceExecutor.submit(() -> {}).get(5, TimeUnit.SECONDS);
            assertEquals(0, transportAllocator.metric().usedDirectMemory());
        } finally {
            sourceExecutor.shutdownNow();
            assertTrue(sourceExecutor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private void transferThroughProxy(RootAllocator allocator, FlightClient upstreamClient, int hop,
                                      AtomicInteger[] forwarded, CountDownLatch backpressured,
                                      CountDownLatch sourceClosed, TransferTermination termination,
                                      int batchCount, int rowsPerBatch,
                                      UnpooledByteBufAllocator transportAllocator) throws Exception {
        ExecutorService hopExecutor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(1), new ThreadPoolExecutor.AbortPolicy());
        CountDownLatch workerFinished = new CountDownLatch(1);
        CountDownLatch allWorkersFinished = new CountDownLatch(
                termination == TransferTermination.BACKPRESSURE_TIMEOUT ? 2 : 1);
        AtomicBoolean observedBackpressure = new AtomicBoolean();
        NoOpFlightProducer proxy = new NoOpFlightProducer() {
            @Override
            public void getStream(CallContext context, Ticket ticket, ServerStreamListener output) {
                ServerStreamListener observed = mock(ServerStreamListener.class, delegatesTo(output));
                doAnswer(invocation -> {
                    boolean isReady = output.isReady();
                    if (!isReady && forwarded[hop].get() > 0 && observedBackpressure.compareAndSet(false, true)) {
                        backpressured.countDown();
                    }
                    return isReady;
                }).when(observed).isReady();
                doAnswer(invocation -> {
                    output.putNext();
                    forwarded[hop].incrementAndGet();
                    return null;
                }).when(observed).putNext();
                ArrowFlightSqlResultProxy.start(task -> hopExecutor.execute(() -> {
                    try {
                        task.run();
                    } finally {
                        workerFinished.countDown();
                        allWorkersFinished.countDown();
                    }
                }), () -> upstreamClient.getStream(ticket), observed, hop == 0 ? "BE" : "FE", "localhost",
                        termination == TransferTermination.BACKPRESSURE_TIMEOUT ? 250 : 300_000);
            }
        };
        try (FlightServer proxyServer = newServer(allocator, transportAllocator, proxy);
                FlightClient downstream = newClient(allocator, transportAllocator, proxyServer)) {
            if (hop + 1 < forwarded.length) {
                transferThroughProxy(allocator, downstream, hop + 1, forwarded, backpressured,
                        sourceClosed, termination, batchCount, rowsPerBatch, transportAllocator);
            } else {
                try (FlightStream result = termination == TransferTermination.RPC_DEADLINE
                        ? downstream.getStream(new Ticket(new byte[0]), CallOptions.timeout(3, TimeUnit.SECONDS))
                        : downstream.getStream(new Ticket(new byte[0]))) {
                    assertTrue(backpressured.await(5, TimeUnit.SECONDS));
                    for (AtomicInteger count : forwarded) {
                        assertTrue(count.get() < batchCount);
                    }
                    if (termination == TransferTermination.CANCEL) {
                        result.cancel("test cancellation", null);
                    } else if (termination == TransferTermination.BACKPRESSURE_TIMEOUT
                            || termination == TransferTermination.RPC_DEADLINE) {
                        assertTrue(workerFinished.await(5, TimeUnit.SECONDS));
                        FlightRuntimeException error = assertThrows(FlightRuntimeException.class, () -> {
                            while (result.next()) {
                                assertEquals(rowsPerBatch, result.getRoot().getRowCount());
                            }
                        });
                        assertEquals(FlightStatusCode.TIMED_OUT, error.status().code());
                    } else {
                        int batches = 0;
                        while (result.next()) {
                            assertEquals(rowsPerBatch, result.getRoot().getRowCount());
                            assertEquals(batches, ((IntVector) result.getRoot().getVector(0)).get(0));
                            batches++;
                        }
                        assertEquals(batchCount, batches);
                    }
                    assertTrue(sourceClosed.await(5, TimeUnit.SECONDS));
                    assertTrue(workerFinished.await(5, TimeUnit.SECONDS));
                }
                if (termination == TransferTermination.BACKPRESSURE_TIMEOUT) {
                    try (FlightStream subsequent = downstream.getStream(new Ticket(new byte[0]))) {
                        int batches = 0;
                        while (subsequent.next()) {
                            assertEquals(rowsPerBatch, subsequent.getRoot().getRowCount());
                            assertEquals(batches, ((IntVector) subsequent.getRoot().getVector(0)).get(0));
                            batches++;
                        }
                        assertEquals(batchCount, batches);
                        assertTrue(allWorkersFinished.await(5, TimeUnit.SECONDS));
                    }
                }
            }
            assertTrue(allWorkersFinished.await(5, TimeUnit.SECONDS));
        } finally {
            hopExecutor.shutdownNow();
            assertTrue(hopExecutor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private static FlightServer newServer(RootAllocator allocator, UnpooledByteBufAllocator transportAllocator,
                                          NoOpFlightProducer producer) throws Exception {
        Consumer<NettyServerBuilder> configure = builder ->
                builder.withChildOption(ChannelOption.ALLOCATOR, transportAllocator);
        return FlightServer.builder(allocator, Location.forGrpcInsecure("localhost", 0), producer)
                .transportHint("grpc.builderConsumer", configure).build().start();
    }

    private static FlightClient newClient(BufferAllocator allocator, UnpooledByteBufAllocator transportAllocator,
                                          FlightServer server) {
        return FlightGrpcUtils.createFlightClient(allocator, NettyChannelBuilder.forAddress("localhost", server.getPort())
                .usePlaintext().withOption(ChannelOption.ALLOCATOR, transportAllocator).build());
    }
}
