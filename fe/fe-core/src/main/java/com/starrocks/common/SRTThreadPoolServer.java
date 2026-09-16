// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.common;

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.metric.MetricRepo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.SocketAddressProvider;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Almost all code is copied from org.apache.thrift.server.TThreadPoolServer v0.13.0
 * https://github.com/apache/thrift
 * The difference is the execute() function:
 *      We do not kill the serve for any exception thrown by ExecutorService,
 *      just close the connection and print the error log.
 *      But TThreadPoolServer will kill the serve, and any new connection will not be processed,
 *      it will cause the connections pileUp in the tcp backlog.
 */
public class SRTThreadPoolServer extends TServer {
    private static final Logger LOG = LogManager.getLogger(SRTThreadPoolServer.class);
    private static final long WARNING_LOG_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(10);

    public static class Args extends AbstractServerArgs<SRTThreadPoolServer.Args> {
        public int minWorkerThreads = 5;
        public int maxWorkerThreads = Integer.MAX_VALUE;
        public ExecutorService executorService;
        public int stopTimeoutVal = 60;
        public TimeUnit stopTimeoutUnit = TimeUnit.SECONDS;
        /**
         * How long a submission was retried before the connection was dropped, and the slot length of
         * the binary exponential backoff between those retries. The server no longer retries at all -
         * a submission it cannot queue fails immediately - so none of the four has any effect. They are
         * kept so that existing callers still compile.
         */
        @Deprecated
        public int requestTimeout = 20;
        @Deprecated
        public TimeUnit requestTimeoutUnit = TimeUnit.SECONDS;
        @Deprecated
        public int beBackoffSlotLength = 100;
        @Deprecated
        public TimeUnit beBackoffSlotLengthUnit = TimeUnit.MILLISECONDS;

        public Args(TServerTransport transport) {
            super(transport);
        }

        public SRTThreadPoolServer.Args minWorkerThreads(int n) {
            minWorkerThreads = n;
            return this;
        }

        public SRTThreadPoolServer.Args maxWorkerThreads(int n) {
            maxWorkerThreads = n;
            return this;
        }

        public SRTThreadPoolServer.Args stopTimeoutVal(int n) {
            stopTimeoutVal = n;
            return this;
        }

        public SRTThreadPoolServer.Args stopTimeoutUnit(TimeUnit tu) {
            stopTimeoutUnit = tu;
            return this;
        }

        /**
         * @deprecated the retry these configured no longer exists; setting it has no effect.
         */
        @Deprecated
        public SRTThreadPoolServer.Args requestTimeout(int n) {
            requestTimeout = n;
            return this;
        }

        /**
         * @deprecated the retry these configured no longer exists; setting it has no effect.
         */
        @Deprecated
        public SRTThreadPoolServer.Args requestTimeoutUnit(TimeUnit tu) {
            requestTimeoutUnit = tu;
            return this;
        }

        /**
         * @deprecated binary exponential backoff slot length; the backoff no longer exists, so
         *     setting it has no effect.
         */
        @Deprecated
        public SRTThreadPoolServer.Args beBackoffSlotLength(int n) {
            beBackoffSlotLength = n;
            return this;
        }

        /**
         * @deprecated binary exponential backoff slot time unit; the backoff no longer exists, so
         *     setting it has no effect.
         */
        @Deprecated
        public SRTThreadPoolServer.Args beBackoffSlotLengthUnit(TimeUnit tu) {
            beBackoffSlotLengthUnit = tu;
            return this;
        }

        public SRTThreadPoolServer.Args executorService(ExecutorService executorService) {
            this.executorService = executorService;
            return this;
        }
    }

    // Executor service for handling client connections
    private final ExecutorService executorService;

    private final TimeUnit stopTimeoutUnit;

    private final long stopTimeoutVal;

    private final LongSupplier nanoTime;

    private volatile long acceptorHeartbeatNanos;

    // The rejection bookkeeping below is only ever touched from the single acceptor thread,
    // so it needs no synchronization.
    private long rejectedSinceLastReport;

    private long rejectionWindowStartNanos;

    private long lastRejectionNanos;

    private long lastReportedSpanMs;

    private boolean rejectionWindowOpen;

    private SocketAddress lastRejectedPeer;

    // The expiry window mirrors the rejection window above, but any worker can expire a connection
    // rather than just the acceptor, so this one synchronizes. expiryWindowOpen is read once per
    // dequeued connection to decide whether the lock is worth taking at all, and is volatile for
    // that read alone. Keeping the two windows separate rate limits the warnings independently, so
    // neither kind can hide the other.
    private final Object expiryLock = new Object();

    private volatile boolean expiryWindowOpen;

    private long expiredSinceLastReport;

    private long expiryWindowStartNanos;

    private long lastExpiryNanos;

    private SocketAddress lastExpiredPeer;

    private long lastExpiredWaitMs;

    private long lastExpiredTimeoutMs;

    public SRTThreadPoolServer(SRTThreadPoolServer.Args args) {
        this(args, System::nanoTime);
    }

    @VisibleForTesting
    SRTThreadPoolServer(SRTThreadPoolServer.Args args, LongSupplier nanoTime) {
        super(args);

        stopTimeoutUnit = args.stopTimeoutUnit;
        stopTimeoutVal = args.stopTimeoutVal;
        this.nanoTime = nanoTime;
        acceptorHeartbeatNanos = nanoTime.getAsLong();

        executorService = args.executorService != null ?
                args.executorService : createDefaultExecutorService(args);
    }

    private static ExecutorService createDefaultExecutorService(SRTThreadPoolServer.Args args) {
        SynchronousQueue<Runnable> executorQueue =
                new SynchronousQueue<>();
        return new ThreadPoolExecutor(args.minWorkerThreads,
                args.maxWorkerThreads,
                args.stopTimeoutVal,
                args.stopTimeoutUnit,
                executorQueue);
    }

    protected boolean preServe() {
        try {
            serverTransport_.listen();
        } catch (TTransportException ttx) {
            LOG.error("Error occurred during listening.", ttx);
            return false;
        }

        // Run the preServe event
        if (eventHandler_ != null) {
            eventHandler_.preServe();
        }
        stopped_ = false;
        setServing(true);

        return true;
    }

    public void serve() {
        if (!preServe()) {
            return;
        }

        // Neither call below returns in the FE, so both force-flushes on this path -- the
        // rejection flush at the end of execute() and the expiry flush at the end of
        // waitForShutdown() -- are unreachable outside tests. stopped_ is set only by stop(),
        // and ThriftServer.stop() has no production caller: StarRocksFEServer starts the thrift
        // server and never stops it. The counters still record every rejection and expiry; only
        // the trailing warning line for a window still open at exit is missed.
        //
        // Wiring a graceful shutdown must also budget for awaitTermination() in waitForShutdown():
        // under saturation the workers sit in their socket reads for up to stopTimeoutVal, so a
        // bounded shutdown hook can expire before the expiry flush runs.
        execute();
        waitForShutdown();

        setServing(false);
    }

    protected void execute() {
        while (!stopped_) {
            try {
                // Deliberately not stamped before accept(): a failing accept() (EMFILE, a closed
                // server socket) spins this loop, and stamping here would report a healthy acceptor
                // through exactly the outage the heartbeat exists to expose.
                TTransport client = serverTransport_.accept();
                markAcceptorProgress();
                reportRejectedConnections(false);
                submitClient(client);
            } catch (TTransportException ttx) {
                if (!stopped_) {
                    LOG.warn("Transport error occurred during acceptance of message.", ttx);
                }
            } catch (Throwable t) {
                LOG.warn("Error occurred during acceptance of message.", t);
            }
        }
        // The acceptor is the only reporter, so flush what the open window still holds.
        // Unreachable outside tests -- see serve().
        reportRejectedConnections(true);
    }

    @VisibleForTesting
    void submitClient(TTransport client) {
        SRTThreadPoolServer.WorkerProcess worker = new SRTThreadPoolServer.WorkerProcess(client);
        try {
            executorService.execute(worker);
        } catch (RejectedExecutionException e) {
            recordRejectedConnection(client);
            client.close();
        } catch (Throwable t) {
            client.close();
            LOG.error("ExecutorService threw error: " + t, t);
        }
    }

    /**
     * The peer of a transport, or null when it cannot report one. TSocket implements
     * {@link SocketAddressProvider}, and going through the interface keeps this working for any other
     * transport that does - a TLS wrapper, say - instead of degrading to an unknown peer.
     */
    private SocketAddress getPeerAddress(TTransport client) {
        if (client instanceof SocketAddressProvider) {
            return ((SocketAddressProvider) client).getRemoteSocketAddress();
        }
        return null;
    }

    @VisibleForTesting
    void markAcceptorProgress() {
        acceptorHeartbeatNanos = nanoTime.getAsLong();
    }

    /**
     * Milliseconds since the accept loop last returned a connection. This rises while the acceptor is
     * wedged, but also on an FE that simply has no thrift traffic, so it only means "stalled" when read
     * together with the connection arrival rate.
     */
    long getAcceptorStallTimeMs() {
        long elapsedNanos = nanoTime.getAsLong() - acceptorHeartbeatNanos;
        return elapsedNanos <= 0 ? 0 : TimeUnit.NANOSECONDS.toMillis(elapsedNanos);
    }

    /**
     * Counts one rejected connection, into the metric and into the window the accept loop reports
     * from. Every rejection is counted, so the counter and the eventual warning agree on the
     * magnitude even though at most one warning is emitted per window.
     */
    @VisibleForTesting
    void recordRejectedConnection(TTransport client) {
        if (MetricRepo.hasInit) {
            MetricRepo.COUNTER_THRIFT_SERVER_REJECTED_CONNECTIONS.increase(1L);
        }
        long nowNanos = nanoTime.getAsLong();
        if (!rejectionWindowOpen) {
            rejectionWindowOpen = true;
            rejectionWindowStartNanos = nowNanos;
        }
        lastRejectionNanos = nowNanos;
        rejectedSinceLastReport++;
        lastRejectedPeer = getPeerAddress(client);
    }

    /**
     * Emits one warning covering every rejection counted in the open window, once that window has
     * closed. The accept loop drives this, so the report waits on the next accepted connection, or on
     * the loop exiting, rather than on another rejection. The span it reports runs from the first to
     * the last rejection in the window, not to whenever the report happened to fire, so a burst that
     * is flushed long after it ended still states its own duration.
     *
     * @param force report a still-open window, for the accept loop to flush on its way out
     * @return how many rejections were reported, 0 if none were
     */
    @VisibleForTesting
    long reportRejectedConnections(boolean force) {
        if (!rejectionWindowOpen) {
            return 0;
        }
        if (!force && nanoTime.getAsLong() - rejectionWindowStartNanos < WARNING_LOG_INTERVAL_NANOS) {
            return 0;
        }
        long reported = rejectedSinceLastReport;
        lastReportedSpanMs = TimeUnit.NANOSECONDS.toMillis(lastRejectionNanos - rejectionWindowStartNanos);
        LOG.warn("Rejected {} thrift connection(s) within {} ms because the server worker pool is "
                        + "saturated, most recent peer {}",
                reported, lastReportedSpanMs, lastRejectedPeer == null ? "unknown" : lastRejectedPeer);
        rejectedSinceLastReport = 0;
        rejectionWindowOpen = false;
        lastRejectedPeer = null;
        return reported;
    }

    /** The span of the burst covered by the last emitted warning. */
    @VisibleForTesting
    long getLastReportedSpanMs() {
        return lastReportedSpanMs;
    }

    /**
     * Counts one expired connection into the open window, opening it if this is the first. The
     * synchronized counterpart of {@link #recordRejectedConnection}: expiries arrive from whichever
     * worker dequeued the connection, so unlike rejections they have no single-threaded owner.
     * <p>
     * The peer and the applied timeout are captured here rather than read back at report time, so a
     * warning emitted later cannot name a limit that was never applied, or a socket already closed.
     */
    @VisibleForTesting
    void recordExpiredConnection(TTransport client, long queueWaitMs, long queueTimeoutMs) {
        if (MetricRepo.hasInit) {
            MetricRepo.COUNTER_THRIFT_SERVER_EXPIRED_CONNECTIONS.increase(1L);
        }
        long nowNanos = nanoTime.getAsLong();
        SocketAddress peerAddress = getPeerAddress(client);
        synchronized (expiryLock) {
            if (!expiryWindowOpen) {
                expiryWindowStartNanos = nowNanos;
                expiryWindowOpen = true;
            }
            lastExpiryNanos = nowNanos;
            expiredSinceLastReport++;
            lastExpiredPeer = peerAddress;
            lastExpiredWaitMs = queueWaitMs;
            lastExpiredTimeoutMs = queueTimeoutMs;
        }
    }

    /**
     * Emits one warning covering every expiry counted in the open window, once that window has
     * closed. Every dequeued connection drives this, which is the worker-side counterpart of the
     * accept loop driving the rejection report: a burst that has ended is reported by the next
     * connection served rather than waiting for another expiry that may never come, and shutdown
     * flushes whatever is still open once the workers have stopped. The span reported runs from the
     * first to the last expiry in the window, not to whenever the report fired.
     *
     * @param force report a still-open window, for shutdown to flush
     * @return how many expiries were reported, 0 if none were
     */
    @VisibleForTesting
    long reportExpiredConnections(boolean force) {
        // One volatile read on the path of every dequeued connection; the lock is only worth taking
        // once something has actually expired, which with the check disarmed is never.
        if (!expiryWindowOpen) {
            return 0;
        }
        long reported;
        long spanMs;
        SocketAddress peerAddress;
        long waitMs;
        long timeoutMs;
        synchronized (expiryLock) {
            if (!expiryWindowOpen) {
                return 0;
            }
            if (!force && nanoTime.getAsLong() - expiryWindowStartNanos < WARNING_LOG_INTERVAL_NANOS) {
                return 0;
            }
            reported = expiredSinceLastReport;
            spanMs = TimeUnit.NANOSECONDS.toMillis(lastExpiryNanos - expiryWindowStartNanos);
            peerAddress = lastExpiredPeer;
            waitMs = lastExpiredWaitMs;
            timeoutMs = lastExpiredTimeoutMs;
            expiredSinceLastReport = 0;
            expiryWindowOpen = false;
            lastExpiredPeer = null;
        }
        LOG.warn("Closed {} queued thrift connection(s) within {} ms unserved because they waited "
                        + "longer than thrift_server_queue_timeout_ms={}, most recent peer {} waited {}ms",
                reported, spanMs, timeoutMs, peerAddress == null ? "unknown" : peerAddress, waitMs);
        return reported;
    }

    protected void waitForShutdown() {
        executorService.shutdown();

        // Loop until awaitTermination finally does return without a interrupted
        // exception. If we don't do this, then we'll shut down prematurely. We want
        // to let the executorService clear it's task queue, closing client sockets
        // appropriately.
        long timeoutMS = stopTimeoutUnit.toMillis(stopTimeoutVal);
        long now = System.currentTimeMillis();
        while (timeoutMS >= 0) {
            try {
                executorService.awaitTermination(timeoutMS, TimeUnit.MILLISECONDS);
                break;
            } catch (InterruptedException ix) {
                long newnow = System.currentTimeMillis();
                timeoutMS -= (newnow - now);
                now = newnow;
            }
        }
        // The workers have stopped, so nothing more can join the window; report what it still holds.
        // Unreachable outside tests -- see serve().
        reportExpiredConnections(true);
    }

    public void stop() {
        stopped_ = true;
        serverTransport_.interrupt();
    }

    private class WorkerProcess implements Runnable {

        /**
         * Client that this services.
         */
        private final TTransport client;

        /**
         * When this connection was handed to the executor. Monotonic, so a wall-clock step cannot
         * make the whole queue look stale at once.
         */
        private final long enqueueNanos;

        /**
         * Default constructor.
         *
         * @param client Transport to process
         */
        private WorkerProcess(TTransport client) {
            this.client = client;
            this.enqueueNanos = nanoTime.getAsLong();
        }

        /**
         * Loops on processing a client forever
         */
        public void run() {
            long queueWaitMs = recordQueueWait();
            // Mirrors the accept loop reporting rejections before it submits: whichever connection
            // gets dequeued next closes out a finished burst, so the last one is never left unsaid.
            reportExpiredConnections(false);
            if (closeIfExpired(queueWaitMs)) {
                return;
            }

            TProcessor processor = null;
            TTransport inputTransport = null;
            TTransport outputTransport = null;
            TProtocol inputProtocol = null;
            TProtocol outputProtocol = null;

            TServerEventHandler eventHandler = null;
            ServerContext connectionContext = null;

            try {
                processor = processorFactory_.getProcessor(client);
                inputTransport = inputTransportFactory_.getTransport(client);
                outputTransport = outputTransportFactory_.getTransport(client);
                inputProtocol = inputProtocolFactory_.getProtocol(inputTransport);
                outputProtocol = outputProtocolFactory_.getProtocol(outputTransport);

                eventHandler = getEventHandler();
                if (eventHandler != null) {
                    connectionContext = eventHandler.createContext(inputProtocol, outputProtocol);
                }
                // we check stopped_ first to make sure we're not supposed to be shutting
                // down. this is necessary for graceful shutdown.
                while (true) {

                    if (eventHandler != null) {
                        eventHandler.processContext(connectionContext, inputTransport, outputTransport);
                    }

                    if (stopped_) {
                        break;
                    }
                    processor.process(inputProtocol, outputProtocol);
                }
            } catch (Exception x) {
                // We'll usually receive RuntimeException types here
                // Need to unwrap to ascertain real causing exception before we choose to ignore
                // Ignore err-logging all transport-level/type exceptions
                if (!isIgnorableException(x)) {
                    // Log the exception at error level and continue
                    LOG.error((x instanceof TException ? "Thrift " : "") + "Error occurred during processing of message.", x);
                }
            } finally {
                if (eventHandler != null) {
                    eventHandler.deleteContext(connectionContext, inputProtocol, outputProtocol);
                }
                if (inputTransport != null) {
                    inputTransport.close();
                }
                if (outputTransport != null) {
                    outputTransport.close();
                }
                if (client.isOpen()) {
                    client.close();
                }
            }
        }

        /**
         * Samples how long this connection sat between the acceptor and this worker. Expired
         * connections are sampled too: sampling only the ones that go on to be served would cap the
         * distribution at the timeout, and so report a calm queue at exactly the moment the queue is
         * being emptied unserved.
         *
         * @return the queue wait in milliseconds
         */
        private long recordQueueWait() {
            long elapsedNanos = nanoTime.getAsLong() - enqueueNanos;
            long queueWaitMs = elapsedNanos <= 0 ? 0 : TimeUnit.NANOSECONDS.toMillis(elapsedNanos);
            if (MetricRepo.hasInit) {
                MetricRepo.HISTO_THRIFT_SERVER_QUEUE_WAIT_MS.update(queueWaitMs);
            }
            return queueWaitMs;
        }

        /**
         * Closes a connection that waited past the configured timeout, on the grounds that its caller
         * has almost certainly abandoned it and serving it only holds a worker away from work someone
         * is still waiting for. Runs before the connection context exists and before the first
         * protocol read, so an expired connection costs a worker a close rather than a request.
         *
         * @return true if the connection was closed unserved
         */
        private boolean closeIfExpired(long queueWaitMs) {
            // One read of the mutable config, so a change between the comparison and the warning
            // cannot report a limit that was never applied.
            long queueTimeoutMs = Config.thrift_server_queue_timeout_ms;
            if (queueTimeoutMs <= 0 || queueWaitMs <= queueTimeoutMs) {
                return false;
            }

            recordExpiredConnection(client, queueWaitMs, queueTimeoutMs);
            client.close();
            return true;
        }

        private boolean isIgnorableException(Exception x) {
            TTransportException tTransportException = null;

            if (x instanceof TTransportException) {
                tTransportException = (TTransportException) x;
            } else if (x.getCause() instanceof TTransportException) {
                tTransportException = (TTransportException) x.getCause();
            }

            if (tTransportException != null) {
                switch (tTransportException.getType()) {
                    case TTransportException.END_OF_FILE:
                    case TTransportException.TIMED_OUT:
                        return true;
                }
                if (tTransportException.getCause() instanceof SocketTimeoutException) {
                    return true;
                }
            }
            return false;
        }
    }
}
