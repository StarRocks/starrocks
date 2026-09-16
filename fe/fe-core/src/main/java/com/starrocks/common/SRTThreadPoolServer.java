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
    private static final long REJECTION_LOG_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(10);

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
        reportRejectedConnections(true);
    }

    private void submitClient(TTransport client) {
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
        if (!force && nanoTime.getAsLong() - rejectionWindowStartNanos < REJECTION_LOG_INTERVAL_NANOS) {
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
         * Default constructor.
         *
         * @param client Transport to process
         */
        private WorkerProcess(TTransport client) {
            this.client = client;
        }

        /**
         * Loops on processing a client forever
         */
        public void run() {
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
