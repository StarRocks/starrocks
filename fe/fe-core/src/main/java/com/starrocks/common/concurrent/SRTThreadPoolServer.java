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

package com.starrocks.common.concurrent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.net.SocketTimeoutException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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

    public static class Args extends AbstractServerArgs<SRTThreadPoolServer.Args> {
        public int minWorkerThreads = 5;
        public int maxWorkerThreads = Integer.MAX_VALUE;
        public ExecutorService executorService;
        public int stopTimeoutVal = 60;
        public TimeUnit stopTimeoutUnit = TimeUnit.SECONDS;
        public int requestTimeout = 20;
        public TimeUnit requestTimeoutUnit = TimeUnit.SECONDS;
        public int beBackoffSlotLength = 100;
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

        public SRTThreadPoolServer.Args requestTimeout(int n) {
            requestTimeout = n;
            return this;
        }

        public SRTThreadPoolServer.Args requestTimeoutUnit(TimeUnit tu) {
            requestTimeoutUnit = tu;
            return this;
        }
        //Binary exponential backoff slot length
        public SRTThreadPoolServer.Args beBackoffSlotLength(int n) {
            beBackoffSlotLength = n;
            return this;
        }

        //Binary exponential backoff slot time unit
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

    private final TimeUnit requestTimeoutUnit;

    private final long requestTimeout;

    private final long beBackoffSlotInMillis;

    private final Random random = new Random(System.currentTimeMillis());

    public SRTThreadPoolServer(SRTThreadPoolServer.Args args) {
        super(args);

        stopTimeoutUnit = args.stopTimeoutUnit;
        stopTimeoutVal = args.stopTimeoutVal;
        requestTimeoutUnit = args.requestTimeoutUnit;
        requestTimeout = args.requestTimeout;
        beBackoffSlotInMillis = args.beBackoffSlotLengthUnit.toMillis(args.beBackoffSlotLength);

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
                TTransport client = serverTransport_.accept();
                SRTThreadPoolServer.WorkerProcess wp = new SRTThreadPoolServer.WorkerProcess(client);

                int retryCount = 0;
                long remainTimeInMillis = requestTimeoutUnit.toMillis(requestTimeout);
                while (true) {
                    try {
                        executorService.execute(wp);
                        break;
                    } catch (Throwable t) {
                        if (t instanceof RejectedExecutionException) {
                            retryCount++;
                            try {
                                if (remainTimeInMillis > 0) {
                                    //do a truncated 20 binary exponential backoff sleep
                                    long sleepTimeInMillis = ((long) (random.nextDouble() *
                                            (1L << Math.min(retryCount, 20)))) * beBackoffSlotInMillis;
                                    sleepTimeInMillis = Math.min(sleepTimeInMillis, remainTimeInMillis);
                                    TimeUnit.MILLISECONDS.sleep(sleepTimeInMillis);
                                    remainTimeInMillis = remainTimeInMillis - sleepTimeInMillis;
                                } else {
                                    client.close();
                                    wp = null;
                                    LOG.warn("Task has been rejected by ExecutorService " + retryCount
                                            + " times till timedout, reason: " + t);
                                    break;
                                }
                            } catch (InterruptedException e) {
                                client.close();
                                wp = null;
                                LOG.warn("Interrupted while waiting to place client on executor queue.");
                                Thread.currentThread().interrupt();
                                break;
                            }
                        } else {
                            client.close();
                            wp = null;
                            LOG.error("ExecutorService threw error: " + t, t);
                            break;
                        }
                    }
                }
            } catch (TTransportException ttx) {
                if (!stopped_) {
                    LOG.warn("Transport error occurred during acceptance of message.", ttx);
                }
            }
        }
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
