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
import org.apache.arrow.flight.BackpressureStrategy.CallbackBackpressureStrategy;
import org.apache.arrow.flight.BackpressureStrategy.WaitResult;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.FlightStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;

/** One remote result stream, with downstream flow control independent of the gRPC callback thread. */
final class ArrowFlightSqlResultProxy extends CallbackBackpressureStrategy {
    private static final Logger LOG = LogManager.getLogger(ArrowFlightSqlResultProxy.class);
    private final ServerStreamListener listener;
    private final String targetType;
    private final String nodeKey;
    private final int backpressureTimeoutMs;
    private volatile FlightStream upstream;

    private ArrowFlightSqlResultProxy(ServerStreamListener listener, String targetType, String nodeKey,
                                      int backpressureTimeoutMs) {
        this.listener = listener;
        this.targetType = targetType;
        this.nodeKey = nodeKey;
        this.backpressureTimeoutMs = backpressureTimeoutMs;
        register(listener);
    }

    static void start(Executor executor, Callable<FlightStream> openStream, ServerStreamListener listener,
                      String targetType, String nodeKey, int backpressureTimeoutMs) {
        if (backpressureTimeoutMs <= 0) {
            listener.error(CallStatus.INTERNAL.withDescription(
                    "arrow_flight_proxy_backpressure_timeout_ms must be positive").toRuntimeException());
            return;
        }
        // gRPC serializes readiness/cancellation callbacks with the producer invocation. Register here,
        // but run all blocking upstream reads and readiness waits only after handing off to a worker.
        ArrowFlightSqlResultProxy proxy = new ArrowFlightSqlResultProxy(
                listener, targetType, nodeKey, backpressureTimeoutMs);
        try {
            executor.execute(Context.current().wrap(() -> proxy.forward(openStream)));
        } catch (RejectedExecutionException e) {
            listener.error(CallStatus.UNAVAILABLE.withDescription("Flight result proxy is busy").toRuntimeException());
        }
    }

    @Override
    protected void cancelCallback() {
        cancelUpstream("Client cancelled request", null);
    }

    private void cancelUpstream(String message, Throwable cause) {
        FlightStream stream = upstream;
        if (stream != null) {
            try {
                stream.cancel(message, cause);
            } catch (Exception e) {
                LOG.warn("[ARROW] Error cancelling {} stream", targetType, e);
            }
        }
    }

    private boolean awaitReady() throws InterruptedException, TimeoutException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Interrupted while proxying result");
        }
        if (listener.isCancelled()) {
            return false;
        }
        WaitResult result = waitForListener(backpressureTimeoutMs);
        if (listener.isCancelled()) {
            return false;
        }
        if (result == WaitResult.TIMEOUT) {
            throw new TimeoutException("Flight client remained not ready for " + backpressureTimeoutMs + " ms");
        }
        if (result == WaitResult.OTHER) {
            throw new InterruptedException("Interrupted while waiting for Flight client");
        }
        return result == WaitResult.READY && !listener.isCancelled();
    }

    private void forward(Callable<FlightStream> openStream) {
        FlightStream stream = null;
        try {
            if (listener.isCancelled()) {
                return;
            }
            stream = openStream.call();
            upstream = stream;
            // Cancellation may have arrived before the upstream stream was published.
            if (listener.isCancelled()) {
                cancelCallback();
                return;
            }
            listener.start(stream.getRoot());
            while (awaitReady()) {
                if (!stream.next()) {
                    if (!listener.isCancelled()) {
                        listener.completed();
                    }
                    return;
                }
                // Readiness can change while next() is blocked. Keep at most this one batch pending.
                if (!awaitReady()) {
                    return;
                }
                listener.putNext();
            }
        } catch (Exception e) {
            LOG.warn("[ARROW] Error proxying result from {} {}", targetType, nodeKey, e);
            cancelUpstream("Error during streaming", e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            if (!listener.isCancelled()) {
                CallStatus status = e instanceof TimeoutException ? CallStatus.TIMED_OUT : CallStatus.INTERNAL;
                listener.error(status
                        .withDescription("Failed to proxy result from " + targetType + ": " + e.getMessage())
                        .toRuntimeException());
            }
        } finally {
            // FlightStream.close() drains its queue using interruptible reads before releasing buffers.
            boolean interrupted = Thread.interrupted();
            try {
                if (stream != null) {
                    try {
                        stream.close();
                    } catch (Exception e) {
                        interrupted |= e instanceof InterruptedException;
                        LOG.warn("[ARROW] Error closing {} stream", targetType, e);
                    }
                }
            } finally {
                upstream = null;
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
