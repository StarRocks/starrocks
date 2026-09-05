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

import com.starrocks.common.Config;
import com.starrocks.proto.PCancelPlanFragmentResult;
import com.starrocks.proto.PExecPlanFragmentResult;
import com.starrocks.proto.PPlanFragmentCancelReason;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TUniqueId;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class BackendServiceClientTest {

    private static final TNetworkAddress BAD_ADDRESS = new TNetworkAddress("127.0.0.1", 1);

    // ---------------------------------------------------------------
    // Tests for isConnectionPoolException()
    // ---------------------------------------------------------------

    @Test
    public void testIsConnectionPoolException_directNoSuchElement() {
        assertTrue(BackendServiceClient.isConnectionPoolException(
                new NoSuchElementException("pool exhausted")));
    }

    @Test
    public void testIsConnectionPoolException_wrappedNoSuchElement() {
        // This is the exact pattern ChannelPool.getChannel() produces:
        //   catch (Exception e) { throw new RuntimeException(e.getMessage(), e); }
        NoSuchElementException cause = new NoSuchElementException("Unable to validate object");
        RuntimeException wrapped = new RuntimeException(cause.getMessage(), cause);
        assertTrue(BackendServiceClient.isConnectionPoolException(wrapped));
    }

    @Test
    public void testIsConnectionPoolException_otherRuntimeException() {
        assertFalse(BackendServiceClient.isConnectionPoolException(
                new RuntimeException("some other error")));
    }

    @Test
    public void testIsConnectionPoolException_unrelatedExceptions() {
        assertFalse(BackendServiceClient.isConnectionPoolException(
                new IllegalStateException("not a pool error")));
        assertFalse(BackendServiceClient.isConnectionPoolException(
                new NullPointerException()));
        // RuntimeException wrapping a non-NoSuchElementException
        assertFalse(BackendServiceClient.isConnectionPoolException(
                new RuntimeException("wrap", new IllegalArgumentException("bad arg"))));
    }

    // ---------------------------------------------------------------
    // Integration test: real brpc connection failure to a bad address
    // ---------------------------------------------------------------

    /**
     * Verify that connecting to a non-existent address via the raw PBackendService proxy
     * throws RuntimeException wrapping NoSuchElementException.
     *
     * This proves the exception wrapping behavior of ChannelPool.getChannel():
     *   GenericObjectPool.borrowObject() throws NoSuchElementException
     *   → ChannelPool.getChannel() catches and re-throws as RuntimeException(msg, cause)
     *   → ProtobufRpcProxy.invoke() propagates it unchanged
     *
     * This is the exact exception pattern that our isConnectionPoolException() fix handles.
     */
    @Test
    public void testRealConnectionFailureThrowsWrappedNoSuchElementException() {
        PBackendService service = BrpcProxy.getBackendService(BAD_ADDRESS);
        assertNotNull(service, "getBackendService should return a lazy proxy (no connection yet)");

        PExecPlanFragmentRequest request = new PExecPlanFragmentRequest();
        request.setRequest(new byte[0]);
        request.setAttachmentProtocol("binary");

        RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> service.execPlanFragmentAsync(request));

        // Verify the exception is a RuntimeException wrapping NoSuchElementException,
        // which is exactly the pattern our fix detects.
        assertInstanceOf(NoSuchElementException.class, thrown.getCause(),
                "ChannelPool.getChannel() should wrap NoSuchElementException in RuntimeException");
        assertTrue(BackendServiceClient.isConnectionPoolException(thrown),
                "isConnectionPoolException must detect the wrapped exception");
    }

    /**
     * Verify that BackendServiceClient retries on connection pool failure and ultimately
     * throws RpcException after both attempts fail (sendRequestAsync path).
     */
    @Test
    public void testExecPlanFragmentRetriesOnConnectionPoolFailure() {
        BackendServiceClient client = BackendServiceClient.getInstance();

        int originalWaitTime = Config.brpc_connection_pool_retry_wait_time_ms;
        Config.brpc_connection_pool_retry_wait_time_ms = 0;
        try {
            RpcException thrown = assertThrows(RpcException.class,
                    () -> client.execPlanFragmentAsync(BAD_ADDRESS, new byte[0], "binary"));
            assertTrue(thrown.getMessage().contains("127.0.0.1"),
                    "RpcException message should contain the target host");
            // After retry failure, the cause chain should be preserved for debugging
            assertNotNull(thrown.getCause(),
                    "RpcException should preserve the retry failure cause for debugging");
            assertTrue(BackendServiceClient.isConnectionPoolException(thrown.getCause()),
                    "The cause should be the connection pool exception from the retry attempt");
        } finally {
            Config.brpc_connection_pool_retry_wait_time_ms = originalWaitTime;
        }
    }

    /**
     * Verify that cancelPlanFragmentAsync also retries on connection pool failure
     * and throws RpcException with cause chain preserved.
     */
    @Test
    public void testCancelPlanFragmentRetriesOnConnectionPoolFailure() {
        BackendServiceClient client = BackendServiceClient.getInstance();
        TUniqueId queryId = new TUniqueId(1L, 2L);
        TUniqueId finstId = new TUniqueId(3L, 4L);

        int originalWaitTime = Config.brpc_connection_pool_retry_wait_time_ms;
        Config.brpc_connection_pool_retry_wait_time_ms = 0;
        try {
            RpcException thrown = assertThrows(RpcException.class,
                    () -> client.cancelPlanFragmentAsync(BAD_ADDRESS, queryId, finstId,
                            PPlanFragmentCancelReason.INTERNAL_ERROR, true, null));
            assertTrue(thrown.getMessage().contains("127.0.0.1"),
                    "RpcException message should contain the target host");
            assertNotNull(thrown.getCause(),
                    "RpcException should preserve the retry failure cause for debugging");
            assertTrue(BackendServiceClient.isConnectionPoolException(thrown.getCause()),
                    "The cause should be the connection pool exception from the retry attempt");
        } finally {
            Config.brpc_connection_pool_retry_wait_time_ms = originalWaitTime;
        }
    }

    // ---------------------------------------------------------------
    // Mock-based tests for retry-success and interrupt handling
    // ---------------------------------------------------------------

    /**
     * Verify that sendRequestAsync retries once and succeeds when the first call fails
     * with a connection pool exception but the second call succeeds.
     */
    @Test
    public void testExecPlanFragmentRetrySucceeds() throws Exception {
        PBackendService mockService = mock(PBackendService.class);
        AtomicInteger callCount = new AtomicInteger(0);

        when(mockService.execPlanFragmentAsync(any())).thenAnswer(invocation -> {
            if (callCount.getAndIncrement() == 0) {
                throw new RuntimeException("Unable to validate object",
                        new NoSuchElementException("Unable to validate object"));
            }
            return CompletableFuture.completedFuture(new PExecPlanFragmentResult());
        });

        int originalWaitTime = Config.brpc_connection_pool_retry_wait_time_ms;
        Config.brpc_connection_pool_retry_wait_time_ms = 0;
        try (MockedStatic<BrpcProxy> mockedBrpcProxy = mockStatic(BrpcProxy.class)) {
            mockedBrpcProxy.when(() -> BrpcProxy.getBackendService(any()))
                    .thenReturn(mockService);

            BackendServiceClient client = BackendServiceClient.getInstance();
            assertNotNull(client.execPlanFragmentAsync(BAD_ADDRESS, new byte[0], "binary"));
            assertTrue(callCount.get() >= 2, "Service should have been called at least twice (initial + retry)");
        } finally {
            Config.brpc_connection_pool_retry_wait_time_ms = originalWaitTime;
        }
    }

    /**
     * Verify that cancelPlanFragmentAsync retries once and succeeds when the first call
     * fails with a connection pool exception but the second call succeeds.
     */
    @Test
    public void testCancelPlanFragmentRetrySucceeds() throws Exception {
        PBackendService mockService = mock(PBackendService.class);
        AtomicInteger callCount = new AtomicInteger(0);

        when(mockService.cancelPlanFragmentAsync(any())).thenAnswer(invocation -> {
            if (callCount.getAndIncrement() == 0) {
                throw new RuntimeException("Unable to validate object",
                        new NoSuchElementException("Unable to validate object"));
            }
            return CompletableFuture.completedFuture(new PCancelPlanFragmentResult());
        });

        int originalWaitTime = Config.brpc_connection_pool_retry_wait_time_ms;
        Config.brpc_connection_pool_retry_wait_time_ms = 0;
        try (MockedStatic<BrpcProxy> mockedBrpcProxy = mockStatic(BrpcProxy.class)) {
            mockedBrpcProxy.when(() -> BrpcProxy.getBackendService(any()))
                    .thenReturn(mockService);

            BackendServiceClient client = BackendServiceClient.getInstance();
            TUniqueId queryId = new TUniqueId(1L, 2L);
            TUniqueId finstId = new TUniqueId(3L, 4L);
            assertNotNull(client.cancelPlanFragmentAsync(BAD_ADDRESS, queryId, finstId,
                    PPlanFragmentCancelReason.INTERNAL_ERROR, true, null));
            assertTrue(callCount.get() >= 2, "Service should have been called at least twice (initial + retry)");
        } finally {
            Config.brpc_connection_pool_retry_wait_time_ms = originalWaitTime;
        }
    }

    /**
     * Verify that the interrupt flag is restored when Thread.sleep is interrupted
     * during retry wait in sendRequestAsync.
     *
     * Key insight: Thread.sleep() checks the interrupt flag on entry. If the flag
     * is already set, it throws InterruptedException immediately without waiting.
     * So we set the interrupt flag inside the first (failing) call, guaranteeing
     * that the subsequent Thread.sleep() will be interrupted deterministically.
     */
    @Test
    public void testRetrySleepInterruptRestoresFlag() throws Exception {
        PBackendService mockService = mock(PBackendService.class);
        when(mockService.execPlanFragmentAsync(any())).thenAnswer(invocation -> {
            // Set interrupt flag before returning — the next Thread.sleep() will
            // see it immediately and throw InterruptedException.
            Thread.currentThread().interrupt();
            throw new RuntimeException("Unable to validate object",
                    new NoSuchElementException("Unable to validate object"));
        });

        int originalWaitTime = Config.brpc_connection_pool_retry_wait_time_ms;
        Config.brpc_connection_pool_retry_wait_time_ms = 10000;
        try (MockedStatic<BrpcProxy> mockedBrpcProxy = mockStatic(BrpcProxy.class)) {
            mockedBrpcProxy.when(() -> BrpcProxy.getBackendService(any()))
                    .thenReturn(mockService);

            BackendServiceClient client = BackendServiceClient.getInstance();
            assertThrows(RpcException.class,
                    () -> client.execPlanFragmentAsync(BAD_ADDRESS, new byte[0], "binary"));

            // The interrupt flag should have been restored by our fix
            assertTrue(Thread.interrupted(),
                    "Interrupt flag should be restored after InterruptedException in retry sleep");
        } finally {
            Config.brpc_connection_pool_retry_wait_time_ms = originalWaitTime;
            Thread.interrupted();
        }
    }

    /**
     * Verify that the interrupt flag is restored when Thread.sleep is interrupted
     * during retry wait in cancelPlanFragmentAsync.
     */
    @Test
    public void testCancelRetrySleepInterruptRestoresFlag() throws Exception {
        PBackendService mockService = mock(PBackendService.class);
        when(mockService.cancelPlanFragmentAsync(any())).thenAnswer(invocation -> {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Unable to validate object",
                    new NoSuchElementException("Unable to validate object"));
        });

        int originalWaitTime = Config.brpc_connection_pool_retry_wait_time_ms;
        Config.brpc_connection_pool_retry_wait_time_ms = 10000;
        try (MockedStatic<BrpcProxy> mockedBrpcProxy = mockStatic(BrpcProxy.class)) {
            mockedBrpcProxy.when(() -> BrpcProxy.getBackendService(any()))
                    .thenReturn(mockService);

            BackendServiceClient client = BackendServiceClient.getInstance();
            TUniqueId queryId = new TUniqueId(1L, 2L);
            TUniqueId finstId = new TUniqueId(3L, 4L);
            assertThrows(RpcException.class,
                    () -> client.cancelPlanFragmentAsync(BAD_ADDRESS, queryId, finstId,
                            PPlanFragmentCancelReason.INTERNAL_ERROR, true, null));

            assertTrue(Thread.interrupted(),
                    "Interrupt flag should be restored after InterruptedException in cancel retry sleep");
        } finally {
            Config.brpc_connection_pool_retry_wait_time_ms = originalWaitTime;
            Thread.interrupted();
        }
    }
}
