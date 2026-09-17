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
package com.starrocks.http;

import com.starrocks.common.Config;
import com.starrocks.server.GracefulExitFlag;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.GenericFutureListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HttpConnectContextTest {

    @AfterEach
    public void tearDown() throws Exception {
        Field flagField = GracefulExitFlag.class.getDeclaredField("GRACEFUL_EXIT");
        flagField.setAccessible(true);
        ((AtomicBoolean) flagField.get(null)).set(false);
        Field beginField = GracefulExitFlag.class.getDeclaredField("BEGIN_NANO");
        beginField.setAccessible(true);
        ((AtomicLong) beginField.get(null)).set(0L);
        GracefulExitFlag.resetHttpAdmissionState();
    }

    @Test
    public void testFinishAdmittedHttpRequestWaitsForLastHttpWrite() throws Exception {
        HttpConnectContext ctx = new HttpConnectContext();
        CountDownLatch enteredAwait = new CountDownLatch(1);
        CountDownLatch releaseWrite = new CountDownLatch(1);
        ChannelFuture future = mock(ChannelFuture.class);
        when(future.awaitUninterruptibly()).thenAnswer(invocation -> {
            enteredAwait.countDown();
            if (!releaseWrite.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("last HTTP write was not completed");
            }
            return future;
        });
        ctx.setLastHttpWrite(future);
        Assertions.assertTrue(GracefulExitFlag.tryStartHttpRequest());
        Assertions.assertEquals(1, GracefulExitFlag.getActiveHttpRequests());

        Thread t = new Thread(ctx::finishAdmittedHttpRequest, "await-last-http-write");
        t.start();
        Assertions.assertTrue(enteredAwait.await(5, TimeUnit.SECONDS));
        Assertions.assertEquals(1, GracefulExitFlag.getActiveHttpRequests());

        releaseWrite.countDown();
        t.join(5000);
        Assertions.assertFalse(t.isAlive());
        Assertions.assertEquals(0, GracefulExitFlag.getActiveHttpRequests());
    }

    @Test
    public void testFinishAdmittedHttpRequestDefersOnEventLoop() throws Exception {
        HttpConnectContext ctx = new HttpConnectContext();
        ChannelFuture future = mock(ChannelFuture.class);
        Channel channel = mock(Channel.class);
        EventLoop loop = mock(EventLoop.class);
        when(future.channel()).thenReturn(channel);
        when(channel.eventLoop()).thenReturn(loop);
        when(loop.inEventLoop()).thenReturn(true);
        AtomicReference<GenericFutureListener> listener = new AtomicReference<>();
        when(future.addListener(any())).thenAnswer(invocation -> {
            listener.set(invocation.getArgument(0));
            return future;
        });
        ctx.setLastHttpWrite(future);
        Assertions.assertTrue(GracefulExitFlag.tryStartHttpRequest());
        Assertions.assertEquals(1, GracefulExitFlag.getActiveHttpRequests());

        ctx.finishAdmittedHttpRequest();
        Assertions.assertEquals(1, GracefulExitFlag.getActiveHttpRequests());
        Assertions.assertNotNull(listener.get());

        listener.get().operationComplete(future);
        Assertions.assertEquals(0, GracefulExitFlag.getActiveHttpRequests());
    }

    @Test
    public void testFinishHttpRequestAndMaybeCloseWaitsForLastHttpWrite() throws Exception {
        HttpConnectContext ctx = new HttpConnectContext();
        CountDownLatch enteredAwait = new CountDownLatch(1);
        CountDownLatch releaseWrite = new CountDownLatch(1);
        ChannelFuture future = mock(ChannelFuture.class);
        when(future.awaitUninterruptibly()).thenAnswer(invocation -> {
            enteredAwait.countDown();
            if (!releaseWrite.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("last HTTP write was not completed");
            }
            return future;
        });
        ctx.setLastHttpWrite(future);
        Assertions.assertTrue(GracefulExitFlag.tryStartHttpRequest());
        Assertions.assertEquals(1, GracefulExitFlag.getActiveHttpRequests());

        Thread t = new Thread(ctx::finishHttpRequestAndMaybeClose, "await-non-sql-http-write");
        t.start();
        Assertions.assertTrue(enteredAwait.await(5, TimeUnit.SECONDS));
        Assertions.assertEquals(1, GracefulExitFlag.getActiveHttpRequests());

        releaseWrite.countDown();
        t.join(5000);
        Assertions.assertFalse(t.isAlive());
        Assertions.assertEquals(0, GracefulExitFlag.getActiveHttpRequests());
    }

    @Test
    public void testFinishHttpRequestAndMaybeCloseClosesAfterWriteWhenRejecting() throws Exception {
        HttpConnectContext ctx = new HttpConnectContext();
        CountDownLatch enteredAwait = new CountDownLatch(1);
        CountDownLatch releaseWrite = new CountDownLatch(1);
        ChannelFuture future = mock(ChannelFuture.class);
        when(future.awaitUninterruptibly()).thenAnswer(invocation -> {
            enteredAwait.countDown();
            if (!releaseWrite.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("last HTTP write was not completed");
            }
            return future;
        });
        ChannelHandlerContext netty = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        when(netty.channel()).thenReturn(channel);
        when(channel.remoteAddress()).thenReturn(new java.net.InetSocketAddress("127.0.0.1", 1));
        when(channel.isActive()).thenReturn(true);
        ctx.setNettyChannel(netty);
        ctx.setLastHttpWrite(future);

        Assertions.assertTrue(GracefulExitFlag.tryStartHttpRequest());
        GracefulExitFlag.markGracefulExit();
        Field beginField = GracefulExitFlag.class.getDeclaredField("BEGIN_NANO");
        beginField.setAccessible(true);
        long acceptWindowNanos = TimeUnit.NANOSECONDS.convert(
                Config.graceful_exit_http_accept_window_ms, TimeUnit.MILLISECONDS);
        ((AtomicLong) beginField.get(null)).set(System.nanoTime() - acceptWindowNanos - 1L);
        Assertions.assertTrue(GracefulExitFlag.isHttpRejecting());

        Thread t = new Thread(ctx::finishHttpRequestAndMaybeClose, "close-after-write-when-rejecting");
        t.start();
        Assertions.assertTrue(enteredAwait.await(5, TimeUnit.SECONDS));
        verify(netty, never()).close();
        Assertions.assertEquals(1, GracefulExitFlag.getActiveHttpRequests());

        releaseWrite.countDown();
        t.join(5000);
        Assertions.assertFalse(t.isAlive());
        verify(netty).close();
        Assertions.assertEquals(0, GracefulExitFlag.getActiveHttpRequests());
    }
}
