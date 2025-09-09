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

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import static com.starrocks.http.HttpServerHandler.HTTP_CONNECT_CONTEXT_ATTRIBUTE_KEY;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class HttpServerTestUtils {

    protected static class MockExecutor implements Executor {

        private boolean rejectExecute = false;
        private final LinkedList<Runnable> pendingTasks = new LinkedList<>();

        public void setRejectExecute(boolean rejectExecute) {
            this.rejectExecute = rejectExecute;
        }

        int pendingTaskCount() {
            return pendingTasks.size();
        }

        @Override
        public void execute(Runnable runnable) {
            if (rejectExecute) {
                throw new RejectedExecutionException("mock reject");
            }
            pendingTasks.add(runnable);
        }

        public void runOneTask() {
            if (!pendingTasks.isEmpty()) {
                pendingTasks.pollFirst().run();
            }
        }
    }

    protected MockChannelHandlerContext createChannelHandlerContext() {
        return mock(MockChannelHandlerContext.class,
                withSettings()
                        .useConstructor()
                        .defaultAnswer(CALLS_REAL_METHODS));
    }

    protected abstract static class MockChannelHandlerContext implements ChannelHandlerContext {

        private final Channel channel;
        private final ChannelFuture channelFuture;
        private final ConcurrentLinkedQueue<Object> responses = new ConcurrentLinkedQueue<>();
        private volatile boolean flushed = false;

        public MockChannelHandlerContext() {
            Attribute attribute = mock(Attribute.class);
            when(attribute.get()).thenReturn(null);
            this.channel = mock(Channel.class);
            when(channel.attr(same(HTTP_CONNECT_CONTEXT_ATTRIBUTE_KEY))).thenReturn(attribute);
            this.channelFuture = mock(ChannelFuture.class);
        }

        @Override
        public ChannelHandlerContext flush() {
            this.flushed = true;
            return this;
        }

        public boolean isFlushed() {
            return flushed;
        }

        @Override
        public ChannelFuture writeAndFlush(Object object) {
            responses.add(object);
            return channelFuture;
        }

        @Override
        public Channel channel() {
            return channel;
        }

        public int numResponses() {
            return responses.size();
        }

        public Object pollResponse() {
            return responses.poll();
        }
    }
}
