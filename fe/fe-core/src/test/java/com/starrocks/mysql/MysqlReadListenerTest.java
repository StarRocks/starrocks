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

package com.starrocks.mysql;

import com.starrocks.mysql.nio.MySQLReadListener;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectProcessor;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Test;
import org.xnio.XnioIoThread;
import org.xnio.XnioWorker;
import org.xnio.channels.StreamSinkChannel;
import org.xnio.conduits.ConduitStreamSourceChannel;
import org.xnio.conduits.ReadReadyHandler;
import org.xnio.conduits.StreamSourceConduit;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

public class MysqlReadListenerTest {
    static class DummyStreamSourceConduit implements StreamSourceConduit {

        @Override
        public long transferTo(long l, long l1, FileChannel fileChannel) throws IOException {
            return 0;
        }

        @Override
        public long transferTo(long l, ByteBuffer byteBuffer, StreamSinkChannel streamSinkChannel) throws IOException {
            return 0;
        }

        @Override
        public int read(ByteBuffer byteBuffer) throws IOException {
            return 0;
        }

        @Override
        public long read(ByteBuffer[] byteBuffers, int i, int i1) throws IOException {
            return 0;
        }

        @Override
        public void terminateReads() throws IOException {

        }

        @Override
        public boolean isReadShutdown() {
            return false;
        }

        @Override
        public void resumeReads() {

        }

        @Override
        public void suspendReads() {

        }

        @Override
        public void wakeupReads() {

        }

        @Override
        public boolean isReadResumed() {
            return false;
        }

        @Override
        public void awaitReadable() throws IOException {

        }

        @Override
        public void awaitReadable(long l, TimeUnit timeUnit) throws IOException {

        }

        @Override
        public XnioIoThread getReadThread() {
            return null;
        }

        @Override
        public void setReadReadyHandler(ReadReadyHandler readReadyHandler) {

        }

        @Override
        public XnioWorker getWorker() {
            return null;
        }
    }

    @Test
    public void test() {

        new MockUp<MysqlPackageDecoder>() {
            @Mock
            public void consume(ByteBuffer srcBuffer) {
            }

            @Mock
            public RequestPackage poll() {
                return new RequestPackage(0, null);
            }
        };

        new MockUp<ConnectProcessor>() {
            @Mock
            public void processOnce(RequestPackage req) {

            }
        };

        new MockUp<ConduitStreamSourceChannel>() {
            private int counter = 0;

            @Mock
            public int read(ByteBuffer dst) throws IOException {
                if (counter++ != 0) {
                    return 0;
                }
                dst.putInt(1);
                return 4;
            }
        };

        final ConnectContext ctx = new ConnectContext();
        final ConnectProcessor processor = new ConnectProcessor(ctx);
        final MySQLReadListener listener = new MySQLReadListener(ctx, processor);
        final StreamSourceConduit source = new DummyStreamSourceConduit();
        final ConduitStreamSourceChannel channel = new ConduitStreamSourceChannel(null, source);
        listener.handleEvent(channel);
    }

    @Test
    public void testException() {
        new MockUp<MysqlPackageDecoder>() {
            @Mock
            public void consume(ByteBuffer srcBuffer) {
            }

            @Mock
            public RequestPackage poll() {
                return new RequestPackage(0, null);
            }
        };

        new MockUp<ConnectProcessor>() {
            @Mock
            public void processOnce(RequestPackage req) {

            }
        };

        new MockUp<ConduitStreamSourceChannel>() {

            @Mock
            public int read(ByteBuffer dst) throws IOException {
                throw new IOException();
            }
        };

        new MockUp<ConnectContext>() {
            @Mock
            public void cleanup() {

            }
        };

        final ConnectContext ctx = new ConnectContext();
        final ConnectProcessor processor = new ConnectProcessor(ctx);
        final MySQLReadListener listener = new MySQLReadListener(ctx, processor);
        final StreamSourceConduit source = new DummyStreamSourceConduit();
        final ConduitStreamSourceChannel channel = new ConduitStreamSourceChannel(null, source);
        listener.handleEvent(channel);
    }
}
