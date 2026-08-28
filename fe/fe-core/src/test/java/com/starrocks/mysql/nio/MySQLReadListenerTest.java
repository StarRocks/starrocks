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

package com.starrocks.mysql.nio;

import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectProcessor;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.xnio.XnioWorker;
import org.xnio.conduits.ConduitStreamSourceChannel;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class MySQLReadListenerTest {
    @Mocked
    private ConnectContext ctx;
    @Mocked
    private ConnectProcessor connectProcessor;

    private MySQLReadListener listener;

    private boolean invokeIsTerminated() throws Exception {
        Method method = MySQLReadListener.class.getDeclaredMethod("isTerminated");
        method.setAccessible(true);
        return (boolean) method.invoke(listener);
    }

    @BeforeEach
    public void setUp() {
        listener = new MySQLReadListener(ctx, connectProcessor);
    }

    @AfterEach
    public void tearDown() {
    }

    @Test
    public void testIsTerminatedWhenTerminatedFlagIsTrue() throws Exception {
        // Set terminated flag to true
        Deencapsulation.setField(listener, "terminated", true);

        boolean result = invokeIsTerminated();

        Assertions.assertTrue(result, "isTerminated should return true when terminated flag is true");
    }

    @Test
    public void testIsTerminatedWhenGracefulCloseIsNotMarked() throws Exception {
        Deencapsulation.setField(listener, "terminated", false);

        new Expectations() {
            {
                ctx.isGracefulCloseConn();
                result = false;
            }
        };

        Assertions.assertFalse(invokeIsTerminated(),
                "isTerminated should return false before the processor marks graceful close");
    }

    @Test
    public void testIsTerminatedWhenGracefulCloseMarkedWithoutExecutor() throws Exception {
        // Control commands (COM_PING, COM_INIT_DB, COM_RESET_CONNECTION) never create a StmtExecutor,
        // but must still terminate a connection marked for graceful close during exit.
        Deencapsulation.setField(listener, "terminated", false);

        new Expectations() {
            {
                ctx.isGracefulCloseConn();
                result = true;
            }
        };

        Assertions.assertTrue(invokeIsTerminated(),
                "isTerminated should return true for non-statement commands once graceful close is marked");
    }

    @Test
    public void testClientDisconnectHandsTheKillToAWorker(@Mocked ConduitStreamSourceChannel channel,
                                                          @Mocked XnioWorker worker) throws Exception {
        boolean savedKillAfterDisconnect = Config.mysql_service_kill_after_disconnect;
        Config.mysql_service_kill_after_disconnect = true;
        List<Runnable> handedToWorker = new ArrayList<>();
        try {
            new Expectations() {
                {
                    channel.read((ByteBuffer) any);
                    result = -1;
                    channel.getWorker();
                    result = worker;
                    worker.execute((Runnable) any);
                    result = new Delegate<Void>() {
                        @SuppressWarnings("unused")
                        void execute(Runnable task) {
                            handedToWorker.add(task);
                        }
                    };
                }
            };

            listener.handleEvent(channel);

            // handleEvent runs on a shared XNIO I/O thread. Killing the running query takes the
            // coordinator's lock, which the query's own thread holds across fragment deployment,
            // so none of it may happen before handleEvent returns.
            Assertions.assertEquals(1, handedToWorker.size(),
                    "the disconnect kill should have been handed to a worker");
            new Verifications() {
                {
                    ctx.cleanup();
                    times = 0;
                }
            };

            handedToWorker.get(0).run();

            new Verifications() {
                {
                    ctx.cleanup();
                    times = 1;
                }
            };
        } finally {
            Config.mysql_service_kill_after_disconnect = savedKillAfterDisconnect;
        }
    }
}
