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
import com.starrocks.mysql.MysqlPackageDecoder;
import com.starrocks.mysql.RequestPackage;
import com.starrocks.mysql.ssl.SSLDecoder;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectProcessor;
import com.starrocks.rpc.RpcException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xnio.ChannelListener;
import org.xnio.conduits.ConduitStreamSourceChannel;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class MySQLReadListener implements ChannelListener<ConduitStreamSourceChannel> {
    private static final Logger LOG = LogManager.getLogger(MySQLReadListener.class);
    private final ConnectContext ctx;
    private final ConnectProcessor connectProcessor;
    private final MysqlPackageDecoder packageDecoder = new MysqlPackageDecoder();

    protected static final int DEFAULT_BUFFER_SIZE = 16 * 1024;
    private final ByteBuffer readBuffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
    private final SSLDecoder sslDecoder;
    private volatile boolean terminated = false;
    private final AtomicInteger pendingTasks = new AtomicInteger(0);

    public MySQLReadListener(ConnectContext connectContext, ConnectProcessor connectProcessor) {
        this.ctx = connectContext;
        this.connectProcessor = connectProcessor;
        this.sslDecoder = this.ctx.getMysqlChannel().getSSLDecoder();
    }

    @Override
    public void handleEvent(ConduitStreamSourceChannel channel) {
        try {
            while (true) {
                int bytesRead = channel.read(readBuffer);

                if (bytesRead == 0) {
                    return;
                }

                if (bytesRead == -1) {
                    terminated = true;
                    LOG.info("Client closed connection: {} remote={}", ctx.getConnectionId(),
                            ctx.getMysqlChannel().getRemoteHostPortString());
                    if (Config.mysql_service_kill_after_disconnect) {
                        killRunningQuery();
                    } else {
                        tryCleanup();
                    }
                    return;
                }

                readBuffer.flip();

                if (sslDecoder != null) {
                    sslDecoder.feed(readBuffer);
                    packageDecoder.consume(sslDecoder.decode());
                } else {
                    packageDecoder.consume(readBuffer);
                }

                readBuffer.compact();

                RequestPackage pkg;
                while ((pkg = packageDecoder.poll()) != null) {
                    final RequestPackage req = pkg;
                    pendingTasks.incrementAndGet();
                    channel.getWorker().execute(() -> {
                        handleRequest(req);
                    });
                }
            }
        } catch (Throwable t) {
            LOG.error("Unexpected error in MySQLReadListener", t);
            ctx.setKilled();
            ctx.cleanup();
        }
    }

    private void tryCleanup() {
        if (terminated && pendingTasks.get() == 0) {
            ctx.cleanup();
        }
    }

    private void taskCompleted() {
        pendingTasks.decrementAndGet();
        tryCleanup();
    }

    private void killRunningQuery() {
        if (!ctx.isKilled() && ctx.getState().isRunning()) {
            ctx.kill(false, "client closed");
        }
        ctx.cleanup();
    }

    private synchronized void handleRequest(RequestPackage req) {
        ctx.setThreadLocalInfo();
        try {
            connectProcessor.processOnce(req);
            if (ctx.isKilled() || terminated) {
                ctx.stopAcceptQuery();
                ctx.cleanup();
            }
        } catch (RpcException e) {
            LOG.debug("Exception happened in one session({}).", ctx, e);
            ctx.setKilled();
            ctx.cleanup();
        } catch (Exception e) {
            LOG.warn("Exception happened in one session({}).", ctx, e);
            ctx.setKilled();
            ctx.cleanup();
        } finally {
            taskCompleted();
            ConnectContext.remove();
        }
    }
}
