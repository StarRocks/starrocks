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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/HttpServer.java

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

package com.starrocks.http;

import com.starrocks.common.Config;
import com.starrocks.common.Log4jConfig;
import com.starrocks.http.action.BackendAction;
import com.starrocks.http.action.HaAction;
import com.starrocks.http.action.IndexAction;
import com.starrocks.http.action.LogAction;
import com.starrocks.http.action.QueryAction;
import com.starrocks.http.action.QueryProfileAction;
import com.starrocks.http.action.SessionAction;
import com.starrocks.http.action.StaticResourceAction;
import com.starrocks.http.action.SystemAction;
import com.starrocks.http.action.VariableAction;
import com.starrocks.http.common.StarRocksHttpPostObjectAggregator;
import com.starrocks.http.meta.ColocateMetaService;
import com.starrocks.http.meta.GlobalDictMetaService;
import com.starrocks.http.meta.MetaService.CheckAction;
import com.starrocks.http.meta.MetaService.DumpAction;
import com.starrocks.http.meta.MetaService.DumpStarMgrAction;
import com.starrocks.http.meta.MetaService.ImageAction;
import com.starrocks.http.meta.MetaService.InfoAction;
import com.starrocks.http.meta.MetaService.JournalIdAction;
import com.starrocks.http.meta.MetaService.PutAction;
import com.starrocks.http.meta.MetaService.RoleAction;
import com.starrocks.http.meta.MetaService.VersionAction;
import com.starrocks.http.rest.BootstrapFinishAction;
import com.starrocks.http.rest.CancelStreamLoad;
import com.starrocks.http.rest.CheckDecommissionAction;
import com.starrocks.http.rest.ConnectionAction;
import com.starrocks.http.rest.ExecuteSqlAction;
import com.starrocks.http.rest.FeatureAction;
import com.starrocks.http.rest.GetClusterSnapshotRestoreStateAction;
import com.starrocks.http.rest.GetDdlStmtAction;
import com.starrocks.http.rest.GetLoadInfoAction;
import com.starrocks.http.rest.GetLogFileAction;
import com.starrocks.http.rest.GetSmallFileAction;
import com.starrocks.http.rest.GetStreamLoadState;
import com.starrocks.http.rest.HealthAction;
import com.starrocks.http.rest.IdleAction;
import com.starrocks.http.rest.LoadAction;
import com.starrocks.http.rest.MetaReplayerCheckAction;
import com.starrocks.http.rest.MetricsAction;
import com.starrocks.http.rest.MigrationAction;
import com.starrocks.http.rest.OAuth2Action;
import com.starrocks.http.rest.ProfileAction;
import com.starrocks.http.rest.QueryDetailAction;
import com.starrocks.http.rest.QueryDumpAction;
import com.starrocks.http.rest.RowCountAction;
import com.starrocks.http.rest.SetConfigAction;
import com.starrocks.http.rest.ShowDataAction;
import com.starrocks.http.rest.ShowMetaInfoAction;
import com.starrocks.http.rest.ShowProcAction;
import com.starrocks.http.rest.ShowRuntimeInfoAction;
import com.starrocks.http.rest.StopFeAction;
import com.starrocks.http.rest.StorageTypeCheckAction;
import com.starrocks.http.rest.StreamLoadMetaAction;
import com.starrocks.http.rest.SyncCloudTableMetaAction;
import com.starrocks.http.rest.TableQueryPlanAction;
import com.starrocks.http.rest.TableRowCountAction;
import com.starrocks.http.rest.TableSchemaAction;
import com.starrocks.http.rest.TransactionLoadAction;
import com.starrocks.http.rest.TriggerAction;
import com.starrocks.http.rest.v2.TablePartitionAction;
import com.starrocks.leader.MetaHelper;
import com.starrocks.metric.GaugeMetric;
import com.starrocks.metric.GaugeMetricImpl;
import com.starrocks.metric.Metric;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoop;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.concurrent.EventExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.starrocks.http.HttpMetricRegistry.HTTP_WORKERS_NUM;
import static com.starrocks.http.HttpMetricRegistry.HTTP_WORKER_PENDING_TASKS_NUM;

public class HttpServer {
    private static final Logger LOG = LogManager.getLogger(HttpServer.class);
    private int port;
    private ActionController controller;

    private Thread serverThread;

    private AtomicBoolean isStarted = new AtomicBoolean(false);

    public HttpServer(int port) {
        this.port = port;
        controller = new ActionController();
    }

    public void setup() throws IllegalArgException {
        registerActions();
    }

    public ActionController getController() {
        return controller;
    }

    private void registerActions() throws IllegalArgException {
        // add rest action
        LoadAction.registerAction(controller);
        StreamLoadMetaAction.registerAction(controller);
        TransactionLoadAction.registerAction(controller);
        GetLoadInfoAction.registerAction(controller);
        SetConfigAction.registerAction(controller);
        GetDdlStmtAction.registerAction(controller);
        MigrationAction.registerAction(controller);
        StorageTypeCheckAction.registerAction(controller);
        CancelStreamLoad.registerAction(controller);
        GetStreamLoadState.registerAction(controller);

        // add web action
        IndexAction.registerAction(controller);
        SystemAction.registerAction(controller);
        BackendAction.registerAction(controller);
        LogAction.registerAction(controller);
        QueryAction.registerAction(controller);
        QueryProfileAction.registerAction(controller);
        SessionAction.registerAction(controller);
        VariableAction.registerAction(controller);
        StaticResourceAction.registerAction(controller);
        HaAction.registerAction(controller);

        // rest action
        HealthAction.registerAction(controller);
        FeatureAction.registerAction(controller);
        GetClusterSnapshotRestoreStateAction.registerAction(controller);
        MetricsAction.registerAction(controller);
        ShowMetaInfoAction.registerAction(controller);
        ShowProcAction.registerAction(controller);
        ShowRuntimeInfoAction.registerAction(controller);
        GetLogFileAction.registerAction(controller);
        TriggerAction.registerAction(controller);
        GetSmallFileAction.registerAction(controller);
        RowCountAction.registerAction(controller);
        CheckDecommissionAction.registerAction(controller);
        MetaReplayerCheckAction.registerAction(controller);
        ColocateMetaService.BucketSeqAction.registerAction(controller);
        ColocateMetaService.ColocateMetaAction.registerAction(controller);
        ColocateMetaService.MarkGroupStableAction.registerAction(controller);
        ColocateMetaService.MarkGroupUnstableAction.registerAction(controller);
        ColocateMetaService.UpdateGroupAction.registerAction(controller);
        GlobalDictMetaService.ForbitTableAction.registerAction(controller);
        ProfileAction.registerAction(controller);
        QueryDetailAction.registerAction(controller);
        ConnectionAction.registerAction(controller);
        ShowDataAction.registerAction(controller);
        QueryDumpAction.registerAction(controller);
        SyncCloudTableMetaAction.registerAction(controller);
        IdleAction.registerAction(controller);
        // for stop FE
        StopFeAction.registerAction(controller);
        ExecuteSqlAction.registerAction(controller);

        // meta service action
        File imageDir = MetaHelper.getLeaderImageDir();
        ImageAction.registerAction(controller, imageDir);
        InfoAction.registerAction(controller, imageDir);
        VersionAction.registerAction(controller, imageDir);
        PutAction.registerAction(controller, imageDir);
        JournalIdAction.registerAction(controller, imageDir);
        CheckAction.registerAction(controller, imageDir);
        DumpAction.registerAction(controller, imageDir);
        DumpStarMgrAction.registerAction(controller, imageDir);
        RoleAction.registerAction(controller, imageDir);

        // external usage
        TableRowCountAction.registerAction(controller);
        TableSchemaAction.registerAction(controller);
        com.starrocks.http.rest.v2.TableSchemaAction.registerAction(controller);
        TablePartitionAction.registerAction(controller);
        TableQueryPlanAction.registerAction(controller);

        BootstrapFinishAction.registerAction(controller);

        OAuth2Action.registerAction(controller);
    }

    public void start() {
        serverThread = new Thread(new HttpServerThread(), "FE Http Server");
        serverThread.start();
    }

    protected class StarrocksHttpServerInitializer extends ChannelInitializer<SocketChannel> {
        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ch.pipeline().addLast(new HttpServerCodec(
                            Config.http_max_initial_line_length,
                            Config.http_max_header_size,
                            Config.http_max_chunk_size,
                            Config.enable_http_validate_headers))
                    .addLast(new StarRocksHttpPostObjectAggregator(100 * 65536))
                    .addLast(new ChunkedWriteHandler())
                    // add content compressor
                    .addLast(new CustomHttpContentCompressor())
                    .addLast(new HttpServerHandler(controller));
        }
    }

    private class CustomHttpContentCompressor extends HttpContentCompressor {
        private boolean compressResponse = false;

        @Override
        protected void decode(ChannelHandlerContext ctx, HttpRequest msg, List<Object> out) throws Exception {
            if (msg.uri().startsWith(MetricsAction.API_PATH)) {
                // only `/metrics` api got compressed right now
                compressResponse = true;
            }
            super.decode(ctx, msg, out);
        }

        @Override
        protected Result beginEncode(HttpResponse headers, String acceptEncoding) throws Exception {
            if (!compressResponse) {
                return null;
            }
            return super.beginEncode(headers, acceptEncoding);
        }
    }

    ServerBootstrap serverBootstrap;

    private class HttpServerThread implements Runnable {
        @Override
        public void run() {
            // Configure the server.
            EventLoopGroup bossGroup = new NioEventLoopGroup();
            int numWorkerThreads = Math.max(0, Config.http_worker_threads_num);
            NioEventLoopGroup workerGroup = new NioEventLoopGroup(numWorkerThreads);
            try {
                serverBootstrap = new ServerBootstrap();
                serverBootstrap.option(ChannelOption.SO_BACKLOG, Config.http_backlog_num);
                // reused address and port to avoid bind already exception
                serverBootstrap.option(ChannelOption.SO_REUSEADDR, true);
                serverBootstrap.childOption(ChannelOption.SO_REUSEADDR, true);
                serverBootstrap.group(bossGroup, workerGroup)
                        .channel(NioServerSocketChannel.class)
                        .childHandler(new StarrocksHttpServerInitializer());
                Channel ch = serverBootstrap.bind(port).sync().channel();

                isStarted.set(true);
                registerMetrics(workerGroup);
                LOG.info("HttpServer started with port {}", port);
                // block until server is closed
                ch.closeFuture().sync();
            } catch (Exception e) {
                LOG.error("Fail to start FE query http server[port: " + port + "] ", e);
                System.exit(-1);
            } finally {
                bossGroup.shutdownGracefully();
                workerGroup.shutdownGracefully();
            }
        }
    }

    private void registerMetrics(NioEventLoopGroup workerGroup) {
        HttpMetricRegistry httpMetricRegistry = HttpMetricRegistry.getInstance();

        GaugeMetricImpl<Long> httpWorkersNum = new GaugeMetricImpl<>(
                HTTP_WORKERS_NUM, Metric.MetricUnit.NOUNIT, "the number of http workers");
        httpWorkersNum.setValue(0L);
        httpMetricRegistry.registerGauge(httpWorkersNum);

        GaugeMetric<Long> pendingTasks = new GaugeMetric<>(HTTP_WORKER_PENDING_TASKS_NUM, Metric.MetricUnit.NOUNIT,
                "the number of tasks that are pending for processing in the queues of http workers") {
            @Override
            public Long getValue() {
                if (!Config.enable_http_detail_metrics) {
                    return 0L;
                }
                long pendingTasks = 0;
                for (EventExecutor executor : workerGroup) {
                    if (executor instanceof NioEventLoop) {
                        pendingTasks += ((NioEventLoop) executor).pendingTasks();
                    }
                }
                return pendingTasks;
            }
        };
        httpMetricRegistry.registerGauge(pendingTasks);
    }

    // used for test, release bound port
    public void shutDown() {
        if (serverBootstrap != null) {
            Future future =
                    serverBootstrap.config().group().shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
            try {
                future.get();
                isStarted.set(false);
                LOG.info("HttpServer was closed completely");
            } catch (Throwable e) {
                LOG.warn("Exception happened when close HttpServer", e);
            }
            serverBootstrap = null;
        }
    }

    public boolean isStarted() {
        return isStarted.get();
    }

    public static void main(String[] args) throws Exception {
        Log4jConfig.initLogging();
        HttpServer httpServer = new HttpServer(8080);
        httpServer.setup();
        System.out.println("before start http server.");
        httpServer.start();
        System.out.println("after start http server.");

        while (true) {
            Thread.sleep(2000);
        }
    }
}
