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

package com.starrocks.leader;

import com.starrocks.common.Config;
import com.starrocks.common.ConfigBase;
import com.starrocks.common.InvalidConfException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.journal.CheckpointException;
import com.starrocks.journal.CheckpointWorker;
import com.starrocks.journal.Journal;
import com.starrocks.persist.ImageFiles;
import com.starrocks.persist.ImageFormatVersion;
import com.starrocks.rpc.ThriftRPCRequestExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.system.Frontend;
import com.starrocks.thrift.FrontendService;
import com.starrocks.thrift.TStartCheckpointRequest;
import com.starrocks.thrift.TStartCheckpointResponse;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.starrocks.persist.image.ImageLifecycleTestSupport.writeV2;
import static com.starrocks.persist.image.ImageLifecycleTestSupport.writeV3;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CheckpointImageV3Test {
    @TempDir
    Path temporary;
    private String oldMeta;
    private int oldPort;
    private boolean oldDisable;
    private boolean oldOnlyLeader;
    private MockedStatic<GlobalStateMgr> globals;
    private GlobalStateMgr state;
    private NodeMgr nodes;
    private Journal journal;
    private CheckpointController controller;
    private HttpServer server;
    private final List<String> requests = new CopyOnWriteArrayList<>();
    private final AtomicBoolean failV3 = new AtomicBoolean();

    @BeforeEach
    public void setUp() throws Exception {
        oldMeta = Config.meta_dir;
        oldPort = Config.http_port;
        oldDisable = Config.disable_image_v2_write;
        oldOnlyLeader = Config.checkpoint_only_on_leader;
        Config.meta_dir = temporary.resolve("leader").toString();
        Config.disable_image_v2_write = false;
        Config.checkpoint_only_on_leader = false;
        state = mock(GlobalStateMgr.class);
        nodes = mock(NodeMgr.class);
        journal = mock(Journal.class);
        when(journal.getPrefix()).thenReturn("");
        when(journal.deleteJournalsAndGetMinJournalId(anyLong())).thenAnswer(i -> i.getArgument(0));
        when(state.getNodeMgr()).thenReturn(nodes);
        when(state.getEpoch()).thenReturn(1L);
        Frontend leader = frontend("leader", "127.0.0.1");
        when(nodes.getMySelf()).thenReturn(leader);
        when(nodes.getNodeName()).thenReturn("leader");
        Frontend follower = frontend("follower", "127.0.0.1");
        Frontend observer = frontend("observer", "127.0.0.2");
        when(nodes.getOtherFrontends()).thenReturn(List.of(follower, observer));
        when(nodes.getFeByName("follower")).thenReturn(follower);
        when(nodes.getFeByName("observer")).thenReturn(observer);
        when(nodes.getFeByName("leader")).thenReturn(leader);
        when(nodes.getAllFrontends()).thenReturn(new java.util.ArrayList<>());
        globals = Mockito.mockStatic(GlobalStateMgr.class, Mockito.CALLS_REAL_METHODS);
        globals.when(GlobalStateMgr::getServingState).thenReturn(state);
        globals.when(GlobalStateMgr::getCurrentState).thenReturn(state);
        controller = new CheckpointController("image-test", journal, "");
        server = HttpServer.create(new InetSocketAddress("0.0.0.0", 0), 0);
        server.createContext("/put", exchange -> {
            String query = exchange.getRequestURI().getQuery();
            requests.add(query);
            Map<String, String> params = Arrays.stream(query.split("&")).map(v -> v.split("=", 2))
                    .collect(Collectors.toMap(v -> v[0], v -> v[1]));
            ImageFormatVersion format = ImageFormatVersion.valueOf(params.get("image_format_version"));
            if (format == ImageFormatVersion.v3 && failV3.compareAndSet(true, false)) {
                exchange.sendResponseHeaders(503, -1);
                exchange.close();
                return;
            }
            long id = Long.parseLong(params.get("version"));
            Path target = temporary.resolve(exchange.getLocalAddress().getAddress().getHostAddress()).resolve(format.name());
            Files.createDirectories(target);
            Path source = root().resolve(format.name());
            Files.copy(source.resolve("image." + id), target.resolve("image." + id), StandardCopyOption.REPLACE_EXISTING);
            if (format == ImageFormatVersion.v2) {
                Files.copy(source.resolve("checksum." + id), target.resolve("checksum." + id),
                        StandardCopyOption.REPLACE_EXISTING);
            }
            ImageFiles.validate(target.resolve("image." + id), format, id);
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        server.createContext("/journal_id", exchange -> {
            exchange.getResponseHeaders().set("id", "1000");
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        server.start();
        Config.http_port = server.getAddress().getPort();
    }

    private static Frontend frontend(String name, String host) {
        Frontend frontend = new Frontend(FrontendNodeType.FOLLOWER, name, host, 9010);
        frontend.setAlive(true);
        return frontend;
    }

    private Path root() {
        return temporary.resolve("leader/image");
    }

    @AfterEach
    public void tearDown() {
        server.stop(0);
        globals.close();
        Config.meta_dir = oldMeta;
        Config.http_port = oldPort;
        Config.disable_image_v2_write = oldDisable;
        Config.checkpoint_only_on_leader = oldOnlyLeader;
    }

    private CheckpointWorker completeLocalCheckpoint() throws Exception {
        Config.checkpoint_only_on_leader = true;
        CheckpointWorker worker = mock(CheckpointWorker.class);
        when(state.getCheckpointWorker()).thenReturn(worker);
        Mockito.doAnswer(invocation -> {
            var results = Deencapsulation.<java.util.concurrent.BlockingQueue<
                    CheckpointController.CheckpointCompletionStatus>>getField(controller, "result");
            results.add(new CheckpointController.CheckpointCompletionStatus(true, "", null));
            return null;
        }).when(worker).setNextCheckpoint(anyLong(), anyLong(), anyBoolean(), anyBoolean());
        return worker;
    }

    @Test
    public void failedHalfOfPairBlocksJournalCleanupUntilRetrySucceeds() throws Exception {
        writeV2(root(), 100, new byte[] {1});
        writeV3(root(), 100);
        CheckpointWorker worker = completeLocalCheckpoint();
        failV3.set(true);
        assertTrue(controller.runCheckpointControllerWithIds(99, 100, false).first);
        assertFalse(controller.nodesToPushImage.isEmpty());
        verify(journal, never()).deleteJournalsAndGetMinJournalId(anyLong());
        int firstRound = requests.size();
        assertTrue(requests.stream().noneMatch(query -> query.contains("write_v2=")));
        controller.runCheckpointControllerWithIds(100, 100, false);
        assertTrue(requests.subList(firstRound, requests.size()).stream()
                .anyMatch(query -> query.contains("image_format_version=v2")));
        assertTrue(requests.stream().noneMatch(query -> query.contains("write_v2=")));
        assertTrue(controller.nodesToPushImage.isEmpty());
        verify(worker, times(1)).setNextCheckpoint(1, 100, false, true);
        verify(journal).deleteJournalsAndGetMinJournalId(101);
        for (String host : List.of("127.0.0.1", "127.0.0.2")) {
            assertTrue(ImageFiles.isComplete(temporary.resolve(host).toString(), 100, true));
        }
        controller.runCheckpointControllerWithIds(100, 100, false);
        verify(journal, times(1)).deleteJournalsAndGetMinJournalId(101);
    }

    @Test
    public void incompleteImageCannotBeAcknowledgedOrDistributed() throws Exception {
        writeV3(root(), 200);
        assertFalse(controller.isImageComplete(200));
        completeLocalCheckpoint();
        controller.nodesToPushImage.add("follower");
        // Even a successful worker callback cannot acknowledge an incomplete local image pair.
        assertFalse(controller.runCheckpointControllerWithIds(200, 200, false).first);
        verify(journal, never()).deleteJournalsAndGetMinJournalId(anyLong());
        assertTrue(requests.isEmpty());
        assertFalse(controller.nodesToPushImage.isEmpty());
        writeV2(root(), 200, new byte[] {1});
        controller.runCheckpointControllerWithIds(200, 200, false);
        verify(journal).deleteJournalsAndGetMinJournalId(201);
    }

    @Test
    public void finalizedJournalCleanupDoesNotTriggerSameJournalBackfill() throws Exception {
        writeV3(root(), 200);
        when(journal.getFinalizedJournalId()).thenReturn(0L);
        CheckpointWorker worker = completeLocalCheckpoint();
        controller.runCheckpointController();
        verify(worker, never()).setNextCheckpoint(anyLong(), anyLong(), anyBoolean(), anyBoolean());
        verify(journal, never()).deleteJournalsAndGetMinJournalId(anyLong());
        assertTrue(requests.isEmpty());
    }

    @Test
    public void newerCheckpointCanSupersedeAFailedDistribution() throws Exception {
        completeLocalCheckpoint();
        writeV2(root(), 100, new byte[] {1});
        writeV3(root(), 100);
        failV3.set(true);
        controller.runCheckpointControllerWithIds(99, 100, false);
        verify(journal, never()).deleteJournalsAndGetMinJournalId(anyLong());
        writeV2(root(), 200, new byte[] {2});
        writeV3(root(), 200);
        assertTrue(controller.runCheckpointControllerWithIds(100, 200, false).first);
        assertTrue(requests.stream().anyMatch(query -> query.contains("version=200")));
        for (String host : List.of("127.0.0.1", "127.0.0.2")) {
            assertTrue(ImageFiles.isComplete(temporary.resolve(host).toString(), 200, true));
        }
        verify(journal, never()).deleteJournalsAndGetMinJournalId(101);
        verify(journal).deleteJournalsAndGetMinJournalId(201);
    }

    @Test
    public void failedNewCheckpointStillRetriesPreviousDistribution() throws Exception {
        completeLocalCheckpoint();
        writeV2(root(), 100, new byte[] {1});
        writeV3(root(), 100);
        failV3.set(true);
        controller.runCheckpointControllerWithIds(99, 100, false);
        // No image.200 was produced despite the mocked worker's success callback.
        assertFalse(controller.runCheckpointControllerWithIds(100, 200, false).first);
        assertTrue(controller.nodesToPushImage.isEmpty());
        verify(journal).deleteJournalsAndGetMinJournalId(101);
        verify(journal, never()).deleteJournalsAndGetMinJournalId(201);
    }

    @Test
    public void completionMustMatchWorkerAndJournalId() throws Exception {
        Deencapsulation.setField(controller, "workerNodeName", "follower");
        Deencapsulation.setField(controller, "journalId", 300L);
        var results = new ArrayBlockingQueue<CheckpointController.CheckpointCompletionStatus>(1);
        Deencapsulation.setField(controller, "result", results);
        assertThrows(CheckpointException.class, () -> controller.finishCheckpoint(299, "follower", null));
        assertThrows(CheckpointException.class, () -> controller.finishCheckpoint(300, "observer", null));
        controller.cancelCheckpoint("observer", "unrelated failure");
        assertTrue(results.isEmpty());
        controller.finishCheckpoint(300, "follower", null);
        assertFalse(results.isEmpty());
    }

    @Test
    public void v3OnlyPolicyReachesLocalWorkerAndDistribution() throws Exception {
        Config.disable_image_v2_write = true;
        CheckpointWorker worker = completeLocalCheckpoint();
        writeV3(root(), 400);
        assertTrue(controller.runCheckpointControllerWithIds(0, 400, false).first);
        verify(worker).setNextCheckpoint(1, 400, false, false);
        assertFalse(requests.isEmpty());
        assertTrue(requests.stream().allMatch(query ->
                query.contains("image_format_version=v3") && !query.contains("write_v2=")));
        verify(journal).deleteJournalsAndGetMinJournalId(401);
    }

    @Test
    public void snapshotInfoStillRequestsWorkerAtExistingJournalId() throws Exception {
        writeV2(root(), 100, new byte[] {1});
        writeV3(root(), 100);
        CheckpointWorker worker = completeLocalCheckpoint();
        assertTrue(controller.runCheckpointControllerWithIds(100, 100, true).first);
        verify(worker).setNextCheckpoint(1, 100, true, true);
    }

    @Test
    public void remoteWorkerReceivesLeaderPolicy() throws Exception {
        Frontend follower = nodes.getFeByName("follower");
        when(nodes.getAllFrontends()).thenReturn(new java.util.ArrayList<>(List.of(follower)));
        FrontendService.Client client = mock(FrontendService.Client.class);
        try (MockedStatic<ThriftRPCRequestExecutor> rpc = Mockito.mockStatic(ThriftRPCRequestExecutor.class)) {
            rpc.when(() -> ThriftRPCRequestExecutor.call(any(), any(), anyInt(), any())).thenAnswer(invocation -> {
                ThriftRPCRequestExecutor.MethodCallable<FrontendService.Client, TStartCheckpointResponse> call =
                        invocation.getArgument(3);
                return call.apply(client);
            });
            for (boolean writeV2 : new boolean[] {false, true}) {
                Config.disable_image_v2_write = !writeV2;
                Mockito.doAnswer(invocation -> {
                    TStartCheckpointRequest request = invocation.getArgument(0);
                    assertTrue(request.isSetWrite_v2());
                    assertEquals(writeV2, request.isWrite_v2());
                    assertEquals(1, request.getEpoch());
                    assertEquals(400, request.getJournal_id());
                    assertTrue(request.isIs_global_state_mgr());
                    return new TStartCheckpointResponse().setStatus(new TStatus(TStatusCode.CANCELLED));
                }).when(client).startCheckpoint(any());
                assertFalse(controller.runCheckpointControllerWithIds(0, 400, false).first);
            }
            verify(client, times(2)).startCheckpoint(any());
        }
    }

    @Test
    public void configurationRequiresRestart() throws Exception {
        var fields = ConfigBase.class.getDeclaredField("configFields");
        fields.setAccessible(true);
        var originalFields = fields.get(null);
        var originalMutable = new java.util.HashMap<>(ConfigBase.getAllMutableConfigs());
        try {
            fields.set(null, Config.class.getFields());
            ConfigBase.initAllMutableConfigs();
            assertThrows(InvalidConfException.class,
                    () -> ConfigBase.setMutableConfig("disable_image_v2_write", "true", false, "root"));
            assertFalse(Config.disable_image_v2_write);
        } finally {
            fields.set(null, originalFields);
            ConfigBase.getAllMutableConfigs().clear();
            ConfigBase.getAllMutableConfigs().putAll(originalMutable);
        }
    }
}
