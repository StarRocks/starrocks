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

package com.starrocks.http.meta;

import com.google.gson.Gson;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.leader.MetaHelper;
import com.starrocks.persist.ImageFiles;
import com.starrocks.persist.ImageFormatVersion;
import com.starrocks.persist.ImageLoader;
import com.starrocks.persist.MetaCleaner;
import com.starrocks.persist.StorageInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.staros.StarMgrServer;
import com.sun.net.httpserver.HttpServer;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.starrocks.persist.image.ImageLifecycleTestSupport.writeV2;
import static com.starrocks.persist.image.ImageLifecycleTestSupport.writeV3;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetaImageV3Test {
    @TempDir
    Path temporary;
    private String oldMeta;
    private int oldPort;
    private HttpServer server;
    private MockedStatic<GlobalStateMgr> globals;
    private AtomicLong completed;
    private volatile StorageInfo helperInfo;

    private static class Info extends MetaService.InfoAction {
        HttpResponseStatus status;

        Info() {
            super(null);
        }

        @Override
        protected void writeResponse(BaseRequest request, BaseResponse response, HttpResponseStatus code) {
            status = code;
        }
    }

    private static class Put extends MetaService.PutAction {
        HttpResponseStatus status;

        Put() {
            super(null);
        }

        @Override
        protected void writeResponse(BaseRequest request, BaseResponse response, HttpResponseStatus code) {
            status = code;
        }
    }

    @BeforeEach
    public void setUp() throws IOException {
        oldMeta = Config.meta_dir;
        oldPort = Config.http_port;
        Config.meta_dir = temporary.resolve("receiver").toString();
        GlobalStateMgr state = mock(GlobalStateMgr.class);
        completed = new AtomicLong();
        when(state.getMaxJournalId()).thenReturn(1000L);
        when(state.getImageJournalId()).thenAnswer(invocation -> completed.get());
        Mockito.doAnswer(invocation -> {
            completed.set(invocation.getArgument(0));
            return null;
        }).when(state).setImageJournalId(Mockito.anyLong());
        globals = Mockito.mockStatic(GlobalStateMgr.class, Mockito.CALLS_REAL_METHODS);
        globals.when(GlobalStateMgr::getCurrentState).thenReturn(state);
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/image", exchange -> {
            Map<String, String> params = Arrays.stream(exchange.getRequestURI().getQuery().split("&"))
                    .map(value -> value.split("=", 2)).collect(Collectors.toMap(value -> value[0], value -> value[1]));
            String format = params.get("image_format_version");
            String id = params.get("version");
            Path file = temporary.resolve("sender").resolve(format).resolve("image." + id);
            byte[] bytes = Files.readAllBytes(file);
            exchange.getResponseHeaders().set(MetaHelper.X_IMAGE_SIZE, Long.toString(bytes.length));
            if ("v2".equals(format)) {
                exchange.getResponseHeaders().set(MetaHelper.X_IMAGE_CHECKSUM,
                        Files.readString(file.resolveSibling("checksum." + id)));
            }
            exchange.sendResponseHeaders(200, bytes.length);
            try (var out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        });
        server.createContext("/info", exchange -> {
            byte[] bytes = new Gson().toJson(helperInfo).getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            try (var out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        });
        server.start();
        Config.http_port = server.getAddress().getPort();
    }

    @AfterEach
    public void tearDown() {
        server.stop(0);
        globals.close();
        Config.meta_dir = oldMeta;
        Config.http_port = oldPort;
    }

    private Path root() {
        return Path.of(Config.meta_dir, "image");
    }

    private BaseRequest request(Map<String, String> parameters) {
        BaseRequest request = mock(BaseRequest.class);
        when(request.getHostString()).thenReturn("127.0.0.1");
        when(request.getSingleParameter(Mockito.anyString())).thenAnswer(i -> parameters.get(i.getArgument(0)));
        return request;
    }

    private void put(long journalId, String format, HttpResponseStatus expected) {
        Put action = new Put();
        action.executeGet(request(Map.of("version", Long.toString(journalId), "port", Integer.toString(Config.http_port),
                "image_format_version", format)), new BaseResponse());
        assertEquals(expected, action.status);
    }

    private StorageInfo info() {
        Info action = new Info();
        BaseResponse response = new BaseResponse();
        action.executeGet(request(Map.of()), response);
        assertEquals(HttpResponseStatus.OK, action.status);
        return new Gson().fromJson(response.getContent().toString(), StorageInfo.class);
    }

    @Test
    public void discoveryReturnsNewestJournalAndPrefersV3OnTie() throws Exception {
        writeV2(root(), 10, new byte[] {1});
        writeV3(root(), 9);
        StorageInfo info = info();
        assertEquals(10, info.getImageJournalId());
        assertEquals(ImageFormatVersion.v2, info.getImageFormatVersion());
        writeV3(root(), 10);
        info = info();
        assertEquals(10, info.getImageJournalId());
        assertEquals(ImageFormatVersion.v3, info.getImageFormatVersion());
        writeV3(root(), 11);
        info = info();
        assertEquals(11, info.getImageJournalId());
        assertEquals(ImageFormatVersion.v3, info.getImageFormatVersion());
        writeV2(root(), 12, new byte[] {2});
        info = info();
        assertEquals(12, info.getImageJournalId());
        assertEquals(ImageFormatVersion.v2, info.getImageFormatVersion());
    }

    @Test
    public void eachFileAdvancesWatermarkAndCleansOnlyItsFormat() throws Exception {
        Path sender = temporary.resolve("sender");
        writeV2(root(), 10, new byte[] {1});
        writeV3(root(), 10);
        writeV2(sender, 20, new byte[] {2});
        writeV3(sender, 20);
        put(20, "v2", HttpResponseStatus.OK);
        assertEquals(20, completed.get());
        assertTrue(ImageFiles.isValid(root().resolve("v2/image.20"), ImageFormatVersion.v2, 20));
        assertFalse(Files.exists(root().resolve("v3/image.20")));
        assertFalse(Files.exists(root().resolve("v2/image.10")));
        assertFalse(Files.exists(root().resolve("v2/checksum.10")));
        assertTrue(Files.exists(root().resolve("v3/image.10")));
        put(20, "v3", HttpResponseStatus.OK);
        assertEquals(20, completed.get());
        assertTrue(ImageFiles.isComplete(root().toString(), 20, true));
        assertFalse(Files.exists(root().resolve("v3/image.10")));
        put(20, "v3", HttpResponseStatus.OK);
        assertEquals(20, completed.get());
    }

    @Test
    public void v3OnlyDoesNotWaitForV2AndDoesNotRegressWatermark() throws Exception {
        writeV3(temporary.resolve("sender"), 30);
        put(30, "v3", HttpResponseStatus.OK);
        assertEquals(30, completed.get());
        assertFalse(Files.exists(root().resolve("v2/image.30")));
        writeV3(temporary.resolve("sender"), 29);
        put(29, "v3", HttpResponseStatus.OK);
        assertEquals(30, completed.get());
    }

    @Test
    public void corruptExistingFileIsDownloadedAgain() throws Exception {
        writeV3(temporary.resolve("sender"), 40);
        Path corrupt = writeV3(root(), 40);
        Files.write(corrupt, new byte[] {0});
        put(40, "v3", HttpResponseStatus.OK);
        assertTrue(ImageFiles.isComplete(root().toString(), 40, false));
    }

    @Test
    public void corruptDownloadDoesNotAdvanceWatermarkAndLaterImageCanSucceed() throws Exception {
        Path corrupt = writeV3(temporary.resolve("sender"), 50);
        byte[] bytes = Files.readAllBytes(corrupt);
        bytes[bytes.length - 1] ^= 1;
        Files.write(corrupt, bytes);
        put(50, "v3", HttpResponseStatus.INTERNAL_SERVER_ERROR);
        assertEquals(0, completed.get());
        assertFalse(Files.exists(root().resolve("v3/image.50")));
        writeV3(temporary.resolve("sender"), 51);
        put(51, "v3", HttpResponseStatus.OK);
        assertEquals(51, completed.get());
    }

    @Test
    public void cleanupFailureDoesNotFailSuccessfulDownload() throws Exception {
        writeV3(temporary.resolve("sender"), 55);
        try (var cleaners = Mockito.mockConstruction(MetaCleaner.class, (cleaner, context) ->
                Mockito.doThrow(new IOException("cleanup failed")).when(cleaner).clean())) {
            put(55, "v3", HttpResponseStatus.OK);
            assertEquals(55, completed.get());
            assertTrue(ImageFiles.isValid(root().resolve("v3/image.55"), ImageFormatVersion.v3, 55));
            assertEquals(1, cleaners.constructed().size());
            Mockito.verify(cleaners.constructed().get(0)).clean();
        }
    }

    @Test
    public void unknownPutFormatIsBadRequest() {
        put(60, "v99", HttpResponseStatus.BAD_REQUEST);
    }

    @Test
    public void startupFromOldHelperUsesV2AndCorrectDirectory() throws Exception {
        writeV2(temporary.resolve("sender"), 70, new byte[] {7});
        helperInfo = new StorageInfo(70, ImageFormatVersion.v2); // Old response has no extra fields.
        Deencapsulation.invoke(new NodeMgr(), "getNewImageOnStartup", Pair.create("127.0.0.1", 9010), "");
        assertTrue(ImageFiles.isValid(root().resolve("v2/image.70"), ImageFormatVersion.v2, 70));
    }

    @Test
    public void startupDownloadsOnlyTheSelectedFormat() throws Exception {
        Path sender = temporary.resolve("sender");
        writeV2(sender, 80, new byte[] {8});
        writeV3(sender, 80);
        helperInfo = new StorageInfo(80, ImageFormatVersion.v3);
        NodeMgr node = new NodeMgr();
        Deencapsulation.invoke(node, "getNewImageOnStartup", Pair.create("127.0.0.1", 9010), "");
        assertTrue(ImageFiles.isValid(root().resolve("v3/image.80"), ImageFormatVersion.v3, 80));
        assertFalse(Files.exists(root().resolve("v2/image.80")));
        writeV3(sender, 81);
        helperInfo = new StorageInfo(81, ImageFormatVersion.v3);
        Deencapsulation.invoke(node, "getNewImageOnStartup", Pair.create("127.0.0.1", 9010), "");
        assertEquals(81, new ImageLoader(root().toString()).getImageJournalId());
        assertFalse(Files.exists(root().resolve("v2/image.81")));
    }

    @Test
    public void startupSkipsSameAndOlderJournal() throws Exception {
        writeV3(root(), 90);
        Path sender = temporary.resolve("sender");
        writeV2(sender, 90, new byte[] {1});
        writeV2(sender, 89, new byte[] {2});
        NodeMgr node = new NodeMgr();
        for (long journalId : new long[] {90, 89}) {
            helperInfo = new StorageInfo(journalId, ImageFormatVersion.v2);
            Deencapsulation.invoke(node, "getNewImageOnStartup", Pair.create("127.0.0.1", 9010), "");
            assertFalse(Files.exists(root().resolve("v2/image." + journalId)));
            assertEquals(90, new ImageLoader(root().toString()).getImageJournalId());
        }
    }

    @Test
    public void discoveryAndStartupKeepStarMgrV1() throws Exception {
        Path local = Path.of(MetaHelper.getImageFileDir(false));
        Files.createDirectories(local);
        Files.write(local.resolve("image.95"), new byte[] {1});
        Info action = new Info();
        BaseResponse response = new BaseResponse();
        action.executeGet(request(Map.of("subdir", StarMgrServer.IMAGE_SUBDIR)), response);
        assertEquals(HttpResponseStatus.OK, action.status);
        StorageInfo info = new Gson().fromJson(response.getContent().toString(), StorageInfo.class);
        assertEquals(95, info.getImageJournalId());
        assertEquals(ImageFormatVersion.v1, info.getImageFormatVersion());
        byte[] content = {2, 3, 4};
        Path sender = temporary.resolve("sender/v1");
        Files.createDirectories(sender);
        Files.write(sender.resolve("image.100"), content);
        helperInfo = new StorageInfo(100, ImageFormatVersion.v1);
        Deencapsulation.invoke(new NodeMgr(), "getNewImageOnStartup", Pair.create("127.0.0.1", 9010),
                StarMgrServer.IMAGE_SUBDIR);
        assertArrayEquals(content, Files.readAllBytes(local.resolve("image.100")));
    }
}
