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

import com.starrocks.persist.ImageFiles;
import com.starrocks.persist.ImageFormatVersion;
import com.starrocks.persist.ImageLoader;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.starrocks.persist.image.ImageLifecycleTestSupport.writeV2;
import static com.starrocks.persist.image.ImageLifecycleTestSupport.writeV3;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ImageDownloadV3Test {
    @TempDir
    Path temporary;
    private HttpServer server;
    private volatile byte[] body;
    private volatile String checksum;
    private volatile long size;

    @BeforeEach
    public void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/image", exchange -> {
            exchange.getResponseHeaders().set(MetaHelper.X_IMAGE_SIZE, Long.toString(size));
            if (checksum != null) {
                exchange.getResponseHeaders().set(MetaHelper.X_IMAGE_CHECKSUM, checksum);
            }
            exchange.sendResponseHeaders(200, body.length);
            try (var out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        server.start();
    }

    @AfterEach
    public void stopServer() {
        server.stop(0);
    }

    private void serve(Path file) throws IOException {
        body = Files.readAllBytes(file);
        size = body.length;
    }

    private Path destination(ImageFormatVersion format, long id) {
        return temporary.resolve("target").resolve(format.name()).resolve("image." + id);
    }

    private void download(ImageFormatVersion format, long id) throws IOException {
        Path target = destination(format, id);
        AtomicBoolean connected = new AtomicBoolean();
        MetaHelper.downloadImageFile("http://127.0.0.1:" + server.getAddress().getPort() + "/image", 5000,
                Long.toString(id), target.getParent().toFile(), format, connection -> connected.set(true));
        assertTrue(connected.get());
    }

    @Test
    public void downloadsV3WithoutChecksumSidecar() throws Exception {
        Path source = writeV3(temporary.resolve("source"), 10);
        serve(source);
        download(ImageFormatVersion.v3, 10);
        Path target = destination(ImageFormatVersion.v3, 10);
        assertArrayEquals(body, Files.readAllBytes(target));
        ImageFiles.validate(target, ImageFormatVersion.v3, 10);
        assertFalse(Files.exists(target.resolveSibling("image.10.part")));
        assertFalse(Files.exists(target.resolveSibling("checksum.10")));
    }

    @Test
    public void downloadsAndValidatesV2Checksum() throws Exception {
        Path source = writeV2(temporary.resolve("source"), 11, new byte[] {3, 4, 5});
        serve(source);
        checksum = Files.readString(source.resolveSibling("checksum.11"));
        download(ImageFormatVersion.v2, 11);
        ImageFiles.validate(destination(ImageFormatVersion.v2, 11), ImageFormatVersion.v2, 11);
    }

    @Test
    public void rejectsMissingAndWrongV2ChecksumBeforePublication() throws Exception {
        serve(writeV2(temporary.resolve("source"), 12, new byte[] {3, 4, 5}));
        assertThrows(IOException.class, () -> download(ImageFormatVersion.v2, 12));
        checksum = "1234";
        assertThrows(IOException.class, () -> download(ImageFormatVersion.v2, 12));
        assertFalse(Files.exists(destination(ImageFormatVersion.v2, 12)));
    }

    @Test
    public void rejectsWrongSizeAndJournalId() throws Exception {
        serve(writeV3(temporary.resolve("source"), 13));
        size++;
        assertThrows(IOException.class, () -> download(ImageFormatVersion.v3, 13));
        assertTrue(Files.isRegularFile(destination(ImageFormatVersion.v3, 13).resolveSibling("image.13.part")));
        assertEquals(0, new ImageLoader(temporary.resolve("target").toString()).getImageJournalId());
        size--;
        assertThrows(IOException.class, () -> download(ImageFormatVersion.v3, 14));
        assertFalse(Files.exists(destination(ImageFormatVersion.v3, 13)));
        assertFalse(Files.exists(destination(ImageFormatVersion.v3, 14)));
    }

    @Test
    public void corruptDownloadDoesNotReplacePublishedImage() throws Exception {
        serve(writeV3(temporary.resolve("source"), 15));
        download(ImageFormatVersion.v3, 15);
        byte[] original = body.clone();
        body[body.length - 1] ^= 1;
        assertThrows(IOException.class, () -> download(ImageFormatVersion.v3, 15));
        assertArrayEquals(original, Files.readAllBytes(destination(ImageFormatVersion.v3, 15)));
    }

    @Test
    public void duplicateValidDownloadIsIdempotent() throws Exception {
        serve(writeV3(temporary.resolve("source"), 16));
        download(ImageFormatVersion.v3, 16);
        download(ImageFormatVersion.v3, 16);
        try (var children = Files.list(destination(ImageFormatVersion.v3, 16).getParent())) {
            assertTrue(children.allMatch(path -> path.getFileName().toString().equals("image.16")));
        }
    }

    @Test
    public void starMgrV1StillDownloadsOpaqueImage() throws Exception {
        Path source = temporary.resolve("star-image");
        Files.write(source, new byte[] {1, 2, 3, 4});
        serve(source);
        download(ImageFormatVersion.v1, 17);
        assertArrayEquals(body, Files.readAllBytes(destination(ImageFormatVersion.v1, 17)));
    }
}
