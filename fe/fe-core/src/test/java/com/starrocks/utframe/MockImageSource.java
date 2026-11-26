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

package com.starrocks.utframe;

import com.starrocks.common.util.NetUtils;
import com.starrocks.leader.MetaHelper;
import com.starrocks.persist.ImageFormatVersion;
import com.starrocks.persist.StorageInfo;
import com.starrocks.persist.gson.GsonUtils;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.zip.CRC32;

/**
 * An FE that serves one checkpoint image the way a real one does: /image with X-Image-Size and, when given,
 * X-Image-Checksum (MetaBaseAction.writeFileResponse), and /info with the image's StorageInfo.
 */
public class MockImageSource implements AutoCloseable {
    private final HttpServer server;
    private final long journalId;

    public MockImageSource(byte[] image, String checksum, long journalId, ImageFormatVersion format)
            throws IOException {
        this.journalId = journalId;
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/image", exchange -> {
            exchange.getResponseHeaders().set(MetaHelper.X_IMAGE_SIZE, String.valueOf(image.length));
            if (checksum != null) {
                exchange.getResponseHeaders().set(MetaHelper.X_IMAGE_CHECKSUM, checksum);
            }
            reply(exchange, image);
        });
        server.createContext("/info", exchange -> reply(exchange,
                GsonUtils.GSON.toJson(new StorageInfo(journalId, format)).getBytes(StandardCharsets.UTF_8)));
        server.start();
    }

    private static void reply(HttpExchange exchange, byte[] body) throws IOException {
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    public String host() {
        return server.getAddress().getHostString();
    }

    public int port() {
        return server.getAddress().getPort();
    }

    public String imageUrl() {
        return "http://" + NetUtils.getHostPortInAccessibleFormat(host(), port()) + "/image?version=" + journalId;
    }

    /**
     * 256 KB of random bytes that start with the image header's meta version (4).
     */
    public static byte[] imageBytes() {
        byte[] bytes = new byte[256 * 1024];
        new Random(42).nextBytes(bytes);
        bytes[0] = 0;
        bytes[1] = 0;
        bytes[2] = 0;
        bytes[3] = 4;
        return bytes;
    }

    /**
     * The checksum ImageWriter writes for these bytes.
     */
    public static String checksum(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes);
        return String.valueOf(crc.getValue());
    }

    /**
     * The same bytes with one bit of the meta version flipped, as a damaged disk would return them.
     */
    public static byte[] damaged(byte[] bytes) {
        byte[] copy = bytes.clone();
        copy[1] ^= 0x20;
        return copy;
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
