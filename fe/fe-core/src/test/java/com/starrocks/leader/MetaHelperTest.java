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
import com.starrocks.common.InvalidMetaDirException;
import com.starrocks.common.util.NetUtils;
import com.starrocks.persist.ImageFormatVersion;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.Random;
import java.util.UUID;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class MetaHelperTest {

    private String testDir = "meta_dir_test_" + UUID.randomUUID();

    @AfterEach
    public void teardown() {
        deleteDir(new File(testDir));
    }

    @Test
    public void testImageExistBDBNotExist() {
        assertThrows(InvalidMetaDirException.class, () -> {
            Config.start_with_incomplete_meta = false;
            Config.meta_dir = testDir + "/meta";
            mkdir(Config.meta_dir + "/image/v2");
            File file = new File(Config.meta_dir + "/image/v2/image.123");
            Assertions.assertTrue(file.createNewFile());

            try {
                MetaHelper.checkMetaDir();
            } finally {
                deleteDir(new File(testDir + "/"));
            }
        });
    }

    @Test
    public void testImageExistBDBNotExistWithConfig() throws IOException,
            InvalidMetaDirException {
        Config.start_with_incomplete_meta = true;
        Config.meta_dir = testDir + "/meta";
        mkdir(Config.meta_dir + "/image");
        File file = new File(Config.meta_dir + "/image/image.123");
        Assertions.assertTrue(file.createNewFile());

        try {
            MetaHelper.checkMetaDir();
        } finally {
            deleteDir(new File(testDir + "/"));
        }
    }

    @Test
    public void testImageExistBDBExist() throws IOException,
            InvalidMetaDirException {
        Config.start_with_incomplete_meta = false;
        Config.meta_dir = testDir + "/meta";
        mkdir(Config.meta_dir + "/image");
        File fileImage = new File(Config.meta_dir + "/image/image.123");
        Assertions.assertTrue(fileImage.createNewFile());
        mkdir(Config.meta_dir + "/bdb");
        File fileBDB = new File(Config.meta_dir + "/bdb/EF889.jdb");
        Assertions.assertTrue(fileBDB.createNewFile());

        try {
            MetaHelper.checkMetaDir();
        } finally {
            deleteDir(new File(testDir + "/"));
        }
    }

    @Test
    public void testImageNotExistBDBExist() throws IOException,
            InvalidMetaDirException {
        Config.start_with_incomplete_meta = false;
        Config.meta_dir = testDir + "/meta";
        mkdir(Config.meta_dir + "/bdb");
        File file = new File(Config.meta_dir + "/bdb/EF889.jdb");
        Assertions.assertTrue(file.createNewFile());

        try {
            MetaHelper.checkMetaDir();
        } finally {
            deleteDir(new File(testDir + "/"));
        }
    }

    @Test
    public void testFreeDiskSizeIsConfigurable() throws IOException, InvalidMetaDirException {
        Config.start_with_incomplete_meta = false;
        Config.meta_dir = testDir + "/meta";
        mkdir(Config.meta_dir + "/bdb");
        File file = new File(Config.meta_dir + "/bdb/EF889.jdb");
        Assertions.assertTrue(file.createNewFile());

        long originalFreeDiskSize = Config.bdbje_free_disk_size;
        try {
            // No volume can satisfy this, so the startup gate must reject the meta dir.
            Config.bdbje_free_disk_size = Long.MAX_VALUE;
            assertThrows(InvalidMetaDirException.class, MetaHelper::checkMetaDir);

            // Lowering the config is the recovery path: the same meta dir now passes.
            Config.bdbje_free_disk_size = 0;
            MetaHelper.checkMetaDir();
        } finally {
            Config.bdbje_free_disk_size = originalFreeDiskSize;
            deleteDir(new File(testDir + "/"));
        }
    }

    private void mkdir(String targetDir) {
        File dir = new File(targetDir);
        if (dir.exists()) {
            deleteDir(dir);
        }
        dir.mkdirs();
    }

    private void deleteDir(File dir) {
        if (!dir.exists()) {
            return;
        }
        if (dir.isFile()) {
            dir.delete();
        } else {
            for (File file : dir.listFiles()) {
                deleteDir(file);
            }
        }
        dir.delete();
    }

    @Test
    public void testGetImageFileDir() {
        Assertions.assertEquals(Config.meta_dir + "/image/v2", MetaHelper.getImageFileDir(true));
        Assertions.assertEquals(Config.meta_dir + "/image/starmgr", MetaHelper.getImageFileDir(false));
        Assertions.assertEquals(Config.meta_dir + "/image",
                MetaHelper.getImageFileDir("", ImageFormatVersion.v1));
        Assertions.assertEquals(Config.meta_dir + "/image/starmgr",
                MetaHelper.getImageFileDir("/starmgr", ImageFormatVersion.v1));
        Assertions.assertEquals(Config.meta_dir + "/image/v2",
                MetaHelper.getImageFileDir("", ImageFormatVersion.v2));
        Assertions.assertEquals(Config.meta_dir + "/image/starmgr/v2",
                MetaHelper.getImageFileDir("/starmgr", ImageFormatVersion.v2));
    }

    /**
     * An image whose bytes do not match the checksum its source sent was damaged before it arrived, for
     * example on the source FE's disk. Publishing it would replace this FE's images with one it cannot load.
     */
    @Test
    public void testDownloadImageFileRejectsBytesThatDoNotMatchTheChecksum() throws Exception {
        byte[] image = imageBytes();
        byte[] damaged = image.clone();
        damaged[1] ^= 0x20;
        File dir = newImageDir();
        try (ImageServer server = new ImageServer(damaged, String.valueOf(crc32(image)))) {
            IOException e = assertThrows(IOException.class,
                    () -> MetaHelper.downloadImageFile(server.url(), 5000, "147304", dir, ImageFormatVersion.v2));
            Assertions.assertTrue(e.getMessage().contains("checksum"), e.getMessage());
        }
        Assertions.assertArrayEquals(new String[0], dir.list());
    }

    @Test
    public void testDownloadImageFileRejectsAChecksumThatIsNotANumber() throws Exception {
        File dir = newImageDir();
        try (ImageServer server = new ImageServer(imageBytes(), "22513?2568")) {
            assertThrows(IOException.class, () ->
                    MetaHelper.downloadImageFile(server.url(), 5000, "147304", dir, ImageFormatVersion.v2));
        }
        Assertions.assertArrayEquals(new String[0], dir.list());
    }

    @Test
    public void testDownloadImageFileKeepsAnImageThatMatchesItsChecksum() throws Exception {
        byte[] image = imageBytes();
        String checksum = String.valueOf(crc32(image));
        File dir = newImageDir();
        try (ImageServer server = new ImageServer(image, checksum)) {
            MetaHelper.downloadImageFile(server.url(), 5000, "147304", dir, ImageFormatVersion.v2);
        }
        Assertions.assertArrayEquals(image, Files.readAllBytes(new File(dir, "image.147304").toPath()));
        Assertions.assertEquals(checksum, Files.readString(new File(dir, "checksum.147304").toPath()));
        Assertions.assertFalse(new File(dir, "image.147304" + MetaHelper.PART_SUFFIX).exists());
    }

    /**
     * A v1 image has no checksum, so one that comes without it is accepted as before.
     */
    @Test
    public void testDownloadImageFileAcceptsAV1ImageWithoutAChecksum() throws Exception {
        byte[] image = imageBytes();
        File dir = newImageDir();
        try (ImageServer server = new ImageServer(image, null)) {
            MetaHelper.downloadImageFile(server.url(), 5000, "147304", dir, ImageFormatVersion.v1);
        }
        Assertions.assertArrayEquals(image, Files.readAllBytes(new File(dir, "image.147304").toPath()));
        Assertions.assertFalse(new File(dir, "checksum.147304").exists());
    }

    /**
     * Every v2 image has a checksum and cannot be loaded without one; a source that sends none (its
     * checksum file lost or unreadable) cannot vouch for the image, so it is not published.
     */
    @Test
    public void testDownloadImageFileRejectsAV2ImageWithoutAChecksum() throws Exception {
        File dir = newImageDir();
        try (ImageServer server = new ImageServer(imageBytes(), null)) {
            assertThrows(IOException.class, () ->
                    MetaHelper.downloadImageFile(server.url(), 5000, "147304", dir, ImageFormatVersion.v2));
        }
        Assertions.assertArrayEquals(new String[0], dir.list());
    }

    private static byte[] imageBytes() {
        byte[] bytes = new byte[256 * 1024];
        new Random(42).nextBytes(bytes);
        // the meta version in the image header
        bytes[0] = 0;
        bytes[1] = 0;
        bytes[2] = 0;
        bytes[3] = 4;
        return bytes;
    }

    private static long crc32(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes);
        return crc.getValue();
    }

    private File newImageDir() {
        File dir = new File(testDir, "image/v2");
        Assertions.assertTrue(dir.mkdirs());
        return dir;
    }

    /**
     * Serves one image the way MetaBaseAction does: with X-Image-Size and, when given, X-Image-Checksum.
     */
    private static class ImageServer implements AutoCloseable {
        private final HttpServer server;

        ImageServer(byte[] body, String checksum) throws IOException {
            server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
            server.createContext("/image", exchange -> {
                exchange.getResponseHeaders().set(MetaHelper.X_IMAGE_SIZE, String.valueOf(body.length));
                if (checksum != null) {
                    exchange.getResponseHeaders().set(MetaHelper.X_IMAGE_CHECKSUM, checksum);
                }
                exchange.sendResponseHeaders(200, body.length);
                try (OutputStream out = exchange.getResponseBody()) {
                    out.write(body);
                }
            });
            server.start();
        }

        String url() {
            return "http://" + NetUtils.getHostPortInAccessibleFormat(server.getAddress().getHostString(),
                    server.getAddress().getPort()) + "/image?version=147304";
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
