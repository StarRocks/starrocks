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
import com.starrocks.persist.ImageFormatVersion;
import com.starrocks.utframe.MockImageSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.UUID;

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
        byte[] image = MockImageSource.imageBytes();
        File dir = newImageDir();
        try (MockImageSource source = new MockImageSource(MockImageSource.damaged(image),
                MockImageSource.checksum(image), 147304, ImageFormatVersion.v2)) {
            IOException e = assertThrows(IOException.class, () ->
                    MetaHelper.downloadImageFile(source.imageUrl(), 5000, "147304", dir, ImageFormatVersion.v2));
            Assertions.assertTrue(e.getMessage().contains("checksum"), e.getMessage());
        }
        Assertions.assertArrayEquals(new String[0], dir.list());
    }

    @Test
    public void testDownloadImageFileRejectsAChecksumThatIsNotANumber() throws Exception {
        File dir = newImageDir();
        try (MockImageSource source = new MockImageSource(MockImageSource.imageBytes(), "22513?2568", 147304,
                ImageFormatVersion.v2)) {
            assertThrows(IOException.class, () ->
                    MetaHelper.downloadImageFile(source.imageUrl(), 5000, "147304", dir, ImageFormatVersion.v2));
        }
        Assertions.assertArrayEquals(new String[0], dir.list());
    }

    @Test
    public void testDownloadImageFileKeepsAnImageThatMatchesItsChecksum() throws Exception {
        byte[] image = MockImageSource.imageBytes();
        String checksum = MockImageSource.checksum(image);
        File dir = newImageDir();
        try (MockImageSource source = new MockImageSource(image, checksum, 147304, ImageFormatVersion.v2)) {
            MetaHelper.downloadImageFile(source.imageUrl(), 5000, "147304", dir, ImageFormatVersion.v2);
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
        byte[] image = MockImageSource.imageBytes();
        File dir = newImageDir();
        try (MockImageSource source = new MockImageSource(image, null, 147304, ImageFormatVersion.v1)) {
            MetaHelper.downloadImageFile(source.imageUrl(), 5000, "147304", dir, ImageFormatVersion.v1);
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
        try (MockImageSource source = new MockImageSource(MockImageSource.imageBytes(), null, 147304,
                ImageFormatVersion.v2)) {
            assertThrows(IOException.class, () ->
                    MetaHelper.downloadImageFile(source.imageUrl(), 5000, "147304", dir, ImageFormatVersion.v2));
        }
        Assertions.assertArrayEquals(new String[0], dir.list());
    }

    /**
     * metadata_ignore_image_checksum_error is for recovery when the check itself is wrong: the image is accepted,
     * and its checksum file keeps the value the source sent, so the mismatch is still seen at load.
     */
    @Test
    public void testDownloadImageFileAcceptsADamagedImageWhenChecksumErrorsAreIgnored() throws Exception {
        byte[] damaged = MockImageSource.damaged(MockImageSource.imageBytes());
        String checksum = MockImageSource.checksum(MockImageSource.imageBytes());
        File dir = newImageDir();
        Config.metadata_ignore_image_checksum_error = true;
        try (MockImageSource source = new MockImageSource(damaged, checksum, 147304, ImageFormatVersion.v2)) {
            MetaHelper.downloadImageFile(source.imageUrl(), 5000, "147304", dir, ImageFormatVersion.v2);
        } finally {
            Config.metadata_ignore_image_checksum_error = false;
        }
        Assertions.assertArrayEquals(damaged, Files.readAllBytes(new File(dir, "image.147304").toPath()));
        Assertions.assertEquals(checksum, Files.readString(new File(dir, "checksum.147304").toPath()));
    }

    @Test
    public void testDownloadImageFileAcceptsAV2ImageWithoutAChecksumWhenChecksumErrorsAreIgnored() throws Exception {
        byte[] image = MockImageSource.imageBytes();
        File dir = newImageDir();
        Config.metadata_ignore_image_checksum_error = true;
        try (MockImageSource source = new MockImageSource(image, null, 147304, ImageFormatVersion.v2)) {
            MetaHelper.downloadImageFile(source.imageUrl(), 5000, "147304", dir, ImageFormatVersion.v2);
        } finally {
            Config.metadata_ignore_image_checksum_error = false;
        }
        Assertions.assertArrayEquals(image, Files.readAllBytes(new File(dir, "image.147304").toPath()));
        Assertions.assertFalse(new File(dir, "checksum.147304").exists());
    }

    private File newImageDir() {
        File dir = new File(testDir, "image/v2");
        Assertions.assertTrue(dir.mkdirs());
        return dir;
    }
}
