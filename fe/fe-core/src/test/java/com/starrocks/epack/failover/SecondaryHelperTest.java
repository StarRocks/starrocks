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

package com.starrocks.epack.failover;

import com.starrocks.common.Config;
import com.starrocks.leader.MetaHelper;
import com.starrocks.persist.Storage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class SecondaryHelperTest {

    private String testDir;
    private String originalMetaDir;
    private int originalTimeout;

    @BeforeEach
    public void setUp() {
        testDir = "meta_dir_test_" + UUID.randomUUID();
        originalMetaDir = Config.meta_dir;
        originalTimeout = Config.failover_group_pull_image_timeout_sec;
        Config.meta_dir = testDir;
        Config.failover_group_pull_image_timeout_sec = 30;
    }

    @AfterEach
    public void tearDown() {
        Config.meta_dir = originalMetaDir;
        Config.failover_group_pull_image_timeout_sec = originalTimeout;
        deleteDir(new File(testDir));
    }

    @Test
    public void testPullImage() throws IOException {
        String imageSubDir = "/test_subdir";
        long imageVersion = 100L;
        String token = "test_token";
        String httpHost = "localhost";
        int httpPort = 8080;

        // The realDir will be: Config.meta_dir + "/image" + imageSubDir + "/v2"
        String expectedImageDir = testDir + "/image" + imageSubDir + "/v2";

        // Mock MetaHelper.downloadImageFile to create files in destDir
        try (MockedStatic<MetaHelper> metaHelperMock = Mockito.mockStatic(MetaHelper.class)) {
            metaHelperMock.when(() -> MetaHelper.downloadImageFile(
                    Mockito.anyString(),
                    Mockito.anyInt(),
                    Mockito.eq(String.valueOf(imageVersion)),
                    Mockito.any(File.class)
            )).thenAnswer(invocation -> {
                File destDir = invocation.getArgument(3);
                // Create image.100 and checksum.100 (the new files)
                File image100 = new File(destDir, Storage.IMAGE + "." + imageVersion);
                try (FileOutputStream fos = new FileOutputStream(image100)) {
                    fos.write("image content 100".getBytes(StandardCharsets.UTF_8));
                }

                File checksum100 = new File(destDir, Storage.CHECKSUM + "." + imageVersion);
                try (FileOutputStream fos = new FileOutputStream(checksum100)) {
                    fos.write("checksum100".getBytes(StandardCharsets.UTF_8));
                }

                // Also create image.99 and checksum.99 (old files that should be deleted)
                File image99 = new File(destDir, Storage.IMAGE + ".99");
                try (FileOutputStream fos = new FileOutputStream(image99)) {
                    fos.write("image content 99".getBytes(StandardCharsets.UTF_8));
                }

                File checksum99 = new File(destDir, Storage.CHECKSUM + ".99");
                try (FileOutputStream fos = new FileOutputStream(checksum99)) {
                    fos.write("checksum99".getBytes(StandardCharsets.UTF_8));
                }
                return null; // void method
            });

            // Run pullImage
            boolean result = SecondaryHelper.pullImage(token, httpHost, httpPort, imageVersion, imageSubDir);
            Assertions.assertTrue(result, "pullImage should return true");

            // Verify that only image.100 and checksum.100 exist in the image directory
            File imageDir = new File(expectedImageDir);
            Assertions.assertTrue(imageDir.exists(), "Image directory should exist");
            File[] files = imageDir.listFiles();
            Assertions.assertNotNull(files, "Image directory should not be null");

            int image100Count = 0;
            int checksum100Count = 0;
            int image99Count = 0;
            int checksum99Count = 0;

            for (File file : files) {
                String fileName = file.getName();
                if (fileName.equals(Storage.IMAGE + ".100")) {
                    image100Count++;
                    Assertions.assertTrue(file.exists(), "image.100 should exist");
                } else if (fileName.equals(Storage.CHECKSUM + ".100")) {
                    checksum100Count++;
                    Assertions.assertTrue(file.exists(), "checksum.100 should exist");
                } else if (fileName.equals(Storage.IMAGE + ".99")) {
                    image99Count++;
                } else if (fileName.equals(Storage.CHECKSUM + ".99")) {
                    checksum99Count++;
                }
            }

            Assertions.assertEquals(1, image100Count, "Should have exactly one image.100 file");
            Assertions.assertEquals(1, checksum100Count, "Should have exactly one checksum.100 file");
            Assertions.assertEquals(0, image99Count, "image.99 should be deleted");
            Assertions.assertEquals(0, checksum99Count, "checksum.99 should be deleted");
        }
    }

    private void deleteDir(File dir) {
        if (!dir.exists()) {
            return;
        }
        if (dir.isFile()) {
            dir.delete();
        } else {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    deleteDir(file);
                }
            }
        }
        dir.delete();
    }
}

