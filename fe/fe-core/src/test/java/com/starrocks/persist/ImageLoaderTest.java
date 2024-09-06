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

package com.starrocks.persist;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public class ImageLoaderTest {
    private static Path imageDir;

    @BeforeClass
    public static void setUp() throws Exception {
        imageDir = Files.createTempDirectory(Paths.get("."), "ImageLoaderTest");
        File v2Dir = new File(imageDir.toString(), "v2");
        v2Dir.mkdirs();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        FileUtils.deleteDirectory(imageDir.toFile());
    }

    /**
     *  meta/image/
     *            image.1000
     *            image.2000
     *            v2/
     *              image.ckpt
     *              image.1000
     *              checksum.100
     *              image.2000
     *              checksum.2000
     */
    @Test
    public void testLoadMaxImage1() throws Exception {
        List<Path> pathList = new ArrayList<>();
        pathList.add(Path.of(imageDir.toString(), "image.1000"));
        pathList.add(Path.of(imageDir.toString(), "image.2000"));
        pathList.add(Path.of(imageDir.toString(), "v2", "image.1000"));
        pathList.add(Path.of(imageDir.toString(), "v2", "checksum.1000"));
        pathList.add(Path.of(imageDir.toString(), "v2", "image.2000"));
        pathList.add(Path.of(imageDir.toString(), "v2", "checksum.2000"));
        pathList.add(Path.of(imageDir.toString(), "v2", "image.ckpt"));
        try {
            for (Path path : pathList) {
                Files.createFile(path);
            }

            ImageLoader imageLoader = new ImageLoader(imageDir.toString());
            Assert.assertEquals("image.2000", imageLoader.getImageFile().getName());
            Assert.assertEquals(ImageFormatVersion.v2, imageLoader.getImageFormatVersion());
        } finally {
            for (Path path : pathList) {
                Files.delete(path);
            }
        }
    }

    /**
     *  meta/image/
     *            image.1000
     *            image.2000
     *            v2/
     *              image.1000
     *              checksum.1000
     *              image.ckpt
     */
    @Test
    public void testLoadMaxImage2() throws Exception {
        List<Path> pathList = new ArrayList<>();
        pathList.add(Path.of(imageDir.toString(), "image.1000"));
        pathList.add(Path.of(imageDir.toString(), "image.2000"));
        pathList.add(Path.of(imageDir.toString(), "v2", "image.1000"));
        pathList.add(Path.of(imageDir.toString(), "v2", "checksum.1000"));
        pathList.add(Path.of(imageDir.toString(), "v2", "image.ckpt"));
        try {
            for (Path path : pathList) {
                Files.createFile(path);
            }

            ImageLoader imageLoader = new ImageLoader(imageDir.toString());
            Assert.assertEquals("image.2000", imageLoader.getImageFile().getName());
            Assert.assertEquals(ImageFormatVersion.v1, imageLoader.getImageFormatVersion());
        } finally {
            for (Path path : pathList) {
                Files.delete(path);
            }
        }
    }

    /**
     *  meta/image/
     *            image.1000
     *            v2/
     *              image.2000
     *              checksum.2000
     */
    @Test
    public void testLoadMaxImage3() throws Exception {
        List<Path> pathList = new ArrayList<>();
        pathList.add(Path.of(imageDir.toString(), "image.1000"));
        pathList.add(Path.of(imageDir.toString(), "v2", "image.2000"));
        pathList.add(Path.of(imageDir.toString(), "v2", "checksum.2000"));
        try {
            for (Path path : pathList) {
                Files.createFile(path);
            }

            ImageLoader imageLoader = new ImageLoader(imageDir.toString());
            Assert.assertEquals("image.2000", imageLoader.getImageFile().getName());
            Assert.assertEquals(ImageFormatVersion.v2, imageLoader.getImageFormatVersion());
        } finally {
            for (Path path : pathList) {
                Files.delete(path);
            }
        }
    }

    @Test
    public void testChecksum() throws Exception {
        List<Path> pathList = new ArrayList<>();
        pathList.add(Path.of(imageDir.toString(), "v2", "image.2000"));
        pathList.add(Path.of(imageDir.toString(), "v2", "checksum.2000"));
        try {
            for (Path path : pathList) {
                Files.createFile(path);
            }

            String content = "xxxxxxx=========aaabbccc";
            Files.writeString(Path.of(imageDir.toString(), "v2", "image.2000"), content);
            Checksum checksum = new CRC32();
            checksum.update(content.getBytes());
            Files.writeString(Path.of(imageDir.toString(), "v2", "checksum.2000"),
                    String.valueOf(checksum.getValue()));

        } finally {
            for (Path path : pathList) {
                Files.delete(path);
            }
        }
    }
}
