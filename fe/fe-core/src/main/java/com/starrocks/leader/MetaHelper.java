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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/master/MetaHelper.java

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

package com.starrocks.leader;

import com.google.common.base.Strings;
import com.sleepycat.je.config.EnvironmentParams;
import com.starrocks.common.Config;
import com.starrocks.common.InvalidMetaDirException;
import com.starrocks.common.io.IOUtils;
import com.starrocks.journal.bdbje.BDBEnvironment;
import com.starrocks.monitor.unit.ByteSizeValue;
import com.starrocks.persist.ImageFormatVersion;
import com.starrocks.persist.Storage;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.staros.StarMgrServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class MetaHelper {
    private static final Logger LOG = LogManager.getLogger(MetaHelper.class);

    public static final String PART_SUFFIX = ".part";
    public static final String X_IMAGE_SIZE = "X-Image-Size";
    public static final String X_IMAGE_CHECKSUM = "X-Image-Checksum";
    private static final int BUFFER_BYTES = 8 * 1024;
    private static final int CHECKPOINT_LIMIT_BYTES = 30 * 1024 * 1024;

    public static int getLimit() {
        return CHECKPOINT_LIMIT_BYTES;
    }

    // rename the .PART_SUFFIX file to filename
    public static File complete(String filename, File dir) throws IOException {
        File file = new File(dir, filename + MetaHelper.PART_SUFFIX);
        File newFile = new File(dir, filename);
        if (!file.renameTo(newFile)) {
            throw new IOException("Complete file" + filename + " failed");
        }
        return newFile;
    }

    public static void downloadImageFile(String urlStr, int timeout, String journalId, File destDir)
            throws IOException {
        HttpURLConnection conn = null;
        String checksum = null;
        String destFilename = Storage.IMAGE + "." + journalId;
        File partFile = new File(destDir, destFilename + MetaHelper.PART_SUFFIX);
        // 1. download to a tmp file image.xxx.part
        try (FileOutputStream out = new FileOutputStream(partFile)) {
            URL url = new URL(urlStr);
            conn = (HttpURLConnection) url.openConnection();
            conn.setConnectTimeout(timeout);
            conn.setReadTimeout(timeout);

            // Get image size
            long imageSize = -1;
            String imageSizeStr = conn.getHeaderField(X_IMAGE_SIZE);
            if (imageSizeStr != null) {
                imageSize = Long.parseLong(imageSizeStr);
            }

            BufferedInputStream bin = new BufferedInputStream(conn.getInputStream());

            // Do not limit speed in client side.
            long bytes = IOUtils.copyBytes(bin, out, BUFFER_BYTES, CHECKPOINT_LIMIT_BYTES, false);
            if ((imageSize > 0) && (bytes != imageSize)) {
                throw new IOException("Unexpected image size, expected: " + imageSize + ", actual: " + bytes);
            }

            out.getChannel().force(true);

            checksum = conn.getHeaderField(X_IMAGE_CHECKSUM);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }

        // 2. write checksum if exists
        if (!Strings.isNullOrEmpty(checksum)) {
            File checksumFile = Path.of(destDir.getAbsolutePath(), Storage.CHECKSUM + "." + journalId).toFile();
            try (FileOutputStream fos = new FileOutputStream(checksumFile)) {
                fos.write(checksum.getBytes(StandardCharsets.UTF_8));
                fos.getChannel().force(true);
            }
        }

        // 3. rename to image.xxx
        File imageFile = new File(destDir, destFilename);
        if (!partFile.renameTo(imageFile)) {
            throw new IOException("rename file:" + partFile.getName() + " to file:" + destFilename + " failed");
        }

        LOG.info("successfully download image file: {}", imageFile.getAbsolutePath());
    }

    public static OutputStream getOutputStream(String filename, File dir)
            throws FileNotFoundException {
        File file = new File(dir, filename + MetaHelper.PART_SUFFIX);
        return new FileOutputStream(file);
    }

    public static void httpGet(String urlStr, int timeout) throws IOException {
        URL url = new URL(urlStr);
        HttpURLConnection conn = null;

        try {
            conn = (HttpURLConnection) url.openConnection();
            conn.setConnectTimeout(timeout);
            conn.setReadTimeout(timeout);

            try (InputStream in = conn.getInputStream()) {
                byte[] buf = new byte[BUFFER_BYTES];
                while (in.read(buf) >= 0) {}
            }
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }


    // download file from remote node
    public static void getRemoteFile(String urlStr, int timeout, OutputStream out)
            throws IOException {
        URL url = new URL(urlStr);
        HttpURLConnection conn = null;

        try {
            conn = (HttpURLConnection) url.openConnection();
            conn.setConnectTimeout(timeout);
            conn.setReadTimeout(timeout);

            // Get image size
            long imageSize = -1;
            String imageSizeStr = conn.getHeaderField(X_IMAGE_SIZE);
            if (imageSizeStr != null) {
                imageSize = Long.parseLong(imageSizeStr);
            }

            BufferedInputStream bin = new BufferedInputStream(conn.getInputStream());

            // Do not limit speed in client side.
            long bytes = IOUtils.copyBytes(bin, out, BUFFER_BYTES, CHECKPOINT_LIMIT_BYTES, true);

            if ((imageSize > 0) && (bytes != imageSize)) {
                throw new IOException("Unexpected image size, expected: " + imageSize + ", actual: " + bytes);
            }
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
            if (out != null) {
                out.flush();
                out.close();
            }
        }
    }

    public static void checkMetaDir() throws InvalidMetaDirException,
                                             IOException {
        Path metaDir = Paths.get(Config.meta_dir);
        File meta = new File(metaDir.toUri());
        if (!meta.exists()) {
            LOG.error("meta dir {} does not exist", metaDir);
            throw new InvalidMetaDirException();
        }

        long lowerFreeDiskSize = Long.parseLong(EnvironmentParams.FREE_DISK.getDefault());
        FileStore store = Files.getFileStore(Paths.get(Config.meta_dir));
        if (store.getUsableSpace() < lowerFreeDiskSize) {
            LOG.error("Free capacity left for meta dir: {} is less than {}",
                    Config.meta_dir, new ByteSizeValue(lowerFreeDiskSize));
            throw new InvalidMetaDirException();
        }

        Path imageDir = Paths.get(MetaHelper.getImageFileDir(true));
        Path bdbDir = Paths.get(BDBEnvironment.getBdbDir());
        boolean haveImageData = false;
        if (Files.exists(imageDir)) {
            try (Stream<Path> stream = Files.walk(imageDir)) {
                haveImageData = stream.anyMatch(path -> path.getFileName().toString().startsWith("image."));
            }
        }
        boolean haveBDBData = false;
        if (Files.exists(bdbDir)) {
            try (Stream<Path> stream = Files.walk(bdbDir)) {
                haveBDBData = stream.anyMatch(path -> path.getFileName().toString().endsWith(".jdb"));
            }
        }
        if (haveImageData && !haveBDBData && !Config.start_with_incomplete_meta) {
            LOG.error("image exists, but bdb dir is empty, " +
                    "set start_with_incomplete_meta to true if you want to forcefully recover from image data, " +
                    "this may end with stale meta data, so please be careful.");
            throw new InvalidMetaDirException();
        }
    }

    public static String getImageFileDir(boolean isGlobalStateMgr) {
        if (isGlobalStateMgr) {
            return getImageFileDir("", ImageFormatVersion.v2);
        } else {
            return getImageFileDir(StarMgrServer.IMAGE_SUBDIR, ImageFormatVersion.v1);
        }
    }

    public static String getImageFileDir(String subDir, ImageFormatVersion imageFormatVersion) {
        if (imageFormatVersion == ImageFormatVersion.v1) {
            return GlobalStateMgr.getImageDirPath() + subDir;
        } else {
            return GlobalStateMgr.getImageDirPath() + subDir + "/" + imageFormatVersion;
        }
    }
}
