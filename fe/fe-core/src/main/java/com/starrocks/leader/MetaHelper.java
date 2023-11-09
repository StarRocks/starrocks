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

import com.starrocks.common.Config;
import com.starrocks.common.InvalidMetaDirException;
import com.starrocks.common.io.IOUtils;
import com.starrocks.journal.bdbje.BDBEnvironment;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class MetaHelper {
    private static final Logger LOG = LogManager.getLogger(MetaHelper.class);

    private static final String PART_SUFFIX = ".part";
    public static final String X_IMAGE_SIZE = "X-Image-Size";
    private static final int BUFFER_BYTES = 8 * 1024;
    private static final int CHECKPOINT_LIMIT_BYTES = 30 * 1024 * 1024;

    public static File getLeaderImageDir() {
        String metaDir = GlobalStateMgr.getCurrentState().getImageDir();
        return new File(metaDir);
    }

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

    public static OutputStream getOutputStream(String filename, File dir)
            throws FileNotFoundException {
        File file = new File(dir, filename + MetaHelper.PART_SUFFIX);
        return new FileOutputStream(file);
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
                out.close();
            }
        }
    }

    public static void checkMetaDir() throws InvalidMetaDirException,
                                             IOException {
        // check meta dir
        //   if metaDir is the default config: StarRocksFE.STARROCKS_HOME_DIR + "/meta",
        //   we should check whether both the new default dir (STARROCKS_HOME_DIR + "/meta")
        //   and the old default dir (DORIS_HOME_DIR + "/doris-meta") are present. If both are present,
        //   we need to let users keep only one to avoid starting from outdated metadata.
        Path oldDefaultMetaDir = Paths.get(System.getenv("DORIS_HOME") + "/doris-meta");
        Path newDefaultMetaDir = Paths.get(System.getenv("STARROCKS_HOME") + "/meta");
        Path metaDir = Paths.get(Config.meta_dir);
        if (metaDir.equals(newDefaultMetaDir)) {
            File oldMeta = new File(oldDefaultMetaDir.toUri());
            File newMeta = new File(newDefaultMetaDir.toUri());
            if (oldMeta.exists() && newMeta.exists()) {
                LOG.error("New default meta dir: {} and Old default meta dir: {} are both present. " +
                                "Please make sure {} has the latest data, and remove the another one.",
                        newDefaultMetaDir, oldDefaultMetaDir, newDefaultMetaDir);
                throw new InvalidMetaDirException();
            }
        }

        File meta = new File(metaDir.toUri());
        if (!meta.exists()) {
            // If metaDir is not the default config, it means the user has specified the other directory
            // We should not use the oldDefaultMetaDir.
            // Just exit in this case
            if (!metaDir.equals(newDefaultMetaDir)) {
                LOG.error("meta dir {} dose not exist", metaDir);
                throw new InvalidMetaDirException();
            }
            File oldMeta = new File(oldDefaultMetaDir.toUri());
            if (oldMeta.exists()) {
                // For backward compatible
                Config.meta_dir = oldDefaultMetaDir.toString();
            } else {
                LOG.error("meta dir {} does not exist", meta.getAbsolutePath());
                throw new InvalidMetaDirException();
            }
        }



        Path imageDir = Paths.get(Config.meta_dir + GlobalStateMgr.IMAGE_DIR);
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
}
