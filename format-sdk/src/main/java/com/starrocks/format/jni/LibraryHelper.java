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

package com.starrocks.format.jni;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Optional;

public class LibraryHelper {

    private static final Logger LOG = LoggerFactory.getLogger(LibraryHelper.class);

    public static final String PROP_FORMAT_LIB_PATH = "com.starrocks.format.lib.path";
    public static final String PROP_FORMAT_LIB_NAME = "com.starrocks.format.lib.name";
    public static final String PROP_FORMAT_WRAPPER_LIB_NAME = "com.starrocks.format.wrapper.lib.name";

    private LibraryHelper() {
    }

    /**
     * Load starrocks_format.so and starrocks_format_wrapper.so.
     */
    public static synchronized void load() {
        long start = System.currentTimeMillis();
        try {
            // 1. load starrocks_format.so
            String formatLibName = Optional
                    .ofNullable(System.getProperty(PROP_FORMAT_LIB_NAME))
                    // Resolve the library file name with a suffix (e.g., dll, .so, etc.)
                    .orElse(System.mapLibraryName("starrocks_format"));
            File formatLibFile = findOrExtractLibrary(formatLibName);
            LOG.info("Start to load native format library {}", formatLibFile.getAbsolutePath());
            System.load(formatLibFile.getAbsolutePath());

            // 2. load starrocks_format_wrapper.so
            String formatWrapperLibName = Optional
                    .ofNullable(System.getProperty(PROP_FORMAT_WRAPPER_LIB_NAME))
                    .orElse(System.mapLibraryName("starrocks_format_wrapper"));
            File formatWrapperLibFile = findOrExtractLibrary(formatWrapperLibName);
            LOG.info("Start to load native format library {}", formatWrapperLibFile.getAbsolutePath());
            System.load(formatWrapperLibFile.getAbsolutePath());

            LOG.info("Load native format library success, elapse {}ms", System.currentTimeMillis() - start);
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load native format library, " + e.getMessage(), e);
        }
    }

    /**
     * Find native library path, or extract from jar if necessary.
     */
    private static File findOrExtractLibrary(String libFileName) throws IOException {
        String libFilePath = System.getProperty(PROP_FORMAT_LIB_PATH);
        if (StringUtils.isNotBlank(libFilePath)) {
            File libFile = new File(libFilePath, libFileName);
            if (libFile.exists()) {
                return libFile;
            } else {
                LOG.warn("{} not found in {}", libFileName, libFilePath);
            }
        }

        String tmpDir = new File(System.getProperty("java.io.tmpdir")).getAbsolutePath();
        File libFile = new File(tmpDir, libFileName);
        if (libFile.exists()) {
            return libFile;
        }

        // Extract and load a native library inside the jar file
        return extractLibraryFileWithLock(tmpDir, libFileName);
    }

    private static File extractLibraryFileWithLock(String extractDir, String libFileName) throws IOException {
        File libFile = new File(extractDir, libFileName);
        File extraDirFile = libFile.getParentFile();
        if (!extraDirFile.exists()) {
            extraDirFile.mkdirs();
        }

        File libLockFile = new File(libFile.getParent(), libFileName + ".lck");
        try (RandomAccessFile file = new RandomAccessFile(libLockFile, "rw")) {
            try (FileChannel channel = file.getChannel()) {
                try (FileLock lock = channel.lock()) {
                    LOG.info("Extract native format library file {}, lock[valid: {}, shared: {}]",
                            libFile.getAbsolutePath(), lock.isValid(), lock.isShared());
                    return doExtractLibraryFile(libFile);
                }
            }
        }
    }

    private static File doExtractLibraryFile(File libFile) throws IOException {
        // Extract a native library file into the target directory
        final String relativeLibPath = "native/" + libFile.getName();
        try (final InputStream jarLibStream = LibraryHelper.class
                .getClassLoader().getResourceAsStream(relativeLibPath)) {
            if (jarLibStream == null) {
                throw new FileNotFoundException(relativeLibPath);
            }

            Files.copy(jarLibStream, libFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            LOG.info("Extract {} to {}", relativeLibPath, libFile.toPath());
        }

        // Set executable (x) flag to enable Java to load the native library
        boolean extracted = libFile.setReadable(true)
                && libFile.setWritable(true, true)
                && libFile.setExecutable(true);
        if (!extracted) {
            throw new IllegalStateException(
                    "Failed to modify access permissions for " + libFile.getAbsolutePath());
        }

        // Check md5sum
        try (final InputStream jarLibStream = LibraryHelper.class
                .getClassLoader().getResourceAsStream(relativeLibPath)) {
            if (jarLibStream == null) {
                throw new FileNotFoundException(relativeLibPath);
            }

            String expectedMd5Sum = DigestUtils.md5Hex(jarLibStream);
            try (InputStream extLibStream = Files.newInputStream(libFile.toPath())) {
                String extractedMd5Sum = DigestUtils.md5Hex(extLibStream);
                if (!expectedMd5Sum.equals(extractedMd5Sum)) {
                    throw new IllegalStateException(
                            String.format("Unexpected md5sum for file %s, expected %s, but got %s",
                                    libFile.getAbsolutePath(), expectedMd5Sum, extractedMd5Sum));
                }
            }
        }

        return libFile;
    }

}
