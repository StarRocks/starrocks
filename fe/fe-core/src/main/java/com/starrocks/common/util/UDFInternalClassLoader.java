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

package com.starrocks.common.util;

import com.starrocks.common.udf.UDFDownloader;
import com.starrocks.credential.CloudConfiguration;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

public class UDFInternalClassLoader extends URLClassLoader {

    private static final Set<String> LOCAL_SCHEMES = Set.of("file", "http", "https");

    private final Path tempFile;

    public UDFInternalClassLoader(String udfPath) throws IOException {
        super(new URL[] {new URL("jar:" + udfPath + "!/")});
        this.tempFile = null;
    }

    private UDFInternalClassLoader(URL[] urls, Path tempFile) {
        super(urls);
        this.tempFile = tempFile;
    }

    /**
     * Creates a class loader for the given UDF path.
     * For cloud storage paths (s3://, oss://, etc.), downloads to a temp file first.
     * The temp file is deleted when the class loader is closed.
     */
    public static UDFInternalClassLoader create(String udfPath, CloudConfiguration cloudConfig) throws IOException {
        String scheme = URI.create(udfPath).getScheme();
        if (scheme == null || LOCAL_SCHEMES.contains(scheme.toLowerCase())) {
            return new UDFInternalClassLoader(udfPath);
        }
        Path tempFile = Files.createTempFile("starrocks-udf-", ".jar");
        tempFile.toFile().deleteOnExit();
        try {
            UDFDownloader.download2Local(udfPath, tempFile.toString(), cloudConfig);
        } catch (Exception e) {
            Files.deleteIfExists(tempFile);
            throw new IOException("Failed to download UDF from " + udfPath + ": " + e.getMessage(), e);
        }
        return new UDFInternalClassLoader(
                new URL[] {new URL("jar:file://" + tempFile + "!/")}, tempFile);
    }

    @Override
    public void close() throws IOException {
        super.close();
        if (tempFile != null) {
            Files.deleteIfExists(tempFile);
        }
    }
}
