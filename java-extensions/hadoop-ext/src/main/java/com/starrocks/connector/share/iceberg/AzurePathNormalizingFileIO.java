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

package com.starrocks.connector.share.iceberg;

import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Rewrites Azure paths a Hadoop filesystem cannot resolve before handing them to the wrapped FileIO.
 *
 * <p>Snowflake Horizon and some other Iceberg REST catalogs return paths like
 * {@code abfss://<container>@<account>.blob.core.windows.net/...}. Hadoop's AzureBlobFileSystem (abfs/abfss)
 * only serves dfs.core.windows.net endpoints, while NativeAzureFileSystem (wasb/wasbs) is the driver for
 * blob.core.windows.net, so such paths are rewritten to {@code wasbs://<container>@<account>.blob.core.windows.net/...}.
 * Every other path is passed through unchanged.
 */
public class AzurePathNormalizingFileIO implements FileIO {
    private static final Pattern ABFSS_BLOB_CORE_PATTERN =
            Pattern.compile("^(abfss?)://([^@]+@[^.]+)\\.blob\\.core\\.windows\\.net(/.*)?$");

    private final FileIO delegate;

    public AzurePathNormalizingFileIO(FileIO delegate) {
        this.delegate = delegate;
    }

    public static String normalizeAzurePath(String path) {
        if (path == null) {
            return null;
        }
        Matcher m = ABFSS_BLOB_CORE_PATTERN.matcher(path);
        if (m.matches()) {
            String scheme = "wasb" + (m.group(1).equals("abfss") ? "s" : "");
            String authority = m.group(2);
            String suffix = m.group(3) != null ? m.group(3) : "";
            return scheme + "://" + authority + ".blob.core.windows.net" + suffix;
        }
        return path;
    }

    public FileIO getDelegate() {
        return delegate;
    }

    @Override
    public InputFile newInputFile(String path) {
        String normalized = normalizeAzurePath(path);
        return keepLocation(delegate.newInputFile(normalized), path, normalized);
    }

    @Override
    public InputFile newInputFile(String path, long length) {
        String normalized = normalizeAzurePath(path);
        return keepLocation(delegate.newInputFile(normalized, length), path, normalized);
    }

    // Only the storage access is normalized: location() stays the caller's path, as IcebergCachingFileIO does on
    // the FE. Iceberg takes the metadata file location from it, which $metadata_log_entries then shows.
    private static InputFile keepLocation(InputFile file, String path, String normalized) {
        if (path == null || path.equals(normalized)) {
            return file;
        }
        return new InputFile() {
            @Override
            public long getLength() {
                return file.getLength();
            }

            @Override
            public SeekableInputStream newStream() {
                return file.newStream();
            }

            @Override
            public String location() {
                return path;
            }

            @Override
            public boolean exists() {
                return file.exists();
            }
        };
    }

    @Override
    public OutputFile newOutputFile(String path) {
        return delegate.newOutputFile(normalizeAzurePath(path));
    }

    @Override
    public void deleteFile(String path) {
        delegate.deleteFile(normalizeAzurePath(path));
    }

    @Override
    public Map<String, String> properties() {
        return delegate.properties();
    }

    @Override
    public void initialize(Map<String, String> properties) {
        delegate.initialize(properties);
    }

    @Override
    public void close() {
        delegate.close();
    }
}
