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

package com.starrocks.connector.delta;

import com.azure.core.util.Context;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import io.delta.kernel.defaults.engine.fileio.FileIO;
import io.delta.kernel.defaults.engine.fileio.InputFile;
import io.delta.kernel.defaults.engine.fileio.OutputFile;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import org.apache.hadoop.conf.Configuration;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;
import java.time.Duration;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;

/** Read-only Delta metadata access to ADLS Gen2 through its Azure Blob API. */
final class AzureDeltaFileIO implements FileIO {
    static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);
    private final Configuration conf;
    private final URI tableUri;
    private final BlobContainerClient container;

    AzureDeltaFileIO(String tablePath, Configuration conf) {
        this(tablePath, conf, AzureDeltaCredentials.createContainer(tablePath, conf));
    }

    AzureDeltaFileIO(String tablePath, Configuration conf, BlobContainerClient container) {
        this.tableUri = parseUri(tablePath);
        this.conf = new Configuration(conf);
        this.container = container;
    }

    static URI parseUri(String path) {
        URI uri;
        try {
            // Kernel paths follow Hadoop Path's literal object-key semantics (including percent signs).
            // Use its URI parser only; native I/O never calls Hadoop FileSystem.
            if (path.indexOf('?') >= 0 || path.indexOf('#') >= 0) {
                throw new IllegalArgumentException("Query and fragment are not supported");
            }
            uri = new org.apache.hadoop.fs.Path(path).toUri();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid native ADLS URI");
        }
        if (!("abfs".equals(uri.getScheme()) || "abfss".equals(uri.getScheme())) ||
                uri.getHost() == null || uri.getUserInfo() == null ||
                !uri.getUserInfo().matches("[a-z0-9][a-z0-9-]*") || uri.getPort() != -1 ||
                uri.getRawQuery() != null || uri.getRawFragment() != null ||
                !uri.getHost().matches("[a-z0-9]+\\.dfs\\.core\\.(windows\\.net|usgovcloudapi\\.net|chinacloudapi\\.cn)")) {
            throw new IllegalArgumentException("Native ADLS requires an abfs[s] URI with a standard Azure DFS endpoint");
        }
        return uri;
    }

    private String blobName(String path) {
        URI uri = parseUri(path);
        if (!tableUri.getRawAuthority().equals(uri.getRawAuthority())) {
            throw new IllegalArgumentException("Native ADLS FileIO cannot access another account or container");
        }
        String name = uri.getPath();
        return name.startsWith("/") ? name.substring(1) : name;
    }

    private String filePath(String name) {
        return tableUri.getScheme() + "://" + tableUri.getAuthority() + "/" + name;
    }

    @Override
    public CloseableIterator<FileStatus> listFrom(String path) throws IOException {
        checkInterrupted();
        String start = blobName(path);
        String parent = start.substring(0, start.lastIndexOf('/') + 1);
        // Azure Blob listing is lexicographically ordered. Keep SDK pagination lazy and exclude subdirectories.
        return new CloseableIterator<>() {
            private Iterator<BlobItem> items;
            private FileStatus next;
            private boolean closed;

            @Override
            public boolean hasNext() {
                if (closed) {
                    return false;
                }
                try {
                    checkInterrupted();
                    if (items == null) {
                        items = container.listBlobsByHierarchy("/", new ListBlobsOptions().setPrefix(parent)
                                .setMaxResultsPerPage(1000), REQUEST_TIMEOUT).iterator();
                    }
                    while (next == null && items.hasNext()) {
                        BlobItem item = items.next();
                        if (!Boolean.TRUE.equals(item.isPrefix()) && item.getName().compareTo(start) >= 0) {
                            next = FileStatus.of(filePath(item.getName()), item.getProperties().getContentLength(),
                                    item.getProperties().getLastModified().toInstant().toEpochMilli());
                        }
                    }
                    return next != null;
                } catch (IOException | RuntimeException e) {
                    throw new io.delta.kernel.exceptions.KernelEngineException("Native ADLS listing failed",
                            e instanceof IOException ? e : ioException("list", (RuntimeException) e));
                }
            }

            @Override
            public FileStatus next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                FileStatus result = next;
                next = null;
                return result;
            }

            @Override
            public void close() {
                closed = true;
                next = null;
            }
        };
    }

    @Override
    public FileStatus getFileStatus(String path) throws IOException {
        checkInterrupted();
        String name = blobName(path);
        try {
            BlobProperties properties = container.getBlobClient(name)
                    .getPropertiesWithResponse(null, REQUEST_TIMEOUT, Context.NONE).getValue();
            return FileStatus.of(filePath(name), properties.getBlobSize(),
                    properties.getLastModified().toInstant().toEpochMilli());
        } catch (RuntimeException e) {
            throw ioException("stat", e);
        }
    }

    @Override
    public String resolvePath(String path) {
        blobName(path);
        return path;
    }

    @Override
    public InputFile newInputFile(String path, long fileSize) {
        return new AzureDeltaInputFile(path, fileSize, container.getBlobClient(blobName(path)));
    }

    @Override
    public Optional<String> getConf(String key) {
        return Optional.ofNullable(conf.get(key));
    }

    @Override
    public boolean mkdirs(String path) {
        throw readOnly();
    }

    @Override
    public OutputFile newOutputFile(String path) {
        throw readOnly();
    }

    @Override
    public boolean delete(String path) {
        throw readOnly();
    }

    @Override
    public void copyFileAtomically(String source, String target, boolean overwrite) {
        throw readOnly();
    }

    private static UnsupportedOperationException readOnly() {
        return new UnsupportedOperationException("Native ADLS Delta metadata FileIO is read-only");
    }

    static void checkInterrupted() throws InterruptedIOException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedIOException("Native ADLS operation interrupted");
        }
    }

    static IOException ioException(String operation, RuntimeException error) {
        // SDK exception messages can contain SAS query parameters. Expose status, never the credential-bearing URL.
        if (error instanceof BlobStorageException) {
            int status = ((BlobStorageException) error).getStatusCode();
            if (status == 404) {
                return new FileNotFoundException("Native ADLS " + operation + ": object not found");
            }
            return new IOException("Native ADLS " + operation + " failed (HTTP " + status + ")");
        }
        return new IOException("Native ADLS " + operation + " failed (" + error.getClass().getSimpleName() + ")");
    }
}
