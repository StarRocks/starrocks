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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.cache.Cache;
import com.starrocks.common.Pair;
import com.starrocks.connector.hadoop.HadoopExt;
import com.starrocks.connector.hadoop.HadoopExt.FileSystemAccess;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.engine.hadoopio.HadoopFileIO;
import io.delta.kernel.engine.JsonHandler;
import io.delta.kernel.engine.ParquetHandler;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.NoSuchElementException;

public class DeltaLakeEngine extends DefaultEngine {
    private final Configuration hadoopConf;
    private final DeltaLakeCatalogProperties properties;
    // Cache for checkpoint metadata, key is file path and read schema, value is list of ColumnarBatch
    private final Cache<Pair<DeltaLakeFileStatus, StructType>, List<ColumnarBatch>> checkpointCache;
    // Cache for json metadata, key is file path, value is list of JsonNode
    private final Cache<DeltaLakeFileStatus, List<JsonNode>> jsonCache;

    protected DeltaLakeEngine(Configuration hadoopConf, DeltaLakeCatalogProperties properties,
                              Cache<Pair<DeltaLakeFileStatus, StructType>, List<ColumnarBatch>> checkpointCache,
                              Cache<DeltaLakeFileStatus, List<JsonNode>> jsonCache) {
        super(new HadoopFileIO(hadoopConf));
        this.hadoopConf = hadoopConf;
        this.properties = properties;
        this.checkpointCache = checkpointCache;
        this.jsonCache = jsonCache;
    }

    @Override
    public JsonHandler getJsonHandler() {
        return properties.isEnableDeltaLakeJsonMetaCache() ? new DeltaLakeJsonHandler(hadoopConf, jsonCache) :
                new TraceDefaultJsonHandler(hadoopConf);
    }

    @Override
    public ParquetHandler getParquetHandler() {
        return properties.isEnableDeltaLakeCheckpointMetaCache() ? new DeltaLakeParquetHandler(hadoopConf, checkpointCache) :
                new TraceDefaultParquetHandler(hadoopConf);
    }

    FileSystemAccess acquireFileSystemAccess() {
        return HadoopExt.getInstance().acquireFileSystemAccess(hadoopConf);
    }

    // The iterator must defer I/O until hasNext/next; close it before releasing the access scope.
    <T> CloseableIterator<T> withFileSystemAccess(CloseableIterator<T> iterator) {
        return new CloseableIterator<>() {
            // Cancellation must wait for an in-flight open to publish the resource before closing it.
            private boolean closed;
            private FileSystemAccess access;

            private void acquire() {
                if (access == null) {
                    access = acquireFileSystemAccess();
                }
            }

            @Override
            public synchronized boolean hasNext() {
                if (closed) {
                    return false;
                }
                try {
                    acquire();
                    if (iterator.hasNext()) {
                        return true;
                    }
                    close();
                    return false;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                } catch (RuntimeException | Error e) {
                    closeOnFailure(e);
                    throw e;
                }
            }

            @Override
            public synchronized T next() {
                if (closed) {
                    throw new NoSuchElementException();
                }
                try {
                    acquire();
                    return iterator.next();
                } catch (RuntimeException | Error e) {
                    closeOnFailure(e);
                    throw e;
                }
            }

            private void closeOnFailure(Throwable failure) {
                try {
                    close();
                } catch (Throwable closeFailure) {
                    if (failure != closeFailure) {
                        failure.addSuppressed(closeFailure);
                    }
                }
            }

            @Override
            public synchronized void close() throws IOException {
                if (!closed) {
                    closed = true;
                    try {
                        iterator.close();
                    } finally {
                        if (access != null) {
                            access.close();
                            access = null;
                        }
                    }
                }
            }
        };
    }

    public static DeltaLakeEngine create(Configuration hadoopConf, DeltaLakeCatalogProperties properties,
                                         Cache<Pair<DeltaLakeFileStatus, StructType>, List<ColumnarBatch>> checkpointCache,
                                         Cache<DeltaLakeFileStatus, List<JsonNode>> jsonCache) {
        return new DeltaLakeEngine(hadoopConf, properties, checkpointCache, jsonCache);
    }
}
