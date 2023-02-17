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
//   https://github.com/apache/iceberg/pull/4518

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

package com.starrocks.connector.iceberg.io;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Weigher;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.DelegatingInputStream;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Implementation of FileIO that adds metadata content caching features.
 */
public class IcebergCachingFileIO implements FileIO {
    private static final Logger LOG = LogManager.getLogger(IcebergCachingFileIO.class);
    private static final int DEFAULT_BUFFER_CHUNK_SIZE = 4 * 1024 * 1024; // 4MB
    private static final long DEFAULT_FILEIO_CACHE_MAX_CONTENT_LENGTH = 1024L * 1024L * 1024L; // 1GB
    private static final long DEFAULT_FILEIO_CACHE_MAX_TOTAL_BYTES = 16L * 1024L * 1024L * 1024L; // 16GB

    public static final String FILEIO_BUFFER_CHUNK_SIZE = "fileIO.buffer-chunk-size";
    public static final String FILEIO_CACHE_MAX_CONTENT_LENGTH = "fileIO.cache.max-file-length";
    public static final String FILEIO_CACHE_MAX_TOTAL_BYTES = "fileIO.cache.max-total-bytes";

    private ContentCache fileContentCache;
    private FileIO wrappedIO;

    public AtomicInteger getAllFileIO() {
        return fileContentCache.getAllFileIO();
    }

    public AtomicInteger getFileIOWithCache() {
        return fileContentCache.getFileIOWithCache();
    }

    public AtomicInteger getFileIOWithCacheHit() {
        return fileContentCache.getFileIOWithCacheHit();
    }

    public IcebergCachingFileIO(FileIO io) {
        this.wrappedIO = io;
    }

    @Override
    public void initialize(Map<String, String> properties) {
        long maxTotalBytes = PropertyUtil.propertyAsLong(properties, FILEIO_CACHE_MAX_TOTAL_BYTES,
                                                        DEFAULT_FILEIO_CACHE_MAX_TOTAL_BYTES);
        long bufferChunkSize = PropertyUtil.propertyAsLong(properties, FILEIO_BUFFER_CHUNK_SIZE, DEFAULT_BUFFER_CHUNK_SIZE);
        long maxFileSize = PropertyUtil.propertyAsLong(properties, FILEIO_CACHE_MAX_CONTENT_LENGTH,
                                                       DEFAULT_FILEIO_CACHE_MAX_CONTENT_LENGTH);
        this.fileContentCache = new ContentCache(maxFileSize, maxTotalBytes, bufferChunkSize);
    }

    @Override
    public InputFile newInputFile(String path) {
        return new CachingInputFile(fileContentCache, wrappedIO.newInputFile(path));
    }

    @Override
    public OutputFile newOutputFile(String path) {
        return wrappedIO.newOutputFile(path);
    }

    @Override
    public void deleteFile(String path) {
        wrappedIO.deleteFile(path);
        // remove from cache.
        fileContentCache.invalidate(path);
    }

    private static class CacheEntry {
        private final long length;
        private final List<ByteBuffer> buffers;

        private CacheEntry(long length, List<ByteBuffer> buffers) {
            this.length = length;
            this.buffers = buffers;
        }
    }

    public static class ContentCache {
        private final long maxTotalBytes;
        private final long maxContentLength;
        private final long bufferChunkSize;
        private final Cache<String, CacheEntry> cache;
        private AtomicInteger fileIOWithCacheHit = new AtomicInteger(0);
        private AtomicInteger allFileIO = new AtomicInteger(0);
        private AtomicInteger fileIOWithCache = new AtomicInteger(0);

        public AtomicInteger getAllFileIO() {
            return allFileIO;
        }

        public AtomicInteger getFileIOWithCache() {
            return fileIOWithCache;
        }

        public AtomicInteger getFileIOWithCacheHit() {
            return fileIOWithCacheHit;
        }


        private ContentCache(long maxContentLength, long maxTotalBytes, long bufferChunkSize) {
            this.maxTotalBytes = maxTotalBytes;
            this.maxContentLength = maxContentLength;
            this.bufferChunkSize = bufferChunkSize;

            Caffeine<Object, Object> builder = Caffeine.newBuilder();
            this.cache = builder.maximumWeight(maxTotalBytes)
                        .weigher((Weigher<String, CacheEntry>) (key, value) -> (int) Math.min(value.length, Integer.MAX_VALUE))
                        .recordStats()
                        .removalListener(((key, value, cause) -> {
                            LOG.debug(key + " to be eliminated, reason: " + cause);
                        }))
                        .build();
        }

        public long maxContentLength() {
            return maxContentLength;
        }

        public long bufferChunkSize() {
            return bufferChunkSize;
        }

        public CacheEntry get(String key, Function<String, CacheEntry> mappingFunction) {
            if (cache.getIfPresent(key) != null) {
                fileIOWithCacheHit.getAndAdd(1);
            }
            return cache.get(key, mappingFunction);
        }

        public CacheEntry getIfPresent(String location) {
            return cache.getIfPresent(location);
        }

        public void invalidate(String key) {
            cache.invalidate(key);
        }
    }

    private static class CachingInputFile implements InputFile {
        private final ContentCache contentCache;
        private final InputFile wrappedInputFile;

        private CachingInputFile(ContentCache cache, InputFile inFile) {
            this.contentCache = cache;
            this.wrappedInputFile = inFile;
        }

        @Override
        public long getLength() {
            CacheEntry buf = contentCache.getIfPresent(location());
            return (buf != null) ? buf.length : wrappedInputFile.getLength();
        }

        @Override
        public SeekableInputStream newStream() {
            try {
                contentCache.getAllFileIO().getAndAdd(1);

                // read-through cache if file length is less than or equal to maximum length allowed to cache.
                if (getLength() <= contentCache.maxContentLength()) {
                    contentCache.getFileIOWithCache().getAndAdd(1);
                    return cachedStream();
                }

                // fallback to non-caching input stream.
                return wrappedInputFile.newStream();
            } catch (FileNotFoundException e) {
                throw new NotFoundException(e, "Failed to open input stream for file: %s", wrappedInputFile.location());
            } catch (IOException e) {
                throw new RuntimeIOException(e, "Failed to open input stream for file: %s", wrappedInputFile.location());
            }
        }

        @Override
        public String location() {
            return wrappedInputFile.location();
        }

        @Override
        public boolean exists() {
            CacheEntry buf = contentCache.getIfPresent(location());
            return buf != null || wrappedInputFile.exists();
        }

        private CacheEntry newCacheEntry() {
            try {
                long fileLength = getLength();
                long totalBytesToRead = fileLength;
                long start = System.currentTimeMillis();
                SeekableInputStream stream = wrappedInputFile.newStream();
                List<ByteBuffer> buffers = Lists.newArrayList();

                while (totalBytesToRead > 0) {
                    // read the stream in 4MB chunk
                    int bytesToRead = (int) Math.min(contentCache.bufferChunkSize(), totalBytesToRead);
                    byte[] buf = new byte[bytesToRead];
                    int bytesRead = IOUtil.readRemaining(stream, buf, 0, bytesToRead);
                    totalBytesToRead -= bytesRead;

                    if (bytesRead < bytesToRead) {
                        // Read less than it should be, possibly hitting EOF.
                        // Set smaller ByteBuffer limit and break out of the loop.
                        buffers.add(ByteBuffer.wrap(buf, 0, bytesRead));
                        break;
                    } else {
                        buffers.add(ByteBuffer.wrap(buf));
                    }
                }
                LOG.error("========== file:[{}], cost:[{}]", wrappedInputFile.location(), System.currentTimeMillis() - start);
                LOG.error("========={}", ((DelegatingInputStream) stream).getDelegate().toString());

                stream.close();
                return new CacheEntry(fileLength - totalBytesToRead, buffers);
            } catch (IOException ex) {
                throw new RuntimeIOException(ex);
            }
        }

        private SeekableInputStream cachedStream() throws IOException {
            try {
                CacheEntry entry = contentCache.get(location(), k -> newCacheEntry());
                Preconditions.checkNotNull(entry, "CacheEntry should not be null when there is no RuntimeException occurs");
                return ByteBufferInputStream.wrap(entry.buffers);
            } catch (RuntimeIOException ex) {
                throw ex.getCause();
            } catch (RuntimeException ex) {
                throw new IOException("Caught an error while reading through cache", ex);
            }
        }
    }
}
