// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
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
import java.util.function.Function;

/**
 * Implementation of FileIO that adds metadata content caching features.
 */
public class IcebergCachingFileIO implements FileIO {
    private static final Logger LOG = LogManager.getLogger(IcebergCachingFileIO.class);
    private static final int BUFFER_CHUNK_SIZE = 4 * 1024 * 1024; // 4MB
    private static final long DEFAULT_FILEIO_CACHE_MAX_CONTENT_LENGTH = 8 * 1024 * 1024;
    private static final long DEFAULT_FILEIO_CACHE_MAX_TOTAL_BYTES = 128 * 1024 * 1024;

    public static final String FILEIO_CACHE_MAX_TOTAL_BYTES = "fileIO.cache.max-total-bytes";

    private ContentCache fileContentCache;
    private FileIO wrappedIO;

    public IcebergCachingFileIO(FileIO io) {
        this.wrappedIO = io;
    }

    @Override
    public void initialize(Map<String, String> properties) {
        long maxTotalBytes = PropertyUtil.propertyAsLong(properties, FILEIO_CACHE_MAX_TOTAL_BYTES,
                                                        DEFAULT_FILEIO_CACHE_MAX_TOTAL_BYTES);
        this.fileContentCache = new ContentCache(DEFAULT_FILEIO_CACHE_MAX_CONTENT_LENGTH, maxTotalBytes);
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
        private final Cache<String, CacheEntry> cache;

        private ContentCache(long maxContentLength, long maxTotalBytes) {
            this.maxTotalBytes = maxTotalBytes;
            this.maxContentLength = maxContentLength;

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

        public CacheEntry get(String key, Function<String, CacheEntry> mappingFunction) {
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
                // read-through cache if file length is less than or equal to maximum length allowed to cache.
                if (getLength() <= contentCache.maxContentLength()) {
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
                SeekableInputStream stream = wrappedInputFile.newStream();
                List<ByteBuffer> buffers = Lists.newArrayList();

                while (totalBytesToRead > 0) {
                    // read the stream in 4MB chunk
                    int bytesToRead = (int) Math.min(BUFFER_CHUNK_SIZE, totalBytesToRead);
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
