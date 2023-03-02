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
import com.starrocks.common.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Implementation of FileIO that adds metadata content caching features.
 */
public class IcebergCachingFileIO implements FileIO {
    private static final Logger LOG = LogManager.getLogger(IcebergCachingFileIO.class);
    private static final int BUFFER_CHUNK_SIZE = 4 * 1024 * 1024; // 4MB
    private static final long CACHE_MAX_ENTRY_SIZE = Config.iceberg_metadata_cache_max_entry_size;
    private static final long MEMORY_CACHE_CAPACITY = Config.iceberg_metadata_memory_cache_capacity;
    private static final long MEMORY_CACHE_EXPIRATION_SECONDS = Config.iceberg_metadata_memory_cache_expiration_seconds;
    private static final boolean ENABLE_DISK_CACHE = Config.enable_iceberg_metadata_disk_cache;
    @Deprecated
    public static final String FILEIO_CACHE_MAX_TOTAL_BYTES = "fileIO.cache.max-total-bytes";
    public static final String METADATA_CACHE_DISK_PATH = Config.iceberg_metadata_cache_disk_path;
    public static final long DISK_CACHE_CAPACITY = Config.iceberg_metadata_disk_cache_capacity;

    private ContentCache fileContentCache;
    private final FileIO wrappedIO;

    public IcebergCachingFileIO(FileIO io) {
        this.wrappedIO = io;
    }

    @Override
    public void initialize(Map<String, String> properties) {
        if (ENABLE_DISK_CACHE) {
            this.fileContentCache = TwoLevelCacheHolder.INSTANCE;
        } else {
            this.fileContentCache = MemoryCacheHolder.INSTANCE;
        }
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

    private static class DiskCacheEntry {
        private final long length;
        private final InputFile inputFile;

        private DiskCacheEntry(long length, InputFile inputFile) {
            this.length = length;
            this.inputFile = inputFile;
        }

        public SeekableInputStream toSeekableInputStream() {
            return this.inputFile.newStream();
        }

        public boolean isExistOnDisk() {
            return this.inputFile != null && this.inputFile.exists();
        }
    }

    public abstract static class ContentCache {
        private final long maxContentLength;

        private ContentCache() {
            this.maxContentLength = CACHE_MAX_ENTRY_SIZE;
        }

        public long maxContentLength() {
            return maxContentLength;
        }

        public abstract CacheEntry get(String key, Function<String, CacheEntry> mappingFunction);

        public abstract void invalidate(String key);

        public abstract long getLength(String key);

        public abstract boolean exists(String key);

        public boolean isExistOnDisk(String key) {
            return false;
        }

        public InputFile getDiskInputFile(String key) {
            return null;
        }
    }

    private static class MemoryCacheHolder {
        static final ContentCache INSTANCE = new MemoryContentCache();
    }
    public static class MemoryContentCache extends ContentCache {
        private final Cache<String, CacheEntry> cache;

        private MemoryContentCache() {
            super();

            Caffeine<Object, Object> builder = Caffeine.newBuilder();
            this.cache = builder.maximumWeight(MEMORY_CACHE_CAPACITY)
                    .expireAfterAccess(MEMORY_CACHE_EXPIRATION_SECONDS, TimeUnit.SECONDS)
                    .weigher((Weigher<String, CacheEntry>) (key, value) -> (int) Math.min(value.length, Integer.MAX_VALUE))
                    .recordStats()
                    .removalListener(((key, value, cause) -> {
                        LOG.debug(key + " to be eliminated, reason: " + cause);
                    })).build();
        }

        @Override
        public CacheEntry get(String key, Function<String, CacheEntry> mappingFunction) {
            return cache.get(key, mappingFunction);
        }

        @Override
        public void invalidate(String key) {
            cache.invalidate(key);
        }

        @Override
        public long getLength(String key) {
            CacheEntry buf = cache.getIfPresent(key);
            if (buf != null) {
                return buf.length;
            } else {
                return -1;
            }
        }

        @Override
        public boolean exists(String key) {
            CacheEntry buf = cache.getIfPresent(key);
            return buf != null;
        }

        @Override
        public boolean isExistOnDisk(String key) {
            return false;
        }
    }

    private static class TwoLevelCacheHolder {
        static final ContentCache INSTANCE = new TwoLevelContentCache();
    }
    public static class TwoLevelContentCache extends ContentCache {
        private final Cache<String, CacheEntry> memCache;
        private final Cache<String, DiskCacheEntry> diskCache;

        private TwoLevelContentCache() {
            super();

            Caffeine<Object, Object> diskCacheBuilder = Caffeine.newBuilder();
            this.diskCache = diskCacheBuilder.maximumWeight(DISK_CACHE_CAPACITY)
                    .weigher((Weigher<String, DiskCacheEntry>) (key, value) -> (int) Math.min(value.length, Integer.MAX_VALUE))
                    .recordStats()
                    .removalListener(((key, value, cause) -> {
                        LOG.debug(key + " to be eliminated from disk, reason: " + cause);
                        HadoopOutputFile hadoopOutputFile = (HadoopOutputFile) IOUtil.getOutputFile(
                                METADATA_CACHE_DISK_PATH, key);
                        try {
                            hadoopOutputFile.getFileSystem().delete(hadoopOutputFile.getPath(), false);
                        } catch (Exception e) {
                            LOG.warn("failed on deleting file :" + hadoopOutputFile.getPath());
                        }
                    })).build();

            Caffeine<Object, Object> builder = Caffeine.newBuilder();
            this.memCache = builder.maximumWeight(MEMORY_CACHE_CAPACITY)
                    .expireAfterAccess(MEMORY_CACHE_EXPIRATION_SECONDS, TimeUnit.SECONDS)
                    .weigher((Weigher<String, CacheEntry>) (key, value) -> (int) Math.min(value.length, Integer.MAX_VALUE))
                    .recordStats()
                    .removalListener(((key, value, cause) -> {
                        LOG.debug(key + " to be eliminated to disk, reason: " + cause);
                        if (isExistOnDisk(key)) {
                            LOG.debug(key + " has cached on disk");
                            return;
                        }
                        HadoopOutputFile tmpOutputFile = (HadoopOutputFile) IOUtil.getTmpOutputFile(
                                METADATA_CACHE_DISK_PATH, key);
                        PositionOutputStream outputStream = tmpOutputFile.createOrOverwrite();
                        Path localFilePath = new Path(IOUtil.remoteToLocalFilePath(METADATA_CACHE_DISK_PATH, key));
                        try {
                            for (ByteBuffer buffer : value.buffers) {
                                outputStream.write(buffer.array());
                            }
                            outputStream.close();
                            if (!tmpOutputFile.getFileSystem().rename(tmpOutputFile.getPath(), localFilePath)) {
                                LOG.warn("failed on rename: {} to {}", tmpOutputFile.getPath().toString(),
                                        localFilePath.toString());
                                tmpOutputFile.getFileSystem().delete(tmpOutputFile.getPath(), false);
                            } else {
                                diskCache.put(key, new DiskCacheEntry(value.length,
                                        IOUtil.getOutputFile(localFilePath).toInputFile()));
                            }
                        } catch (IOException e) {
                            LOG.warn("failed on writing file to disk" + e.getMessage());
                            try {
                                if (outputStream != null) {
                                    outputStream.close();
                                }
                                tmpOutputFile.getFileSystem().delete(tmpOutputFile.getPath(), false);
                            } catch (IOException ioException) {
                                LOG.warn("failed on deleting file :" + tmpOutputFile.getPath());
                            }
                        }
                    })).build();

            loadMetadataDiskCache();
        }

        private void loadMetadataDiskCache() {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            FileSystem fs = Util.getFs(IOUtil.getLocalDiskDirPath(METADATA_CACHE_DISK_PATH), new Configuration());
            executor.submit(() -> {
                try {
                    RemoteIterator<LocatedFileStatus>
                            it = fs.listFiles(IOUtil.getLocalDiskDirPath(METADATA_CACHE_DISK_PATH), true);
                    while (it.hasNext()) {
                        LocatedFileStatus locatedFileStatus = it.next();
                        if (locatedFileStatus.isDirectory()) {
                            continue;
                        }
                        Path localPath = locatedFileStatus.getPath();
                        OutputFile localOutputFile = IOUtil.getOutputFile(localPath);
                        String key = IOUtil.localFileToRemote(localPath, METADATA_CACHE_DISK_PATH).toString();
                        diskCache.put(key, new DiskCacheEntry(locatedFileStatus.getLen(), localOutputFile.toInputFile()));
                        LOG.debug("load metadata to disk cache: {} from {}", key, localPath.toString());
                    }
                } catch (Exception e) {
                    // Ignore, exception would not have affection on Diskcache
                    LOG.warn("Encountered exception when loading disk metadata " + e.getMessage());
                }
            });
            executor.shutdown();
        }

        @Override
        public CacheEntry get(String key, Function<String, CacheEntry> mappingFunction) {
            return memCache.get(key, mappingFunction);
        }

        @Override
        public long getLength(String key) {
            CacheEntry buf = memCache.getIfPresent(key);
            if (buf != null) {
                return buf.length;
            } else {
                DiskCacheEntry entry = diskCache.getIfPresent(key);
                if (entry != null) {
                    return entry.length;
                }
                return -1;
            }
        }

        @Override
        public boolean exists(String key) {
            CacheEntry buf = memCache.getIfPresent(key);
            return buf != null || isExistOnDisk(key);
        }

        @Override
        public void invalidate(String key) {
            memCache.invalidate(key);
            diskCache.invalidate(key);
        }

        @Override
        public boolean isExistOnDisk(String key) {
            DiskCacheEntry entry = diskCache.getIfPresent(key);
            return entry != null && entry.isExistOnDisk();
        }

        @Override
        public InputFile getDiskInputFile(String key) {
            if (diskCache.getIfPresent(key) != null) {
                return diskCache.getIfPresent(key).inputFile;
            } else {
                return null;
            }
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
            return contentCache.getLength(location()) == -1 ?
                    wrappedInputFile.getLength() : contentCache.getLength(location());
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
            return contentCache.exists(location()) || wrappedInputFile.exists();
        }

        private CacheEntry newCacheEntry() {
            try {
                long fileLength = getLength();
                long totalBytesToRead = fileLength;
                SeekableInputStream stream;
                if (contentCache.isExistOnDisk(location())) {
                    LOG.debug(location() + " hit on disk cache");
                    InputFile diskFile = contentCache.getDiskInputFile(location());
                    if (diskFile != null) {
                        stream = diskFile.newStream();
                    } else {
                        LOG.debug(location() + " load from remote");
                        stream = wrappedInputFile.newStream();
                    }
                } else {
                    LOG.debug(location() + " load from remote");
                    stream = wrappedInputFile.newStream();
                }
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