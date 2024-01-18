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
import com.starrocks.common.conf.Config;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
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
public class IcebergCachingFileIO implements FileIO, Configurable {
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
    public static final long DISK_CACHE_EXPIRATION_SECONDS = Config.iceberg_metadata_disk_cache_expiration_seconds;

    private ContentCache fileContentCache;
    private FileIO wrappedIO;
    private Configuration conf;

    @Override
    public void initialize(Map<String, String> properties) {
        ResolvingFileIO resolvingFileIO = new ResolvingFileIO();
        resolvingFileIO.setConf(conf);
        wrappedIO = resolvingFileIO;
        wrappedIO.initialize(properties);

        if (ENABLE_DISK_CACHE) {
            this.fileContentCache = TwoLevelCacheHolder.INSTANCE;
        } else {
            this.fileContentCache = MemoryCacheHolder.INSTANCE;
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
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

    @Override
    public Map<String, String> properties() {
        return wrappedIO.properties();
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
        private int useCount;

        private DiskCacheEntry(long length, InputFile inputFile) {
            this.length = length;
            this.inputFile = inputFile;
            this.useCount = 0;
        }

        public SeekableInputStream toSeekableInputStream() {
            return this.inputFile.newStream();
        }

        public boolean isExistOnDisk() {
            if (this.inputFile == null) {
                return false;
            } else {
                HadoopInputFile hadoopInputFile = (HadoopInputFile) this.inputFile;
                try {
                    return hadoopInputFile.getFileSystem().exists(hadoopInputFile.getPath());
                } catch (IOException e) {
                    return false;
                }
            }
        }

        public void pin() {
            useCount += 1;
        }
        public void unpin() {
            useCount -= 1;
        }

        public static DiskCacheEntry newDiskCacheEntry(SeekableInputStream stream, long fileLength, String key) {
            long totalBytesToRead = fileLength;
            HadoopOutputFile tmpOutputFile = (HadoopOutputFile) IOUtil.getTmpOutputFile(
                    METADATA_CACHE_DISK_PATH, key);
            PositionOutputStream outputStream = tmpOutputFile.createOrOverwrite();
            try {
                Path localFilePath = new Path(IOUtil.remoteToLocalFilePath(METADATA_CACHE_DISK_PATH, key));
                while (totalBytesToRead > 0) {
                    // read the stream in 4MB chunk
                    int bytesToRead = (int) Math.min(BUFFER_CHUNK_SIZE, totalBytesToRead);
                    byte[] buf = new byte[bytesToRead];
                    int bytesRead = IOUtil.readRemaining(stream, buf, 0, bytesToRead);
                    totalBytesToRead -= bytesRead;

                    if (bytesRead < bytesToRead) {
                        // Read less than it should be, possibly hitting EOF.
                        // Set smaller ByteBuffer limit and break out of the loop.
                        outputStream.write(buf, 0, bytesRead);
                        break;
                    } else {
                        outputStream.write(buf);
                    }
                }
                stream.close();
                outputStream.close();
                if (!tmpOutputFile.getFileSystem().rename(tmpOutputFile.getPath(), localFilePath)) {
                    LOG.warn("failed on rename: {} to {}", tmpOutputFile.getPath().toString(),
                            localFilePath.toString());
                    tmpOutputFile.getFileSystem().delete(tmpOutputFile.getPath(), false);
                    // for mapping function :If the specified key is not already associated with a value,
                    // attempts to compute its value using the given mapping function
                    // and enters it into this cache unless null.
                    return null;
                } else {
                    return new DiskCacheEntry(fileLength - totalBytesToRead,
                            IOUtil.getInputFile(localFilePath));
                }
            } catch (IOException ex) {
                try {
                    IOUtil.closeInputStreamIgnoreException(stream);
                    IOUtil.closeOutputStreamIgnoreException(outputStream);
                    tmpOutputFile.getFileSystem().delete(tmpOutputFile.getPath(), false);
                } catch (IOException ioException) {
                    LOG.warn("failed on deleting file : {}. msg: {}", tmpOutputFile.getPath(), ioException);
                }
                throw new UncheckedIOException(ex);
            } catch (IllegalArgumentException e) {
                IOUtil.closeInputStreamIgnoreException(stream);
                IOUtil.closeOutputStreamIgnoreException(outputStream);
                throw e;
            }
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

        public boolean isExistOnDiskCache(String key) {
            return false;
        }

        public SeekableInputStream getDiskSeekableStream(String key) {
            return null;
        }

        public DiskCacheEntry getDiskCacheEntry(String key, Function<String, DiskCacheEntry> mappingFunction) {
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
                    .expireAfterAccess(DISK_CACHE_EXPIRATION_SECONDS, TimeUnit.SECONDS)
                    .weigher((Weigher<String, DiskCacheEntry>) (key, value) ->
                            value.useCount == 0 ? (int) Math.min(value.length, Integer.MAX_VALUE) : 0)
                    .recordStats()
                    // use sync evictionListener to avoid delete file newly generated by another thread
                    .evictionListener((key, value, cause) -> {
                        LOG.debug("{} to be eliminated from disk, reason: {}", key, cause);
                        IOUtil.deleteLocalFileWithRemotePath(key);
                    }).build();

            Caffeine<Object, Object> builder = Caffeine.newBuilder();
            this.memCache = builder.maximumWeight(MEMORY_CACHE_CAPACITY)
                    .expireAfterAccess(MEMORY_CACHE_EXPIRATION_SECONDS, TimeUnit.SECONDS)
                    .weigher((Weigher<String, CacheEntry>) (key, value) -> (int) Math.min(value.length, Integer.MAX_VALUE))
                    .recordStats()
                    .removalListener(((key, value, cause) -> {
                        LOG.debug(key + " to be eliminated to disk, reason: " + cause);
                        if (isExistOnDiskCache(key)) {
                            LOG.debug(key + " has cached on disk");
                            return;
                        }
                        this.getDiskCacheEntry(key, k -> DiskCacheEntry.newDiskCacheEntry(
                                ByteBufferInputStream.wrap(value.buffers), value.length, key));
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
                        InputFile localInputFile = IOUtil.getInputFile(localPath);
                        String key = IOUtil.localFileToRemote(localPath, METADATA_CACHE_DISK_PATH);
                        diskCache.put(key, new DiskCacheEntry(locatedFileStatus.getLen(), localInputFile));
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
            return buf != null || isExistOnDiskCache(key);
        }

        @Override
        public void invalidate(String key) {
            memCache.invalidate(key);
            diskCache.asMap().computeIfPresent(key, (k, v) -> {
                IOUtil.deleteLocalFileWithRemotePath(k);
                return null;
            });
        }

        @Override
        public boolean isExistOnDiskCache(String key) {
            DiskCacheEntry entry = diskCache.getIfPresent(key);
            if (entry != null && !entry.isExistOnDisk()) {
                diskCache.asMap().computeIfPresent(key, (k, v) -> {
                    IOUtil.deleteLocalFileWithRemotePath(k);
                    return null;
                });
                return false;
            }
            return entry != null;
        }

        @Override
        public SeekableInputStream getDiskSeekableStream(String key) {
            DiskCacheEntry diskCacheEntry = diskCache.asMap().computeIfPresent(key, (k, v) -> {
                v.pin();
                return v;
            });
            if (diskCacheEntry != null) {
                try {
                    SeekableInputStream stream = diskCacheEntry.toSeekableInputStream();
                    return new DiskCacheSeekableInputStream(stream, diskCache, key);
                } catch (Exception e) {
                    diskCache.asMap().computeIfPresent(key, (k, v) -> {
                        v.unpin();
                        return v;
                    });
                    return null;
                }
            } else {
                return null;
            }
        }

        @Override
        public DiskCacheEntry getDiskCacheEntry(String key, Function<String, DiskCacheEntry> mappingFunction) {
            return diskCache.get(key, mappingFunction);
        }
    }

    public static class DiskCacheSeekableInputStream extends SeekableInputStream {
        private final SeekableInputStream stream;
        private final Cache<String, DiskCacheEntry> diskCache;
        private final String key;

        DiskCacheSeekableInputStream(SeekableInputStream stream, Cache<String, DiskCacheEntry> diskCache, String key) {
            this.stream = stream;
            this.diskCache = diskCache;
            this.key = key;
        }
        @Override
        public void close() throws IOException {
            try {
                stream.close();
            } finally {
                diskCache.asMap().computeIfPresent(key, (k, v) -> {
                    v.unpin();
                    return v;
                });
            }
        }

        @Override
        public long getPos() throws IOException {
            return stream.getPos();
        }

        @Override
        public void seek(long newPos) throws IOException {
            stream.seek(newPos);
        }

        @Override
        public int read() throws IOException {
            return stream.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return stream.read(b, off, len);
        }

        public int read(ByteBuffer buf) throws IOException {
            return stream.read(buf.array());
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
            long len = contentCache.getLength(location());
            return len == -1 ? wrappedInputFile.getLength() : len;
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
                throw new UncheckedIOException(
                        String.format("Failed to open input stream for file: %s", wrappedInputFile.location()), e);
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
            SeekableInputStream stream = null;
            try {
                long fileLength = getLength();
                long totalBytesToRead = fileLength;
                if (contentCache.isExistOnDiskCache(location())) {
                    LOG.debug(location() + " hit on disk cache");
                    stream = contentCache.getDiskSeekableStream(location());
                    if (stream == null) {
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
                if (stream != null) {
                    IOUtil.closeInputStreamIgnoreException(stream);
                }
                throw new UncheckedIOException(ex);
            }
        }

        private SeekableInputStream cachedStream() throws IOException {
            try {
                CacheEntry entry = contentCache.get(location(), k -> newCacheEntry());
                Preconditions.checkNotNull(entry, "CacheEntry should not be null when there is no RuntimeException occurs");
                return ByteBufferInputStream.wrap(entry.buffers);
            } catch (UncheckedIOException ex) {
                throw ex.getCause();
            } catch (RuntimeException ex) {
                throw new IOException("Caught an error while reading through cache", ex);
            }
        }

        @Override
        public String toString() {
            return location();
        }
    }
}