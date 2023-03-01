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
import org.apache.commons.io.FileUtils;
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
import org.apache.iceberg.util.PropertyUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
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
    protected static final int BUFFER_CHUNK_SIZE = 4 * 1024 * 1024; // 4MB
    public static final String FILEIO_CACHE_MAX_TOTAL_BYTES = "fileIO.cache.max-total-bytes";
    public static final String METADATA_CACHE_DISK_PATH = Config.iceberg_metadata_cache_disk_path;

    private ContentCache fileContentCache;
    private final FileIO wrappedIO;

    public IcebergCachingFileIO(FileIO io) {
        this.wrappedIO = io;
    }

    @Override
    public void initialize(Map<String, String> properties) {
        long maxTotalBytes = PropertyUtil.propertyAsLong(properties, FILEIO_CACHE_MAX_TOTAL_BYTES,
                Config.iceberg_metadata_cache_capacity);
        this.fileContentCache = new ContentCache(Config.iceberg_metadata_cache_max_entry_size, maxTotalBytes);
        loadMetadataDiskCache();
    }

    public void loadMetadataDiskCache() {
        //loadMetadataDiskCache asynchronous
        ExecutorService executor = Executors.newSingleThreadExecutor();
        FileSystem fs = Util.getFs(IOUtil.getLocalDiskDirPath(METADATA_CACHE_DISK_PATH), new Configuration());
        executor.submit(() -> {
            try {
                RemoteIterator<LocatedFileStatus> it = fs.listFiles(IOUtil.getLocalDiskDirPath(METADATA_CACHE_DISK_PATH), true);
                while (it.hasNext()) {
                    LocatedFileStatus locatedFileStatus = it.next();
                    if (locatedFileStatus.isDirectory()) {
                        continue;
                    }
                    Path localPath = locatedFileStatus.getPath();
                    OutputFile localOutputFile = IOUtil.getOutputFile(localPath);
                    String s3aKey = IOUtil.localFileToS3a(localPath, METADATA_CACHE_DISK_PATH).toString();
                    LOG.info("load iceberg disk metadata to cache: {} from {}", s3aKey, localPath.toString());
                    fileContentCache.put(s3aKey,
                            new CacheEntry(locatedFileStatus.getLen(), localOutputFile.toInputFile()));
                }
            } catch (Exception e) {
                // Ignore
            }
        });
        executor.shutdown();
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

    protected static class CacheEntry {
        private final long length;
        private final InputFile inputFile;

        protected CacheEntry(long length, InputFile inputFile) {
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

    public static class ContentCache {
        private final long maxContentLength;
        private final Cache<String, CacheEntry> cache;

        private ContentCache(long maxContentLength, long maxTotalBytes) {
            this.maxContentLength = maxContentLength;

            Caffeine<Object, Object> builder = Caffeine.newBuilder();
            this.cache = builder.maximumWeight(maxTotalBytes)
                    .expireAfterAccess(Config.iceberg_metadata_cache_expiration_seconds, TimeUnit.SECONDS)
                    .weigher((Weigher<String, CacheEntry>) (key, value) -> (int) Math.min(value.length, Integer.MAX_VALUE))
                    .recordStats()
                    .removalListener(((key, value, cause) -> {
                        // COLLECTED: The entry was removed automatically because its key or value was garbage-collected.
                        // EXPIRED: The entry's expiration timestamp has passed.
                        // EXPLICIT: The entry was manually removed by the user.
                        // REPLACED: The entry itself was not actually removed, but its value was replaced by the user.
                        // SIZE: The entry was evicted due to size constraints.
                        LOG.info(key + " to be eliminated, reason: " + cause);
                        // delete local file
                        HadoopOutputFile hadoopOutputFile = (HadoopOutputFile) IOUtil.getOutputFile(
                                METADATA_CACHE_DISK_PATH,
                                key);
                        try {
                            hadoopOutputFile.getFileSystem().delete(hadoopOutputFile.getPath());
                        } catch (Exception e) {
                            LOG.warn("evict iceberg local disk file: {} failed: {}", key, e.getMessage());
                        }
                    }))
                    .build();
        }

        public long maxContentLength() {
            return maxContentLength;
        }

        public CacheEntry get(String key, Function<String, CacheEntry> mappingFunction) {
            return cache.get(key, mappingFunction);
        }

        public void put(String key, CacheEntry cacheEntry) {
            cache.put(key, cacheEntry);
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
                LOG.info("iceberg matadata file {} size {} large than max content length, skip cache",
                        location(), getLength());
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
            LOG.debug("iceberg metadata cache hit rate: {}", contentCache.cache.stats().hitRate());
            long fileLength = getLength();
            long totalBytesToRead = fileLength;
            OutputFile tmpOutputFile = IOUtil.getTmpOutputFile(METADATA_CACHE_DISK_PATH, location());
            HadoopOutputFile hadoopOutputFile = (HadoopOutputFile) tmpOutputFile;
            try {
                OutputFile localOutputFile = IOUtil.getOutputFile(METADATA_CACHE_DISK_PATH, location());
                if (localOutputFile.toInputFile().exists()) {
                    return new CacheEntry(fileLength, localOutputFile.toInputFile());
                }
                SeekableInputStream stream = wrappedInputFile.newStream();
                PositionOutputStream positionOutputStream =  tmpOutputFile.createOrOverwrite();

                while (totalBytesToRead > 0) {
                    // read the stream in 4MB chunk
                    int bytesToRead = (int) Math.min(BUFFER_CHUNK_SIZE, totalBytesToRead);
                    byte[] buf = new byte[bytesToRead];
                    int bytesRead = IOUtil.readRemaining(stream, buf, 0, bytesToRead);
                    totalBytesToRead -= bytesRead;

                    if (bytesRead < bytesToRead) {
                        // Read less than it should be, possibly hitting EOF.
                        // Set smaller ByteBuffer limit and break out of the loop.
                        positionOutputStream.write(buf, 0, bytesRead);
                        break;
                    } else {
                        positionOutputStream.write(buf);
                    }
                }
                stream.close();
                positionOutputStream.close();
                hadoopOutputFile.getFileSystem().rename(
                        hadoopOutputFile.getPath(),
                        IOUtil.s3aToLocalFilePath(METADATA_CACHE_DISK_PATH, location()));
                LOG.debug("load iceberg metadata {}:{} to cache", location(), FileUtils.byteCountToDisplaySize(fileLength));
                return new CacheEntry(fileLength - totalBytesToRead, localOutputFile.toInputFile());
            } catch (IOException ex) {
                // remove local tmp file
                try {
                    hadoopOutputFile.getFileSystem().deleteOnExit(hadoopOutputFile.getPath());
                } catch (Exception e) {
                    // Ignore
                }
                throw new RuntimeIOException(ex);
            }
        }

        private SeekableInputStream cachedStream() throws IOException {
            try {
                CacheEntry entry = contentCache.get(location(), k -> newCacheEntry());
                Preconditions.checkNotNull(entry, "CacheEntry should not be null when there is no RuntimeException occurs");
                // someone have deleted local files, but still in cache
                if (!entry.isExistOnDisk()) {
                    LOG.info("local file {} has been deleted, but still in cache, reload it", location());
                    contentCache.invalidate(location());
                    entry = contentCache.get(location(), k -> newCacheEntry());
                }
                Preconditions.checkNotNull(entry, "CacheEntry should not be null when there is no RuntimeException occurs");
                return entry.toSeekableInputStream();
            } catch (RuntimeIOException ex) {
                throw ex.getCause();
            } catch (RuntimeException ex) {
                throw new IOException("Caught an error while reading through cache", ex);
            }
        }
    }
}
