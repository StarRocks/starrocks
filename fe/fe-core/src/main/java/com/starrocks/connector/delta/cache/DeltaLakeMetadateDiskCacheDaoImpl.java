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
package com.starrocks.connector.delta.cache;

import com.starrocks.connector.delta.StarRocksDeltaLakeException;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.internal.util.DeltaJsonUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.CompressionOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Transaction;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DeltaLakeMetadateDiskCacheDaoImpl implements DeltaLakeMetadateDiskCacheDao {

    private static final Logger LOG = LogManager.getLogger(DeltaLakeMetadateDiskCacheDaoImpl.class);

    private OptimisticTransactionDB rocksDB;
    private final DeltaLakeTableName tbl;
    private final String pathPrefix;
    //64M, db_writer_buffer_number=2 ,total 128M
    private static final long DB_WRITER_BUFFER_SIZE = 64 * 1024 * 1024;

    private static final byte[] SCHEMA_KEY = "_SCHEMA_KEY".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SNAPSHOT_VERSION = "_SNAPSHOT_VERSION".getBytes(StandardCharsets.UTF_8);

    public DeltaLakeMetadateDiskCacheDaoImpl(DeltaLakeTableName tbl, String pathPrefix) {
        this.tbl = tbl;
        this.pathPrefix = pathPrefix;
    }

    private boolean existLocalCache() {
        String path = generatorCachePath();
        String dataPath = pathPrefix + "/" + path;
        File file = new File(dataPath);
        return file.exists();
    }

    @Override
    public DeltaTableMetadata getMetadata() {
        if (!existLocalCache()) {
            return null;
        }
        if (null == rocksDB) {
            openConnect();
        }
        try {
            byte[] schemaByte = rocksDB.get(SCHEMA_KEY);
            byte[] versionByte = rocksDB.get(SNAPSHOT_VERSION);
            String schema = new String(schemaByte, StandardCharsets.UTF_8);
            Long version = Long.parseLong(new String(versionByte, StandardCharsets.UTF_8));
            return new DeltaTableMetadata(schema, version);
        } catch (RocksDBException e) {
            LOG.error(e);
            throw new StarRocksDeltaLakeException(e.getMessage());
        }
    }

    private synchronized void openConnect() {
        if (null == rocksDB) {
            String path = generatorCachePath();
            String dataPath = pathPrefix + "/" + path;
            File file = new File(dataPath);
            if (!file.exists()) {
                file.mkdirs();
            }
            LOG.info("load deltaLake table:{} from path:{}", tbl, dataPath);
            Options options = new Options().setCreateIfMissing(true);
            options.setMaxBackgroundJobs(Runtime.getRuntime().availableProcessors() - 2);
            CompressionOptions compressionOptions = new CompressionOptions();
            compressionOptions.setEnabled(false);
            options.setLevel0FileNumCompactionTrigger(Integer.MAX_VALUE);
            options.setDbWriteBufferSize(DB_WRITER_BUFFER_SIZE);
            options.setCompressionOptions(compressionOptions);
            try {
                rocksDB = OptimisticTransactionDB.open(options, dataPath);
            } catch (RocksDBException e) {
                LOG.error("Failed to create Rocksdb connection dir:{}", dataPath, e);
                throw new StarRocksDeltaLakeException(e.getMessage());
            }
        }
    }

    @Override
    public List<AddFile> getAddFilesByPartitions(Set<String> partitions) {
        if (!existLocalCache()) {
            return null;
        }
        if (null == rocksDB) {
            openConnect();
        }

        List<AddFile> addFiles = new ArrayList<>();
        for (String deltaPartitionKey : partitions) {
            try {
                byte[] bytes = rocksDB.get(deltaPartitionKey.getBytes(StandardCharsets.UTF_8));
                String partitionAddFiles = new String(bytes, StandardCharsets.UTF_8);
                List<AddFile> tmpAddFiles;
                try {
                    tmpAddFiles = DeltaJsonUtil
                            .readerAddFilesFromJson(partitionAddFiles);
                    addFiles.addAll(tmpAddFiles);
                } catch (IOException e) {
                    LOG.error("addFiles parser error partitioned by partition values", e);
                    throw new StarRocksDeltaLakeException("addFiles parser error partitioned by partition values", e);
                }
            } catch (RocksDBException e) {
                LOG.error("get addfiles failed using partition {}", deltaPartitionKey, e);
                throw new StarRocksDeltaLakeException(e.getMessage());
            }
        }
        return addFiles;
    }

    @Override
    public Set<String> getAllPartition() {
        if (!existLocalCache()) {
            return null;
        }
        if (null == rocksDB) {
            openConnect();
        }
        ReadOptions readOptions = new ReadOptions();
        final RocksIterator iter = rocksDB.newIterator(readOptions);
        Set<String> partitions = new HashSet<>();
        byte[] key;
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            key = iter.key();
            if (Arrays.equals(key, SCHEMA_KEY) || Arrays.equals(key, SNAPSHOT_VERSION)) {
                continue;
            }
            partitions.add(new String(iter.key()));
        }
        iter.close();
        readOptions.close();

        return partitions;
    }

    @Override
    public synchronized void persistAllData(DeltaTableMetadata metadata,
                                            Map<String, Set<String>> partitionedAddFilesMap) throws RocksDBException {
        if (null == rocksDB) {
            openConnect();
        }
        Transaction txn = rocksDB.beginTransaction(new WriteOptions());
        boolean persistStatus = true;
        try {

            final Set<String> lastVersionAllPartition = getAllPartition();
            putMetadata(metadata);
            LOG.info("persist partition addfiles number：" + partitionedAddFilesMap.size());
            //persist all partitioned addFiles
            for (Map.Entry<String, Set<String>> entry : partitionedAddFilesMap.entrySet()) {
                lastVersionAllPartition.remove(entry.getKey());
                String addFileJsons = DeltaJsonUtil.toJson(entry.getValue());
                putPartitionValues(entry.getKey(), addFileJsons);
            }

            for (String deltaPartitionKey : lastVersionAllPartition) {
                rocksDB.delete(deltaPartitionKey.getBytes(StandardCharsets.UTF_8));
            }

        } catch (RocksDBException | IOException e) {
            LOG.error("persist table " + tbl + " delta metadata fail", e);
            persistStatus = false;
            throw new StarRocksDeltaLakeException("persist table " + tbl + " delta metadata fail", e);
        } finally {
            if (persistStatus) {
                txn.commit();
            } else {
                txn.rollback();
                LOG.error("cache delta table:{}'s metadata fail!", tbl);
            }
            rocksDB.flush(new FlushOptions());
            rocksDB.compactRange();
        }
    }

    private void putPartitionValues(String key, String addFileJsons) throws RocksDBException {
        rocksDB.put(key.getBytes(StandardCharsets.UTF_8), addFileJsons.getBytes(StandardCharsets.UTF_8));
    }

    private void putMetadata(DeltaTableMetadata metadata) throws RocksDBException {
        rocksDB.put(new WriteOptions(), SCHEMA_KEY, metadata.getMetadata().getBytes(StandardCharsets.UTF_8));
        byte[] version = metadata.getSnapshotVersion().toString().getBytes(StandardCharsets.UTF_8);
        rocksDB.put(new WriteOptions(), SNAPSHOT_VERSION, version);
    }

    @Override
    public void closeConnection() {
        if (null != rocksDB) {
            rocksDB.close();
        }

    }

    private String generatorCachePath() {
        return tbl.getCatalog() + "/" + tbl.getDbName() + "@" + tbl.getTblName();
    }
}
