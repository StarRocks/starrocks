package com.starrocks.external.hive;

import com.starrocks.connector.hive.HiveMetadata;
import com.starrocks.external.RemoteFileIO;
import com.starrocks.external.RemoteFileOperations;

import static com.starrocks.external.CachingRemoteFileIO.memoizeRemoteIO;
import static com.starrocks.external.hive.CachingHiveMetastore.memoizeMetastore;

public class HiveMetadataFactory {
    private final IHiveMetastore metastore;
    private final RemoteFileIO remoteFileIO;
    private final int perQueryCacheMaxNum;

    public HiveMetadataFactory(IHiveMetastore metastore,
                               RemoteFileIO remoteFileIO,
                               int perQueryCacheMaxNum) {
        this.metastore = metastore;
        this.remoteFileIO = remoteFileIO;
        this.perQueryCacheMaxNum = perQueryCacheMaxNum;
    }

    public HiveMetadata create() {
        HiveMetastoreOperations hmsOps = new HiveMetastoreOperations(memoizeMetastore(metastore, perQueryCacheMaxNum));
        RemoteFileOperations remoteFileOperations = new RemoteFileOperations(memoizeRemoteIO(remoteFileIO, perQueryCacheMaxNum));

        HiveStatisticsProvider statisticsProvider = new HiveStatisticsProvider(hmsOps, remoteFileOperations);
        return new HiveMetadata(hmsOps, remoteFileOperations, statisticsProvider);
    }

}
