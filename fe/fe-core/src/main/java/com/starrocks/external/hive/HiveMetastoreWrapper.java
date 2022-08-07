//package com.starrocks.external.hive;
//
//import java.util.Map;
//
//public class HiveMetastoreWrapper {
//    private final boolean enabled;
//    private final String catalogName;
//    private final long xxx;
//    private final long xxxtime;
//    private final CachingHiveMetastore metastore;
//
//    private final boolean partitionCacheEnabled;
//
//    // TODO pass config?
//    public HiveMetastoreWrapper(Map<String, String> conf) {
//        enabled = conf.get("x");
//
//
//        // init
//    }
//
//    public IHiveMetastoreFactory createCachedHiveMetastoreFactory(IHiveMetastoreFactory metastoreFactory)
//    {
//        if (!enabled) {
//            return metastoreFactory;
//        }
//
//        CachingHiveMetastore cachedHiveMetastore = CachingHiveMetastore.cachingHiveMetastore(metastoreFactory.createMetastore());
//        return new CachedHiveMetastoreFactory(cachedHiveMetastore);
//    }
//
//
//    public class CachedHiveMetastoreFactory implements IHiveMetastoreFactory {
//        private final CachingHiveMetastore metastore;
//
//        public CachedHiveMetastoreFactory(CachingHiveMetastore metastore) {
//        }
//
//        public HiveMetastore createMetastore()
//        {
//            return metastore;
//        }
//    }
//}
