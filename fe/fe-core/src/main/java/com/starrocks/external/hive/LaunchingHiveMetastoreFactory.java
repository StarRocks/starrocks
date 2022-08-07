//package com.starrocks.external.hive;
//
//public class LaunchingHiveMetastoreFactory implements IHiveMetastoreFactory {
//    private final HiveMetaClient client;
//
//    public LaunchingHiveMetastoreFactory(HiveMetaClient client) {
//        this.client = client;
//    }
//
//    public IHiveMetastore createMetastore() {
//        return new HiveMetastore(client);
//    }
//
//}
