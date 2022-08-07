package com.starrocks.external.hive;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;

import java.util.Map;

public class HiveMetaClientFactory {
    private final Map<String, String> params;
    private final HiveThriftStats stats = new HiveThriftStats();
    // retry / timeout etc var

    public HiveMetaClientFactory(Map<String, String> params, HiveThriftStats stats) {
        this.params = params;
    }

    public HiveMetaClient createClient() {
        // build conf by params or xml
        HiveConf conf = new HiveConf();
        return new HiveMetaClient(conf);
    }
}
