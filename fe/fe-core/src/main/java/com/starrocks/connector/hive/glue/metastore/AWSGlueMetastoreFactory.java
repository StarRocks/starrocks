// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive.glue.metastore;

import com.amazonaws.services.glue.AWSGlue;
import com.starrocks.connector.hive.glue.util.AWSGlueConfig;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;

public class AWSGlueMetastoreFactory {

    public AWSGlueMetastore newMetastore(HiveConf conf) throws MetaException {
        AWSGlue glueClient = new AWSGlueClientFactory(conf).newClient();
        AWSGlueMetastore defaultMetastore = new DefaultAWSGlueMetastore(conf, glueClient);
        if (isCacheEnabled(conf)) {
            return new AWSGlueMetastoreCacheDecorator(conf, defaultMetastore);
        }
        return defaultMetastore;
    }

    private boolean isCacheEnabled(HiveConf conf) {
        boolean databaseCacheEnabled = conf.getBoolean(AWSGlueConfig.AWS_GLUE_DB_CACHE_ENABLE, false);
        boolean tableCacheEnabled = conf.getBoolean(AWSGlueConfig.AWS_GLUE_TABLE_CACHE_ENABLE, false);
        return (databaseCacheEnabled || tableCacheEnabled);
    }
}
