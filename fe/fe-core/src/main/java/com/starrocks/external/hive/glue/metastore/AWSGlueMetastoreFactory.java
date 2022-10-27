// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive.glue.metastore;

import com.amazonaws.services.glue.AWSGlue;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;

import static com.starrocks.external.hive.glue.util.AWSGlueConfig.AWS_GLUE_DB_CACHE_ENABLE;
import static com.starrocks.external.hive.glue.util.AWSGlueConfig.AWS_GLUE_TABLE_CACHE_ENABLE;

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
        boolean databaseCacheEnabled = conf.getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false);
        boolean tableCacheEnabled = conf.getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false);
        return (databaseCacheEnabled || tableCacheEnabled);
    }
}
