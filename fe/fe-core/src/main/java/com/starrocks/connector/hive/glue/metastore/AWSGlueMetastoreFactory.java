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


package com.starrocks.connector.hive.glue.metastore;

<<<<<<< HEAD
import com.amazonaws.services.glue.AWSGlue;
import com.starrocks.connector.hive.glue.util.AWSGlueConfig;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
=======
import com.starrocks.connector.hive.glue.util.AWSGlueConfig;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import software.amazon.awssdk.services.glue.GlueClient;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

public class AWSGlueMetastoreFactory {

    public AWSGlueMetastore newMetastore(HiveConf conf) throws MetaException {
<<<<<<< HEAD
        AWSGlue glueClient = new AWSGlueClientFactory(conf).newClient();
=======
        GlueClient glueClient = new AWSGlueClientFactory(conf).newClient();
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
