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


package com.starrocks.connector.iceberg.glue;

import com.starrocks.connector.hive.glue.AWSCatalogMetastoreClient;
import com.starrocks.connector.iceberg.hive.HiveClientPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.iceberg.ClientPoolImpl;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.hive.RuntimeMetaException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

public class GlueClientPool extends ClientPoolImpl<IMetaStoreClient, TException> {
    private static final DynMethods.StaticMethod GET_CLIENT = DynMethods.builder("getProxy")
            .impl(RetryingMetaStoreClient.class, Configuration.class, HiveMetaHookLoader.class, String.class)
            .buildStatic();

    private final HiveConf hiveConf;

    public GlueClientPool(int poolSize, Configuration conf) {
        super(poolSize, TTransportException.class, false);
        this.hiveConf = new HiveConf(conf, HiveClientPool.class);
        this.hiveConf.addResource(conf);
    }

    @Override
    protected IMetaStoreClient newClient() {
        try {
            try {
                return GET_CLIENT.invoke(hiveConf, (HiveMetaHookLoader) tbl -> null,
                        AWSCatalogMetastoreClient.class.getName());
            } catch (RuntimeException e) {
                // any MetaException would be wrapped into RuntimeException during reflection, so let's double-check type here
                if (e.getCause() instanceof MetaException) {
                    throw (MetaException) e.getCause();
                }
                throw e;
            }
        } catch (MetaException e) {
            throw new RuntimeMetaException(e, "Failed to connect to Hive Metastore");
        } catch (Throwable t) {
            if (t.getMessage().contains("Another instance of Derby may have already booted")) {
                throw new RuntimeMetaException(t, "Failed to start an embedded metastore because embedded " +
                        "Derby supports only one client at a time. To fix this, use a metastore that supports " +
                        "multiple clients.");
            }

            throw new RuntimeMetaException(t, "Failed to connect to Hive Metastore");
        }
    }

    @Override
    protected IMetaStoreClient reconnect(IMetaStoreClient client) {
        try {
            client.close();
            client.reconnect();
        } catch (MetaException e) {
            throw new RuntimeMetaException(e, "Failed to reconnect to Hive Metastore");
        }
        return client;
    }

    @Override
    protected boolean isConnectionException(Exception e) {
        return super.isConnectionException(e) || (e instanceof MetaException &&
                e.getMessage().contains("Got exception: org.apache.thrift.transport.TTransportException"));
    }

    @Override
    protected void close(IMetaStoreClient client) {
        client.close();
    }
}
