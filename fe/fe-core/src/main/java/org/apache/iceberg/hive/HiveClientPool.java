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
//   https://github.com/apache/iceberg/blob/main/hive-metastore/src/main/java/org/apache/iceberg/hive/HiveClientPool.java

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

package org.apache.iceberg.hive;

import com.starrocks.connector.hive.DLFProxyMetaStoreClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.iceberg.ClientPoolImpl;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import java.util.HashMap;
import java.util.Map;

import static com.starrocks.connector.iceberg.IcebergCatalogProperties.ICEBERG_CATALOG_TYPE;

public class HiveClientPool extends ClientPoolImpl<IMetaStoreClient, TException> {

    private static final DynMethods.StaticMethod GET_CLIENT =
            DynMethods.builder("getProxy")
                    .impl(
                            RetryingMetaStoreClient.class,
                            HiveConf.class,
                            HiveMetaHookLoader.class,
                            String.class) // Hive 1 and 2
                    .impl(
                            RetryingMetaStoreClient.class,
                            Configuration.class,
                            HiveMetaHookLoader.class,
                            String.class) // Hive 3
                    .buildStatic();

    private final HiveConf hiveConf;
    private final Map<String, String> properties;

    public HiveClientPool(int poolSize, Configuration conf) {
        this(poolSize, conf, new HashMap<String, String>());
    }

    public HiveClientPool(int poolSize, Configuration conf, Map<String, String> properties) {
        // Do not allow retry by default as we rely on RetryingHiveClient
        super(poolSize, TTransportException.class, false);
        this.hiveConf = new HiveConf(conf, HiveClientPool.class);
        this.hiveConf.addResource(conf);
        this.properties = properties;
    }

    @Override
    protected IMetaStoreClient newClient() {
        try {
            try {
                if ("dlf".equalsIgnoreCase(properties.get(ICEBERG_CATALOG_TYPE))) {
                    return GET_CLIENT.invoke(
                            hiveConf, (HiveMetaHookLoader) tbl -> null, DLFProxyMetaStoreClient.class.getName());
                } else {
                    return GET_CLIENT.invoke(
                            hiveConf, (HiveMetaHookLoader) tbl -> null, HiveMetaStoreClient.class.getName());
                }
            } catch (RuntimeException e) {
                // any MetaException would be wrapped into RuntimeException during reflection, so let's
                // double-check type here
                if (e.getCause() instanceof MetaException) {
                    throw (MetaException) e.getCause();
                }
                throw e;
            }
        } catch (MetaException e) {
            throw new RuntimeMetaException(e, "Failed to connect to Hive Metastore");
        } catch (Throwable t) {
            if (t.getMessage().contains("Another instance of Derby may have already booted")) {
                throw new RuntimeMetaException(
                        t,
                        "Failed to start an embedded metastore because embedded "
                                + "Derby supports only one client at a time. To fix this, use a metastore that supports "
                                + "multiple clients.");
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
        return super.isConnectionException(e)
                || (e != null
                && e instanceof MetaException
                && e.getMessage()
                .contains("Got exception: org.apache.thrift.transport.TTransportException"));
    }

    @Override
    protected void close(IMetaStoreClient client) {
        client.close();
    }

    @VisibleForTesting
    HiveConf hiveConf() {
        return hiveConf;
    }
}
