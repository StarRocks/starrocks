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

package com.starrocks.connector.hive;

import com.starrocks.connector.hive.glue.AWSCatalogMetastoreClient;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedList;

import static com.starrocks.connector.hive.HiveConnector.HIVE_METASTORE_TYPE;

public class RecyclableClient {
    private static final Logger LOG = LogManager.getLogger(RecyclableClient.class);
    private static final LinkedList<RecyclableClient> CLIENT_POOL = new LinkedList<>();
    private static final Object CLIENT_POOL_LOCK = new Object();

    private final IMetaStoreClient hiveClient;

    // Maximum number of idle metastore connections in the connection pool at any point.
    static final int MAX_HMS_CONNECTION_POOL_SIZE = 32;
    private static final HiveMetaHookLoader DUMMY_HOOK_LOADER = tbl -> null;
    static final String DLF_HIVE_METASTORE = "dlf";
    static final String GLUE_HIVE_METASTORE = "glue";
    static final String HADOOP_HIVE_METASTORE = "hadoop";

    private RecyclableClient(HiveConf conf) throws MetaException {
        if (DLF_HIVE_METASTORE.equalsIgnoreCase(conf.get(HIVE_METASTORE_TYPE))) {
            hiveClient = RetryingMetaStoreClient.getProxy(conf, DUMMY_HOOK_LOADER,
                    DLFProxyMetaStoreClient.class.getName());
        } else if (GLUE_HIVE_METASTORE.equalsIgnoreCase(conf.get(HIVE_METASTORE_TYPE))) {
            hiveClient = RetryingMetaStoreClient.getProxy(conf, DUMMY_HOOK_LOADER,
                    AWSCatalogMetastoreClient.class.getName());
        } else {
            hiveClient = RetryingMetaStoreClient.getProxy(conf, DUMMY_HOOK_LOADER,
                    HiveMetaStoreClient.class.getName());
        }
    }

    // When the number of currently used clients is less than MAX_HMS_CONNECTION_POOL_SIZE,
    // the client will be recycled and reused. If it does, we close the client.
    public void finish() {
        synchronized (CLIENT_POOL_LOCK) {
            if (CLIENT_POOL.size() >= MAX_HMS_CONNECTION_POOL_SIZE) {
                LOG.warn("There are more than {} connections currently accessing the metastore",
                        MAX_HMS_CONNECTION_POOL_SIZE);
                close();
            } else {
                CLIENT_POOL.offer(this);
            }
        }
    }

    public void close() {
        hiveClient.close();
    }

    public IMetaStoreClient getHiveClient() {
        return hiveClient;
    }

    public static RecyclableClient getInstance(HiveConf conf) throws MetaException {
        // The MetaStoreClient c'tor relies on knowing the Hadoop version by asking
        // org.apache.hadoop.util.VersionInfo. The VersionInfo class relies on opening
        // the 'common-version-info.properties' file as a resource from hadoop-common*.jar
        // using the Thread's context classloader. If necessary, set the Thread's context
        // classloader, otherwise VersionInfo will fail in it's c'tor.
        if (Thread.currentThread().getContextClassLoader() == null) {
            Thread.currentThread().setContextClassLoader(ClassLoader.getSystemClassLoader());
        }

        synchronized (CLIENT_POOL_LOCK) {
            RecyclableClient client = CLIENT_POOL.poll();
            // The pool was empty so create a new client and return that.
            // Serialize client creation to defend against possible race conditions accessing
            // local Kerberos state
            if (client == null) {
                return new RecyclableClient(conf);
            } else {
                return client;
            }
        }
    }

    public static int size() {
        synchronized (CLIENT_POOL_LOCK) {
            return CLIENT_POOL.size();
        }
    }

    public static void clear() {
        synchronized (CLIENT_POOL_LOCK) {
            for (; ; ) {
                RecyclableClient client = CLIENT_POOL.poll();
                if (client == null) {
                    break;
                }
                client.close();
            }
        }
    }
}