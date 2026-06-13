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

package com.starrocks.connector.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedAction;

public class HadoopExt {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(HadoopExt.class);
    private static final HadoopExt INSTANCE = new HadoopExt();
    public static final String LOGGER_MESSAGE_PREFIX = "[hadoop-ext]";

    public static final String HADOOP_CONFIG_RESOURCES = "hadoop.config.resources";
    public static final String HADOOP_RUNTIME_JARS = "hadoop.runtime.jars";
    public static final String HADOOP_CLOUD_CONFIGURATION_STRING = "hadoop.cloud.configuration.string";
    public static final String HADOOP_USERNAME = "hadoop.username";

    // Catalog property keys for per-catalog Kerberos credential override.
    // IcebergConnector.buildIcebergNativeCatalog / HiveMetaClient.createHiveMetaClient
    // inject these keys into the Hadoop Configuration via properties.forEach(conf::set).
    public static final String HADOOP_KERBEROS_PRINCIPAL = "hadoop.kerberos.principal";
    public static final String HADOOP_KERBEROS_KEYTAB = "hadoop.kerberos.keytab";

    public static HadoopExt getInstance() {
        return INSTANCE;
    }

    public void rewriteConfiguration(Configuration conf) {
    }

    public FileSystem bindUGIToFileSystem(FileSystem fs, UserGroupInformation ugi) {
        return fs;
    }

    public String getCloudConfString(Configuration conf) {
        return conf.get(HADOOP_CLOUD_CONFIGURATION_STRING, "");
    }

    public UserGroupInformation getHMSUGI(Configuration conf) {
        return loginFromCatalogConf(conf);
    }

    public UserGroupInformation getHDFSUGI(Configuration conf) {
        return loginFromCatalogConf(conf);
    }

    /**
     * The patched HMS Client.open / FileSystem.createFileSystemInternal call
     * HadoopExt.doAs(getHMSUGI/getHDFSUGI(conf), action) to explicitly push the catalog UGI
     * down into the SASL handshake. Returning null here would make doAs run the action with
     * the current process loginUser, which is not enough for full per-catalog isolation on
     * its own, hence the real implementation here.
     */
    private UserGroupInformation loginFromCatalogConf(Configuration conf) {
        String principal = conf.get(HADOOP_KERBEROS_PRINCIPAL);
        String keytab = conf.get(HADOOP_KERBEROS_KEYTAB);
        if (principal == null || principal.isEmpty() || keytab == null || keytab.isEmpty()) {
            return null;
        }
        try {
            return UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
        } catch (IOException e) {
            LOGGER.warn("{} loginUserFromKeytabAndReturnUGI failed for {}: {}",
                    LOGGER_MESSAGE_PREFIX, principal, e.getMessage());
            return null;
        }
    }

    public <R, E extends Exception> R doAs(UserGroupInformation ugi, GenericExceptionAction<R, E> action) throws E {
        if (ugi == null) {
            return action.run();
        }
        return executeActionInDoAs(ugi, action);
    }

    /**
     * Process-wide UGI swap pattern for per-catalog keytab isolation.
     *
     * <p>{@link #doAs(UserGroupInformation, GenericExceptionAction)} only changes the
     * thread-local Subject, which does not propagate into Hadoop's HMS thrift client / SASL
     * handshake (those reach process-wide credential state). In a multi-catalog setup the
     * first call's UGI gets entrenched there and subsequent calls from other catalogs reuse
     * it.
     *
     * <p>Under a single process-wide lock this method atomically does:
     * <ol>
     *   <li>{@link UserGroupInformation#loginUserFromKeytab} to swap the process loginUser
     *       to the catalog keytab's UGI</li>
     *   <li>runs {@code action} inside that UGI's doAs so the HMS thrift / HDFS RPC SASL
     *       handshake uses the catalog UGI's credential</li>
     *   <li>in finally, restores the system default loginUser from KRB5PRINCIPAL/KRB5KEYTAB
     *       env (skipped if env is unset; the next swap will simply set it again)</li>
     * </ol>
     *
     * <p>All catalog calls serialize on the single lock, but the critical section covers
     * only metadata fetch — data scan / result handling happens outside the lock. This is
     * the trade-off for per-catalog keytab isolation without splitting processes.
     *
     * <p>If principal/keytab are empty, the action runs directly (falls back to the system
     * default UGI, identical to the original code path).
     */
    public <R, E extends Exception> R doAsWithSwap(String principal, String keytab,
                                                    GenericExceptionAction<R, E> action) throws E {
        if (principal == null || principal.isEmpty() || keytab == null || keytab.isEmpty()) {
            return action.run();
        }
        synchronized (LOGIN_USER_LOCK) {
            String defPrincipal = System.getenv("KRB5PRINCIPAL");
            String defKeytab = System.getenv("KRB5KEYTAB");
            try {
                UserGroupInformation.loginUserFromKeytab(principal, keytab);
                UserGroupInformation ugi = UserGroupInformation.getLoginUser();
                LOGGER.debug("{} doAsWithSwap: loginUser swapped to {}", LOGGER_MESSAGE_PREFIX, ugi);
                return executeActionInDoAs(ugi, action);
            } catch (IOException e) {
                throw new RuntimeException("loginUserFromKeytab failed for " + principal, e);
            } finally {
                if (defPrincipal != null && !defPrincipal.isEmpty()
                        && defKeytab != null && !defKeytab.isEmpty()) {
                    try {
                        UserGroupInformation.loginUserFromKeytab(defPrincipal, defKeytab);
                    } catch (IOException restore) {
                        LOGGER.warn("{} Failed to restore default UGI: {}",
                                LOGGER_MESSAGE_PREFIX, restore.getMessage());
                    }
                }
            }
        }
    }

    // Single lock that serializes process-wide loginUser swaps.
    private static final Object LOGIN_USER_LOCK = new Object();

    static <R, E extends Exception> R executeActionInDoAs(UserGroupInformation userGroupInformation,
                                                          GenericExceptionAction<R, E> action) throws E {
        return userGroupInformation.doAs((PrivilegedAction<ResultOrException<R, E>>) () -> {
            try {
                return new ResultOrException<>(action.run(), null);
            } catch (Throwable e) {
                return new ResultOrException<>(null, e);
            }
        }).get();
    }

    private static class ResultOrException<T, E extends Exception> {
        private final T result;
        private final Throwable exception;

        public ResultOrException(T result, Throwable exception) {
            this.result = result;
            this.exception = exception;
        }

        @SuppressWarnings("unchecked")
        public T get()
                throws E {
            if (exception != null) {
                if (exception instanceof Error) {
                    throw (Error) exception;
                }
                if (exception instanceof RuntimeException) {
                    throw (RuntimeException) exception;
                }
                throw (E) exception;
            }
            return result;
        }
    }
}
