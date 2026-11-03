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
import java.io.UncheckedIOException;
import java.security.PrivilegedAction;
import java.util.concurrent.ConcurrentHashMap;

public class HadoopExt {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(HadoopExt.class);
    private static final HadoopExt DEFAULT_INSTANCE = new HadoopExt();
    private static volatile HadoopExt instance = DEFAULT_INSTANCE;
    public static final String LOGGER_MESSAGE_PREFIX = "[hadoop-ext]";

    public static final String HADOOP_CONFIG_RESOURCES = "hadoop.config.resources";
    public static final String HADOOP_RUNTIME_JARS = "hadoop.runtime.jars";
    public static final String HADOOP_CLOUD_CONFIGURATION_STRING = "hadoop.cloud.configuration.string";
    public static final String HADOOP_USERNAME = "hadoop.username";

    // Catalog-level Kerberos credentials. Only the FE sets these keys, after it has decided that the catalog's
    // principal and keytab should be used, so the user-facing catalog properties alone never switch the identity.
    public static final String CATALOG_KERBEROS_PRINCIPAL = "starrocks.catalog.kerberos.principal";
    public static final String CATALOG_KERBEROS_KEYTAB = "starrocks.catalog.kerberos.keytab";

    private final ConcurrentHashMap<String, UserGroupInformation> catalogKerberosUgis = new ConcurrentHashMap<>();

    public static HadoopExt getInstance() {
        return instance;
    }

    /**
     * Replaces the instance returned by {@link #getInstance()}. For tests only; {@code null} restores the default.
     */
    public static void setInstance(HadoopExt ext) {
        instance = ext == null ? DEFAULT_INSTANCE : ext;
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
        return getCatalogKerberosUGI(conf);
    }

    public UserGroupInformation getHDFSUGI(Configuration conf) {
        return getCatalogKerberosUGI(conf);
    }

    /**
     * Returns the UGI logged in from the catalog's keytab, or {@code null} when the configuration carries no
     * catalog-level Kerberos credentials, in which case callers keep using the process login user.
     * One UGI is kept per (principal, keytab) and re-logged in only when its ticket is about to expire.
     * The process login user is never changed.
     */
    protected UserGroupInformation getCatalogKerberosUGI(Configuration conf) {
        if (conf == null) {
            return null;
        }
        String principal = conf.getTrimmed(CATALOG_KERBEROS_PRINCIPAL, "");
        String keytab = conf.getTrimmed(CATALOG_KERBEROS_KEYTAB, "");
        if (principal.isEmpty() || keytab.isEmpty()) {
            return null;
        }
        if (!isSecurityEnabled()) {
            // UserGroupInformation would skip the keytab login and hand back the process user instead.
            throw new IllegalStateException("Catalog-level Kerberos credentials for " + principal
                    + " require hadoop.security.authentication=kerberos in the Hadoop configuration"
                    + " (core-site.xml) of the FE and BE processes");
        }
        UserGroupInformation ugi = catalogKerberosUgis.computeIfAbsent(principal + '\0' + keytab, k -> {
            try {
                UserGroupInformation loggedIn = loginFromKeytab(principal, keytab);
                LOGGER.info("{} logged in from keytab for catalog principal {}", LOGGER_MESSAGE_PREFIX, principal);
                return loggedIn;
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to log in from keytab for catalog principal " + principal, e);
            }
        });
        try {
            reloginIfNeeded(ugi);
        } catch (IOException e) {
            LOGGER.warn("{} relogin from keytab failed for catalog principal {}", LOGGER_MESSAGE_PREFIX, principal, e);
        }
        return ugi;
    }

    protected boolean isSecurityEnabled() {
        return UserGroupInformation.isSecurityEnabled();
    }

    protected UserGroupInformation loginFromKeytab(String principal, String keytab) throws IOException {
        return UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
    }

    protected void reloginIfNeeded(UserGroupInformation ugi) throws IOException {
        ugi.checkTGTAndReloginFromKeytab();
    }

    public <R, E extends Exception> R doAs(UserGroupInformation ugi, GenericExceptionAction<R, E> action) throws E {
        if (ugi == null) {
            return action.run();
        }
        return executeActionInDoAs(ugi, action);
    }

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