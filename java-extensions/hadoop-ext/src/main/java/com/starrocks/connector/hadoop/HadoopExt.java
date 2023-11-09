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
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivilegedAction;

public class HadoopExt {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(HadoopExt.class);
    private static final HadoopExt INSTANCE = new HadoopExt();
    public static final String HADOOP_CONFIG_RESOURCES = "hadoop.config.resources";
    public static final String HADOOP_RUNTIME_JARS = "hadoop.runtime.jars";
    public static final String HADOOP_CLOUD_CONFIGURATION_STRING = "hadoop.cloud.configuration.string";
    public static final String HADOOP_USERNAME = "hadoop.username";

    public static HadoopExt getInstance() {
        return INSTANCE;
    }

    public void rewriteConfiguration(Configuration conf) {
    }

    public String getCloudConfString(Configuration conf) {
        return conf.get(HADOOP_CLOUD_CONFIGURATION_STRING, "");
    }

    public UserGroupInformation getHMSUGI(Configuration conf) {
        return null;
    }

    public UserGroupInformation getHDFSUGI(Configuration conf) {
        return null;
    }

    public <R, E extends Exception> R doAs(UserGroupInformation ugi, GenericExceptionAction<R, E> action) throws E {
        if (ugi == null) {
            return action.run();
        }
        return executeActionInDoAs(ugi, action);
    }

    private static <R, E extends Exception> R executeActionInDoAs(UserGroupInformation userGroupInformation,
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