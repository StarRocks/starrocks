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

package com.starrocks.format;


import org.apache.commons.collections4.MapUtils;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Config {

    @Inherited
    @Target({ElementType.FIELD, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Option {

        String value();

    }

    @Option("starrocks.fe.http.url")
    private final String feHttpUrl;

    @Option("starrocks.fe.jdbc.url")
    private final String feJdbcUrl;

    @Option("starrocks.database")
    private final String database;

    @Option("starrocks.user")
    private final String username;

    @Option("starrocks.password")
    private final String password;

    @Option("starrocks.format.query_plan")
    private final String queryPlan;

    @Option("starrocks.format.using_column_uid")
    private final Boolean usingColumnUid;

    @Option("starrocks.format.unreleased.warning.threshold")
    private final Integer unreleasedWarningThreshold;

    /* S3 Options */

    @Option("fs.s3a.endpoint")
    private final String s3Endpoint;

    @Option("fs.s3a.endpoint.region")
    private final String s3EndpointRegion;

    @Option("fs.s3a.connection.ssl.enabled")
    private final Boolean s3ConnectionSslEnabled;

    @Option("fs.s3a.path.style.access")
    private final Boolean s3PathStyleAccess;

    @Option("fs.s3a.access.key")
    private final String s3AccessKey;

    @Option("fs.s3a.secret.key")
    private final String s3SecretKey;

    private final Map<String, Object> customConfigs;

    private Config(Builder builder) {
        this.feHttpUrl = builder.feHttpUrl;
        this.feJdbcUrl = builder.feJdbcUrl;
        this.database = builder.database;
        this.username = builder.username;
        this.password = builder.password;
        this.queryPlan = builder.queryPlan;
        this.usingColumnUid = builder.usingColumnUid;
        this.unreleasedWarningThreshold = builder.unreleasedWarningThreshold;
        this.s3Endpoint = builder.s3Endpoint;
        this.s3EndpointRegion = builder.s3EndpointRegion;
        this.s3ConnectionSslEnabled = builder.s3ConnectionSslEnabled;
        this.s3PathStyleAccess = builder.s3PathStyleAccess;
        this.s3AccessKey = builder.s3AccessKey;
        this.s3SecretKey = builder.s3SecretKey;
        this.customConfigs = new HashMap<>(builder.customConfigs);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public Builder toBuilder() {
        return new Builder()
                .feHttpUrl(feHttpUrl)
                .feJdbcUrl(feJdbcUrl)
                .database(database)
                .username(username)
                .password(password)
                .queryPlan(queryPlan)
                .usingColumnUid(usingColumnUid)
                .unreleasedWarningThreshold(unreleasedWarningThreshold)
                .s3Endpoint(s3Endpoint)
                .s3EndpointRegion(s3EndpointRegion)
                .s3ConnectionSslEnabled(s3ConnectionSslEnabled)
                .s3PathStyleAccess(s3PathStyleAccess)
                .s3AccessKey(s3AccessKey)
                .s3SecretKey(s3SecretKey);
    }

    /**
     * Config Builder.
     */
    public static class Builder {

        private String feHttpUrl;

        private String feJdbcUrl;

        private String database;

        private String username;

        private String password;

        private String queryPlan;

        private Boolean usingColumnUid;

        private Integer unreleasedWarningThreshold = 128;

        private String s3Endpoint;

        private String s3EndpointRegion;

        private Boolean s3ConnectionSslEnabled;

        private Boolean s3PathStyleAccess;

        private String s3AccessKey;

        private String s3SecretKey;

        private final Map<String, Object> customConfigs = new HashMap<>();

        private Builder() {
        }

        public Builder feHttpUrl(String feHttpUrl) {
            this.feHttpUrl = feHttpUrl;
            return this;
        }

        public Builder feJdbcUrl(String feJdbcUrl) {
            this.feJdbcUrl = feJdbcUrl;
            return this;
        }

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder queryPlan(String queryPlan) {
            this.queryPlan = queryPlan;
            return this;
        }

        public Builder usingColumnUid(Boolean usingColumnUid) {
            this.usingColumnUid = usingColumnUid;
            return this;
        }

        public Builder unreleasedWarningThreshold(Integer unreleasedWarningThreshold) {
            this.unreleasedWarningThreshold = unreleasedWarningThreshold;
            return this;
        }

        public Builder s3Endpoint(String s3Endpoint) {
            this.s3Endpoint = s3Endpoint;
            return this;
        }

        public Builder s3EndpointRegion(String s3EndpointRegion) {
            this.s3EndpointRegion = s3EndpointRegion;
            return this;
        }

        public Builder s3ConnectionSslEnabled(Boolean s3ConnectionSslEnabled) {
            this.s3ConnectionSslEnabled = s3ConnectionSslEnabled;
            return this;
        }

        public Builder s3PathStyleAccess(Boolean s3PathStyleAccess) {
            this.s3PathStyleAccess = s3PathStyleAccess;
            return this;
        }

        public Builder s3AccessKey(String s3AccessKey) {
            this.s3AccessKey = s3AccessKey;
            return this;
        }

        public Builder s3SecretKey(String s3SecretKey) {
            this.s3SecretKey = s3SecretKey;
            return this;
        }

        public Builder customConfig(String name, Object value) {
            this.customConfigs.put(name, value);
            return this;
        }

        public Config build() {
            return new Config(this);
        }
    }

    /**
     * Convert to {@link Map} object.
     */
    public Map<String, String> toMap() {
        Map<String, String> mapping = new HashMap<>();
        Field[] fields = this.getClass().getDeclaredFields();
        for (Field field : fields) {
            if (!field.isAnnotationPresent(Option.class)) {
                continue;
            }

            Option option = field.getAnnotation(Option.class);
            String propName = option.value();
            try {
                Object propValue = field.get(this);
                if (null != propValue) {
                    mapping.put(propName, Objects.toString(propValue));
                }
            } catch (IllegalAccessException e) {
                throw new IllegalStateException("Resolve option error: " + propName, e);
            }
        }

        if (MapUtils.isNotEmpty(this.customConfigs)) {
            for (Map.Entry<String, Object> entry : this.customConfigs.entrySet()) {
                if (mapping.containsKey(entry.getKey())) {
                    throw new IllegalStateException("Duplicate config: " + entry.getKey());
                }

                mapping.put(entry.getKey(), Objects.toString(entry.getValue()));
            }
        }
        return mapping;
    }

    /* getters */

    public String getFeHttpUrl() {
        return feHttpUrl;
    }

    public String getFeJdbcUrl() {
        return feJdbcUrl;
    }

    public String getDatabase() {
        return database;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getQueryPlan() {
        return queryPlan;
    }

    public Boolean getUsingColumnUid() {
        return usingColumnUid;
    }

    public Integer getUnreleasedWarningThreshold() {
        return unreleasedWarningThreshold;
    }

    public String getS3Endpoint() {
        return s3Endpoint;
    }

    public String getS3EndpointRegion() {
        return s3EndpointRegion;
    }

    public Boolean getS3ConnectionSslEnabled() {
        return s3ConnectionSslEnabled;
    }

    public Boolean getS3PathStyleAccess() {
        return s3PathStyleAccess;
    }

    public String getS3AccessKey() {
        return s3AccessKey;
    }

    public String getS3SecretKey() {
        return s3SecretKey;
    }
}
