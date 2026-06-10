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

package com.starrocks.planner.lance;

import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/** Lance Configuration. */
public class LanceConfig implements Serializable {
    private static final long serialVersionUID = 827364827364823764L;
    public static final String CONFIG_DATASET_URI = "path"; // Path is default spark option key
    public static final String CONFIG_PUSH_DOWN_FILTERS = "pushDownFilters";
    public static final String LANCE_FILE_SUFFIX = ".lance";

    // Standard StarRocks S3 property keys accepted in the table PROPERTIES clause.
    public static final String PROP_S3_ACCESS_KEY = "aws.s3.access_key";
    public static final String PROP_S3_SECRET_KEY = "aws.s3.secret_key";
    public static final String PROP_S3_ENDPOINT = "aws.s3.endpoint";
    public static final String PROP_S3_REGION = "aws.s3.region";
    public static final String PROP_S3_PATH_STYLE = "aws.s3.enable_path_style_access";
    // Escape hatch: any property prefixed with this is forwarded verbatim (prefix stripped)
    // as a raw Lance storage option, e.g. lance.option.allow_http = true.
    public static final String PROP_RAW_OPTION_PREFIX = "lance.option.";

    private final String dbPath;
    private final String datasetName;
    private final String datasetUri;
    private final boolean pushDownFilters;
    private final Map<String, String> options = Maps.newHashMap();

    private LanceConfig(
            String dbPath,
            String datasetName,
            String datasetUri,
            boolean pushDownFilters) {
        this.dbPath = dbPath;
        this.datasetName = datasetName;
        this.datasetUri = datasetUri;
        this.pushDownFilters = pushDownFilters;
    }

    public static String getDatasetUri(String dbPath, String datasetUri) {
        StringBuilder sb = new StringBuilder().append(dbPath);
        if (!dbPath.endsWith("/")) {
            sb.append("/");
        }
        return sb.append(datasetUri).append(LANCE_FILE_SUFFIX).toString();
    }

    /**
     * Build the Lance storage option map from user-supplied table PROPERTIES.
     * Credentials are never hardcoded; if no credential properties are present an empty map is
     * returned and Lance falls back to the local filesystem / default credential chain.
     */
    public static Map<String, String> buildStorageOptions(Map<String, String> properties) {
        Map<String, String> storageOptions = Maps.newHashMap();
        if (properties == null) {
            return storageOptions;
        }
        putIfPresent(storageOptions, "aws_access_key_id", properties.get(PROP_S3_ACCESS_KEY));
        putIfPresent(storageOptions, "aws_secret_access_key", properties.get(PROP_S3_SECRET_KEY));
        putIfPresent(storageOptions, "aws_endpoint", properties.get(PROP_S3_ENDPOINT));
        putIfPresent(storageOptions, "aws_region", properties.get(PROP_S3_REGION));
        String pathStyle = properties.get(PROP_S3_PATH_STYLE);
        if (pathStyle != null) {
            // path-style access is the inverse of virtual-hosted-style requests
            storageOptions.put("aws_virtual_hosted_style_request", Boolean.toString(!Boolean.parseBoolean(pathStyle)));
        }
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(PROP_RAW_OPTION_PREFIX)) {
                storageOptions.put(entry.getKey().substring(PROP_RAW_OPTION_PREFIX.length()), entry.getValue());
            }
        }
        return storageOptions;
    }

    private static void putIfPresent(Map<String, String> target, String key, String value) {
        if (value != null && !value.isEmpty()) {
            target.put(key, value);
        }
    }

    public String getDbPath() {
        return dbPath;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public String getDatasetUri() {
        return datasetUri;
    }

    public boolean isPushDownFilters() {
        return pushDownFilters;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LanceConfig config = (LanceConfig) o;
        return pushDownFilters == config.pushDownFilters
                && Objects.equals(dbPath, config.dbPath)
                && Objects.equals(datasetName, config.datasetName)
                && Objects.equals(datasetUri, config.datasetUri)
                && Objects.equals(options, config.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbPath, datasetName, datasetUri, pushDownFilters, options);
    }
}
