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

package com.starrocks.connector.bigquery;

import com.starrocks.connector.exception.StarRocksConnectorException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class BigQueryProperties {

    // ---- Required ----
    public static final String PROJECT_ID = "bigquery.project.id";

    // ---- Authentication (all optional; default = Application Default Credentials) ----
    /** Inline service-account JSON key (mutually exclusive with CREDENTIALS_FILE). */
    public static final String CREDENTIALS_JSON = "bigquery.credentials.json";
    /** Path to a service-account JSON key file (mutually exclusive with CREDENTIALS_JSON). */
    public static final String CREDENTIALS_FILE = "bigquery.credentials.file";
    /**
     * Explicit auth type. Supported values:
     *   "service_account_json"  – use CREDENTIALS_JSON
     *   "service_account_file"  – use CREDENTIALS_FILE
     *   "application_default"   – use Application Default Credentials (ADC)
     *
     * When this property is absent and neither CREDENTIALS_JSON nor CREDENTIALS_FILE is
     * set, StarRocks defaults to ADC, which transparently uses the GCE / GKE node SA,
     * Workload Identity, gcloud credentials, or GOOGLE_APPLICATION_CREDENTIALS env var.
     */
    public static final String AUTH_TYPE = "bigquery.auth.type";

    public static final String AUTH_TYPE_SERVICE_ACCOUNT_JSON = "service_account_json";
    public static final String AUTH_TYPE_SERVICE_ACCOUNT_FILE = "service_account_file";
    public static final String AUTH_TYPE_APPLICATION_DEFAULT  = "application_default";

    // ---- Optional scan/catalog settings ----
    /** Comma-separated list of dataset names to expose. Empty = all datasets. */
    public static final String DATASET_FILTER         = "bigquery.dataset.filter";
    /** BQ dataset location, e.g. "US", "EU", "us-central1". Default: "US". */
    public static final String LOCATION               = "bigquery.location";
    /**
     * Preferred minimum number of streams per read session.
     * 0 = let BigQuery decide (recommended). Increase to raise parallelism for large tables.
     */
    public static final String MAX_STREAMS            = "bigquery.max.streams";

    // ---- View materialisation ----
    /** Set to "false" to hide views from SHOW TABLES and fail queries against them. Default: "true". */
    public static final String VIEW_ENABLED            = "bigquery.view.enabled";
    /** GCP project used for the temp table created when materialising a view. Defaults to PROJECT_ID. */
    public static final String VIEW_MATERIALIZE_PROJECT = "bigquery.view.materialize.project";
    /**
     * Dataset used for temp tables created during view materialisation.
     * Defaults to "_bq_tmp_sr_". The dataset must already exist and the SA must have
     * bigquery.tables.create permission on it (or bigquery.datasets.create on the project).
     */
    public static final String VIEW_MATERIALIZE_DATASET = "bigquery.view.materialize.dataset";
    /** Maximum seconds to wait for the materialisation query job. Default: 300. */
    public static final String VIEW_JOB_TIMEOUT_SECONDS = "bigquery.view.job.timeout.seconds";

    // ---- Metadata cache ----
    public static final String ENABLE_TABLE_CACHE        = "bigquery.cache.table.enable";
    public static final String TABLE_CACHE_EXPIRE_TIME   = "bigquery.cache.table.expire";
    public static final String TABLE_CACHE_SIZE          = "bigquery.cache.table.size";
    public static final String ENABLE_DATASET_CACHE      = "bigquery.cache.dataset.enable";
    public static final String DATASET_CACHE_EXPIRE_TIME = "bigquery.cache.dataset.expire";
    public static final String DATASET_CACHE_SIZE        = "bigquery.cache.dataset.size";

    // ---- Defaults ----
    private static final Map<String, String> DEFAULT_VALUES = new HashMap<>();
    private static final Set<String> REQUIRED_PROPERTIES = new HashSet<>();

    static {
        newProperty(PROJECT_ID).isRequired();

        newProperty(AUTH_TYPE).noDefaultValue();
        newProperty(CREDENTIALS_JSON).noDefaultValue();
        newProperty(CREDENTIALS_FILE).noDefaultValue();

        newProperty(DATASET_FILTER).noDefaultValue();
        newProperty(LOCATION).withDefaultValue("US");
        newProperty(MAX_STREAMS).withDefaultValue("0");

        newProperty(VIEW_ENABLED).withDefaultValue("true");
        newProperty(VIEW_MATERIALIZE_PROJECT).noDefaultValue();
        newProperty(VIEW_MATERIALIZE_DATASET).withDefaultValue("_bq_tmp_sr_");
        newProperty(VIEW_JOB_TIMEOUT_SECONDS).withDefaultValue("300");

        newProperty(ENABLE_TABLE_CACHE).withDefaultValue("true");
        newProperty(TABLE_CACHE_EXPIRE_TIME).withDefaultValue("86400");
        newProperty(TABLE_CACHE_SIZE).withDefaultValue("1000");
        newProperty(ENABLE_DATASET_CACHE).withDefaultValue("true");
        newProperty(DATASET_CACHE_EXPIRE_TIME).withDefaultValue("86400");
        newProperty(DATASET_CACHE_SIZE).withDefaultValue("1000");
    }

    private final Map<String, String> properties;

    public BigQueryProperties(Map<String, String> properties) {
        this.properties = properties;
        validate();
    }

    public String get(String key) {
        return properties.getOrDefault(key, DEFAULT_VALUES.get(key));
    }

    public boolean getBoolean(String key) {
        return Boolean.parseBoolean(get(key));
    }

    public long getLong(String key) {
        return Long.parseLong(get(key));
    }

    public int getInt(String key) {
        return Integer.parseInt(get(key));
    }

    public Map<String, String> getAll() {
        return properties;
    }

    private void validate() {
        for (String required : REQUIRED_PROPERTIES) {
            if (!properties.containsKey(required)) {
                throw new StarRocksConnectorException(
                        "BigQuery catalog requires property '" + required + "'");
            }
        }
        String credJson = properties.get(CREDENTIALS_JSON);
        String credFile = properties.get(CREDENTIALS_FILE);
        if (credJson != null && credFile != null) {
            throw new StarRocksConnectorException(
                    "Specify at most one of '" + CREDENTIALS_JSON + "' and '" + CREDENTIALS_FILE +
                            "'. Leave both unset to use Application Default Credentials (ADC).");
        }
    }

    // ---- Builder helpers (mirror OdpsProperties pattern) ----

    private static PropertyBuilder newProperty(String key) {
        return new PropertyBuilder(key);
    }

    private static class PropertyBuilder {
        private final String key;

        PropertyBuilder(String key) {
            this.key = key;
        }

        void isRequired() {
            REQUIRED_PROPERTIES.add(key);
        }

        void withDefaultValue(String value) {
            DEFAULT_VALUES.put(key, value);
        }

        void withDefaultValue(boolean value) {
            DEFAULT_VALUES.put(key, String.valueOf(value));
        }

        void withDefaultValue(long value) {
            DEFAULT_VALUES.put(key, String.valueOf(value));
        }

        void noDefaultValue() {
            // explicit no-op — just documents that this key is intentionally optional with no default
        }
    }
}
