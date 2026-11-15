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

package com.starrocks.authorization.ranger.hive;

import com.starrocks.common.Config;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;

import java.util.Map;

/**
 * Catalog properties of an external catalog that delegates authorization to a Ranger hive service.
 * External catalogs such as hive, iceberg and hudi may set {@link #SERVICE_NAME}; the catalog is then
 * authorized by {@link RangerHiveAccessController} and every property here applies to it. A connector
 * with a strict property list, such as {@code type = starrocks}, rejects these properties.
 *
 * <p>{@link #CATALOG_PREFIX_ENABLED}: the Ranger {@code hive} service def has no catalog level. When
 * several external catalogs share one Ranger hive service (for example several AWS Glue catalogs), a
 * schema that exists in more than one catalog would otherwise be sent as the same bare {@code database}
 * and match the same policies.
 * With the prefix enabled, Ranger sees {@code <catalog><separator><schema>} instead.
 *
 * <p>Resolution order: the catalog property {@link #CATALOG_PREFIX_ENABLED} when set, otherwise the FE config
 * {@code ranger_hive_catalog_prefix_enabled}. The separator is always the FE config
 * {@code ranger_hive_catalog_prefix_separator}. Both configs are read at check time so a change
 * applies immediately.
 */
public final class RangerHiveCatalogProperties {
    /** Name of the Ranger hive service; non-empty means "authorize this catalog through Ranger hive". */
    public static final String SERVICE_NAME = "ranger.plugin.hive.service.name";
    public static final String CATALOG_PREFIX_ENABLED = "ranger.plugin.hive.catalog_prefix.enabled";

    private RangerHiveCatalogProperties() {
    }

    /**
     * @return null when {@link #CATALOG_PREFIX_ENABLED} is absent (follow the FE config); otherwise its value
     * @throws SemanticException when the value is neither "true" nor "false"
     */
    public static Boolean parseCatalogPrefixEnabled(Map<String, String> catalogProperties) {
        String value = catalogProperties.get(CATALOG_PREFIX_ENABLED);
        if (value == null) {
            return null;
        }
        return PropertyAnalyzer.parseBooleanStrictly(CATALOG_PREFIX_ENABLED, value);
    }

    /** The single place that decides what {@code database} the Ranger hive service sees. */
    public static String resolveDatabase(Boolean catalogPrefixEnabled, String catalogName, String db) {
        boolean enabled = catalogPrefixEnabled != null ? catalogPrefixEnabled : Config.ranger_hive_catalog_prefix_enabled;
        return enabled ? catalogName + Config.ranger_hive_catalog_prefix_separator + db : db;
    }
}
