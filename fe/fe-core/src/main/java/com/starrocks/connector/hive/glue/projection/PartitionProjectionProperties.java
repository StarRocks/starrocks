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

package com.starrocks.connector.hive.glue.projection;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Parses and holds partition projection configuration from table properties.
 *
 * AWS Athena Partition Projection allows defining partition values dynamically
 * without storing them in the metastore. This class parses the table properties
 * following the AWS Athena specification.
 *
 * @see <a href="https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html">
 *      AWS Athena Partition Projection</a>
 */
public class PartitionProjectionProperties {

    // Property keys for partition projection
    public static final String PROJECTION_ENABLED = "projection.enabled";
    // Alternative key used by some implementations
    public static final String PROJECTION_ENABLE = "projection.enable";
    public static final String STORAGE_LOCATION_TEMPLATE = "storage.location.template";

    // Property suffixes for column-specific settings
    public static final String TYPE_SUFFIX = ".type";
    public static final String VALUES_SUFFIX = ".values";
    public static final String RANGE_SUFFIX = ".range";
    public static final String FORMAT_SUFFIX = ".format";
    public static final String INTERVAL_SUFFIX = ".interval";
    public static final String INTERVAL_UNIT_SUFFIX = ".interval.unit";
    public static final String DIGITS_SUFFIX = ".digits";

    private final boolean enabled;
    private final Map<String, ColumnProjectionConfig> columnConfigs;
    private final Optional<String> storageLocationTemplate;

    private PartitionProjectionProperties(boolean enabled,
                                          Map<String, ColumnProjectionConfig> columnConfigs,
                                          Optional<String> storageLocationTemplate) {
        this.enabled = enabled;
        this.columnConfigs = ImmutableMap.copyOf(columnConfigs);
        this.storageLocationTemplate = storageLocationTemplate;
    }

    /**
     * Parses table properties to extract partition projection configuration.
     *
     * @param tableProperties the table properties from Hive/Glue metastore
     * @return parsed partition projection properties
     */
    public static PartitionProjectionProperties parse(Map<String, String> tableProperties) {
        if (tableProperties == null) {
            return new PartitionProjectionProperties(false, new HashMap<>(), Optional.empty());
        }

        // Check if projection is enabled (support both 'projection.enabled' and 'projection.enable')
        boolean enabled = isProjectionEnabled(tableProperties);
        if (!enabled) {
            return new PartitionProjectionProperties(false, new HashMap<>(), Optional.empty());
        }

        // Parse storage location template
        Optional<String> storageTemplate = Optional.ofNullable(
                tableProperties.get(STORAGE_LOCATION_TEMPLATE));

        // Parse column-specific configurations
        Map<String, ColumnProjectionConfig> columnConfigs = parseColumnConfigs(tableProperties);

        return new PartitionProjectionProperties(enabled, columnConfigs, storageTemplate);
    }

    /**
     * Checks if partition projection is enabled in table properties.
     */
    public static boolean isProjectionEnabled(Map<String, String> tableProperties) {
        if (tableProperties == null) {
            return false;
        }
        String enabledValue = tableProperties.get(PROJECTION_ENABLED);
        if (enabledValue == null) {
            // Fallback to alternative key
            enabledValue = tableProperties.get(PROJECTION_ENABLE);
        }
        return "true".equalsIgnoreCase(enabledValue);
    }

    private static Map<String, ColumnProjectionConfig> parseColumnConfigs(Map<String, String> properties) {
        Map<String, ColumnProjectionConfig> configs = new HashMap<>();

        // Find all column names by looking for projection.<column>.type entries
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("projection.") && key.endsWith(TYPE_SUFFIX)) {
                // Extract column name: projection.<column>.type
                String columnName = extractColumnName(key);
                if (columnName != null) {
                    ColumnProjectionConfig config = parseColumnConfig(columnName, properties);
                    configs.put(columnName, config);
                }
            }
        }

        return configs;
    }

    private static String extractColumnName(String typeKey) {
        // Format: projection.<column>.type
        String prefix = "projection.";
        String suffix = TYPE_SUFFIX;
        if (typeKey.startsWith(prefix) && typeKey.endsWith(suffix)) {
            return typeKey.substring(prefix.length(), typeKey.length() - suffix.length());
        }
        return null;
    }

    private static ColumnProjectionConfig parseColumnConfig(String columnName, Map<String, String> properties) {
        String prefix = "projection." + columnName;
        String typeStr = properties.get(prefix + TYPE_SUFFIX);
        ProjectionType type = ProjectionType.fromString(typeStr);

        Map<String, String> columnProperties = new HashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(prefix + ".") && !key.equals(prefix + TYPE_SUFFIX)) {
                // Extract property name after prefix
                String propName = key.substring(prefix.length() + 1);
                columnProperties.put(propName, entry.getValue());
            }
        }

        return new ColumnProjectionConfig(type, columnProperties);
    }

    public boolean isEnabled() {
        return enabled;
    }

    public Map<String, ColumnProjectionConfig> getColumnConfigs() {
        return columnConfigs;
    }

    public Optional<String> getStorageLocationTemplate() {
        return storageLocationTemplate;
    }

    /**
     * Gets the projection configuration for a specific column.
     *
     * @param columnName the partition column name
     * @return the column projection config, or null if not found
     */
    public ColumnProjectionConfig getColumnConfig(String columnName) {
        return columnConfigs.get(columnName);
    }

    /**
     * Configuration for a single partition column's projection.
     */
    public static class ColumnProjectionConfig {
        private final ProjectionType type;
        private final Map<String, String> properties;

        public ColumnProjectionConfig(ProjectionType type, Map<String, String> properties) {
            this.type = type;
            this.properties = ImmutableMap.copyOf(properties);
        }

        public ProjectionType getType() {
            return type;
        }

        public Map<String, String> getProperties() {
            return properties;
        }

        public String getProperty(String key) {
            return properties.get(key);
        }

        public Optional<String> getOptionalProperty(String key) {
            return Optional.ofNullable(properties.get(key));
        }

        @Override
        public String toString() {
            return "ColumnProjectionConfig{" +
                    "type=" + type +
                    ", properties=" + properties +
                    '}';
        }
    }

    /**
     * Supported partition projection types.
     */
    public enum ProjectionType {
        ENUM,
        INTEGER,
        DATE,
        INJECTED;

        public static ProjectionType fromString(String type) {
            if (type == null) {
                throw new IllegalArgumentException("Projection type cannot be null");
            }
            switch (type.toLowerCase()) {
                case "enum":
                    return ENUM;
                case "integer":
                    return INTEGER;
                case "date":
                    return DATE;
                case "injected":
                    return INJECTED;
                default:
                    throw new IllegalArgumentException("Unknown projection type: " + type);
            }
        }
    }

    @Override
    public String toString() {
        return "PartitionProjectionProperties{" +
                "enabled=" + enabled +
                ", columnConfigs=" + columnConfigs +
                ", storageLocationTemplate=" + storageLocationTemplate +
                '}';
    }
}
