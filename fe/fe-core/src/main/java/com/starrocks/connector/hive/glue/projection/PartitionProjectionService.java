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

import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.hive.HiveMetastoreApiConverter;
import com.starrocks.connector.hive.Partition;
import com.starrocks.connector.hive.RemoteFileInputFormat;
import com.starrocks.connector.hive.TextFileFormatDesc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Service for handling Partition Projection in AWS Glue/Athena tables.
 *
 * Partition Projection allows defining partition values dynamically through
 * table properties, eliminating the need to store partition metadata in the
 * metastore. This is particularly useful for:
 * - Time-series data with predictable partition patterns
 * - Tables with a large number of partitions
 * - Reducing metastore load and query latency
 *
 * @see <a href="https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html">
 *      AWS Athena Partition Projection</a>
 */
public class PartitionProjectionService {

    // Pre-compiled pattern for template variable expansion (e.g., ${column_name})
    private static final Pattern TEMPLATE_PATTERN = Pattern.compile("\\$\\{([^}]+)\\}");

    /**
     * Checks if partition projection is enabled for the given table.
     *
     * @param table the table to check
     * @return true if partition projection is enabled
     */
    public boolean isEnabled(Table table) {
        if (!(table instanceof HiveTable)) {
            return false;
        }
        HiveTable hiveTable = (HiveTable) table;
        Map<String, String> properties = hiveTable.getProperties();
        return PartitionProjectionProperties.isProjectionEnabled(properties);
    }

    /**
     * Creates a PartitionProjection configuration from table properties.
     *
     * @param table the Hive table
     * @return the partition projection configuration
     * @throws IllegalArgumentException if the configuration is invalid
     */
    public PartitionProjection createProjection(HiveTable table) {
        Map<String, String> properties = table.getProperties();
        List<String> partitionColumnNames = table.getPartitionColumnNames();
        String baseLocation = table.getTableLocation();

        // Parse projection properties
        PartitionProjectionProperties projProps = PartitionProjectionProperties.parse(properties);

        // Get storage location template
        Optional<String> storageLocationTemplate = projProps.getStorageLocationTemplate();

        // Validate base location when no storage location template is configured
        if (!storageLocationTemplate.isPresent() && (baseLocation == null || baseLocation.isEmpty())) {
            throw new IllegalArgumentException(
                    "Table location is required for partition projection when no storage.location.template is configured. " +
                    "Please set the table LOCATION or configure 'projection.storage.location.template' property.");
        }

        // Get input format and text file format desc from storage format
        RemoteFileInputFormat inputFormat = getInputFormat(table);
        TextFileFormatDesc textFileFormatDesc = getTextFileFormatDesc(table);

        // Create column projections for each partition column
        Map<String, ColumnProjection> columnProjections = new HashMap<>();
        for (String columnName : partitionColumnNames) {
            ColumnProjection projection = createColumnProjection(columnName, projProps);
            columnProjections.put(columnName, projection);
        }

        return new PartitionProjection(
                baseLocation,
                storageLocationTemplate,
                columnProjections,
                partitionColumnNames,
                inputFormat,
                textFileFormatDesc
        );
    }

    private RemoteFileInputFormat getInputFormat(HiveTable table) {
        if (table.getStorageFormat() == null) {
            return RemoteFileInputFormat.PARQUET;
        }
        // Reuse HiveMetastoreApiConverter for consistent format conversion
        String inputFormatClass = table.getStorageFormat().getInputFormat();
        return HiveMetastoreApiConverter.toRemoteFileInputFormat(inputFormatClass);
    }

    private TextFileFormatDesc getTextFileFormatDesc(HiveTable table) {
        // Return null for non-text formats
        if (table.getStorageFormat() == null ||
                table.getStorageFormat() != com.starrocks.connector.hive.HiveStorageFormat.TEXTFILE) {
            return null;
        }
        // Reuse the converter method for consistency with other Hive metadata handling
        return HiveMetastoreApiConverter.toTextFileFormatDesc(table.getSerdeProperties());
    }

    private ColumnProjection createColumnProjection(String columnName,
                                                     PartitionProjectionProperties projProps) {
        PartitionProjectionProperties.ColumnProjectionConfig config = projProps.getColumnConfig(columnName);
        if (config == null) {
            throw new IllegalArgumentException(
                    "Missing projection configuration for partition column: " + columnName +
                            ". Please add 'projection." + columnName + ".type' property.");
        }

        PartitionProjectionProperties.ProjectionType type = config.getType();
        Map<String, String> props = config.getProperties();

        switch (type) {
            case ENUM:
                return new EnumProjection(
                        columnName,
                        getRequiredProperty(props, "values", columnName)
                );

            case INTEGER:
                return new IntegerProjection(
                        columnName,
                        getRequiredProperty(props, "range", columnName),
                        config.getOptionalProperty("interval"),
                        config.getOptionalProperty("digits")
                );

            case DATE:
                return new DateProjection(
                        columnName,
                        getRequiredProperty(props, "range", columnName),
                        getRequiredProperty(props, "format", columnName),
                        config.getOptionalProperty("interval"),
                        config.getOptionalProperty("interval.unit")
                );

            case INJECTED:
                return new InjectedProjection(columnName);

            default:
                throw new IllegalArgumentException(
                        "Unknown projection type: " + type + " for column: " + columnName);
        }
    }

    private String getRequiredProperty(Map<String, String> props, String key, String columnName) {
        String value = props.get(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Missing required property 'projection." + columnName + "." + key + "'");
        }
        return value;
    }

    /**
     * Gets projected partitions for the given partition keys.
     *
     * This method creates virtual partitions based on the projection configuration
     * without querying the metastore. The partition locations are computed from
     * the table's base location and optional storage location template.
     *
     * @param table the Hive table with projection enabled
     * @param partitionKeys partition keys from query predicates
     * @return map of partition name to Partition object
     */
    public Map<String, Partition> getProjectedPartitions(HiveTable table,
                                                          List<PartitionKey> partitionKeys) {
        PartitionProjection projection = createProjection(table);
        List<String> partitionColumnNames = table.getPartitionColumnNames();

        if (partitionKeys.isEmpty()) {
            // No filter values - use empty optionals for all columns
            Map<String, Optional<Object>> emptyFilters = new HashMap<>();
            for (String columnName : partitionColumnNames) {
                emptyFilters.put(columnName, Optional.empty());
            }
            return projection.getProjectedPartitions(emptyFilters);
        }

        // Process ALL partition keys, not just the first one
        // This handles cases like WHERE year IN (2023, 2024) correctly
        Map<String, Partition> result = new HashMap<>();
        for (PartitionKey partitionKey : partitionKeys) {
            Map<String, Optional<Object>> partitionFilters = new HashMap<>();
            for (int i = 0; i < partitionColumnNames.size() && i < partitionKey.getKeys().size(); i++) {
                String columnName = partitionColumnNames.get(i);
                String value = partitionKey.getKeys().get(i).getStringValue();
                partitionFilters.put(columnName, Optional.ofNullable(value));
            }
            // Merge partitions from this key into the result
            Map<String, Partition> partitions = projection.getProjectedPartitions(partitionFilters);
            result.putAll(partitions);
        }

        return result;
    }

    /**
     * Gets projected partitions using explicit filter values.
     *
     * @param table the Hive table with projection enabled
     * @param partitionFilters map of column name to filter value
     * @return map of partition name to Partition object
     */
    public Map<String, Partition> getProjectedPartitions(HiveTable table,
                                                          Map<String, Optional<Object>> partitionFilters) {
        PartitionProjection projection = createProjection(table);
        return projection.getProjectedPartitions(partitionFilters);
    }

    /**
     * Gets all projected partition names for a table.
     * This is used to list all possible partitions based on projection configuration.
     *
     * Note: Tables with INJECTED projection columns cannot enumerate all partitions
     * because INJECTED columns require explicit filter values in the WHERE clause.
     *
     * @param table the Hive table with projection enabled
     * @return list of partition names in the format "col1=val1/col2=val2"
     * @throws IllegalArgumentException if the table has INJECTED projection columns
     */
    public List<String> getProjectedPartitionNames(HiveTable table) {
        Map<String, String> properties = table.getProperties();
        PartitionProjectionProperties projProps = PartitionProjectionProperties.parse(properties);

        // Check for INJECTED columns before attempting to enumerate partitions
        List<String> injectedColumns = new ArrayList<>();
        for (String columnName : table.getPartitionColumnNames()) {
            PartitionProjectionProperties.ColumnProjectionConfig config = projProps.getColumnConfig(columnName);
            if (config != null && config.getType() == PartitionProjectionProperties.ProjectionType.INJECTED) {
                injectedColumns.add(columnName);
            }
        }

        if (!injectedColumns.isEmpty()) {
            throw new IllegalArgumentException(
                    "Cannot enumerate all partitions for table with INJECTED projection columns: " +
                    injectedColumns + ". INJECTED columns require explicit filter values in the WHERE clause. " +
                    "Add a filter like 'WHERE " + injectedColumns.get(0) + " = <value>' to query this table.");
        }

        PartitionProjection projection = createProjection(table);

        // Get all projected partitions without any filter
        Map<String, Optional<Object>> emptyFilters = new HashMap<>();
        for (String columnName : table.getPartitionColumnNames()) {
            emptyFilters.put(columnName, Optional.empty());
        }

        Map<String, Partition> partitions = projection.getProjectedPartitions(emptyFilters);
        List<String> partitionNames = new ArrayList<>(partitions.keySet());
        return partitionNames;
    }

    /**
     * Gets projected partition names filtered by partition values.
     * This is used when the query has partition predicates.
     *
     * @param table the Hive table with projection enabled
     * @param partitionValues list of partition values (Optional.empty() means no filter for that column)
     * @return list of partition names matching the filter
     */
    public List<String> getProjectedPartitionNamesByValue(HiveTable table, List<Optional<String>> partitionValues) {
        PartitionProjection projection = createProjection(table);
        List<String> partitionColumnNames = table.getPartitionColumnNames();

        // Convert partition values to filter map
        Map<String, Optional<Object>> filters = new HashMap<>();
        for (int i = 0; i < partitionColumnNames.size(); i++) {
            String columnName = partitionColumnNames.get(i);
            if (i < partitionValues.size()) {
                Optional<String> optValue = partitionValues.get(i);
                if (optValue.isPresent()) {
                    filters.put(columnName, Optional.of(optValue.get()));
                } else {
                    filters.put(columnName, Optional.empty());
                }
            } else {
                filters.put(columnName, Optional.empty());
            }
        }

        Map<String, Partition> partitions = projection.getProjectedPartitions(filters);
        List<String> partitionNames = new ArrayList<>(partitions.keySet());
        return partitionNames;
    }

    /**
     * Gets projected partitions from partition names.
     * Partition names are in the format "col1=val1/col2=val2".
     *
     * @param table the Hive table with projection enabled
     * @param partitionNames list of partition names
     * @return map of partition name to Partition object
     */
    public Map<String, Partition> getProjectedPartitionsFromNames(HiveTable table,
                                                                   List<String> partitionNames) {
        String baseLocation = table.getTableLocation();
        RemoteFileInputFormat inputFormat = getInputFormat(table);
        TextFileFormatDesc textFileFormatDesc = getTextFileFormatDesc(table);

        // Get storage location template if defined
        Map<String, String> properties = table.getProperties();
        PartitionProjectionProperties projProps = PartitionProjectionProperties.parse(properties);
        Optional<String> storageLocationTemplate = projProps.getStorageLocationTemplate();
        List<String> partitionColumnNames = table.getPartitionColumnNames();

        // Validate base location when no storage location template is configured
        if (!storageLocationTemplate.isPresent() && (baseLocation == null || baseLocation.isEmpty())) {
            throw new IllegalArgumentException(
                    "Table location is required for partition projection when no storage.location.template is configured. " +
                    "Please set the table LOCATION or configure 'projection.storage.location.template' property.");
        }

        Map<String, Partition> result = new HashMap<>();
        for (String partitionName : partitionNames) {
            String partitionLocation = buildPartitionLocation(
                    baseLocation, partitionName, storageLocationTemplate, partitionColumnNames);

            // Create partition object
            Map<String, String> parameters = new HashMap<>();
            parameters.put(Partition.TRANSIENT_LAST_DDL_TIME,
                    String.valueOf(System.currentTimeMillis() / 1000));

            Partition partition = new Partition(
                    parameters,
                    inputFormat,
                    textFileFormatDesc,
                    partitionLocation,
                    PartitionProjection.isSplittable(inputFormat)
            );
            result.put(partitionName, partition);
        }

        return result;
    }

    /**
     * Builds partition location using storage location template if available.
     */
    private String buildPartitionLocation(String baseLocation, String partitionName,
                                          Optional<String> storageLocationTemplate,
                                          List<String> partitionColumnNames) {
        if (storageLocationTemplate.isPresent()) {
            // Parse partition values from partition name (format: col1=val1/col2=val2)
            Map<String, String> partitionValues = parsePartitionName(partitionName, partitionColumnNames);
            return expandTemplate(storageLocationTemplate.get(), partitionValues);
        }

        // Default: baseLocation/partitionName
        return PartitionUtil.getPathWithSlash(baseLocation) + partitionName;
    }

    /**
     * Parses partition name into column-value map using PartitionUtil for proper unescaping.
     * Input: "region=us/year=2024"
     * Output: {region=us, year=2024}
     *
     * @param partitionName the partition name in Hive format (col1=val1/col2=val2)
     * @param partitionColumnNames ordered list of partition column names
     * @return map of column name to value
     */
    private Map<String, String> parsePartitionName(String partitionName, List<String> partitionColumnNames) {
        // Use PartitionUtil.toPartitionValues for proper unescaping of special characters
        List<String> values = PartitionUtil.toPartitionValues(partitionName);
        Map<String, String> result = new HashMap<>();
        for (int i = 0; i < partitionColumnNames.size() && i < values.size(); i++) {
            result.put(partitionColumnNames.get(i), values.get(i));
        }
        return result;
    }

    /**
     * Expands storage location template with partition values.
     * Template format: s3://bucket/data/${region}/${year}/
     * Note: Template variable matching is case-insensitive to handle AWS Athena's
     * case-insensitive property handling.
     */
    private String expandTemplate(String template, Map<String, String> partitionValues) {
        // Build lowercase key map for case-insensitive lookup
        Map<String, String> lowercaseKeyMap = new HashMap<>();
        for (Map.Entry<String, String> entry : partitionValues.entrySet()) {
            lowercaseKeyMap.put(entry.getKey().toLowerCase(), entry.getValue());
        }

        // Use pre-compiled pattern to replace template variables case-insensitively
        Matcher matcher = TEMPLATE_PATTERN.matcher(template);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            String columnName = matcher.group(1);
            String value = lowercaseKeyMap.getOrDefault(columnName.toLowerCase(), "");
            matcher.appendReplacement(sb, Matcher.quoteReplacement(value));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }
}
