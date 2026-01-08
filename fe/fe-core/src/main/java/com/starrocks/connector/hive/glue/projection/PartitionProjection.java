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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
 * Generates partitions dynamically based on projection configuration.
 *
 * This class takes column projections and filter values, generates all
 * possible partition combinations (Cartesian product), and creates
 * Partition objects with computed storage locations.
 */
public class PartitionProjection {

    // Pattern for template variables: ${column_name}
    private static final Pattern TEMPLATE_PATTERN = Pattern.compile("\\$\\{([^}]+)\\}");

    // Maximum number of partitions to generate
    private static final int MAX_PARTITIONS = 100000;

    private final String baseLocation;
    private final Optional<String> storageLocationTemplate;
    private final Map<String, ColumnProjection> columnProjections;
    private final List<String> partitionColumnNames;
    private final RemoteFileInputFormat inputFormat;
    private final TextFileFormatDesc textFileFormatDesc;

    /**
     * Creates a partition projection.
     *
     * @param baseLocation the table's base storage location
     * @param storageLocationTemplate optional custom location template
     * @param columnProjections map of column name to projection
     * @param partitionColumnNames ordered list of partition column names
     * @param inputFormat the file input format
     * @param textFileFormatDesc text file format descriptor (may be null)
     */
    public PartitionProjection(String baseLocation,
                               Optional<String> storageLocationTemplate,
                               Map<String, ColumnProjection> columnProjections,
                               List<String> partitionColumnNames,
                               RemoteFileInputFormat inputFormat,
                               TextFileFormatDesc textFileFormatDesc) {
        this.baseLocation = baseLocation;
        this.storageLocationTemplate = storageLocationTemplate;
        this.columnProjections = ImmutableMap.copyOf(columnProjections);
        this.partitionColumnNames = ImmutableList.copyOf(partitionColumnNames);
        this.inputFormat = inputFormat;
        this.textFileFormatDesc = textFileFormatDesc;
    }

    /**
     * Generates partitions based on the projection configuration and filters.
     *
     * @param partitionFilters map of column name to filter value from query
     * @return map of partition name to Partition object
     * @throws IllegalArgumentException if too many partitions would be generated
     *         or if a required filter is missing
     */
    public Map<String, Partition> getProjectedPartitions(
            Map<String, Optional<Object>> partitionFilters) {

        // 1. Generate projected values for each column
        List<List<String>> projectedColumnValues = new ArrayList<>();
        for (String columnName : partitionColumnNames) {
            ColumnProjection projection = columnProjections.get(columnName);
            if (projection == null) {
                throw new IllegalArgumentException(
                        "No projection defined for partition column: " + columnName);
            }
            Optional<Object> filter = partitionFilters.getOrDefault(columnName, Optional.empty());
            List<String> values = projection.getProjectedValues(filter);
            if (values.isEmpty()) {
                // No matching values for this column - return empty result
                return new HashMap<>();
            }
            projectedColumnValues.add(values);
        }

        // 2. Calculate total combinations and validate (use multiplyExact to prevent overflow)
        long totalCombinations = 1;
        for (List<String> values : projectedColumnValues) {
            try {
                totalCombinations = Math.multiplyExact(totalCombinations, values.size());
            } catch (ArithmeticException e) {
                throw new IllegalArgumentException(
                        "Partition projection would generate too many partitions (overflow). " +
                                "Please add more specific filters to the query.");
            }
            if (totalCombinations > MAX_PARTITIONS) {
                throw new IllegalArgumentException(
                        "Partition projection would generate more than " + MAX_PARTITIONS +
                                " partitions. Please add more specific filters to the query.");
            }
        }

        // 3. Generate Cartesian product of all values
        List<List<String>> allCombinations = cartesianProduct(projectedColumnValues);

        // 4. Create Partition object for each combination
        Map<String, Partition> result = new HashMap<>();
        for (List<String> combination : allCombinations) {
            String partitionName = buildPartitionName(combination);
            String partitionLocation = buildPartitionLocation(combination);
            Partition partition = buildPartitionObject(partitionLocation);
            result.put(partitionName, partition);
        }

        return result;
    }

    /**
     * Builds the Hive-style partition name (e.g., "region=us/year=2024").
     */
    private String buildPartitionName(List<String> values) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < partitionColumnNames.size(); i++) {
            if (i > 0) {
                sb.append("/");
            }
            sb.append(partitionColumnNames.get(i))
                    .append("=")
                    .append(values.get(i));
        }
        return sb.toString();
    }

    /**
     * Builds the storage location for a partition.
     */
    private String buildPartitionLocation(List<String> values) {
        if (storageLocationTemplate.isPresent()) {
            // Use custom template
            return expandTemplate(storageLocationTemplate.get(), values);
        }

        // Default: baseLocation/column1=value1/column2=value2/...
        String partitionPath = buildPartitionName(values);
        String location = baseLocation;
        if (!location.endsWith("/")) {
            location += "/";
        }
        return location + partitionPath;
    }

    /**
     * Expands a storage location template with partition values.
     *
     * Template format: s3://bucket/data/${region}/${year}/${date}/
     */
    private String expandTemplate(String template, List<String> values) {
        // Build value map
        Map<String, String> valueMap = new HashMap<>();
        for (int i = 0; i < partitionColumnNames.size(); i++) {
            valueMap.put(partitionColumnNames.get(i), values.get(i));
        }

        // Replace template variables
        Matcher matcher = TEMPLATE_PATTERN.matcher(template);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            String columnName = matcher.group(1);
            String value = valueMap.getOrDefault(columnName, "");
            // Escape special regex characters in replacement
            matcher.appendReplacement(sb, Matcher.quoteReplacement(value));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    /**
     * Creates a Partition object for the given location.
     */
    private Partition buildPartitionObject(String location) {
        // Create partition parameters with current timestamp
        Map<String, String> parameters = new HashMap<>();
        parameters.put(Partition.TRANSIENT_LAST_DDL_TIME,
                String.valueOf(System.currentTimeMillis() / 1000));

        return new Partition(
                parameters,
                inputFormat,
                textFileFormatDesc,
                location,
                isSplittable(inputFormat)
        );
    }

    /**
     * Determines if the file format is splittable.
     */
    private boolean isSplittable(RemoteFileInputFormat format) {
        if (format == null) {
            return true;
        }
        switch (format) {
            case PARQUET:
            case ORC:
            case TEXTFILE:
            case AVRO:
            case RCFILE:
            case SEQUENCE:
                return true;
            default:
                return false;
        }
    }

    /**
     * Computes the Cartesian product of lists.
     *
     * Example: [[a, b], [1, 2]] -> [[a, 1], [a, 2], [b, 1], [b, 2]]
     */
    private List<List<String>> cartesianProduct(List<List<String>> lists) {
        List<List<String>> result = new ArrayList<>();
        if (lists.isEmpty()) {
            result.add(new ArrayList<>());
            return result;
        }

        cartesianProductHelper(lists, 0, new ArrayList<>(), result);
        return result;
    }

    private void cartesianProductHelper(List<List<String>> lists, int index,
                                        List<String> current, List<List<String>> result) {
        if (index == lists.size()) {
            result.add(new ArrayList<>(current));
            return;
        }

        for (String value : lists.get(index)) {
            current.add(value);
            cartesianProductHelper(lists, index + 1, current, result);
            current.remove(current.size() - 1);
        }
    }

    public String getBaseLocation() {
        return baseLocation;
    }

    public Optional<String> getStorageLocationTemplate() {
        return storageLocationTemplate;
    }

    public Map<String, ColumnProjection> getColumnProjections() {
        return columnProjections;
    }

    public List<String> getPartitionColumnNames() {
        return partitionColumnNames;
    }

    @Override
    public String toString() {
        return "PartitionProjection{" +
                "baseLocation='" + baseLocation + '\'' +
                ", storageLocationTemplate=" + storageLocationTemplate +
                ", partitionColumnNames=" + partitionColumnNames +
                ", columnProjections=" + columnProjections.keySet() +
                '}';
    }
}
