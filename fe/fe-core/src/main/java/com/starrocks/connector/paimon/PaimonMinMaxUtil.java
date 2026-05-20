\// Copyright 2021-present StarRocks, Inc. All rights reserved.
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

package com.starrocks.connector.paimon;

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.thrift.TExprMinMaxValue;
import com.starrocks.thrift.TExprNodeType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utility class to convert Paimon DataFileMeta value statistics into thrift TExprMinMaxValue
 * structures for per-file min/max skipping in the native C++ reader (Parquet/ORC).
 *
 */
public final class PaimonMinMaxUtil {
    private static final Logger LOG = LogManager.getLogger(PaimonMinMaxUtil.class);

    private static final Set<DataTypeRoot> SUPPORTED_TYPES = Set.of(
            DataTypeRoot.BOOLEAN,
            DataTypeRoot.TINYINT,
            DataTypeRoot.SMALLINT,
            DataTypeRoot.INTEGER,
            DataTypeRoot.BIGINT,
            DataTypeRoot.FLOAT,
            DataTypeRoot.DOUBLE,
            DataTypeRoot.DATE,
            DataTypeRoot.TIME_WITHOUT_TIME_ZONE,
            DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
            DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE
    );

    private PaimonMinMaxUtil() {
    }

    /**
     * Extracts min/max values from Paimon and converts them to a map of slot ID
     * suitable for populating THdfsScanRange.min_max_values
     */
    public static Map<Integer, TExprMinMaxValue> toThriftMinMaxValueBySlots(
            RowType rowType,
            DataFileMeta dataFileMeta,
            List<SlotDescriptor> slots) {

        SimpleStats valueStats = dataFileMeta.valueStats();
        if (valueStats == null || valueStats == SimpleStats.EMPTY_STATS) {
            return Map.of();
        }

        long rowCount = dataFileMeta.rowCount();
        List<String> valueStatsCols = dataFileMeta.valueStatsCols();

        return toThriftMinMaxValueBySlots(rowType, valueStats, valueStatsCols, rowCount, slots);
    }

    /**
     * Core conversion logic
     */
    @VisibleForTesting
    public static Map<Integer, TExprMinMaxValue> toThriftMinMaxValueBySlots(
            RowType rowType,
            SimpleStats valueStats,
            List<String> valueStatsCols,
            long rowCount,
            List<SlotDescriptor> slots) {

        if (valueStats == null || valueStats == SimpleStats.EMPTY_STATS) {
            return Map.of();
        }

        BinaryRow minValues = valueStats.minValues();
        BinaryRow maxValues = valueStats.maxValues();
        BinaryArray nullCounts = valueStats.nullCounts();

        if (minValues == null || maxValues == null) {
            return Map.of();
        }

        // Build column name to position mapping in the Paimon row type
        List<DataField> fields = rowType.getFields();
        Map<String, Integer> columnNameToPosition = new HashMap<>(fields.size());
        Map<String, DataType> columnNameToType = new HashMap<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            columnNameToPosition.put(field.name(), i);
            columnNameToType.put(field.name(), field.type());
        }

        // If valueStatsCols is non-null and non-empty, only those columns have valid stats.
        // Otherwise, all value columns have stats and the BinaryRow indices correspond to
        // column positions in the row type.
        Set<String> statsColumnSet = null;
        Map<String, Integer> statsColumnToIndex = null;
        boolean useSubsetMapping = valueStatsCols != null && !valueStatsCols.isEmpty();

        if (useSubsetMapping) {
            statsColumnSet = new HashSet<>(valueStatsCols);
            // When valueStatsCols is a subset, the BinaryRow indices correspond to the
            // position within the valueStatsCols list, NOT the position in the full row type.
            statsColumnToIndex = new HashMap<>(valueStatsCols.size());
            for (int i = 0; i < valueStatsCols.size(); i++) {
                statsColumnToIndex.put(valueStatsCols.get(i), i);
            }
        }

        Map<Integer, TExprMinMaxValue> result = new HashMap<>();

        for (SlotDescriptor slot : slots) {
            if (!slot.getType().isScalarType()) {
                continue;
            }

            String columnName = slot.getColumn().getName();

            // Skip columns not in the stats subset
            if (useSubsetMapping && !statsColumnSet.contains(columnName)) {
                continue;
            }

            // Find the Paimon data type for this column
            DataType paimonType = columnNameToType.get(columnName);
            if (paimonType == null) {
                continue;
            }

            // Skip unsupported types
            if (!SUPPORTED_TYPES.contains(paimonType.getTypeRoot())) {
                continue;
            }

            // Determine the index into the BinaryRow for min/max values
            int statsIndex;
            if (useSubsetMapping) {
                Integer idx = statsColumnToIndex.get(columnName);
                if (idx == null) {
                    continue;
                }
                statsIndex = idx;
            } else {
                Integer pos = columnNameToPosition.get(columnName);
                if (pos == null) {
                    continue;
                }
                statsIndex = pos;
            }

            // Bounds check
            if (statsIndex >= minValues.getFieldCount() || statsIndex >= maxValues.getFieldCount()) {
                continue;
            }

            // Extract null count for this column
            long nullCount = 0;
            if (nullCounts != null && statsIndex < nullCounts.size()) {
                if (!nullCounts.isNullAt(statsIndex)) {
                    nullCount = nullCounts.getLong(statsIndex);
                }
            }

            TExprMinMaxValue texpr = new TExprMinMaxValue();
            texpr.setHas_null(nullCount > 0);
            texpr.setAll_null(rowCount > 0 && nullCount >= rowCount);

            if (rowCount > 0 && nullCount >= rowCount) {
                // All values are null
                texpr.setType(TExprNodeType.NULL_LITERAL);
                result.put(slot.getId().asInt(), texpr);
                continue;
            }

            boolean minNull = minValues.isNullAt(statsIndex);
            boolean maxNull = maxValues.isNullAt(statsIndex);
            if (minNull || maxNull) {
                // Cannot use min/max if either bound is null (but not all-null)
                continue;
            }

            boolean success = fillMinMaxFromPaimonType(texpr, slot, paimonType, minValues, maxValues, statsIndex);
            if (success) {
                result.put(slot.getId().asInt(), texpr);
            }
        }

        return result;
    }

    /**
     * Reads min/max values from BinaryRow based on the Paimon data type and populates
     * the TExprMinMaxValue. Returns true if successful.
     */
    private static boolean fillMinMaxFromPaimonType(
            TExprMinMaxValue texpr,
            SlotDescriptor slot,
            DataType paimonType,
            BinaryRow minValues,
            BinaryRow maxValues,
            int index) {

        switch (paimonType.getTypeRoot()) {
            case BOOLEAN:
                texpr.setType(TExprNodeType.BOOL_LITERAL);
                texpr.setMin_int_value(minValues.getBoolean(index) ? 1 : 0);
                texpr.setMax_int_value(maxValues.getBoolean(index) ? 1 : 0);
                return true;

            case TINYINT:
                texpr.setType(TExprNodeType.INT_LITERAL);
                texpr.setMin_int_value(minValues.getByte(index));
                texpr.setMax_int_value(maxValues.getByte(index));
                return true;

            case SMALLINT:
                texpr.setType(TExprNodeType.INT_LITERAL);
                texpr.setMin_int_value(minValues.getShort(index));
                texpr.setMax_int_value(maxValues.getShort(index));
                return true;

            case INTEGER:
            case DATE:
                // Paimon stores DATE as days since epoch (int), same as Iceberg
                texpr.setType(TExprNodeType.INT_LITERAL);
                texpr.setMin_int_value(minValues.getInt(index));
                texpr.setMax_int_value(maxValues.getInt(index));
                return true;

            case BIGINT:
            case TIME_WITHOUT_TIME_ZONE:
                texpr.setType(TExprNodeType.INT_LITERAL);
                texpr.setMin_int_value(minValues.getLong(index));
                texpr.setMax_int_value(maxValues.getLong(index));
                return true;

            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // Paimon stores timestamps using the Timestamp class.
                // For min/max filtering, we need the microsecond value.
                // Paimon's BinaryRow stores TIMESTAMP as a composite (nanoOfMillisecond + millisecond),
                // accessed via getTimestamp(index, precision). We use the millis-to-micros conversion.
                try {
                    org.apache.paimon.data.Timestamp minTs = minValues.getTimestamp(index, 6);
                    org.apache.paimon.data.Timestamp maxTs = maxValues.getTimestamp(index, 6);
                    if (minTs == null || maxTs == null) {
                        return false;
                    }
                    texpr.setType(TExprNodeType.INT_LITERAL);
                    texpr.setMin_int_value(timestampToMicros(minTs));
                    texpr.setMax_int_value(timestampToMicros(maxTs));
                    return true;
                } catch (Exception e) {
                    LOG.debug("Failed to read timestamp min/max for column {}: {}",
                            slot.getColumn().getName(), e.getMessage());
                    return false;
                }

            case FLOAT:
                texpr.setType(TExprNodeType.FLOAT_LITERAL);
                texpr.setMin_float_value(minValues.getFloat(index));
                texpr.setMax_float_value(maxValues.getFloat(index));
                return true;

            case DOUBLE:
                texpr.setType(TExprNodeType.FLOAT_LITERAL);
                texpr.setMin_float_value(minValues.getDouble(index));
                texpr.setMax_float_value(maxValues.getDouble(index));
                return true;

            default:
                return false;
        }
    }

    /**
     * Converts a Paimon Timestamp to microseconds since epoch.
     */
    private static long timestampToMicros(org.apache.paimon.data.Timestamp ts) {
        long millis = ts.getMillisecond();
        int nanoOfMillisecond = ts.getNanoOfMillisecond();
        // Convert millis to micros, then add the sub-millisecond microseconds
        return millis * 1000L + nanoOfMillisecond / 1000;
    }
}
