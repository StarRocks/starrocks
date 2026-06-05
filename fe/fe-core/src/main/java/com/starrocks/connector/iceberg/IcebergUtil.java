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

package com.starrocks.connector.iceberg;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.credential.CloudType;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TExprMinMaxValue;
import com.starrocks.thrift.TExprNodeType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.LocationUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class IcebergUtil {
    private static final Logger LOG = LogManager.getLogger(IcebergUtil.class);

    // Fallback target file size when neither the Iceberg table property
    // `write.target-file-size-bytes` nor the session variable
    // `connector_sink_target_max_file_size` is set. Kept at 1 GiB to preserve
    // StarRocks' historical default; Iceberg's own default is 512 MiB.
    static final long DEFAULT_TARGET_FILE_SIZE_BYTES = 1024L * 1024 * 1024;

    public static String fileName(String path) {
        return path.substring(path.lastIndexOf('/') + 1);
    }

    public static class MinMaxValue {
        Object minValue;
        Object maxValue;
        long nullValueCount;
        long valueCount;

        private boolean toThrift(SlotDescriptor slot, TExprMinMaxValue texpr) {
            texpr.setHas_null((nullValueCount > 0));
            texpr.setAll_null((valueCount == nullValueCount));
            if (valueCount == nullValueCount) {
                texpr.setType(TExprNodeType.NULL_LITERAL);
                return true;
            }
            if (minValue == null || maxValue == null) {
                return false;
            }
            switch (slot.getType().getPrimitiveType()) {
                case BOOLEAN:
                    texpr.setType(TExprNodeType.BOOL_LITERAL);
                    texpr.setMin_int_value((Boolean) minValue ? 1 : 0);
                    texpr.setMax_int_value((Boolean) maxValue ? 1 : 0);
                    break;
                case TINYINT:
                case SMALLINT:
                case INT:
                case DATE:
                    texpr.setType(TExprNodeType.INT_LITERAL);
                    texpr.setMin_int_value((Integer) minValue);
                    texpr.setMax_int_value((Integer) maxValue);
                    break;
                case BIGINT:
                case TIME:
                case DATETIME:
                    texpr.setType(TExprNodeType.INT_LITERAL);
                    if (minValue instanceof Integer) {
                        texpr.setMin_int_value(((Integer) minValue).longValue());
                    } else {
                        texpr.setMin_int_value((Long) minValue);
                    }
                    if (maxValue instanceof Integer) {
                        texpr.setMax_int_value(((Integer) maxValue).longValue());
                    } else {
                        texpr.setMax_int_value((Long) maxValue);
                    }
                    break;
                case FLOAT:
                    texpr.setType(TExprNodeType.FLOAT_LITERAL);
                    texpr.setMin_float_value((Float) minValue);
                    texpr.setMax_float_value((Float) maxValue);
                    break;
                case DOUBLE:
                    texpr.setType(TExprNodeType.FLOAT_LITERAL);
                    texpr.setMin_float_value((Double) minValue);
                    texpr.setMax_float_value((Double) maxValue);
                    break;
                default:
                    // Unsupported type for min/max optimization
                    return false;
            }
            return true;
        }

        public void toThrift(Map<Integer, TExprMinMaxValue> tExprMinMaxValueMap, SlotDescriptor slot) {
            TExprMinMaxValue texpr = new TExprMinMaxValue();
            if (toThrift(slot, texpr)) {
                tExprMinMaxValueMap.put(slot.getId().asInt(), texpr);
            }
        }
    }

    private static final Set<Type.TypeID> MIN_MAX_SUPPORTED_TYPES = Set.of(
            // TODO(yanz): to support more types.
            // datetime and timestamp: need to consider timezone.
            // decimal: need to consider precision and scale.
            // binary: min/max is not accurate for binary type.
            Type.TypeID.BOOLEAN,
            Type.TypeID.INTEGER,
            Type.TypeID.LONG,
            Type.TypeID.FLOAT,
            Type.TypeID.DOUBLE,
            Type.TypeID.DATE,
            Type.TypeID.TIME,
            Type.TypeID.TIMESTAMP
    );

    @VisibleForTesting
    public static Map<Integer, MinMaxValue> parseMinMaxValueBySlots(Schema schema,
                                                                    Map<Integer, ByteBuffer> lowerBounds,
                                                                    Map<Integer, ByteBuffer> upperBounds,
                                                                    Map<Integer, Long> nullValueCounts,
                                                                    Map<Integer, Long> valueCounts,
                                                                    List<SlotDescriptor> slots) {

        Preconditions.checkArgument(nullValueCounts != null && valueCounts != null,
                "nullValueCounts and valueCounts cannot be null");
        lowerBounds = lowerBounds == null ? Map.of() : lowerBounds;
        upperBounds = upperBounds == null ? Map.of() : upperBounds;
        Map<Integer, MinMaxValue> minMaxValues = new HashMap<>();
        for (SlotDescriptor slot : slots) {
            // has to be a scalar type
            if (!slot.getType().isScalarType()) {
                continue;
            }
            Types.NestedField field = schema.findField(slot.getColumn().getName());
            if (field == null) {
                continue;
            }
            Type type = field.type();
            // Skip unsupported types
            if (!MIN_MAX_SUPPORTED_TYPES.contains(type.typeId())) {
                continue;
            }
            if (!nullValueCounts.containsKey(field.fieldId()) || !valueCounts.containsKey(field.fieldId())) {
                continue;
            }
            // create the min/max value object to put into map
            MinMaxValue minMaxValue = new MinMaxValue();
            minMaxValues.put(field.fieldId(), minMaxValue);
            minMaxValue.nullValueCount = nullValueCounts.get(field.fieldId());
            minMaxValue.valueCount = valueCounts.get(field.fieldId());
            // parse lower and upper bounds
            Object low = Conversions.fromByteBuffer(field.type(), lowerBounds.get(field.fieldId()));
            Object high = Conversions.fromByteBuffer(field.type(), upperBounds.get(field.fieldId()));
            minMaxValue.minValue = low;
            minMaxValue.maxValue = high;
            if (type.typeId() == Type.TypeID.TIMESTAMP) {
                Types.TimestampType timestampType = (Types.TimestampType) type;
                if (timestampType.shouldAdjustToUTC() && low instanceof Long && high instanceof Long) {
                    // Iceberg TIMESTAMP WITH TIME ZONE stores instants in UTC, while StarRocks compares DATETIME
                    // values in the session timezone, so we convert file-level bounds into session-local micros
                    // before sending them to BE.
                    //
                    // Note that this is an endpoint conversion on file-level min/max only. For timezones whose
                    // UTC offset changes over time (for example DST or historical rule changes), UTC->local is
                    // not strictly monotonic over an arbitrary interval, so the converted low/high remain a
                    // best-effort approximation rather than an exact local min/max for the full file.
                    minMaxValue.minValue = adjustTimestampMicrosToSessionTz((Long) low);
                    minMaxValue.maxValue = adjustTimestampMicrosToSessionTz((Long) high);
                }
            }
        }
        return minMaxValues;
    }

    private static long adjustTimestampMicrosToSessionTz(long micros) {
        long seconds = Math.floorDiv(micros, 1_000_000L);
        long microsRemainder = Math.floorMod(micros, 1_000_000L);
        int nanos = (int) (microsRemainder * 1000L);
        ZoneId zoneId = TimeUtils.getTimeZone().toZoneId();
        ZoneOffset offset = zoneId.getRules().getOffset(Instant.ofEpochSecond(seconds, nanos));
        return micros + offset.getTotalSeconds() * 1_000_000L;
    }

    public static Map<Integer, TExprMinMaxValue> toThriftMinMaxValueBySlots(Schema schema,
                                                                            Map<Integer, ByteBuffer> lowerBounds,
                                                                            Map<Integer, ByteBuffer> upperBounds,
                                                                            Map<Integer, Long> nullValueCounts,
                                                                            Map<Integer, Long> valueCounts,
                                                                            List<SlotDescriptor> slots) {
        Map<Integer, TExprMinMaxValue> result = new HashMap<>();
        Map<Integer, MinMaxValue> minMaxValues =
                parseMinMaxValueBySlots(schema, lowerBounds, upperBounds, nullValueCounts, valueCounts, slots);
        for (SlotDescriptor slot : slots) {
            Types.NestedField field = schema.findField(slot.getColumn().getName());
            if (field == null) {
                continue;
            }
            int fieldId = field.fieldId();
            MinMaxValue minMaxValue = minMaxValues.get(fieldId);
            if (minMaxValue == null) {
                continue; // No min/max value for this slot
            }
            minMaxValue.toThrift(result, slot);
        }
        return result;
    }

    public static String tableDataLocation(Table table) {
        Preconditions.checkArgument(table != null, "table is null");
        String tableLocation = table.location();
        return table.properties().getOrDefault(TableProperties.WRITE_DATA_LOCATION,
                String.format("%s/data", LocationUtil.stripTrailingSlash(tableLocation)));
    }

    /**
     * Resolve the target max file size for an Iceberg sink. Priority:
     *   1. Table property `write.target-file-size-bytes` (Iceberg standard)
     *   2. Session variable `connector_sink_target_max_file_size` (when &gt; 0)
     *   3. {@link #DEFAULT_TARGET_FILE_SIZE_BYTES}
     * Unparseable property values are skipped with a WARN log so a misconfigured
     * table never breaks DML; the next source in the chain is used instead.
     */
    public static long resolveTargetMaxFileSize(Table nativeTable, SessionVariable sessionVariable) {
        Preconditions.checkArgument(nativeTable != null, "nativeTable is null");
        String raw = nativeTable.properties().get(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES);
        if (raw != null) {
            try {
                long parsed = Long.parseLong(raw.trim());
                if (parsed > 0) {
                    return parsed;
                }
                LOG.warn("Iceberg table property {}={} is not positive; falling back to session/default",
                        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, raw);
            } catch (NumberFormatException e) {
                LOG.warn("Iceberg table property {}={} is not a valid long; falling back to session/default",
                        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, raw);
            }
        }
        if (sessionVariable != null && sessionVariable.getConnectorSinkTargetMaxFileSize() > 0) {
            return sessionVariable.getConnectorSinkTargetMaxFileSize();
        }
        return DEFAULT_TARGET_FILE_SIZE_BYTES;
    }

    public static CloudConfiguration getVendedCloudConfiguration(String catalogName, IcebergTable icebergTable) {
        CatalogConnector connector = GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalogName);
        Preconditions.checkState(connector != null,
                String.format("connector of catalog %s should not be null", catalogName));

        // Try to get vended credentials from loadTable response
        CloudConfiguration vendedCredentialsCloudConfiguration = CloudConfigurationFactory.
                buildCloudConfigurationForVendedCredentials(icebergTable.getNativeTable().io().properties(),
                        icebergTable.getNativeTable().location());
        if (vendedCredentialsCloudConfiguration.getCloudType() != CloudType.DEFAULT) {
            return vendedCredentialsCloudConfiguration;
        }

        // Try to get credentials from catalog config (/v1/config response).
        // This is used as fallback when STS is unavailable (e.g., Apache Polaris without STS).
        CloudConfiguration catalogConfigCloudConfiguration = CloudConfigurationFactory.
                buildCloudConfigurationForVendedCredentials(connector.getMetadata().getCatalogProperties(),
                        icebergTable.getNativeTable().location());
        if (catalogConfigCloudConfiguration.getCloudType() != CloudType.DEFAULT) {
            return catalogConfigCloudConfiguration;
        }

        // Fall back to user-provided catalog credentials
        CloudConfiguration cloudConfiguration = connector.getMetadata().getCloudConfiguration();
        Preconditions.checkState(cloudConfiguration != null,
                String.format("cloudConfiguration of catalog %s should not be null", catalogName));
        return cloudConfiguration;
    }

    public static void checkFileFormatSupportedDelete(FileScanTask fileScanTask, boolean uedForDelete) {
        // Check file format for DELETE operations
        // Only Parquet format is supported for Iceberg DELETE operations now
        if (uedForDelete && fileScanTask.file().format() != FileFormat.PARQUET) {
            throw new StarRocksConnectorException(
                    String.format("Delete operations on Iceberg tables are only supported for " +
                                    "Parquet format files. Found %s format file: %s",
                            fileScanTask.file().format(), fileScanTask.file().location()));

        }
    }
}
