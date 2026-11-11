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

package com.starrocks.connector.paimon;

import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.SessionVariable.PaimonReaderMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FallbackReadFileStoreTable.FallbackDataSplit;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongFunction;
import java.util.function.Predicate;

/** Reader selection with ordered compatibility checks for the packaged paimon-cpp 0.3.0 reader. */
public final class PaimonReaderSelector {
    private static final Logger LOG = LogManager.getLogger(PaimonReaderSelector.class);

    public enum Reader { STARROCKS, NATIVE, JNI }

    public enum Reason {
        RAW_FILES, NATIVE_SUPPORTED, EXPLICIT_MODE, SYSTEM_SPLIT, INDEXED_SPLIT,
        UNKNOWN_SCHEMA, FILE_FORMAT, TABLE_SCHEMA, SCHEMA_EVOLUTION, MERGE_FUNCTION,
        DELETION_VECTOR, ORC_LEGACY_TIMESTAMP, TIMESTAMP, NULLABLE_MAP_KEY, SPLIT_PROTOCOL,
        PRIMARY_KEY_INDEXED_SPLIT
    }

    public record Decision(Reader reader, Reason reason) { }

    private static final Set<String> CPP_FORMATS = Set.of("parquet", "orc", "avro", "blob");
    private static final Set<String> VALUE_AGGREGATES = Set.of(
            "first_value", "last_value", "first_non_null_value", "last_non_null_value");

    private final FileStoreTable table;
    private final TableSchema schema;
    private final Set<String> fields;
    private final List<DataType> selectedTypes;
    private final LongFunction<TableSchema> schemaLoader;
    // A selector belongs to one scan node. Schema ids are immutable within its table/branch.
    private final Map<Long, TableSchema> schemas = new HashMap<>();

    private final Map<Long, SchemaFields> schemaFields = new HashMap<>();
    private PaimonReaderSelector fallbackSelector;
    private Reason nativeInitializationReason;

    private record SchemaFields(Map<String, DataField> names, Map<Integer, DataField> ids) {
        SchemaFields(TableSchema schema) {
            this(schema.nameToFieldMap(), schema.idToFieldMap());
        }
    }

    public PaimonReaderSelector(Table table, List<String> fields) {
        this.table = table instanceof FileStoreTable ? (FileStoreTable) table : null;
        this.schema = this.table == null ? null : this.table.schema();
        this.fields = new HashSet<>(fields);
        this.selectedTypes = selectedTypes(schema != null ? schema.fields() :
                table == null ? List.of() : table.rowType().getFields(), this.fields);
        this.schemaLoader = id -> this.table.schemaManager().schema(id);
        if (schema != null) {
            schemas.put(schema.id(), schema);
        }
    }

    PaimonReaderSelector(TableSchema schema, List<String> fields, LongFunction<TableSchema> schemaLoader) {
        this.table = null;
        this.schema = schema;
        this.fields = new HashSet<>(fields);
        this.selectedTypes = selectedTypes(schema.fields(), this.fields);
        this.schemaLoader = schemaLoader;
        schemas.put(schema.id(), schema);
    }

    public Decision select(Split split, PaimonReaderMode mode) {
        // Neither SDK can deserialize this wrapper without corrupting its row ranges.
        if (PaimonSplitUtils.isGlobalIndexSplit(split) &&
                PaimonSplitUtils.getDataSplit(split).orElse(null) instanceof FallbackDataSplit) {
            throw new StarRocksConnectorException(
                    "Paimon cannot read IndexedSplit wrapping FallbackDataSplit: neither JNI nor native supports " +
                            "this split protocol (%s).", Reason.SPLIT_PROTOCOL);
        }
        if (mode == PaimonReaderMode.JNI) {
            checkJniSupported(Reason.EXPLICIT_MODE);
            return new Decision(Reader.JNI, Reason.EXPLICIT_MODE);
        }
        if (canUseStarRocksRawFile(split)) {
            return new Decision(Reader.STARROCKS, Reason.RAW_FILES);
        }
        Decision decision = selectMode(split);
        if (mode == PaimonReaderMode.NATIVE && decision.reader() != Reader.NATIVE) {
            String details = "additional diagnostics unavailable";
            try {
                details = nativeFailureDetails(split, decision.reason());
            } catch (Exception e) {
                LOG.warn("Failed to collect Paimon native reader diagnostics for reason {}", decision.reason(), e);
            }
            throw new StarRocksConnectorException(
                    "Paimon native reader cannot read this split: %s; %s", decision.reason(),
                    details);
        }
        if (decision.reader() == Reader.JNI) {
            checkJniSupported(decision.reason());
        }
        return decision;
    }

    private Decision selectMode(Split split) {
        DataSplit data = PaimonSplitUtils.getDataSplit(split).orElse(null);
        if (data == null) {
            // The current CPP split protocol cannot deserialize Java system-table or other non-data splits.
            return new Decision(Reader.JNI, Reason.SYSTEM_SPLIT);
        }
        PaimonReaderSelector selector = selectorFor(data);
        if (selector == null || selector.schema == null) {
            return new Decision(Reader.JNI, Reason.UNKNOWN_SCHEMA);
        }
        if (!hasCompatibleFallbackSchema(data, selector)) {
            return new Decision(Reader.JNI, Reason.SCHEMA_EVOLUTION);
        }
        // CPP's primary-key reader requires DataSplit; Java can retain indexed row ranges.
        if (PaimonSplitUtils.isGlobalIndexSplit(split) && !selector.schema.primaryKeys().isEmpty()) {
            return new Decision(Reader.JNI, Reason.PRIMARY_KEY_INDEXED_SPLIT);
        }

        Reason initialization = checkNativeInitialization();
        if (initialization != Reason.NATIVE_SUPPORTED) {
            return new Decision(Reader.JNI, initialization);
        }
        if (!selector.supportsFileFormats(data)) {
            return new Decision(Reader.JNI, Reason.FILE_FORMAT);
        }

        boolean needsMerge = selector.requiresMerge(data);
        if (!selector.supportsMergeEngine(needsMerge)) {
            return new Decision(Reader.JNI, Reason.MERGE_FUNCTION);
        }

        Set<String> required = selector.requiredFields(needsMerge);
        if (!selector.supportsAggregateFunctions(needsMerge, required)) {
            return new Decision(Reader.JNI, Reason.MERGE_FUNCTION);
        }
        // Delay historical schema loads until the cheaper merge checks have passed.
        if (!selector.supportsHistoricalFileFormats(data)) {
            return new Decision(Reader.JNI, Reason.FILE_FORMAT);
        }
        if (!selector.supportsHistoricalSchemaTypes(data)) {
            return new Decision(Reader.JNI, Reason.TABLE_SCHEMA);
        }
        if (!selector.supportsDeletionVectors(data)) {
            return new Decision(Reader.JNI, Reason.DELETION_VECTOR);
        }
        if (!selector.supportsOrcTimestampEncoding(data)) {
            return new Decision(Reader.JNI, Reason.ORC_LEGACY_TIMESTAMP);
        }
        if (!selector.supportsSchemaEvolution(data, required)) {
            return new Decision(Reader.JNI, Reason.SCHEMA_EVOLUTION);
        }
        if (!selector.supportsTimestampConversion(required)) {
            return new Decision(Reader.JNI, Reason.TIMESTAMP);
        }
        if (!selector.supportsNullableMapKeys(data, required)) {
            return new Decision(Reader.JNI, Reason.NULLABLE_MAP_KEY);
        }

        return new Decision(Reader.NATIVE,
                PaimonSplitUtils.isGlobalIndexSplit(split) ? Reason.INDEXED_SPLIT : Reason.NATIVE_SUPPORTED);
    }

    // ==================== SR raw-file selection ====================

    private boolean canUseStarRocksRawFile(Split split) {
        DataSplit data = PaimonSplitUtils.getDataSplit(split).orElse(null);
        if (data == null || PaimonSplitUtils.isGlobalIndexSplit(split)) {
            return false;
        }
        PaimonReaderSelector selector = selectorFor(data);
        return selector != null && selector.schema != null && hasCompatibleFallbackSchema(data, selector) &&
                selector.canReadRawFiles(data);
    }

    private boolean canReadRawFiles(DataSplit split) {
        var rawFiles = split.convertToRawFiles();
        if (rawFiles.isEmpty()) {
            return false;
        }
        for (DataType type : selectedTypes) {
            if (contains(type, t -> t.is(DataTypeRoot.BLOB))) {
                return false;
            }
        }
        for (var file : rawFiles.get()) {
            String format = file.format();
            if (!format.equals("parquet") && !format.equals("orc")) {
                return false;
            }
            for (DataType type : selectedTypes) {
                if (contains(type, t -> isUnsupportedRawTimeType(t, format))) {
                    return false;
                }
            }
            if (!hasCompatibleSchema(fileSchema(file.schemaId()))) {
                return false;
            }
        }
        return true;
    }

    private boolean hasCompatibleSchema(TableSchema fileSchema) {
        SchemaFields currentFields = fieldMaps(schema);
        SchemaFields fileFields = fieldMaps(fileSchema);
        for (String name : fields) {
            DataField current = currentFields.names().get(name);
            if (current == null) {
                return false;
            }
            DataField previous = fileFields.ids().get(current.id());
            if (previous == null) {
                // A newly added field is safe unless the file has a different field with the same name.
                if (fileFields.names().containsKey(name)) {
                    return false;
                }
            } else if (!previous.name().equals(name) || !previous.type().equalsIgnoreNullable(current.type())) {
                return false;
            }
        }
        return true;
    }

    private static boolean isUnsupportedRawTimeType(DataType type, String format) {
        if (type instanceof TimestampType) {
            return ((TimestampType) type).getPrecision() > 6 || format.equals("orc");
        }
        if (type instanceof LocalZonedTimestampType) {
            return ((LocalZonedTimestampType) type).getPrecision() > 6 || format.equals("orc");
        }
        return format.equals("orc") && type.is(DataTypeRoot.TIME_WITHOUT_TIME_ZONE);
    }

    // ==================== Paimon native compatibility checks ====================

    private Reason checkNativeInitialization() {
        if (nativeInitializationReason != null) {
            return nativeInitializationReason;
        }
        // CPP initializes both branch readers even when the split belongs to the main branch.
        PaimonReaderSelector fallback = getFallbackSelector();
        if ((!schema.options().getOrDefault("scan.fallback-branch", "").isBlank() && fallback == null) ||
                (fallback != null && fallback.schema == null)) {
            return nativeInitializationReason = Reason.UNKNOWN_SCHEMA;
        }
        if (!supportsFileFormatOptions(schema) ||
                (fallback != null && !supportsFileFormatOptions(fallback.schema))) {
            return nativeInitializationReason = Reason.FILE_FORMAT;
        }
        if (!nativeSchemaSupported(schema) || (fallback != null && !nativeSchemaSupported(fallback.schema))) {
            return nativeInitializationReason = Reason.TABLE_SCHEMA;
        }
        return nativeInitializationReason = Reason.NATIVE_SUPPORTED;
    }

    private static boolean supportsFileFormatOptions(TableSchema schema) {
        var options = new CoreOptions(schema.options());
        return CPP_FORMATS.contains(options.fileFormatString()) &&
                options.fileFormatPerLevel().values().stream().allMatch(CPP_FORMATS::contains);
    }

    private boolean supportsFileFormats(DataSplit split) {
        for (var file : split.dataFiles()) {
            if (!CPP_FORMATS.contains(file.fileFormat())) {
                return false;
            }
        }
        return true;
    }

    private boolean supportsHistoricalFileFormats(DataSplit split) {
        for (var file : split.dataFiles()) {
            if (!CPP_FORMATS.contains(fileSchema(file.schemaId()).options().getOrDefault("file.format", "parquet"))) {
                return false;
            }
        }
        return true;
    }

    private boolean supportsHistoricalSchemaTypes(DataSplit split) {
        // CPP parses complete schemas before applying the selected columns.
        for (var file : split.dataFiles()) {
            if (!nativeSchemaSupported(fileSchema(file.schemaId()))) {
                return false;
            }
        }
        return true;
    }

    private boolean supportsMergeEngine(boolean needsMerge) {
        if (!needsMerge) {
            return true;
        }
        return switch (schema.options().getOrDefault("merge-engine", "deduplicate")) {
            case "deduplicate", "first-row", "aggregation", "partial-update" -> true;
            default -> false;
        };
    }

    private boolean supportsAggregateFunctions(boolean needsMerge, Set<String> required) {
        if (!needsMerge) {
            return true;
        }
        String engine = schema.options().getOrDefault("merge-engine", "deduplicate");
        if (!engine.equals("aggregation") && !engine.equals("partial-update")) {
            return true;
        }
        Set<String> sequences = new HashSet<>();
        addNames(sequences, schema.options().get("sequence.field"));
        for (String name : required) {
            if (schema.primaryKeys().contains(name) || sequences.contains(name)) {
                continue;
            }
            DataField field = fieldMaps(schema).names().get(name);
            String function = schema.options().getOrDefault("fields." + name + ".aggregate-function",
                    schema.options().getOrDefault("fields.default-aggregate-function", "last_non_null_value"));
            if (field != null && !aggregateSupported(function, field.type())) {
                return false;
            }
        }
        return true;
    }

    private boolean supportsDeletionVectors(DataSplit split) {
        if (split.deletionFiles().isEmpty() || split.deletionFiles().get().stream().noneMatch(file -> file != null)) {
            return true;
        }
        if (option(schema, "deletion-vectors.bitmap64", false)) {
            return false;
        }
        for (var file : split.dataFiles()) {
            if (option(fileSchema(file.schemaId()), "deletion-vectors.bitmap64", false)) {
                return false;
            }
        }
        return true;
    }

    private boolean supportsOrcTimestampEncoding(DataSplit split) {
        for (var file : split.dataFiles()) {
            if (!file.fileFormat().equals("orc")) {
                continue;
            }
            TableSchema old = fileSchema(file.schemaId());
            if ((option(schema, "orc.timestamp-ltz.legacy.type", true) ||
                    option(old, "orc.timestamp-ltz.legacy.type", true)) &&
                    old.fields().stream().anyMatch(field -> contains(field.type(),
                            type -> type.is(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)))) {
                return false;
            }
        }
        return true;
    }

    private boolean supportsSchemaEvolution(DataSplit split, Set<String> required) {
        for (var file : split.dataFiles()) {
            TableSchema old = fileSchema(file.schemaId());
            for (String name : required) {
                DataField current = fieldMaps(schema).names().get(name);
                if (current == null) {
                    continue;
                }
                DataField previous = fieldMaps(old).ids().get(current.id());
                if (previous != null && !safeNativeEvolution(previous.type(), current.type())) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean supportsTimestampConversion(Set<String> required) {
        // Negative fractional values and nanosecond overflow cannot be ruled out by type alone.
        for (String name : required) {
            DataField field = fieldMaps(schema).names().get(name);
            if (field != null && contains(field.type(), PaimonReaderSelector::isTimestamp)) {
                return false;
            }
        }
        return true;
    }

    private boolean supportsNullableMapKeys(DataSplit split, Set<String> required) {
        if (split.dataFiles().stream().allMatch(file -> file.fileFormat().equals("parquet"))) {
            return true;
        }
        for (String name : required) {
            DataField field = fieldMaps(schema).names().get(name);
            if (field != null && contains(field.type(),
                    type -> type instanceof MapType && ((MapType) type).getKeyType().isNullable())) {
                return false;
            }
        }
        return true;
    }

    private boolean requiresMerge(DataSplit split) {
        // Legacy files without delete counts still use CPP's merge reader, even if raw-convertible.
        return !schema.primaryKeys().isEmpty() && !split.isStreaming() && split.bucket() != -2 &&
                (!split.rawConvertible() || split.dataFiles().stream().anyMatch(file -> file.deleteRowCount().isEmpty()));
    }

    private Set<String> requiredFields(boolean needsMerge) {
        Set<String> required = new HashSet<>(fields);
        if (!needsMerge) {
            return required;
        }
        required.addAll(schema.primaryKeys());
        addNames(required, schema.options().get("sequence.field"));
        // Sequence groups may depend on other groups. Expand until no dependencies remain.
        int previousSize;
        do {
            previousSize = required.size();
            schema.options().forEach((key, value) -> {
                if (key.startsWith("fields.") && key.endsWith(".sequence-group") &&
                        Arrays.stream(value.split(",")).map(String::trim).anyMatch(required::contains)) {
                    String sequenceFields = key.substring("fields.".length(), key.length() - ".sequence-group".length());
                    addNames(required, sequenceFields);
                }
            });
        } while (required.size() != previousSize);
        return required;
    }

    private static boolean aggregateSupported(String function, DataType type) {
        if (VALUE_AGGREGATES.contains(function)) {
            return true;
        }
        boolean numeric = integralRank(type.getTypeRoot()) > 0 ||
                type.isAnyOf(DataTypeRoot.FLOAT, DataTypeRoot.DOUBLE, DataTypeRoot.DECIMAL);
        return switch (function) {
            case "sum" -> numeric;
            case "bool_or", "bool_and" -> type.is(DataTypeRoot.BOOLEAN);
            case "listagg" -> type.isAnyOf(DataTypeRoot.CHAR, DataTypeRoot.VARCHAR);
            case "min", "max" -> numeric || isTimestamp(type) || type.isAnyOf(
                    DataTypeRoot.CHAR, DataTypeRoot.VARCHAR, DataTypeRoot.BINARY, DataTypeRoot.VARBINARY, DataTypeRoot.DATE);
            default -> false;
        };
    }

    private static boolean safeNativeEvolution(DataType old, DataType current) {
        if (old.equalsIgnoreNullable(current)) {
            return true;
        }
        int from = integralRank(old.getTypeRoot());
        int to = integralRank(current.getTypeRoot());
        return (from > 0 && to >= from) || (old.is(DataTypeRoot.FLOAT) && current.is(DataTypeRoot.DOUBLE));
    }

    private static boolean nativeSchemaSupported(TableSchema schema) {
        return schema.fields().stream().noneMatch(field -> contains(field.type(), t -> t.isAnyOf(
                DataTypeRoot.TIME_WITHOUT_TIME_ZONE, DataTypeRoot.MULTISET, DataTypeRoot.VECTOR)));
    }

    private static int integralRank(DataTypeRoot type) {
        return switch (type) {
            case TINYINT -> 1;
            case SMALLINT -> 2;
            case INTEGER -> 3;
            case BIGINT -> 4;
            default -> 0;
        };
    }

    private static boolean option(TableSchema schema, String key, boolean fallback) {
        return Boolean.parseBoolean(schema.options().getOrDefault(key, Boolean.toString(fallback)));
    }

    private static void addNames(Set<String> names, String value) {
        if (value != null) {
            Arrays.stream(value.split(",")).map(String::trim).filter(s -> !s.isEmpty()).forEach(names::add);
        }
    }

    private static boolean isTimestamp(DataType type) {
        return type.isAnyOf(DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE, DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    }

    // ==================== JNI compatibility checks ====================

    private void checkJniSupported(Reason reason) {
        for (DataTypeRoot unsupported : List.of(DataTypeRoot.BLOB, DataTypeRoot.VARIANT)) {
            for (DataType type : selectedTypes) {
                if (contains(type, nested -> nested.is(unsupported))) {
                    String message = unsupported == DataTypeRoot.VARIANT ?
                            "Reading VARIANT through the Paimon JNI reader is not yet supported" :
                            "Unsupported type: BLOB in Paimon JNI reader";
                    throw new StarRocksConnectorException(
                            "%s (reader selection reason: %s). " +
                                    "Exclude unsupported columns or use a compatible native read.", message, reason);
                }
            }
        }
    }

    // ==================== Error diagnostics ====================

    // Called only when throwing an explicit NATIVE error. Never load metadata for diagnostics.
    private String nativeFailureDetails(Split split, Reason reason) {
        DataSplit data = PaimonSplitUtils.getDataSplit(split).orElse(null);
        if (data == null) {
            return "split=" + split.getClass().getSimpleName();
        }
        PaimonReaderSelector source = data instanceof FallbackDataSplit && ((FallbackDataSplit) data).isFallback()
                ? fallbackSelector : this;
        if (source == null || source.schema == null) {
            return "split=" + split.getClass().getSimpleName() + ", schema unavailable";
        }
        Set<String> required = source.requiredFields(source.requiresMerge(data));
        Set<Integer> fieldIds = new HashSet<>();
        source.schema.fields().stream().filter(field -> required.contains(field.name()))
                .forEach(field -> fieldIds.add(field.id()));
        var fileFormats = data.dataFiles().stream().map(file -> file.fileFormat()).distinct().sorted().limit(8).toList();
        var fileSchemas = data.dataFiles().stream().map(file -> file.schemaId()).distinct().sorted().limit(8).toList();
        StringBuilder details = new StringBuilder("split=").append(split.getClass().getSimpleName())
                .append(", file formats=").append(fileFormats).append(", file schemas=").append(fileSchemas)
                .append(", current {").append(schemaDetails(schema, required, fieldIds, reason)).append('}');
        if (fallbackSelector != null) {
            details.append(", fallback branch=").append(schema.options().get("scan.fallback-branch"))
                    .append(", fallback {")
                    .append(schemaDetails(fallbackSelector.schema, required, fieldIds, reason)).append('}');
        }
        for (long id : fileSchemas) {
            TableSchema historical = source.schemas.get(id);
            if (historical != null && id != source.schema.id()) {
                details.append(", historical {").append(schemaDetails(historical, required, fieldIds, reason)).append('}');
            }
        }
        return details.toString();
    }

    private static String schemaDetails(TableSchema schema, Set<String> required, Set<Integer> fieldIds, Reason reason) {
        if (schema == null) {
            return "schema unavailable";
        }
        // Bound error size on wide tables; include field ids to correlate renamed historical fields.
        var columns = schema.fields().stream()
                .filter(field -> reason == Reason.TABLE_SCHEMA || reason == Reason.SCHEMA_EVOLUTION ||
                        required.contains(field.name()) || fieldIds.contains(field.id()))
                .limit(16).map(field -> field.name() + "#" + field.id() + "=" + field.type().asSQLString()).toList();
        String details = "schema=" + schema.id() + ", columns(first 16)=" + columns +
                ", file.format=" + schema.options().getOrDefault("file.format", "parquet") +
                ", file.format.per.level=" + schema.options().getOrDefault("file.format.per.level", "");
        if (reason == Reason.MERGE_FUNCTION) {
            var aggregates = schema.options().entrySet().stream()
                    .filter(entry -> entry.getKey().startsWith("fields.") && entry.getKey().endsWith(".aggregate-function"))
                    .sorted(Map.Entry.comparingByKey()).limit(16).toList();
            details += ", merge-engine=" + schema.options().getOrDefault("merge-engine", "deduplicate") +
                    ", fields.default-aggregate-function=" +
                    schema.options().getOrDefault("fields.default-aggregate-function", "") +
                    ", aggregate-functions(first 16)=" + aggregates;
        }
        return details;
    }

    // ==================== Shared schema and type helpers ====================

    private PaimonReaderSelector selectorFor(DataSplit split) {
        if (!(split instanceof FallbackDataSplit) || !((FallbackDataSplit) split).isFallback()) {
            return this;
        }
        return getFallbackSelector();
    }

    private PaimonReaderSelector getFallbackSelector() {
        String branch = schema == null ? null : schema.options().get("scan.fallback-branch");
        if (table == null || branch == null || branch.isBlank()) {
            return null;
        }
        if (fallbackSelector == null) {
            fallbackSelector = new PaimonReaderSelector(table.switchToBranch(branch), List.copyOf(fields));
        }
        return fallbackSelector;
    }

    private boolean hasCompatibleFallbackSchema(DataSplit split, PaimonReaderSelector selector) {
        if (!(split instanceof FallbackDataSplit) || !((FallbackDataSplit) split).isFallback()) {
            return true;
        }
        return schema != null && schema.logicalRowType().equals(selector.schema.logicalRowType());
    }

    private TableSchema fileSchema(long id) {
        return schemas.computeIfAbsent(id, schemaLoader::apply);
    }

    private SchemaFields fieldMaps(TableSchema schema) {
        return schemaFields.computeIfAbsent(schema.id(), id -> new SchemaFields(schema));
    }

    private static boolean contains(DataType type, Predicate<DataType> predicate) {
        if (predicate.test(type)) {
            return true;
        }
        if (type instanceof RowType) {
            return ((RowType) type).getFields().stream().anyMatch(field -> contains(field.type(), predicate));
        }
        if (type instanceof ArrayType) {
            return contains(((ArrayType) type).getElementType(), predicate);
        }
        if (type instanceof MapType) {
            return contains(((MapType) type).getKeyType(), predicate) ||
                    contains(((MapType) type).getValueType(), predicate);
        }
        return false;
    }

    private static List<DataType> selectedTypes(List<DataField> schemaFields, Set<String> fields) {
        return schemaFields.stream().filter(field -> fields.contains(field.name()))
                .map(DataField::type).toList();
    }
}
